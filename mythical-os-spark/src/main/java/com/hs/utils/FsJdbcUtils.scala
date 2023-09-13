/*
 *@Author   : DoubleTrey
 *@Time     : 2022/11/24 14:17
 */

package com.hs.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils.getJdbcType
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
//import org.postgresql.util.PGobject

import java.sql.{Connection, Date, PreparedStatement, SQLException}
import java.text.SimpleDateFormat
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.util.control.NonFatal

// TODO: 除pg、mysql外的其他jdbc数据库upsert支持
object FsJdbcUtils extends Logging {
  private type JDBCValueSetter = (PreparedStatement, Row, Int, Int, Map[String, String]) => Unit
  private type JDBCValueSetterBatch = (PreparedStatement, Row, Int, Int, Map[String, String], Int) => Unit
  val timeFormat = new SimpleDateFormat("yyyy-MM-dd")
  val timeFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val breakpointResumeOffsetPath = "/delta/_offset/breakpoint/%s"
  private val jdbcTypes = List[String]("postgresql", "mysql", "sqlserver", "kingbase8", "oracle", "dm")
  var enableBreakpointResume = false;
  var breakpointResumeColumn: String = null;
  var spark: SparkSession = null;
  var schema: StructType = null;
  var taskId: String = null;

  /**
   * Saves the RDD to the database in a single transaction.
   */
  def upsertTable(df: DataFrame,
                  upsertOptions: Map[String, String],
                  mergeKeys: String,
                  isCaseSensitive: Boolean = false,
                  enableBreakpointResume: Boolean = false,
                  breakpointResumeColumn: String = "insert_time",
                  spark: SparkSession = null,
                  taskId: String = null): Unit = {
    this.enableBreakpointResume = enableBreakpointResume
    this.breakpointResumeColumn = breakpointResumeColumn
    this.spark = spark
    this.schema = df.schema
    this.taskId = taskId

    val options = new JdbcOptionsInWrite(upsertOptions)

    val url = options.url
    val table = options.table
    val dialect = JdbcDialects.get(url)
    val rddSchema = df.schema
    val batchSize = options.batchSize
    val isolationLevel = options.isolationLevel


    /**
     * JDBC connection
     */
    val connection = dialect.createConnectionFactory(options)(-1)
    val tableSchema = JdbcUtils.getSchemaOption(connection, options)

    //获取jdbcUrl后面的参数
    val urlMap = UrlUtil.urlToMap(url)
    val upsertBatch = urlMap.get("upsertBatch")

    // upsert sql
    val upsertStmtSingle = getUpsertStatement(url, table, rddSchema, tableSchema, isCaseSensitive, dialect, mergeKeys)
    println("upsertStmtSingle = " + upsertStmtSingle)
    var upsertStmt: String = null
    if (upsertBatch.exists(_.equals("true"))) {
      upsertStmt = getUpsertStatementByBatchInsert(url, table, rddSchema, tableSchema, isCaseSensitive, dialect, mergeKeys, batchSize)
      println("upsertStmt = " + upsertStmt)
    }
    val jdbcType = jdbcTypes.find(t => url.toLowerCase.contains(t)).getOrElse {
      throw new Exception(s"""Unsupported jdbc type "$url" """)
    }
    val repartitionedDF = options.numPartitions match {
      case Some(n) if n <= 0 => throw new IllegalArgumentException(
        s"Invalid value `$n` for parameter `${JDBCOptions.JDBC_NUM_PARTITIONS}` in table writing " +
          "via JDBC. The minimum value is 1.")
      case Some(n) if n < df.rdd.getNumPartitions => df.coalesce(n)
      case _ => df
    }

    if (this.enableBreakpointResume && this.breakpointResumeColumn != null && this.spark != null) {
      savePartitionBreakpointResume(table, jdbcType, repartitionedDF.toLocalIterator().asScala, rddSchema, upsertStmtSingle, batchSize, dialect, isolationLevel, options, mergeKeys)
    } else {
      repartitionedDF.rdd.foreachPartition { iterator =>
        if (upsertBatch.exists(_.equals("true"))) {
          savePartitionByBatchInsert(table, jdbcType, iterator, rddSchema, upsertStmtSingle, batchSize, dialect, isolationLevel, options, mergeKeys, upsertStmt)
        } else {
          savePartition(table, jdbcType, iterator, rddSchema, upsertStmtSingle, batchSize, dialect, isolationLevel, options, mergeKeys)
        }
      }
    }
  }

  /**
   * Returns an Upsert SQL statement for updating/inserting a row into the target table via JDBC conn.
   *
   * @param table
   * @param rddSchema
   * @param tableSchema
   * @param isCaseSensitive
   * @param dialect
   * @return INSERT INTO tablename (column1, column2) VALUES
   *         (value1, value2)
   *         ON CONFLICT (column1)
   *         DO UPDATE SET column2 = EXCLUDED.column2;
   */
  def getUpsertStatement(url: String,
                         table: String,
                         rddSchema: StructType,
                         tableSchema: Option[StructType],
                         isCaseSensitive: Boolean,
                         dialect: JdbcDialect,
                         mergeKeys: String): String = {
    val columns = if (tableSchema.isEmpty) {
      rddSchema.fields.map(x => dialect.quoteIdentifier(x.name)).mkString(",")
    } else {
      val columnNameEquality = if (isCaseSensitive) {
        org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
      } else {
        org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
      }
      // The generated insert statement needs to follow rddSchema's column sequence and
      // tableSchema's column names. When appending data into some case-sensitive DBMSs like
      // PostgreSQL/Oracle, we need to respect the existing case-sensitive column names instead of
      // RDD column names for user convenience.
      val tableColumnNames = tableSchema.get.fieldNames
      rddSchema.fields.map { col =>
        val normalizedName = tableColumnNames.find(f => columnNameEquality(f, col.name)).getOrElse {
          throw new Exception(s"""Column "${col.name}" not found in schema $tableSchema""")
        }
        dialect.quoteIdentifier(normalizedName)
      }.mkString(",")
    }

    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")

    val jdbcType = jdbcTypes.find(t => url.toLowerCase.contains(t)).getOrElse {
      throw new Exception(s"""Unsupported jdbc type "$url" """)
    }

    jdbcType match {
      case "sqlserver" => sqlserverMerge(table, rddSchema, dialect, mergeKeys)
      case "mysql" | "postgresql" | "kingbase8" => {
        val upsertTail = getUpsertTail(url, columns, placeholders, mergeKeys)
        s"INSERT INTO $table ($columns) VALUES ($placeholders) $upsertTail "
      }
      case "oracle" | "dm" => getUpsertByOracleAndDm(table, rddSchema, dialect, mergeKeys)
      case _ => throw new IllegalArgumentException(s"Jdbc type $jdbcType 不支持upsert")
    }

  }

  def getUpsertByOracleAndDm(table: String,
                             rddSchema: StructType,
                             dialect: JdbcDialect,
                             mergeKeys: String): String = {

    val columns = rddSchema.fields.map(x => dialect.quoteIdentifier(x.name)).mkString(",")
    val mergeKeyArray: Array[String] = mergeKeys.split(",").map(x => x.trim()).map(s => "\"" + s + "\"")
    val onJoinStr = mergeKeyArray.map(x => s"T1.$x = T2.$x").mkString(" and ")

    val columnsNotContainPK = columns.split(",") filterNot (mergeKeyArray.contains)

    s"MERGE INTO $table  T1 " +
      s"USING (SELECT ${columns.split(",").map(x => s"? as $x").mkString(",")} FROM DUAL) T2 " +
      s"ON ($onJoinStr) " +
      s"WHEN MATCHED THEN " +
      s"UPDATE SET ${columnsNotContainPK.map(x => s"T1.$x = T2.$x").mkString(",")} " +
      s"WHEN NOT MATCHED THEN " +
      s"INSERT ($columns) VALUES ( ${columns.split(",").map(x => s"T2.$x").mkString(",")} )"
  }

  /*
  MERGE test1  WITH (serializable) AS tgt
  USING (SELECT ? AS ID,? as name ) AS src
        ON tgt.ID = src.ID
  WHEN MATCHED THEN
      UPDATE  SET tgt.name = src.name
  WHEN NOT MATCHED THEN
      INSERT ( id, name ) VALUES (src.id, src.name);
   */
  def sqlserverMerge(table: String,
                     rddSchema: StructType,
                     dialect: JdbcDialect,
                     mergeKeys: String): String = {

    val columns = rddSchema.fields.map(x => dialect.quoteIdentifier(x.name)).mkString(",")
    val mergeKeyArray = mergeKeys.split(",").map(x => x.trim())
    val onJoinStr = mergeKeyArray.map(x => s"tgt.$x = src.$x").mkString(" and ")

    //    A MERGE statement must be terminated by a semi-colon (;)
    s"MERGE $table  WITH (serializable) AS tgt " +
      s"USING(SELECT ${columns.split(",").map(x => s"? as $x").mkString(",")}) AS src " +
      s"ON $onJoinStr " +
      s"WHEN MATCHED THEN " +
      s"UPDATE SET ${columns.split(",").map(x => s"tgt.$x = ?").mkString(",")} " +
      s"WHEN NOT MATCHED THEN " +
      s"INSERT ($columns) VALUES ( ${columns.split(",").map(x => s"src.$x").mkString(",")} ); "
  }

  def getUpsertTail(url: String, columns: String, placeholders: String, mergeKeys: String): String = {
    val jdbcType = jdbcTypes.find(t => url.toLowerCase.contains(t)).getOrElse {
      throw new Exception(s"""Unsupported jdbc type "$url" """)
    }
    val mr = mergeKeys.split(",").map(e => "\"" + e + "\"")
    jdbcType match {
      case "mysql" => s" ON DUPLICATE KEY UPDATE ${columns.split(",").map(x => s"$x = ?").mkString(",")}"
      //      case "postgresql" => s"ON CONFLICT ($mergeKeys) DO UPDATE SET ${columns.split(",").filter(x => !mr.contains(x)).map(x => s"$x = ?").mkString(",")}"
      case "postgresql" | "kingbase8" => s"ON CONFLICT ($mergeKeys) DO UPDATE SET ${columns.split(",").filter(x => !mr.contains(x)).map(x => s"$x =excluded.$x").mkString(",")}"
      case _ => throw new IllegalArgumentException(s"Jdbc type $jdbcType 不支持upsert")
    }
  }

  /**
   * Returns an Upsert SQL statement for updating/inserting a row into the target table via JDBC conn.
   *
   * @param table
   * @param rddSchema
   * @param tableSchema
   * @param isCaseSensitive
   * @param dialect
   * @return INSERT INTO tablename (column1, column2) VALUES
   *         (value1, value2),
   *         (value3, value4),
   *         ...
   *         ON CONFLICT (column1)
   *         DO UPDATE SET column2 = EXCLUDED.column2;
   */
  def getUpsertStatementByBatchInsert(url: String,
                                      table: String,
                                      rddSchema: StructType,
                                      tableSchema: Option[StructType],
                                      isCaseSensitive: Boolean,
                                      dialect: JdbcDialect,
                                      mergeKeys: String,
                                      batchSize: Int): String = {
    val columns = if (tableSchema.isEmpty) {
      rddSchema.fields.map(x => dialect.quoteIdentifier(x.name)).mkString(",")
    } else {
      val columnNameEquality = if (isCaseSensitive) {
        org.apache.spark.sql.catalyst.analysis.caseSensitiveResolution
      } else {
        org.apache.spark.sql.catalyst.analysis.caseInsensitiveResolution
      }
      // The generated insert statement needs to follow rddSchema's column sequence and
      // tableSchema's column names. When appending data into some case-sensitive DBMSs like
      // PostgreSQL/Oracle, we need to respect the existing case-sensitive column names instead of
      // RDD column names for user convenience.
      val tableColumnNames = tableSchema.get.fieldNames
      rddSchema.fields.map { col =>
        val normalizedName = tableColumnNames.find(f => columnNameEquality(f, col.name)).getOrElse {
          throw new Exception(s"""Column "${col.name}" not found in schema $tableSchema""")
        }
        dialect.quoteIdentifier(normalizedName)
      }.mkString(",")
    }

    val placeholders = rddSchema.fields.map(_ => "?").mkString(",")
    val batchPlaceholders = List.fill(batchSize)("(" + placeholders + ")").mkString(",")
    val jdbcType = jdbcTypes.find(t => url.toLowerCase.contains(t)).getOrElse {
      throw new Exception(s"""Unsupported jdbc type "$url" """)
    }

    jdbcType match {
      case "sqlserver" => sqlserverMerge(table, rddSchema, dialect, mergeKeys)
      case "mysql" | "postgresql" | "kingbase8" => {
        val upsertTail = getUpsertTail(url, columns, placeholders, mergeKeys)
        //        s"INSERT INTO $table ($columns) VALUES ($placeholders) $upsertTail "
        s"INSERT INTO $table ($columns) VALUES $batchPlaceholders $upsertTail "
      }
      case "oracle" | "dm" => getUpsertByOracleAndDm(table, rddSchema, dialect, mergeKeys)
      case _ => throw new IllegalArgumentException(s"Jdbc type $jdbcType 不支持upsert")
    }

  }

  def savePartitionBreakpointResume(table: String, jdbcType: String,
                                    iterator: Iterator[Row],
                                    rddSchema: StructType,
                                    insertStmt: String,
                                    batchSize: Int,
                                    dialect: JdbcDialect,
                                    isolationLevel: Int,
                                    options: JDBCOptions,
                                    mergeKeys: String): Unit = {

    if (iterator.isEmpty) {
      return
    }
    val conn = dialect.createConnectionFactory(options)(-1)
    var committed = false
    val isUpdate = true


    //获取目标库数据字段类型
    var targetDT: Map[String, String] = Map()
    var ps0: PreparedStatement = null

    if ("mysql".equalsIgnoreCase(jdbcType)) {
      ps0 = conn.prepareStatement("select column_name as columnName,data_type as columnType from information_schema.columns where  table_schema=? and table_name=? ");
      if (table.contains(".")) {
        ps0.setString(1, table.split("\\.")(0))
        ps0.setString(2, table.split("\\.")(1))
      } else {
        ps0 = conn.prepareStatement("select column_name as columnName,data_type as columnType from information_schema.columns where  table_name=? ");
        ps0.setString(1, table)
      }

    } else if ("postgresql".equalsIgnoreCase(jdbcType)) {

      ps0 = conn.prepareStatement("SELECT a.attname as columnName,pg_catalog.format_type(a.atttypid, a.atttypmod) as columnType FROM pg_catalog.pg_attribute a WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attrelid = (SELECT c.oid FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE  n.nspname=? and c.relname =? )"); //AND pg_catalog.pg_table_is_visible(c.oid)
      if (table.contains(".")) {
        ps0.setString(1, table.split("\\.")(0))
        ps0.setString(2, table.split("\\.")(1))
      } else {
        ps0.setString(1, "public")
        ps0.setString(2, table)
      }
    } else {

    }

    if (ps0 != null) {
      val set = ps0.executeQuery()
      while (set.next) {
        targetDT += (set.getString("columnName") -> set.getString("columnType"))
      }
      ps0.close()
    }

    var finalIsolationLevel = Connection.TRANSACTION_NONE
    if (isolationLevel != Connection.TRANSACTION_NONE) {
      try {
        val metadata = conn.getMetaData
        if (metadata.supportsTransactions()) {
          // Update to at least use the default isolation, if any transaction level
          // has been chosen and transactions are supported
          val defaultIsolation = metadata.getDefaultTransactionIsolation
          finalIsolationLevel = defaultIsolation
          if (metadata.supportsTransactionIsolationLevel(isolationLevel)) {
            // Finally update to actually requested level if possible
            finalIsolationLevel = isolationLevel
          } else {
            logWarning(s"Requested isolation level $isolationLevel is not supported; " +
              s"falling back to default isolation level $defaultIsolation")
          }
        } else {
          logWarning(s"Requested isolation level $isolationLevel, but transactions are unsupported")
        }
      } catch {
        case NonFatal(e) => logWarning("Exception while detecting transaction support", e)
      }
    }
    val supportsTransactions = finalIsolationLevel != Connection.TRANSACTION_NONE
    var totalRowCount = 0L
    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
        conn.setTransactionIsolation(finalIsolationLevel)
      }
      val stmt = conn.prepareStatement(insertStmt)

      val setters: Array[JDBCValueSetter] = isUpdate match {
        case true =>
          val setters: Array[JDBCValueSetter] = rddSchema.fields
            .map(f => makeSetter(conn, dialect, f.name, f.dataType))
          Array.fill(2)(setters).flatten
        case _ =>
          rddSchema.fields
            .map(f => makeSetter(conn, dialect, f.name, f.dataType))
      }

      val nullTypes = rddSchema.fields.map(f => {

        if (targetDT.get(f.name).exists(_.equals("jsonb")) || targetDT.get(f.name).exists(_.equals("json"))) {
          java.sql.Types.OTHER
        } else if (targetDT.get(f.name).exists(_.equals("date"))) {
          java.sql.Types.DATE
        } else if (String.valueOf(targetDT.get(f.name)).contains("timestamp")) {
          java.sql.Types.TIMESTAMP
        } else {
          if (f.dataType.catalogString.equalsIgnoreCase("void")) {
            java.sql.Types.VARCHAR
          } else {
            getJdbcType(f.dataType, dialect).jdbcNullType
          }
        }
      })


      val numFieldsLength = rddSchema.fields.length
      var numFields = isUpdate match {
        case true => numFieldsLength
        case _ => numFieldsLength
      }
      val cursorBegin = numFields
      if (!jdbcType.equalsIgnoreCase("postgresql") && !jdbcType.equalsIgnoreCase("oracle") && !jdbcType.equalsIgnoreCase("dm") && !jdbcType.equalsIgnoreCase("kingbase8")) {
        numFields = numFieldsLength * 2
      }

      var row: Row = null
      try {
        var rowCount = 0
        stmt.setQueryTimeout(options.queryTimeout)

        while (iterator.hasNext) {
          row = iterator.next()
          var i = 0
          while (i < numFields) {
            if (isUpdate) {
              //需要判断当前游标是否走到了ON DUPLICATE KEY UPDATE
              i < cursorBegin match {
                //说明还没走到update阶段
                case true =>
                  //row.isNullAt 判空,则设置空值
                  if (row.isNullAt(i)) {
                    stmt.setNull(i + 1, nullTypes(i))
                  } else {
                    setters(i).apply(stmt, row, i, 0, targetDT)
                  }
                //说明走到了update阶段
                case false =>
                  if (row.isNullAt(i - cursorBegin)) {
                    //pos - offset
                    stmt.setNull(i + 1, nullTypes(i - cursorBegin))
                  } else {
                    setters(i).apply(stmt, row, i, cursorBegin, targetDT)
                  }
              }
            } else {
              if (row.isNullAt(i)) {
                stmt.setNull(i + 1, nullTypes(i))
              } else {
                setters(i).apply(stmt, row, i, 0, targetDT)
              }
            }
            //滚动游标
            i = i + 1
          }
          stmt.addBatch()
          rowCount += 1
          totalRowCount += 1
          if (rowCount % batchSize == 0) {
            stmt.executeBatch()
            if (supportsTransactions) {
              conn.commit()
              savepointOffset(row)
            }
            rowCount = 0
          }
        }
        if (rowCount > 0) {
          stmt.executeBatch()
          if (supportsTransactions) {
            conn.commit()
            savepointOffset(row)
          }
        }
      } finally {
        stmt.close()
      }
      committed = true
    } catch {
      case e: SQLException =>
        val cause = e.getNextException
        if (cause != null && e.getCause != cause) {

          if (e.getCause == null) {
            try {
              e.initCause(cause)
            } catch {
              case _: IllegalStateException => e.addSuppressed(cause)
            }
          } else {
            e.addSuppressed(cause)
          }
        }
        throw e
    } finally {
      if (!committed) {
        if (supportsTransactions) {
          conn.rollback()
        }
        conn.close()
      } else {
        try {
          conn.close()
        } catch {
          case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
        }
      }
    }
  }

  private def savepointOffset(row: Row): Unit = {
    //todo savepoint csv
    if (this.enableBreakpointResume && this.breakpointResumeColumn != null && this.spark != null) {
      val rows: java.util.ArrayList[Row] = new java.util.ArrayList[Row]()
      rows.add(row)
      val frame: DataFrame = this.spark.createDataFrame(rows, this.schema)
      frame.selectExpr(this.breakpointResumeColumn).write.option("header", value = true).mode(SaveMode.Overwrite).csv(breakpointResumeOffsetPath.format(this.taskId))
    }
  }

  def savePartition(table: String, jdbcType: String,
                    iterator: Iterator[Row],
                    rddSchema: StructType,
                    insertStmt: String,
                    batchSize: Int,
                    dialect: JdbcDialect,
                    isolationLevel: Int,
                    options: JDBCOptions,
                    mergeKeys: String): Unit = {

    if (iterator.isEmpty) {
      return
    }
    val conn = dialect.createConnectionFactory(options)(-1)
    var committed = false
    val isUpdate = true


    //获取目标库数据字段类型
    var targetDT: Map[String, String] = Map()
    var ps0: PreparedStatement = null

    if ("mysql".equalsIgnoreCase(jdbcType)) {
      ps0 = conn.prepareStatement("select column_name as columnName,data_type as columnType from information_schema.columns where  table_schema=? and table_name=? ");
      if (table.contains(".")) {
        ps0.setString(1, table.split("\\.")(0))
        ps0.setString(2, table.split("\\.")(1))
      } else {
        ps0 = conn.prepareStatement("select column_name as columnName,data_type as columnType from information_schema.columns where  table_name=? ");
        ps0.setString(1, table)
      }

    } else if ("postgresql".equalsIgnoreCase(jdbcType)) {

      ps0 = conn.prepareStatement("SELECT a.attname as columnName,pg_catalog.format_type(a.atttypid, a.atttypmod) as columnType FROM pg_catalog.pg_attribute a WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attrelid = (SELECT c.oid FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE  n.nspname=? and c.relname =? )"); //AND pg_catalog.pg_table_is_visible(c.oid)
      if (table.contains(".")) {
        ps0.setString(1, table.split("\\.")(0))
        ps0.setString(2, table.split("\\.")(1))
      } else {
        ps0.setString(1, "public")
        ps0.setString(2, table)
      }
    } else {

    }

    if (ps0 != null) {
      val set = ps0.executeQuery()
      while (set.next) {
        targetDT += (set.getString("columnName") -> set.getString("columnType"))
      }
      ps0.close()
    }
    //////////////////////////////


    var finalIsolationLevel = Connection.TRANSACTION_NONE
    if (isolationLevel != Connection.TRANSACTION_NONE) {
      try {
        val metadata = conn.getMetaData
        if (metadata.supportsTransactions()) {
          // Update to at least use the default isolation, if any transaction level
          // has been chosen and transactions are supported
          val defaultIsolation = metadata.getDefaultTransactionIsolation
          finalIsolationLevel = defaultIsolation
          if (metadata.supportsTransactionIsolationLevel(isolationLevel)) {
            // Finally update to actually requested level if possible
            finalIsolationLevel = isolationLevel
          } else {
            logWarning(s"Requested isolation level $isolationLevel is not supported; " +
              s"falling back to default isolation level $defaultIsolation")
          }
        } else {
          logWarning(s"Requested isolation level $isolationLevel, but transactions are unsupported")
        }
      } catch {
        case NonFatal(e) => logWarning("Exception while detecting transaction support", e)
      }
    }
    val supportsTransactions = finalIsolationLevel != Connection.TRANSACTION_NONE
    var totalRowCount = 0L
    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
        conn.setTransactionIsolation(finalIsolationLevel)
      }
      val stmt = conn.prepareStatement(insertStmt)

      val setters: Array[JDBCValueSetter] = isUpdate match {
        case true =>
          val setters: Array[JDBCValueSetter] = rddSchema.fields
            .map(f => makeSetter(conn, dialect, f.name, f.dataType))
          Array.fill(2)(setters).flatten
        case _ =>
          rddSchema.fields
            .map(f => makeSetter(conn, dialect, f.name, f.dataType))
      }

      //      val setters_1: Array[JDBCValueSetter] = isUpdate match {
      //        case true =>
      //          val setters: Array[JDBCValueSetter] = (rddSchema.fields ++ rddSchema.fields.filter(e=> ! mergeKeys.split(",").contains(e.name))).map(_.dataType) .map(makeSetter(conn, dialect, _))
      //            Array.fill(1)(setters).flatten
      //        case _ =>
      //          rddSchema.fields.filter(e=> ! mergeKeys.split(",").contains(e.name)).map(_.dataType)
      //            .map(makeSetter(conn, dialect, _))
      //      }

      val nullTypes = rddSchema.fields.map(f => {

        //        if (targetDT(f.name) == "jsonb" || targetDT(f.name) == "json") {
        if (targetDT.get(f.name).exists(_.equals("jsonb")) || targetDT.get(f.name).exists(_.equals("json"))) {
          java.sql.Types.OTHER
          //        } else if (targetDT(f.name) == "date") {
        } else if (targetDT.get(f.name).exists(_.equals("date"))) {
          java.sql.Types.DATE
          //        } else if (String.valueOf(targetDT(f.name)).contains("timestamp")) {
        } else if (String.valueOf(targetDT.get(f.name)).contains("timestamp")) {
          java.sql.Types.TIMESTAMP
        } else {
          if (f.dataType.catalogString.equalsIgnoreCase("void")) {
            java.sql.Types.VARCHAR
          } else {
            getJdbcType(f.dataType, dialect).jdbcNullType
          }
        }
      })


      val numFieldsLength = rddSchema.fields.length
      var numFields = isUpdate match {
        case true => numFieldsLength
        case _ => numFieldsLength
      }
      val cursorBegin = numFields
      if (!jdbcType.equalsIgnoreCase("postgresql") && !jdbcType.equalsIgnoreCase("oracle") && !jdbcType.equalsIgnoreCase("dm") && !jdbcType.equalsIgnoreCase("kingbase8")) {
        numFields = numFieldsLength * 2
      }

      try {
        var rowCount = 0
        stmt.setQueryTimeout(options.queryTimeout)

        while (iterator.hasNext) {
          val row = iterator.next()
          var i = 0
          //          while (i < numFields - mergeKeyNum) {
          while (i < numFields) {
            if (isUpdate) {
              //需要判断当前游标是否走到了ON DUPLICATE KEY UPDATE
              i < cursorBegin match {
                //说明还没走到update阶段
                case true =>
                  //row.isNullAt 判空,则设置空值
                  if (row.isNullAt(i)) {
                    stmt.setNull(i + 1, nullTypes(i))
                  } else {
                    setters(i).apply(stmt, row, i, 0, targetDT)
                  }
                //说明走到了update阶段
                case false =>
                  if (row.isNullAt(i - cursorBegin)) {
                    //pos - offset
                    stmt.setNull(i + 1, nullTypes(i - cursorBegin))
                  } else {
                    setters(i).apply(stmt, row, i, cursorBegin, targetDT)
                    //                    if(table.equalsIgnoreCase("postgresql")){
                    //                      setters_1(i).apply(stmt, row, i, cursorBegin - mergeKeyNum)
                    //                    }else{
                    //                      setters(i).apply(stmt, row, i, cursorBegin)
                    //                    }
                  }
              }
            } else {
              if (row.isNullAt(i)) {
                stmt.setNull(i + 1, nullTypes(i))
              } else {
                setters(i).apply(stmt, row, i, 0, targetDT)
              }
            }
            //滚动游标
            i = i + 1
          }
          stmt.addBatch()
          rowCount += 1
          totalRowCount += 1
          if (rowCount % batchSize == 0) {
            stmt.executeBatch()
            rowCount = 0
          }
        }
        if (rowCount > 0) {
          stmt.executeBatch()
        }
      } finally {
        stmt.close()
      }
      if (supportsTransactions) {
        conn.commit()
      }
      committed = true
    } catch {
      case e: SQLException =>
        val cause = e.getNextException
        if (cause != null && e.getCause != cause) {
          // If there is no cause already, set 'next exception' as cause. If cause is null,
          // it *may* be because no cause was set yet
          if (e.getCause == null) {
            try {
              e.initCause(cause)
            } catch {
              // Or it may be null because the cause *was* explicitly initialized, to *null*,
              // in which case this fails. There is no other way to detect it.
              // addSuppressed in this case as well.
              case _: IllegalStateException => e.addSuppressed(cause)
            }
          } else {
            e.addSuppressed(cause)
          }
        }
        throw e
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        if (supportsTransactions) {
          conn.rollback()
        }
        //        else {
        //          outMetrics.setRecordsWritten(totalRowCount)
        //        }
        conn.close()
      } else {
        //        outMetrics.setRecordsWritten(totalRowCount)

        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close()
        } catch {
          case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
        }
      }
    }
  }

  private def makeSetter(
                          conn: Connection,
                          dialect: JdbcDialect, filedNmae: String,
                          dataType: DataType
                        ): JDBCValueSetter = dataType match {
    case IntegerType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String]) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setInt(pos + 1, row.getInt(pos - cursor))
        } else {
          stmt.setNull(pos + 1, java.sql.Types.INTEGER)
        }

    case LongType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String]) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setLong(pos + 1, row.getLong(pos - cursor))
        } else {
          stmt.setNull(pos + 1, java.sql.Types.BIGINT)
        }

    case DoubleType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String]) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setDouble(pos + 1, row.getDouble(pos - cursor))
        } else {
          stmt.setNull(pos + 1, java.sql.Types.DOUBLE)
        }

    case FloatType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String]) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setFloat(pos + 1, row.getFloat(pos - cursor))
        } else {
          stmt.setNull(pos + 1, java.sql.Types.FLOAT)
        }

    case ShortType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String]) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setInt(pos + 1, row.getShort(pos - cursor))
        } else {
          stmt.setNull(pos + 1, java.sql.Types.SMALLINT)
        }

    case ByteType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String]) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setInt(pos + 1, row.getByte(pos - cursor))
        } else {
          stmt.setNull(pos + 1, java.sql.Types.TINYINT)
        }

    case BooleanType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String]) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setBoolean(pos + 1, row.getBoolean(pos - cursor))
        } else {
          stmt.setNull(pos + 1, java.sql.Types.BOOLEAN)
        }

    case StringType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String]) =>

        if (!row.isNullAt(pos - cursor)) {
          if ("jsonb".equalsIgnoreCase(targetDT(filedNmae)) || "json".equalsIgnoreCase(targetDT(filedNmae))) {
            //            val jsonb: PGobject = new PGobject()
            //            jsonb.setType("jsonb")
            //            jsonb.setValue(row.getString(pos - cursor))
            //            stmt.setObject(pos + 1, jsonb)

          } else if ("date".equalsIgnoreCase(targetDT(filedNmae))) {
            try {
              val value: String = row.getString(pos - cursor)
              var date: Date = new Date(new java.util.Date().getTime)
              if (value != null && value.trim.length == 10) {
                date = new Date(timeFormat.parse(value).getTime)
                stmt.setDate(pos + 1, date)
              }
              else if (value != null && value.trim.length == 19) {
                date = new Date(timeFormat2.parse(value).getTime)
                stmt.setDate(pos + 1, date)

              } else if (value != null && value.trim.length == 14 && value.endsWith(" +08")) {
                date = new Date(timeFormat.parse(value.substring(0, 10)).getTime)
                stmt.setDate(pos + 1, date)
              } else if (value != null && value.trim.length >= 10 && value.endsWith("00000")) {
                stmt.setDate(pos + 1, new java.sql.Date(value.toLong))
              } else {
                stmt.setString(pos + 1, value)
              }
            } catch {

              case e: Exception => {
                println("---出错了：字段：" + filedNmae + " 内容:" + row.getString(pos - cursor))
                e.printStackTrace()
                stmt.setNull(pos + 1, java.sql.Types.DATE)
              }
            }
          } else {
            stmt.setString(pos + 1, row.getString(pos - cursor))
          }

        } else {
          stmt.setNull(pos + 1, java.sql.Types.VARCHAR)
        }

    case BinaryType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String]) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setBytes(pos + 1, row.getAs[Array[Byte]](pos - cursor))
        } else {
          stmt.setNull(pos + 1, java.sql.Types.BINARY)
        }

    case TimestampType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String]) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setTimestamp(pos + 1, row.getAs[java.sql.Timestamp](pos
            - cursor))
        } else {
          stmt.setNull(pos + 1, java.sql.Types.TIMESTAMP)
        }

    case DateType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String]) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setDate(pos + 1, row.getAs[java.sql.Date](pos - cursor))
        } else {
          stmt.setNull(pos + 1, java.sql.Types.DATE)
        }

    case t: DecimalType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String]) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setBigDecimal(pos + 1, row.getDecimal(pos - cursor))
        } else {
          stmt.setNull(pos + 1, java.sql.Types.DECIMAL)
        }

    case ArrayType(et, _) =>
      // remove type length parameters from end of type name
      val typeName = getJdbcType(et, dialect).databaseTypeDefinition
        .toLowerCase.split("\\(")(0)
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String]) =>
        if (!row.isNullAt(pos - cursor)) {
          val array = conn.createArrayOf(
            typeName,
            row.getSeq[AnyRef](pos - cursor).toArray)
          stmt.setArray(pos + 1, array)
        } else {
          stmt.setNull(pos + 1, java.sql.Types.ARRAY)
        }

    case _ =>
      (_: PreparedStatement, _: Row, pos: Int, _: Int, targetDT: Map[String, String]) =>
        throw new IllegalArgumentException(
          s"Can't translate non-null value for field $pos")
  }

  def savePartitionByBatchInsert(table: String, jdbcType: String,
                                 iterator: Iterator[Row],
                                 rddSchema: StructType,
                                 upsertStmtSingle: String,
                                 batchSize: Int,
                                 dialect: JdbcDialect,
                                 isolationLevel: Int,
                                 options: JDBCOptions,
                                 mergeKeys: String,
                                 upsertStmt: String): Unit = {

    if (iterator.isEmpty) {
      return
    }
    val conn = dialect.createConnectionFactory(options)(-1)
    var committed = false
    val isUpdate = true


    //获取目标库数据字段类型
    var targetDT: Map[String, String] = Map()
    var ps0: PreparedStatement = null

    if ("mysql".equalsIgnoreCase(jdbcType)) {
      ps0 = conn.prepareStatement("select column_name as columnName,data_type as columnType from information_schema.columns where  table_schema=? and table_name=? ");
      if (table.contains(".")) {
        ps0.setString(1, table.split("\\.")(0))
        ps0.setString(2, table.split("\\.")(1))
      } else {
        ps0 = conn.prepareStatement("select column_name as columnName,data_type as columnType from information_schema.columns where  table_name=? ");
        ps0.setString(1, table)
      }

    } else if ("postgresql".equalsIgnoreCase(jdbcType)) {

      ps0 = conn.prepareStatement("SELECT a.attname as columnName,pg_catalog.format_type(a.atttypid, a.atttypmod) as columnType FROM pg_catalog.pg_attribute a WHERE a.attnum > 0 AND NOT a.attisdropped AND a.attrelid = (SELECT c.oid FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE  n.nspname=? and c.relname =? )"); //AND pg_catalog.pg_table_is_visible(c.oid)
      if (table.contains(".")) {
        ps0.setString(1, table.split("\\.")(0))
        ps0.setString(2, table.split("\\.")(1))
      } else {
        ps0.setString(1, "public")
        ps0.setString(2, table)
      }
    } else {

    }

    if (ps0 != null) {
      val set = ps0.executeQuery()
      while (set.next) {
        targetDT += (set.getString("columnName") -> set.getString("columnType"))
      }
      ps0.close()
    }
    //////////////////////////////


    var finalIsolationLevel = Connection.TRANSACTION_NONE
    if (isolationLevel != Connection.TRANSACTION_NONE) {
      try {
        val metadata = conn.getMetaData
        if (metadata.supportsTransactions()) {
          // Update to at least use the default isolation, if any transaction level
          // has been chosen and transactions are supported
          val defaultIsolation = metadata.getDefaultTransactionIsolation
          finalIsolationLevel = defaultIsolation
          if (metadata.supportsTransactionIsolationLevel(isolationLevel)) {
            // Finally update to actually requested level if possible
            finalIsolationLevel = isolationLevel
          } else {
            logWarning(s"Requested isolation level $isolationLevel is not supported; " +
              s"falling back to default isolation level $defaultIsolation")
          }
        } else {
          logWarning(s"Requested isolation level $isolationLevel, but transactions are unsupported")
        }
      } catch {
        case NonFatal(e) => logWarning("Exception while detecting transaction support", e)
      }
    }
    val supportsTransactions = finalIsolationLevel != Connection.TRANSACTION_NONE
    var totalRowCount = 0L
    try {
      if (supportsTransactions) {
        conn.setAutoCommit(false) // Everything in the same db transaction.
        conn.setTransactionIsolation(finalIsolationLevel)
      }
      val stmt = conn.prepareStatement(upsertStmt)
      val stmtSingle = conn.prepareStatement(upsertStmtSingle)

      val setters: Array[JDBCValueSetterBatch] = isUpdate match {
        case true =>
          val setters: Array[JDBCValueSetterBatch] = rddSchema.fields
            .map(f => makeSetterBatch(conn, dialect, f.name, f.dataType))
          Array.fill(2)(setters).flatten
        case _ =>
          rddSchema.fields
            .map(f => makeSetterBatch(conn, dialect, f.name, f.dataType))
      }

      //      val setters_1: Array[JDBCValueSetter] = isUpdate match {
      //        case true =>
      //          val setters: Array[JDBCValueSetter] = (rddSchema.fields ++ rddSchema.fields.filter(e=> ! mergeKeys.split(",").contains(e.name))).map(_.dataType) .map(makeSetter(conn, dialect, _))
      //            Array.fill(1)(setters).flatten
      //        case _ =>
      //          rddSchema.fields.filter(e=> ! mergeKeys.split(",").contains(e.name)).map(_.dataType)
      //            .map(makeSetter(conn, dialect, _))
      //      }

      val nullTypes = rddSchema.fields.map(f => {

        if (targetDT.get(f.name).exists(_.equals("jsonb")) || targetDT.get(f.name).exists(_.equals("json"))) {
          java.sql.Types.OTHER
        } else if (targetDT.get(f.name).exists(_.equals("date"))) {
          java.sql.Types.DATE
        } else if (targetDT.get(f.name).exists(_.contains("timestamp"))) {
          java.sql.Types.TIMESTAMP
        } else {
          if (f.dataType.catalogString.equalsIgnoreCase("void")) {
            java.sql.Types.VARCHAR
          } else {
            getJdbcType(f.dataType, dialect).jdbcNullType
          }
        }
      })

      val numFieldsLength = rddSchema.fields.length
      var numFields = numFieldsLength
      if (!jdbcType.equalsIgnoreCase("postgresql") && !jdbcType.equalsIgnoreCase("oracle") && !jdbcType.equalsIgnoreCase("dm") && !jdbcType.equalsIgnoreCase("kingbase8")) {
        numFields = numFieldsLength * 2
      }

      try {
        var rowCount = 0
        stmt.setQueryTimeout(options.queryTimeout)
        while (iterator.hasNext) {
          val row = iterator.next()
          var i = 0
          while (i < numFields) {
            if (isUpdate) {
              //row.isNullAt 判空,则设置空值
              if (row.isNullAt(i)) {
                stmt.setNull(rowCount * numFields + i + 1, nullTypes(i))
                stmtSingle.setNull(i + 1, nullTypes(i))
              } else {
                setters(i).apply(stmt, row, i, 0, targetDT, rowCount * numFields + i)
                setters(i).apply(stmtSingle, row, i, 0, targetDT, i)
              }
            }
            //滚动游标
            i = i + 1
            if (rowCount == batchSize - 1 && i == numFields) {
              stmt.execute()
              rowCount = -1
              stmtSingle.clearBatch()
              stmtSingle.clearParameters()
            }
          }
          if (rowCount != -1) {
            stmtSingle.addBatch()
          }
          rowCount += 1
          totalRowCount += 1
        }
        if (rowCount > 0) {
          stmtSingle.executeBatch()
        }

      } finally {
        stmtSingle.close()
        stmt.close()
      }
      if (supportsTransactions) {
        conn.commit()
        //        savepointOffset()
      }
      committed = true
    } catch {
      case e: SQLException =>
        val cause = e.getNextException
        if (cause != null && e.getCause != cause) {
          // If there is no cause already, set 'next exception' as cause. If cause is null,
          // it *may* be because no cause was set yet
          if (e.getCause == null) {
            try {
              e.initCause(cause)
            } catch {
              // Or it may be null because the cause *was* explicitly initialized, to *null*,
              // in which case this fails. There is no other way to detect it.
              // addSuppressed in this case as well.
              case _: IllegalStateException => e.addSuppressed(cause)
            }
          } else {
            e.addSuppressed(cause)
          }
        }
        throw e
    } finally {
      if (!committed) {
        // The stage must fail.  We got here through an exception path, so
        // let the exception through unless rollback() or close() want to
        // tell the user about another problem.
        if (supportsTransactions) {
          conn.rollback()
        }
        //        else {
        //          outMetrics.setRecordsWritten(totalRowCount)
        //        }
        conn.close()
      } else {
        //        outMetrics.setRecordsWritten(totalRowCount)

        // The stage must succeed.  We cannot propagate any exception close() might throw.
        try {
          conn.close()
        } catch {
          case e: Exception => logWarning("Transaction succeeded, but closing failed", e)
        }
      }
    }
  }

  private def makeSetterBatch(
                               conn: Connection,
                               dialect: JdbcDialect, filedNmae: String,
                               dataType: DataType
                             ): JDBCValueSetterBatch = dataType match {
    case IntegerType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String], batchFlag: Int) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setInt(batchFlag + 1, row.getInt(pos - cursor))
        } else {
          stmt.setNull(batchFlag + 1, java.sql.Types.INTEGER)
        }

    case LongType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String], batchFlag: Int) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setLong(batchFlag + 1, row.getLong(pos - cursor))
        } else {
          stmt.setNull(batchFlag + 1, java.sql.Types.BIGINT)
        }

    case DoubleType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String], batchFlag: Int) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setDouble(batchFlag + 1, row.getDouble(pos - cursor))
        } else {
          stmt.setNull(batchFlag + 1, java.sql.Types.DOUBLE)
        }

    case FloatType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String], batchFlag: Int) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setFloat(batchFlag + 1, row.getFloat(pos - cursor))
        } else {
          stmt.setNull(batchFlag + 1, java.sql.Types.FLOAT)
        }

    case ShortType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String], batchFlag: Int) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setInt(batchFlag + 1, row.getShort(pos - cursor))
        } else {
          stmt.setNull(batchFlag + 1, java.sql.Types.SMALLINT)
        }

    case ByteType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String], batchFlag: Int) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setInt(batchFlag + 1, row.getByte(pos - cursor))
        } else {
          stmt.setNull(batchFlag + 1, java.sql.Types.TINYINT)
        }

    case BooleanType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String], batchFlag: Int) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setBoolean(batchFlag + 1, row.getBoolean(pos - cursor))
        } else {
          stmt.setNull(batchFlag + 1, java.sql.Types.BOOLEAN)
        }

    case StringType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String], batchFlag: Int) =>

        if (!row.isNullAt(pos - cursor)) {
          if ("jsonb".equalsIgnoreCase(targetDT(filedNmae)) || "json".equalsIgnoreCase(targetDT(filedNmae))) {
            //            val jsonb: PGobject = new PGobject()
            //            jsonb.setType("jsonb")
            //            jsonb.setValue(row.getString(pos - cursor))
            //            stmt.setObject(batchFlag + 1, jsonb)

          } else if ("date".equalsIgnoreCase(targetDT(filedNmae))) {
            try {
              val value: String = row.getString(pos - cursor)
              var date: Date = new Date(new java.util.Date().getTime)
              if (value != null && value.trim.length == 10) {
                date = new Date(timeFormat.parse(value).getTime)
                stmt.setDate(batchFlag + 1, date)
              }
              else if (value != null && value.trim.length == 19) {
                date = new Date(timeFormat2.parse(value).getTime)
                stmt.setDate(batchFlag + 1, date)

              } else if (value != null && value.trim.length == 14 && value.endsWith(" +08")) {
                date = new Date(timeFormat.parse(value.substring(0, 10)).getTime)
                stmt.setDate(batchFlag + 1, date)
              } else if (value != null && value.trim.length >= 10 && value.endsWith("00000")) {
                stmt.setDate(batchFlag + 1, new java.sql.Date(value.toLong))
              } else {
                stmt.setString(batchFlag + 1, value)
              }
            } catch {

              case e: Exception => {
                println("---出错了：字段：" + filedNmae + " 内容:" + row.getString(pos - cursor))
                e.printStackTrace()
                stmt.setNull(batchFlag + 1, java.sql.Types.DATE)
              }
            }
          } else {
            stmt.setString(batchFlag + 1, row.getString(pos - cursor))
          }

        } else {
          stmt.setNull(batchFlag + 1, java.sql.Types.VARCHAR)
        }

    case BinaryType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String], batchFlag: Int) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setBytes(batchFlag + 1, row.getAs[Array[Byte]](pos - cursor))
        } else {
          stmt.setNull(batchFlag + 1, java.sql.Types.BINARY)
        }

    case TimestampType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String], batchFlag: Int) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setTimestamp(batchFlag + 1, row.getAs[java.sql.Timestamp](pos
            - cursor))
        } else {
          stmt.setNull(batchFlag + 1, java.sql.Types.TIMESTAMP)
        }

    case DateType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String], batchFlag: Int) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setDate(batchFlag + 1, row.getAs[java.sql.Date](pos - cursor))
        } else {
          stmt.setNull(batchFlag + 1, java.sql.Types.DATE)
        }

    case t: DecimalType =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String], batchFlag: Int) =>
        if (!row.isNullAt(pos - cursor)) {
          stmt.setBigDecimal(batchFlag + 1, row.getDecimal(pos - cursor))
        } else {
          stmt.setNull(batchFlag + 1, java.sql.Types.DECIMAL)
        }

    case ArrayType(et, _) =>
      // remove type length parameters from end of type name
      val typeName = getJdbcType(et, dialect).databaseTypeDefinition
        .toLowerCase.split("\\(")(0)
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String], batchFlag: Int) =>
        if (!row.isNullAt(pos - cursor)) {
          val array = conn.createArrayOf(
            typeName,
            row.getSeq[AnyRef](pos - cursor).toArray)
          stmt.setArray(batchFlag + 1, array)
        } else {
          stmt.setNull(batchFlag + 1, java.sql.Types.ARRAY)
        }

    case _ =>
      (stmt: PreparedStatement, row: Row, pos: Int, cursor: Int, targetDT: Map[String, String], batchFlag: Int) =>
        throw new IllegalArgumentException(
          s"Can't translate non-null value for field $pos")
  }


}
