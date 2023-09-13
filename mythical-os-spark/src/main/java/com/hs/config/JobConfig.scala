/*
 *@Author   : DoubleTrey
 *@Time     : 2022/10/31 11:11
 */

package com.hs.config

import com.hs.common.{KafkaConsumeMode, ReadMode}

import java.util.UUID

/**
 * @param appName         [[String]]  spark app name
 * @param tablePathFormat [[String]]  数据表路径格式 /user/hive/warehouse/%s.db/%s
 * @param sourceDB        [[String]]  来源表 database
 * @param sourceTable     [[String]]  来源表名称
 * @param targetDB        [[String]]  目标表 database
 * @param targetTable     [[String]]  目标表名称
 * @param writeMode       [[String]]  数据写入目标表方式，append/update
 * @param mergeKeys       [[String]]  merge 条件字段，仅当 saveMode 为 update 时生效
 * @param sortColumns     [[String]]  merge 前，数据去重所需的字段排序配置，去重结果取同组排序第一条
 */
//TODO: 来源表数据筛选条件
case class DeltaJobConfig(appName: String,
                          tablePathFormat: String = SystemConfig.tablePathFormat,
                          sourceDB: String = "default",
                          sourceTable: String,
                          targetDB: String = "default",
                          targetTable: String,
                          writeMode: String,
                          mergeKeys: String,
                          sortColumns: String)

/**
 *
 * @param columnName    [[String]]  敏感字段
 * @param encryptName   [[String]]  加密算法名称
 * @param encryptParams [[List]].[[String]] 加密算法参数列表
 */
case class EncryptColConfig(columnName: String,
                            encryptName: String,
                            encryptParams: List[Any])

/**
 *
 * @param columnName    [[String]]  敏感字段
 * @param decryptName   [[String]]  加密算法名称
 * @param decryptParams [[List]].[[String]] 加密算法参数列表
 */
case class DecryptColConfig(columnName: String,
                            decryptName: String,
                            decryptParams: List[Any])

/**
 * @param appName          [[String]]  spark app name
 * @param tablePathFormat  [[String]]  数据表路径格式 eg. /user/hive/warehouse/%s.db/%s
 * @param offsetPathFormat [[String]]  数据表offset路径格式 eg. /delta/dev/_offset/%s2%s/%s-%s
 * @param calculationMode  [[String]]  计算模式：full(全量)/incr(增量) 默认全量计算
 * @param sourceDbTableMap [[String]]  source 端库与表{database:table:tableReadMode}映射关系 eg. ods:t1:incr,ods:t2:incr,dim:t3:full
 * @param sinkDbTableMap   [[String]]  sink 端库与表{database:table}映射关系 eg. ods:t1,ods:t2
 * @param sql              [[String]]  基于 sourceDbTableMap 中传入的多表编写的复杂sql,支持参数传入和非参数传入
 * @param incrColumn       [[String]]  来源表增量字段,default: insert_time
 * @param writeMode        [[String]]  写入方式 append/update
 * @param mergeKeys        [[String]]  merge 条件字段,仅当 saveMode 为 update 时生效
 * @param sortColumns      [[String]]  merge 前,数据去重所需的字段排序配置，去重结果取同组排序第一条
 */
case class DeltaMulTableConfig(appName: String,
                               tablePathFormat: String = SystemConfig.tablePathFormat,
                               offsetPathFormat: String = "/delta/dev/_offset/%s2%s/%s-%s",
                               calculationMode: String = "full",
                               sourceDbTableMap: String,
                               sinkDbTableMap: String = "",
                               sql: String,
                               writeMode: String,
                               incrColumn: String = "insert_time",
                               mergeKeys: String,
                               sortColumns: String = "update_time:-1",
                               columnMap: String,
                               //query: String,
                               jdbcUrl: String,
                               username: String,
                               password: String,
                               taskID: String = "111",
                               targetDatabase: String,
                               targetTable: String,
                               batchSize: String = "200",
                               kafkaServers: String = null,
                               topic: String = null,
                               isWatermark: String = "0",
                               fullJobOffsetCol: String = "")

/**
 * delta realtime to sr
 *
 * @param taskID
 * @param tablePathFormat
 * @param sourceDbTableMap
 * @param isCDC
 * @param columnMap
 * @param sql
 * @param sinkDbTableMap
 * @param feHttp
 * @param jdbcUrl
 * @param username
 * @param password
 */
case class DeltaToStarRocksConfig(taskID: String,
                                  tablePathFormat: String = "/user/hive/warehouse/%s.db/%s",
                                  sourceDbTableMap: String,
                                  isCDC: String = "false",
                                  columnMap: String = "",
                                  sql: String = "",
                                  sinkDbTableMap: String,
                                  sinkColumn: String = "",
                                  feHttp: String,
                                  jdbcUrl: String,
                                  username: String,
                                  password: String = ""
                                 )

/**
 *
 * @param taskID              [[String]]  任务id
 * @param tablePathFormat     [[String]]  数据表路径格式 /user/hive/warehouse/%s.db/%s
 * @param kafkaServers        [[String]]  kafka broker servers
 * @param topicName           [[String]]  kafka topic 名称
 * @param consumeMode         [[String]]  消费模式：earliest/latest
 * @param batchSize           [[String]]  poll size
 * @param consumeDelay        [[String]]  消费延时
 * @param failOnDataLoss      [[String]]
 * @param sourceColumns       [[String]]  kafka value 解析数据结构
 * @param targetTable         [[String]]  目标表名称
 * @param mergeKeys           [[String]]  merge 条件字段，仅当 saveMode 为 update 时生效
 * @param writeMode           [[String]]  数据写入目标表方式，append/update
 * @param zoneType            [[String]]  分区类型  "time/type"
 * @param zoneFieldCode       [[String]]  分区字段(来源)
 * @param zoneTargetFieldCode [[String]]  分区字段(目的)
 * @param zoneTypeUnit        [[String]]  分区类型单位  time:day/month/year  type:待扩充(默认不变)
 */
case class KafkaToDeltaJobConfig(taskID: String,
                                 tablePathFormat: String = SystemConfig.tablePathFormat,
                                 kafkaServers: String,
                                 topicName: String,
                                 consumeMode: String = KafkaConsumeMode.latest.toString,
                                 batchSize: String = "100000",
                                 consumeDelay: String = "1m",
                                 failOnDataLoss: String = false.toString,
                                 sourceColumns: String,
                                 targetDatabase: String,
                                 targetTable: String,
                                 mergeKeys: String,
                                 writeMode: String,
                                 isKafkaConnector: String = false.toString,
                                 isCDC: String = false.toString,
                                 columnMap: String,
                                 zoneType: String = "",
                                 zoneFieldCode: String = "",
                                 zoneTargetFieldCode: String = "",
                                 zoneTypeUnit: String = "unChange",
                                 encrypt: List[EncryptColConfig] = null,
                                 dataCountKafkaServers: String = null,
                                 dataCountTopicName: String = null)

case class RocketmqToDeltaJobConfig(taskID: String,
                                    tablePathFormat: String = SystemConfig.tablePathFormat,
                                    nameServer: String,
                                    topic: String,
                                    group: String = s"spark-rocketmq-source-${UUID.randomUUID}",
                                    pullTimeout: String = "3000",
                                    pullBatchSize: String = "32",
                                    jsonPath: String = "",
                                    valueType: String = "string",
                                    consumeMode: String = KafkaConsumeMode.latest.toString,
                                    failOnDataLoss: String = false.toString,
                                    sourceColumns: String,
                                    targetDatabase: String,
                                    targetTable: String,
                                    mergeKeys: String,
                                    writeMode: String,
                                    columnMap: String,
                                    zoneType: String = "",
                                    zoneFieldCode: String = "",
                                    zoneTargetFieldCode: String = "",
                                    zoneTypeUnit: String = "unChange",
                                    encrypt: List[EncryptColConfig] = null,
                                    dataCountKafkaServers: String = null,
                                    dataCountTopicName: String = null)


/**
 *
 * @param taskID              [[String]]  任务id
 * @param esNodes             [[String]] esNodes
 * @param esIndex             [[String]] es索引
 * @param esType              [[String]] esType
 * @param password            [[String]] 密码
 * @param username            [[String]] 账号
 * @param esQuery             [[String]] esQuery
 * @param sourceColumns       [[String]]  源表字段
 * @param columnMap           [[String]] 源表目标表字段映射
 * @param mergeKeys           [[String]]  目标表唯一主键
 * @param writeMode           [[String]] 写入模式
 * @param targetDatabase      [[String]] delta库名
 * @param targetTable         [[String]] delta 表名
 * @param offsetColumns       [[String]] 源表offset字段
 * @param esConf              [[String]] es额外配置
 * @param zoneType            [[String]]  分区类型  "time/type"
 * @param zoneFieldCode       [[String]]  分区字段(来源)
 * @param zoneTargetFieldCode [[String]]  分区字段(目的)
 * @param zoneTypeUnit        [[String]]  分区类型单位  time:day/month/year  type:待扩充(默认不变)
 */
case class EsToDeltaJobConfig(taskID: String,
                              tablePathFormat: String = SystemConfig.tablePathFormat,
                              esNodes: String,
                              //esPort: String,
                              esIndex: String,
                              esType: String = "_doc",
                              password: String = "",
                              username: String = "",
                              esQuery: String = "{\"query\":{\"match_all\":{}}}",
                              sourceColumns: String,
                              columnMap: String,
                              //sinkColumnType: String,
                              mergeKeys: String = "id",
                              writeMode: String,
                              targetDatabase: String,
                              targetTable: String,
                              offsetColumns: String,
                              readMode: String = ReadMode.full.toString,
                              esConf: String = "",
                              zoneType: String = "",
                              zoneFieldCode: String = "",
                              zoneTargetFieldCode: String = "",
                              zoneTypeUnit: String = "unChange",
                              encrypt: List[EncryptColConfig] = null,
                              dataCountKafkaServers: String = null,
                              dataCountTopicName: String = null)

/**
 *
 * @param taskID              [[String]]  任务id
 * @param tablePathFormat     [[String]]  delta 数据表路径格式 /user/hive/warehouse/%s.db/%s
 * @param jdbcUrl             [[String]]  jdbc 链接  e.g., jdbc:postgresql://localhost/test?user=fred&password=secret
 * @param username            [[String]]  jdbc 用户名
 * @param password            [[String]]  jdbc 密码
 * @param query               [[String]]  jdbc 查询
 * @param sourceColumns       [[String]]  来源表字段
 * @param fetchSize           [[String]]  jdbc fetch size
 * @param targetDatabase      [[String]]  目标表 database
 * @param targetTable         [[String]]  目标表名称
 * @param writeMode           [[String]]  数据写入目标表方式，append/update
 * @param mergeKeys           [[String]]  merge 条件字段，仅当 saveMode 为 update 时生效
 * @param sortColumns         [[String]]  merge 前，数据去重所需的字段排序配置，去重结果取同组排序第一条
 * @param offsetColumns       [[String]]
 * @param columnMap           [[String]]
 * @param version             [[String]]
 * @param zoneType            [[String]]  分区类型  "time/type"
 * @param zoneFieldCode       [[String]]  分区字段(来源)
 * @param zoneTargetFieldCode [[String]]  分区字段(目的)
 * @param zoneTypeUnit        [[String]]  分区类型单位  time:day/month/year  type:待扩充(默认不变)
 */
case class JdbcToDeltaJobConfig(taskID: String,
                                tablePathFormat: String = SystemConfig.tablePathFormat,
                                jdbcUrl: String,
                                username: String = "",
                                password: String = "",
                                query: String,
                                //sourceTable: String,
                                sourceColumns: String,
                                fetchSize: String = "100",
                                targetDatabase: String = "default",
                                targetTable: String,
                                writeMode: String,
                                mergeKeys: String,
                                sortColumns: String = "",
                                offsetColumns: String,
                                readMode: String = ReadMode.full.toString,
                                columnMap: String = null,
                                version: String = "",
                                zoneType: String = "",
                                zoneFieldCode: String = "",
                                zoneTargetFieldCode: String = "",
                                zoneTypeUnit: String = "unChange",
                                encrypt: List[EncryptColConfig] = null,
                                dataCountKafkaServers: String = null,
                                dataCountTopicName: String = null)

/**
 *
 * @param taskID          [[String]]  任务id
 * @param tablePathFormat [[String]]  delta 数据表路径格式 { /user/hive/warehouse/%s.db/%s }
 * @param jdbcUrl         [[String]]  jdbc 链接
 * @param username        [[String]]  jdbc 用户名
 * @param password        [[String]]  jdbc 密码
 * @param sql             [[String]]  jdbc 查询
 * @param columnMap       [[String]]  source与sink端表字段映射
 * @param sinkColumnsType [[String]]  sink端表字段及类型
 * @param writeMode       [[String]]  写入目标表方式
 * @param sourceTable     [[String]]  source端表名
 * @param sinkTable       [[String]]  sink端表名
 * @param database        [[String]]  sink端库名
 * @param upsertKey       [[String]]  merge条件字段
 * @param offsetColumns
 * @param partitionSource [[String]]  指定source端表分区字段
 * @param partitionTarget [[String]]  指定sink端表分区字段
 * @param partitionFormat [[String]]  指定分区格式 { day/month/year}
 */
@deprecated
case class HiveToDeltaJobConfig(taskID: String,
                                tablePathFormat: String = SystemConfig.tablePathFormat,
                                jdbcUrl: String,
                                username: String,
                                password: String,
                                sql: String,
                                columnMap: String,
                                sinkColumnsType: String,
                                writeMode: String,
                                sourceTable: String,
                                sinkTable: String,
                                database: String,
                                upsertKey: String,
                                offsetColumns: String,
                                partitionSource: String,
                                partitionTarget: String,
                                readMode: String = ReadMode.full.toString,
                                partitionFormat: String)

/**
 *
 * @param taskID              [[String]]  任务id
 * @param tablePathFormat     [[String]]  delta 数据表路径格式 { /user/hive/warehouse/%s.db/%s }
 * @param url                 [[String]]  mongo地址
 * @param username            [[String]]  jdbc 用户名
 * @param password            [[String]]  jdbc 密码
 * @param sql                 [[String]]  jdbc 查询
 * @param columnMap           [[String]]  source->delta映射关系: id:id,name:name,age:age
 * @param sourceColumns       [[String]]  delta表字段类型	id:int,name:string,age:int  原：sinkColumnType
 * @param writeMode           [[String]]  写入目标表方式
 * @param collection          [[String]] 读取的collection名
 * @param sourceDatabase      [[String]]  sink端库名
 * @param targetDatabase      [[String]]  source端数据库 原 sinkDatabase
 * @param targetTable         [[String]]  sink端数据库   原 sinkDatabase
 * @param mergeKeys           [[String]]  merge 条件字段，仅当 saveMode 为 update 时生效
 * @param sortColumns         [[String]]  merge 前，数据去重所需的字段排序配置，去重结果取同组排序第一条
 * @param offsetColumns       [[String]]  当writeMode为upsert时，需要指定原表的增量字段
 * @param zoneType            [[String]]  分区类型  "time/type"
 * @param zoneFieldCode       [[String]]  分区字段(来源)
 * @param zoneTargetFieldCode [[String]]  分区字段(目的)
 * @param zoneTypeUnit        [[String]]  分区类型单位  time:day/month/year  type:待扩充(默认不变)
 */
case class MongoToDeltaJobConfig(taskID: String,
                                 tablePathFormat: String = SystemConfig.tablePathFormat,
                                 url: String,
                                 username: String = "",
                                 password: String = "",
                                 sql: String = "",
                                 columnMap: String,
                                 sourceColumns: String,
                                 writeMode: String,
                                 collection: String,
                                 //sourceTable: String ,
                                 sourceDatabase: String,
                                 targetDatabase: String,
                                 targetTable: String,
                                 //upsertKey: String,
                                 mergeKeys: String,
                                 sortColumns: String = "",
                                 readMode: String = ReadMode.full.toString,
                                 offsetColumns: String,
                                 zoneType: String = "",
                                 zoneFieldCode: String = "",
                                 zoneTargetFieldCode: String = "",
                                 zoneTypeUnit: String = "unChange",
                                 encrypt: List[EncryptColConfig] = null,
                                 dataCountKafkaServers: String = null,
                                 dataCountTopicName: String = null)

/**
 *
 * @param taskID              [[String]]
 * @param tablePathFormat     [[String]]
 * @param filePath            [[String]]
 * @param delimiter           [[String]]
 * @param includeHeader       [[String]]
 * @param targetDatabase      [[String]]
 * @param targetTable         [[String]]
 * @param writeMode           [[String]]
 * @param mergeKeys           [[String]]
 * @param sortColumns         [[String]]
 * @param offsetColumns       [[String]]
 * @param columnMap           [[String]]
 * @param zoneType            [[String]]  分区类型  "time/type"
 * @param zoneFieldCode       [[String]]  分区字段(来源)
 * @param zoneTargetFieldCode [[String]]  分区字段(目的)
 * @param zoneTypeUnit        [[String]]  分区类型单位  time:day/month/year  type:待扩充(默认不变)
 */
case class CsvToDeltaJobConfig(taskID: String,
                               tablePathFormat: String = SystemConfig.tablePathFormat,
                               filePath: String,
                               delimiter: String = ";",
                               includeHeader: String = true.toString,
                               //sourceColumns: String,
                               encoding: String = SystemConfig.defaultEncoding,
                               targetDatabase: String,
                               targetTable: String,
                               writeMode: String,
                               mergeKeys: String = "",
                               sortColumns: String = "",
                               offsetColumns: String,
                               columnMap: String = null,
                               zoneType: String = "",
                               zoneFieldCode: String = "",
                               zoneTargetFieldCode: String = "",
                               zoneTypeUnit: String = "unChange",
                               encrypt: List[EncryptColConfig] = null,
                               dataCountKafkaServers: String = null,
                               dataCountTopicName: String = null)


/**
 *
 * @param taskID              [[String]]
 * @param tablePathFormat     [[String]]
 * @param filePath            [[String]]
 * @param includeHeader       [[String]]
 * @param targetDatabase      [[String]]
 * @param targetTable         [[String]]
 * @param writeMode           [[String]]
 * @param mergeKeys           [[String]]
 * @param sortColumns         [[String]]
 * @param offsetColumns       [[String]]
 * @param columnMap           [[String]]
 * @param zoneType            [[String]]  分区类型  "time/type"
 * @param zoneFieldCode       [[String]]  分区字段(来源)
 * @param zoneTargetFieldCode [[String]]  分区字段(目的)
 * @param zoneTypeUnit        [[String]]  分区类型单位  time:day/month/year  type:待扩充(默认不变)
 */
case class ExcelToDeltaJobConfig(taskID: String,
                                 tablePathFormat: String = SystemConfig.tablePathFormat,
                                 filePath: String,
                                 includeHeader: String = true.toString,
                                 targetDatabase: String,
                                 targetTable: String,
                                 writeMode: String,
                                 mergeKeys: String = "",
                                 sortColumns: String = "",
                                 offsetColumns: String,
                                 columnMap: String = null,
                                 zoneType: String = "",
                                 zoneFieldCode: String = "",
                                 zoneTargetFieldCode: String = "",
                                 zoneTypeUnit: String = "unChange",
                                 encrypt: List[EncryptColConfig] = null,
                                 dataCountKafkaServers: String = null,
                                 dataCountTopicName: String = null)

/**
 *
 * @param taskID              [[String]]
 * @param tablePathFormat     [[String]]
 * @param filePath            [[String]]
 * @param targetDatabase      [[String]]
 * @param targetTable         [[String]]
 * @param writeMode           [[String]]
 * @param mergeKeys           [[String]]
 * @param sortColumns         [[String]]
 * @param offsetColumns       [[String]]
 * @param columnMap           [[String]]
 * @param zoneType            [[String]]  分区类型  "time/type"
 * @param zoneFieldCode       [[String]]  分区字段(来源)
 * @param zoneTargetFieldCode [[String]]  分区字段(目的)
 * @param zoneTypeUnit        [[String]]  分区类型单位  time:day/month/year  type:待扩充(默认不变)
 */
case class TextToDeltaJobConfig(taskID: String,
                                tablePathFormat: String = SystemConfig.tablePathFormat,
                                filePath: String,
                                lineSep: String = "\n",
                                wholetext: String = false.toString,
                                encoding: String = SystemConfig.defaultEncoding,
                                targetDatabase: String,
                                targetTable: String,
                                writeMode: String,
                                mergeKeys: String = "",
                                sortColumns: String = "",
                                offsetColumns: String,
                                columnMap: String = null,
                                zoneType: String = "",
                                zoneFieldCode: String = "",
                                zoneTargetFieldCode: String = "",
                                zoneTypeUnit: String = "unChange",
                                encrypt: List[EncryptColConfig] = null,
                                dataCountKafkaServers: String = null,
                                dataCountTopicName: String = null)


case class RedisToDeltaJobConfig(taskID: String,
                                 tablePathFormat: String = SystemConfig.tablePathFormat,
                                 host: String,
                                 port: String,
                                 redisDb: String = "0",
                                 keysPattern: String,
                                 auth: String = "password",
                                 inferSchema: String = true.toString,
                                 targetDatabase: String,
                                 targetTable: String,
                                 writeMode: String,
                                 mergeKeys: String = "",
                                 //                                 sortColumns: String = "",
                                 columnMap: String = null,
                                 zoneType: String = "",
                                 zoneFieldCode: String = "",
                                 zoneTargetFieldCode: String = "",
                                 zoneTypeUnit: String = "unChange",
                                 encrypt: List[EncryptColConfig] = null,
                                 dataCountKafkaServers: String = null,
                                 dataCountTopicName: String = null)


/**
 *
 * @param taskID          [[String]]
 * @param tablePathFormat [[String]]
 * @param sourceDatabase  [[String]]  来源表库名
 * @param sourceTable     [[String]]  来源表名称
 * @param sourceColumns   [[String]]  源表字段：`id:int,name:string,age:int`
 * @param query           [[String]]  来源表查询sql, TODO：db.table 替换delta.`tablePath`
 * @param jdbcUrl         [[String]]  jdbc url
 * @param username        [[String]]  jdbc user
 * @param password        [[String]]
 * @param batchSize       [[String]]  jdbc write batch size
 * @param targetDatabase  [[String]]
 * @param targetTable     [[String]]
 * @param writeMode       [[String]]  append/overwrite/ignore/errorifexists/update
 * @param mergeKeys       [[String]]  Fixme: 不同jdbc upsert实现
 * @param sortColumns     [[String]]
 * @param offsetColumns   [[String]]
 * @param columnMap       [[String]]
 */
case class DeltaToJdbcJobConfig(taskID: String,
                                tablePathFormat: String = SystemConfig.tablePathFormat,
                                sourceDatabase: String = "default",
                                sourceTable: String,
                                sourceColumns: String,
                                query: String,
                                jdbcUrl: String,
                                username: String,
                                password: String,
                                batchSize: String = "1000",
                                targetDatabase: String = null,
                                targetTable: String,
                                writeMode: String,
                                mergeKeys: String,
                                sortColumns: String = "",
                                offsetColumns: String = "",
                                columnMap: String = null)

/**
 *
 * @param taskID          任务id
 * @param tablePathFormat delta表路径
 * @param filePath        ftp文件路径 ，截取掉ftp url的文件地址，例如 /a/b.csv
 * @param url             连接地址
 * @param port            端口号
 * @param username        用户名
 * @param password        密码
 * @param fileType        文件类型
 * @param delimiter       列分隔符 默认，
 * @param encoding        读取文件编码格式 默认utf-8
 * @param compressionType 压缩类型，默认不压缩
 * @param includeHeader   是否包含表头 默认不包含
 * @param targetDatabase  目标库
 * @param targetTable     目标表
 * @param writeMode       写入模式
 * @param mergeKeys       合并key 仅当 saveMode 为 update 时生效
 * @param sortColumns     排序字段
 * @param offsetColumns   当writeMode为upsert时，需要指定原表的增量字段
 * @param columnMap
 */
case class FtpToDeltaJobConfig(taskID: String,
                               tablePathFormat: String = SystemConfig.tablePathFormat,
                               url: String,
                               port: Int,
                               username: String,
                               password: String,
                               filePath: String,
                               fileType: String,
                               nullValue: String = null,
                               delimiter: String = ",",
                               encoding: String = SystemConfig.defaultEncoding,
                               compressionType: String = "",
                               includeHeader: String,
                               targetDatabase: String,
                               targetTable: String,
                               writeMode: String,
                               mergeKeys: String = "",
                               sortColumns: String = "",
                               offsetColumns: String,
                               columnMap: String = null,
                               zoneType: String = "",
                               zoneFieldCode: String = "",
                               zoneTargetFieldCode: String = "",
                               zoneTypeUnit: String = "unChange",
                               encrypt: List[EncryptColConfig] = null,
                               dataCountKafkaServers: String = null,
                               dataCountTopicName: String = null)

/**
 *
 * @param taskID          任务id
 * @param tablePathFormat delta表路径
 * @param apiUrl          api地址
 * @param method          请求方法
 * @param dataMode        请求返回的结果JSON数据的格式。oneData：从返回的JSON中取其1条数据。multiData：从返回的JSON中取一个JSON数组，传递多条数据给writer。
 * @param dataPath        从返回结果中查询单个JSON对象或者JSON数组的路径。
 * @param responseType    返回结果的数据格式，目前仅支持JSON格式。
 * @param header          传递给接口的header信息。
 * @param parameters      传递给接口的参数信息。get方法填入abc=1&def=1。post方法填入JSON类型参数。
 * @param requestTimes    从接口地址中请求数据的次数。single：只进行一次请求。multiple：进行多次请求。这一期默认single
 * @param authType        Basic Auth/Token Auth
 * @param username        Basic Auth验证的用户名和密码。
 * @param password
 * @param token           Token Auth验证的token。
 * @param targetDatabase
 * @param targetTable
 * @param writeMode
 * @param mergeKeys
 * @param sortColumns
 * @param offsetColumns
 * @param columnMap
 */
case class ApiToDeltaJobConfig(taskID: String,
                               tablePathFormat: String = SystemConfig.tablePathFormat,
                               apiUrl: String,
                               method: String,
                               dataMode: String,
                               dataPath: String,
                               responseType: String,
                               header: String,
                               parameters: String,
                               requestTimes: String,
                               authType: String,
                               username: String,
                               password: String,
                               token: String,
                               targetDatabase: String,
                               targetTable: String,
                               writeMode: String,
                               mergeKeys: String = "",
                               sortColumns: String = "",
                               offsetColumns: String,
                               columnMap: String = null,
                               zoneType: String = "",
                               zoneFieldCode: String = "",
                               zoneTargetFieldCode: String = "",
                               zoneTypeUnit: String = "unChange",
                               encrypt: List[EncryptColConfig] = null,
                               dataCountKafkaServers: String = null,
                               dataCountTopicName: String = null)

/**
 * delta写入到jdbc
 *
 * @param taskID
 * @param sourceDatabase
 * @param sourceTable
 * @param jdbcUrl
 * @param username
 * @param password
 * @param targetTable
 * @param writeMode
 * @param columnMap
 * @param offsetColumns
 * @param readMode
 */
case class DeltaToJdbcConfigV2(taskID: String,
                               tablePathFormat: String = SystemConfig.tablePathFormat,
                               sourceDatabase: String,
                               sourceTable: String,
                               query: String,
                               jdbcUrl: String,
                               username: String,
                               password: String,
                               targetDatabase: String,
                               targetTable: String,
                               writeMode: String,
                               batchSize: String = "200",
                               columnMap: String,
                               offsetColumns: String,
                               mergeKeys: String,
                               readMode: String,
                               kafkaServers: String = null,
                               topic: String = null,
                               isWatermark: String = "0")

case class KafkaToJdbcJobConfig(taskID: String,
                                kafkaServers: String,
                                topicName: String,
                                keyType: String,
                                valueType: String,
                                groupId: String,
                                consumeMode: String = KafkaConsumeMode.latest.toString,
                                jsonPath: String = "",
                                batchSize: String = "200",
                                customParams: String = "",
                                jdbcUrl: String,
                                username: String,
                                password: String,
                                targetTable: String,
                                writeMode: String,
                                columnMap: String,
                                sourceColumns: String,
                                mergeKeys: String = "",
                                DataCountKafkaServers: String = "",
                                DataCountTopicName: String = "")


case class JdbcToJdbcJobConfig(taskID: String,
                               sourceJdbcUrl: String,
                               sourceUsername: String = "",
                               sourcePassword: String = "",
                               query: String,
                               sourceColumns: String,
                               fetchSize: String = "100",
                               targetJdbcUrl: String,
                               targetUsername: String = "",
                               targetPassword: String = "",
                               targetTable: String,
                               batchSize: String = "200",
                               writeMode: String,
                               mergeKeys: String,
                               sortColumns: String = "",
                               offsetColumns: String,
                               readMode: String = ReadMode.full.toString,
                               columnMap: String = null,
                               encrypt: List[EncryptColConfig] = null)

case class JdbcToHiveJobConfig(taskID: String,
                               sourceJdbcUrl: String,
                               sourceUsername: String = "",
                               sourcePassword: String = "",
                               query: String,
                               sourceColumns: String,
                               fetchSize: String = "100",
                               targetJdbcUrl: String,
                               targetUsername: String = "",
                               targetPassword: String = "",
                               targetTable: String,
                               hiveHdfsPath: String,
                               batchSize: String = "200",
                               writeMode: String,
                               mergeKeys: String,
                               sortColumns: String = "",
                               offsetColumns: String,
                               readMode: String = ReadMode.full.toString,
                               columnMap: String = null,
                               encrypt: List[EncryptColConfig] = null)




