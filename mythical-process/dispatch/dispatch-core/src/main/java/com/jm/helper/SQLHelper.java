package com.jm.helper;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/5/18 14:15
 */
public class SQLHelper {

    private static final String INSERT_PREFIX = "insert";
    private static final String UPDATE_PREFIX = "update";
    private static final String SELECT_PREFIX = "select";
    private static final String SHOW_PREFIX = "show";
    private static final String DESC_PREFIX = "desc";
    private static final String EXPLAIN_PREFIX = "explain";
    private static final String WITH_AS = "with";
    private static final String regular = "([\\w\\W]*)(?i)(\\s)(update|insert|delete)(\\s)([\\w\\W]*)";

   public List<String> analyzeSQL(String SQLType,String SQL){
       // todo 后续会根据sqlType 不同的解析方式

       String[] splitList = SQL.split(";");
       return Arrays.stream(splitList).map(e->e.trim()).collect(Collectors.toList());
   }


   public boolean checkSelect(String SQL){
       return SQL.startsWith(SELECT_PREFIX)
               ||SQL.startsWith(DESC_PREFIX)
               ||SQL.startsWith(WITH_AS)
               ||SQL.startsWith(SHOW_PREFIX);
   }

   public boolean checkUpdate(String SQL){
       return SQL.startsWith(UPDATE_PREFIX);
   }

   public boolean checkInsert(String SQL){
       return SQL.startsWith(INSERT_PREFIX);
   }

   public InputStream transformCSVStream(ResultSet resultSet){
       List<String[]> recordList = new ArrayList<>();

       ResultSetMetaData metaData = null;
       try {
           metaData = resultSet.getMetaData();
           int columnCnt = metaData.getColumnCount();
           String[] headers = new String[columnCnt];

           for (int columnIndex = 1; columnIndex <= columnCnt; ++columnIndex) {
               int index = columnIndex - 1;
               String columnName = metaData.getColumnLabel(columnIndex);
               if (columnName.split("\\.").length == 2) {
                   columnName = columnName.split("\\.")[1];
               }
               headers[index] = columnName;
           }
           recordList.add(headers);
           while (resultSet.next()) {
               String[] items = new String[columnCnt];
               for (int columnIndex = 1; columnIndex <= columnCnt; ++columnIndex) {
                   int index = columnIndex - 1;
                   items[index] = resultSet.getString(columnIndex);
                   if (StringUtils.isNotBlank(items[index])) {
                       items[index] = items[index].replaceAll(" ", " ");
                   } else if ("null".equals(items[index]) || items[index] == null) {
                       items[index] = "NULL";
                   } else {
                       items[index] = "";
                   }
               }
               recordList.add(items);
           }

           ByteArrayInputStream csvInputStream;
           try (ByteArrayOutputStream bout = new ByteArrayOutputStream();
                PrintWriter printWriter = new PrintWriter(bout);
                CSVPrinter csvPrinter = CSVFormat.EXCEL.print(printWriter);) {

               for (String[] str : recordList) {
                   csvPrinter.printRecord(str);
                   csvPrinter.flush();
               }
               csvInputStream = new ByteArrayInputStream(bout.toByteArray());
           } catch (IOException e) {
               throw new RuntimeException(e);
           }
           return csvInputStream;
       } catch (SQLException e) {
           throw new RuntimeException(e);
       }
   }

}
