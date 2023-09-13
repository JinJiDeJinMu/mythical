package com.jm.calcite;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/9/11 17:22
 */
public class CalciteTest {

    public static void main(String[] args) {
        String sql = "select a.c1 AS cc,a.c2 ,b.c3 from a left join (select c1,c3,c4,c5,id from d) as b on a.id = b.id";

        SqlParser parser = SqlParser.create(sql.toLowerCase(), SqlParser.Config.DEFAULT);

        try {
            SqlNode sqlNode = parser.parseStmt();
            CalciteSQLHelper calciteSQLHelper = new CalciteSQLHelper();
            calciteSQLHelper.parserSQLDetail(sqlNode);
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }

    }
}
