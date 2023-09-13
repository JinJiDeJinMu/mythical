package com.jm.spark;

import com.jm.spark.parser.PostProcessor;
import com.jm.spark.parser.SparkSqlProcessBaseVisitor;
import com.jm.spark.sql.SqlBaseLexer;
import com.jm.spark.sql.SqlBaseParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.apache.commons.lang.StringUtils;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/7/24 10:41
 */
public class SparkSqlHelper {


    public String getStatementData(String command) {
        String sql = StringUtils.trim(command);
        SqlBaseLexer sqlBaseLexer = new SqlBaseLexer(CharStreams.fromString(sql));
        sqlBaseLexer.removeErrorListeners();

        CommonTokenStream tokenStream = new CommonTokenStream(sqlBaseLexer);
        SqlBaseParser parser = new SqlBaseParser(tokenStream);
        parser.addParseListener(new PostProcessor());
        parser.removeErrorListeners();

        SparkSqlProcessBaseVisitor sqlVisitor = new SparkSqlProcessBaseVisitor();
        sqlVisitor.setCommand(command);

        try {
            return sqlVisitor.visit(parser.singleStatement());
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    public Boolean checkSupportSql(String command) {
        return true;
    }

    public static void main(String[] args) {
        String sql = "select a.c1 AS cc,a.c2 ,b.c3 FROM a LEFT JOIN (SELECT c1,c3,c4,c5,id FROM d) as b on a.id = b.id";

        SparkSqlHelper sparkSqlHelper = new SparkSqlHelper();
        String data = sparkSqlHelper.getStatementData(sql);
        System.out.println(data);
    }
}
