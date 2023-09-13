package com.jm.spark;

import com.jm.spark.demo.ProcessParserListener;
import com.jm.spark.sql.SqlBaseLexer;
import com.jm.spark.sql.SqlBaseParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.apache.commons.lang.StringUtils;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/7/4 10:45
 */
public class Test {

    public static void main(String[] args) {
        //String sql = "SELECT id,name FROM b";
        String orginSQL = "select a.c1 AS cc,a.c2 ,b.c3 FROM a LEFT JOIN (SELECT c1,c3,c4,c5,id FROM d) as b on a.id = b.id";


        String ss = "insert into t1 select * from t2";
        //String sql = "create table test.sale_detail_like like demo.sale_detail";
        String sql = StringUtils.trim(ss);
        SqlBaseLexer sqlBaseLexer = new SqlBaseLexer(CharStreams.fromString(sql));
        sqlBaseLexer.removeErrorListeners();

        CommonTokenStream tokenStream = new CommonTokenStream(sqlBaseLexer);
        SqlBaseParser parser = new SqlBaseParser(tokenStream);
        parser.removeErrorListeners();

        ParseTreeWalker walker = new ParseTreeWalker();

        //ProcessParserVisitor sqlVisitor = new ProcessParserVisitor();
        ProcessParserListener processParserListener = new ProcessParserListener();

        try {
            walker.walk(processParserListener, parser.statement());
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
