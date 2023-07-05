package com.jm.spark;

import com.jm.spark.parser.ProcessBaseListener;
import com.jm.spark.parser.ProcessBaseVisitor;
import com.jm.spark.sql.SqlBaseLexer;
import com.jm.spark.sql.SqlBaseParser;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/7/4 10:45
 */
public class Test {

    public static void main(String[] args) {
        //String sql = "SELECT id,name FROM b";
        String sql = "SELECT a.c1 AS cc,a.c2 ,b.c3 FROM a LEFT JOIN (SELECT c1,c3,c4,c5,id FROM d) as b on a.id = b.id";


        SqlBaseLexer sqlBaseLexer = new SqlBaseLexer(CharStreams.fromString(sql));
        CommonTokenStream commonTokenStream = new CommonTokenStream(sqlBaseLexer);
        SqlBaseParser parser = new SqlBaseParser(commonTokenStream);

        //SqlBaseParser.StatementContext statement = parser.statement();

        ProcessBaseVisitor processBaseVisitor = new ProcessBaseVisitor();

        //SparkSqlProcessListener processListener = new SparkSqlProcessListener();

        ProcessBaseListener processBaseListener = new ProcessBaseListener();

        ParseTreeWalker walker = new ParseTreeWalker();
        walker.walk(processBaseListener, parser.singleStatement());


        //Object visit = processBaseVisitor.visit(parser.singleStatement());

    }
}
