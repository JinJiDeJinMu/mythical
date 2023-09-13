package com.jm.spark.demo;

import com.jm.spark.sql.SqlBaseParser;
import com.jm.spark.sql.SqlBaseParserBaseListener;


/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/9/12 17:00
 */
public class ProcessParserListener extends SqlBaseParserBaseListener {

    @Override
    public void enterTableIdentifier(SqlBaseParser.TableIdentifierContext ctx) {
        System.out.println("1111111111");
        super.enterTableIdentifier(ctx);
    }

    @Override
    public void enterStatementDefault(SqlBaseParser.StatementDefaultContext ctx) {
        System.out.println("222222222");
        super.enterStatementDefault(ctx);
    }

    @Override
    public void enterSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
        System.out.println("33333333");
        super.enterSingleStatement(ctx);
    }

    @Override
    public void enterTable(SqlBaseParser.TableContext ctx) {
        System.out.println("444444444");
        super.enterTable(ctx);
    }

    @Override
    public void enterInsertIntoTable(SqlBaseParser.InsertIntoTableContext ctx) {
        System.out.println("1111111");
        super.enterInsertIntoTable(ctx);
    }
}
