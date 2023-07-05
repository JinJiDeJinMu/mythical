package com.jm.spark.parser;

import com.jm.spark.sql.SqlBaseParser;
import com.jm.spark.sql.SqlBaseParserBaseListener;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/7/4 14:18
 */
public class ProcessBaseListener extends SqlBaseParserBaseListener {


    @Override
    public void enterSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
        super.enterSingleStatement(ctx);
        System.out.println("11111");
    }

    @Override
    public void enterStatementDefault(SqlBaseParser.StatementDefaultContext ctx) {
        super.enterStatementDefault(ctx);
        System.out.println("222222");
    }

    @Override
    public void exitStatementDefault(SqlBaseParser.StatementDefaultContext ctx) {
        super.exitStatementDefault(ctx);
        System.out.println("22223333");
    }

    @Override
    public void enterSelectClause(SqlBaseParser.SelectClauseContext ctx) {
        super.enterSelectClause(ctx);
    }

    @Override
    public void enterFromStmt(SqlBaseParser.FromStmtContext ctx) {
        super.enterFromStmt(ctx);
        System.out.println(33333333);
    }


    @Override
    public void enterQuery(SqlBaseParser.QueryContext ctx) {
        super.enterQuery(ctx);
    }

    @Override
    public void enterTable(SqlBaseParser.TableContext ctx) {
        String text = ctx.getText();
        System.out.println("table=" + text);
        super.enterTable(ctx);
    }

    @Override
    public void enterTableAlias(SqlBaseParser.TableAliasContext ctx) {
        String text = ctx.getText();
        System.out.println("table as = " + text);
        super.enterTableAlias(ctx);
    }
}
