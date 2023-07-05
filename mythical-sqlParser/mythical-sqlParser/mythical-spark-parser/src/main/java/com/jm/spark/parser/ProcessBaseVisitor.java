package com.jm.spark.parser;

import com.jm.spark.sql.SqlBaseParser;
import com.jm.spark.sql.SqlBaseParserBaseVisitor;
import org.antlr.v4.runtime.Token;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/7/4 16:27
 */
public class ProcessBaseVisitor extends SqlBaseParserBaseVisitor {

    @Override
    public Object visitStatementDefault(SqlBaseParser.StatementDefaultContext ctx) {
        System.out.println("v1111111");
        return super.visitStatementDefault(ctx);
    }

    @Override
    public Object visitSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
        System.out.println("s1111111");

        Object visit = visit(ctx.statement());
        System.out.println("visit = " + visit);
        return visit(ctx.statement());
    }

    @Override
    public Object visitQuery(SqlBaseParser.QueryContext ctx) {
        System.out.println("query11111" + ctx.getText());
        Token start = ctx.queryTerm().start;
        Token stop = ctx.queryTerm().stop;


        return super.visitQuery(ctx);
    }


    @Override
    public Object visitQueryPrimaryDefault(SqlBaseParser.QueryPrimaryDefaultContext ctx) {
        System.out.println("query primary" + ctx.querySpecification().getText());
        ctx.querySpecification().children.stream().forEach(e -> {
            System.out.println("query primary = " + e.getText());
        });
        return super.visitQueryPrimaryDefault(ctx);
    }

    @Override
    public Object visitQueryTermDefault(SqlBaseParser.QueryTermDefaultContext ctx) {
        System.out.println("query term" + ctx.queryPrimary().getText());
        SqlBaseParser.QueryPrimaryContext queryPrimary = ctx.queryPrimary();
        queryPrimary.children.stream().forEach(e -> {
            System.out.println("query term = " + e.getText());
        });
        return super.visitQueryTermDefault(ctx);
    }

    @Override
    public Object visitTransformQuerySpecification(SqlBaseParser.TransformQuerySpecificationContext ctx) {
        System.out.println("q11111111");
        return super.visitTransformQuerySpecification(ctx);
    }

    @Override
    public Object visitRegularQuerySpecification(SqlBaseParser.RegularQuerySpecificationContext ctx) {
        System.out.println("regula 1111111");
        return super.visitRegularQuerySpecification(ctx);
    }

    @Override
    public Object visitIdentifierReference(SqlBaseParser.IdentifierReferenceContext ctx) {
        System.out.println("iden 11111111");
        return super.visitIdentifierReference(ctx);
    }

    @Override
    public Object visitSelectClause(SqlBaseParser.SelectClauseContext ctx) {
        System.out.println("select clause");
        return super.visitSelectClause(ctx);
    }

    @Override
    public Object visitFromClause(SqlBaseParser.FromClauseContext ctx) {
        System.out.println("from clause");
        return super.visitFromClause(ctx);
    }

    @Override
    public Object visitTableName(SqlBaseParser.TableNameContext ctx) {
        System.out.println("table name");
        String text = ctx.getText();
        return super.visitTableName(ctx);
    }


}
