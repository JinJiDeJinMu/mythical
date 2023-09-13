package com.jm.spark.demo;

import com.jm.spark.sql.SqlBaseParser;
import com.jm.spark.sql.SqlBaseParserBaseVisitor;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/9/12 16:49
 */
public class ProcessParserVisitor extends SqlBaseParserBaseVisitor {


    @Override
    public Object visitSelectClause(SqlBaseParser.SelectClauseContext ctx) {
        System.out.println("11111111");
        return super.visitSelectClause(ctx);
    }

    @Override
    public Object visitStatementDefault(SqlBaseParser.StatementDefaultContext ctx) {
        System.out.println("222222222");
        return super.visitStatementDefault(ctx);
    }
}
