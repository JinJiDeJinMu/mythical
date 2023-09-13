package com.jm.spark.parser;

import com.jm.spark.StatementData;
import com.jm.spark.StatementType;
import com.jm.spark.sql.SqlBaseParser;
import com.jm.spark.sql.SqlBaseParserBaseVisitor;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.lang.StringUtils;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/7/4 16:27
 */
public class SparkSqlProcessBaseVisitor extends SqlBaseParserBaseVisitor<String> {

    private String command = null;
    private String limit = null;
    private StatementData statementData = null;
    private StatementType statementType = StatementType.UNKOWN;


    @Override
    public String visit(ParseTree tree) {
        return super.visit(tree);
    }

    public void setCommand(String command) {
        this.command = command;
    }

    @Override
    public String visitCreateTableLike(SqlBaseParser.CreateTableLikeContext ctx) {
        String odb = ctx.source.db.getText();
        String otb = ctx.source.table.getText();

        String ndb = ctx.target.db.getText();
        String ntb = ctx.target.table.getText();
        System.out.println(odb);
        System.out.println(otb);
        System.out.println(ndb);
        System.out.println(ntb);

        return ndb + "." + ntb;
    }

    @Override
    public String visitStatementDefault(SqlBaseParser.StatementDefaultContext ctx) {

        System.out.println("1111111111");
        if (StringUtils.equalsIgnoreCase("select", ctx.start.getText())) {
            return "222";
        } else if (StringUtils.equalsIgnoreCase("insert", ctx.start.getText())) {
            return "3333";
        } else {
            return null;
        }

    }

    @Override
    public String visitSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
        System.out.println("22222222");
        return super.visitSingleStatement(ctx);
    }

    @Override
    public String visitTableIdentifier(SqlBaseParser.TableIdentifierContext ctx) {
        String db = ctx.db.getText();
        String table = ctx.table.getText();
        System.out.println(db + "." + table);
        return "111";
    }
}
