package com.jm.calcite;

import org.apache.calcite.sql.*;

import static org.apache.calcite.sql.SqlKind.*;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/9/11 17:11
 */
public class CalciteSQLHelper {

    public void parserSQLDetail(SqlNode sqlNode) {
        if (sqlNode.getKind() == JOIN) {
            SqlJoin sqlKind = (SqlJoin) sqlNode;
            System.out.println(sqlKind.getLeft() + "-----join------" + sqlKind.getRight() + ",,,,join Type = " + sqlKind.getJoinType());
            parserSQLDetail(sqlKind.getLeft());
            parserSQLDetail(sqlKind.getRight());
        }

        if (sqlNode.getKind() == IDENTIFIER) {
            System.out.println("-----identifier----- ,tableName =" + sqlNode);
        }

        if (sqlNode.getKind() == INSERT) {
            SqlInsert sqlKind = (SqlInsert) sqlNode;
            System.out.println("-----insert------");
            parserSQLDetail(sqlKind.getSource());
        }

        if (sqlNode.getKind() == SELECT) {
            SqlSelect sqlKind = (SqlSelect) sqlNode;

            sqlKind.getSelectList().forEach(e -> {
                if (e.getKind().equals(AS)) {
                    SqlBasicCall basicCall = (SqlBasicCall) e;
                    System.out.println("column as = " + basicCall.getOperandList().get(0).toString() + " AS " + basicCall.getOperandList().get(1).toString());
                } else {
                    System.out.println("column = " + e);
                }

            });
            //System.out.println("-----select------" + sqlKind.getFrom().toString());
            parserSQLDetail(sqlKind.getFrom());
        }

        if (sqlNode.getKind() == AS) {
            SqlBasicCall sqlKind = (SqlBasicCall) sqlNode;
            System.out.println("table as = " + sqlKind.getOperandList().get(0) + "----as----" + sqlKind.getOperandList().get(1));
            parserSQLDetail(sqlKind.getOperandList().get(0));
        }

        if (sqlNode.getKind() == UNION) {
            SqlBasicCall sqlKind = (SqlBasicCall) sqlNode;
            //System.out.println("----union-----");

            parserSQLDetail(sqlKind.getOperandList().get(0));
            parserSQLDetail(sqlKind.getOperandList().get(1));
        }

        if (sqlNode.getKind() == ORDER_BY) {
            SqlOrderBy sqlKind = (SqlOrderBy) sqlNode;
            //System.out.println("----order_by-----");
            parserSQLDetail(sqlKind.getOperandList().get(0));
        }
    }
}
