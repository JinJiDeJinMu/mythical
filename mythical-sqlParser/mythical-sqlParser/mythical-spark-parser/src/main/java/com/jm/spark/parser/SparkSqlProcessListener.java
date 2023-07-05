package com.jm.spark.parser;

import com.jm.spark.sql.SqlBaseParser;
import com.jm.spark.sql.SqlBaseParserListener;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.List;

/**
 * TODO
 *
 * @Author jinmu
 * @Date 2023/7/4 10:06
 */
public class SparkSqlProcessListener implements SqlBaseParserListener {


    @Override
    public void enterSingleStatement(SqlBaseParser.SingleStatementContext ctx) {
        System.out.println("enter single statement");

        List<ParseTree> children = ctx.children;
        children.stream().forEach(e -> {
            System.out.println(e.getText());
        });
    }

    @Override
    public void exitSingleStatement(SqlBaseParser.SingleStatementContext ctx) {

    }

    @Override
    public void enterSingleExpression(SqlBaseParser.SingleExpressionContext ctx) {

    }

    @Override
    public void exitSingleExpression(SqlBaseParser.SingleExpressionContext ctx) {

    }

    @Override
    public void enterSingleTableIdentifier(SqlBaseParser.SingleTableIdentifierContext ctx) {

    }

    @Override
    public void exitSingleTableIdentifier(SqlBaseParser.SingleTableIdentifierContext ctx) {

    }

    @Override
    public void enterSingleMultipartIdentifier(SqlBaseParser.SingleMultipartIdentifierContext ctx) {

    }

    @Override
    public void exitSingleMultipartIdentifier(SqlBaseParser.SingleMultipartIdentifierContext ctx) {

    }

    @Override
    public void enterSingleFunctionIdentifier(SqlBaseParser.SingleFunctionIdentifierContext ctx) {

    }

    @Override
    public void exitSingleFunctionIdentifier(SqlBaseParser.SingleFunctionIdentifierContext ctx) {

    }

    @Override
    public void enterSingleDataType(SqlBaseParser.SingleDataTypeContext ctx) {

    }

    @Override
    public void exitSingleDataType(SqlBaseParser.SingleDataTypeContext ctx) {

    }

    @Override
    public void enterSingleTableSchema(SqlBaseParser.SingleTableSchemaContext ctx) {

    }

    @Override
    public void exitSingleTableSchema(SqlBaseParser.SingleTableSchemaContext ctx) {

    }

    @Override
    public void enterStatementDefault(SqlBaseParser.StatementDefaultContext ctx) {

    }

    @Override
    public void exitStatementDefault(SqlBaseParser.StatementDefaultContext ctx) {

    }

    @Override
    public void enterDmlStatement(SqlBaseParser.DmlStatementContext ctx) {

    }

    @Override
    public void exitDmlStatement(SqlBaseParser.DmlStatementContext ctx) {

    }

    @Override
    public void enterUse(SqlBaseParser.UseContext ctx) {

    }

    @Override
    public void exitUse(SqlBaseParser.UseContext ctx) {

    }

    @Override
    public void enterUseNamespace(SqlBaseParser.UseNamespaceContext ctx) {

    }

    @Override
    public void exitUseNamespace(SqlBaseParser.UseNamespaceContext ctx) {

    }

    @Override
    public void enterSetCatalog(SqlBaseParser.SetCatalogContext ctx) {

    }

    @Override
    public void exitSetCatalog(SqlBaseParser.SetCatalogContext ctx) {

    }

    @Override
    public void enterCreateNamespace(SqlBaseParser.CreateNamespaceContext ctx) {

    }

    @Override
    public void exitCreateNamespace(SqlBaseParser.CreateNamespaceContext ctx) {

    }

    @Override
    public void enterSetNamespaceProperties(SqlBaseParser.SetNamespacePropertiesContext ctx) {

    }

    @Override
    public void exitSetNamespaceProperties(SqlBaseParser.SetNamespacePropertiesContext ctx) {

    }

    @Override
    public void enterSetNamespaceLocation(SqlBaseParser.SetNamespaceLocationContext ctx) {

    }

    @Override
    public void exitSetNamespaceLocation(SqlBaseParser.SetNamespaceLocationContext ctx) {

    }

    @Override
    public void enterDropNamespace(SqlBaseParser.DropNamespaceContext ctx) {

    }

    @Override
    public void exitDropNamespace(SqlBaseParser.DropNamespaceContext ctx) {

    }

    @Override
    public void enterShowNamespaces(SqlBaseParser.ShowNamespacesContext ctx) {

    }

    @Override
    public void exitShowNamespaces(SqlBaseParser.ShowNamespacesContext ctx) {

    }

    @Override
    public void enterCreateTable(SqlBaseParser.CreateTableContext ctx) {

    }

    @Override
    public void exitCreateTable(SqlBaseParser.CreateTableContext ctx) {

    }

    @Override
    public void enterCreateTableLike(SqlBaseParser.CreateTableLikeContext ctx) {

    }

    @Override
    public void exitCreateTableLike(SqlBaseParser.CreateTableLikeContext ctx) {

    }

    @Override
    public void enterReplaceTable(SqlBaseParser.ReplaceTableContext ctx) {

    }

    @Override
    public void exitReplaceTable(SqlBaseParser.ReplaceTableContext ctx) {

    }

    @Override
    public void enterAnalyze(SqlBaseParser.AnalyzeContext ctx) {

    }

    @Override
    public void exitAnalyze(SqlBaseParser.AnalyzeContext ctx) {

    }

    @Override
    public void enterAnalyzeTables(SqlBaseParser.AnalyzeTablesContext ctx) {

    }

    @Override
    public void exitAnalyzeTables(SqlBaseParser.AnalyzeTablesContext ctx) {

    }

    @Override
    public void enterAddTableColumns(SqlBaseParser.AddTableColumnsContext ctx) {

    }

    @Override
    public void exitAddTableColumns(SqlBaseParser.AddTableColumnsContext ctx) {

    }

    @Override
    public void enterRenameTableColumn(SqlBaseParser.RenameTableColumnContext ctx) {

    }

    @Override
    public void exitRenameTableColumn(SqlBaseParser.RenameTableColumnContext ctx) {

    }

    @Override
    public void enterDropTableColumns(SqlBaseParser.DropTableColumnsContext ctx) {

    }

    @Override
    public void exitDropTableColumns(SqlBaseParser.DropTableColumnsContext ctx) {

    }

    @Override
    public void enterRenameTable(SqlBaseParser.RenameTableContext ctx) {

    }

    @Override
    public void exitRenameTable(SqlBaseParser.RenameTableContext ctx) {

    }

    @Override
    public void enterSetTableProperties(SqlBaseParser.SetTablePropertiesContext ctx) {

    }

    @Override
    public void exitSetTableProperties(SqlBaseParser.SetTablePropertiesContext ctx) {

    }

    @Override
    public void enterUnsetTableProperties(SqlBaseParser.UnsetTablePropertiesContext ctx) {

    }

    @Override
    public void exitUnsetTableProperties(SqlBaseParser.UnsetTablePropertiesContext ctx) {

    }

    @Override
    public void enterAlterTableAlterColumn(SqlBaseParser.AlterTableAlterColumnContext ctx) {

    }

    @Override
    public void exitAlterTableAlterColumn(SqlBaseParser.AlterTableAlterColumnContext ctx) {

    }

    @Override
    public void enterHiveChangeColumn(SqlBaseParser.HiveChangeColumnContext ctx) {

    }

    @Override
    public void exitHiveChangeColumn(SqlBaseParser.HiveChangeColumnContext ctx) {

    }

    @Override
    public void enterHiveReplaceColumns(SqlBaseParser.HiveReplaceColumnsContext ctx) {

    }

    @Override
    public void exitHiveReplaceColumns(SqlBaseParser.HiveReplaceColumnsContext ctx) {

    }

    @Override
    public void enterSetTableSerDe(SqlBaseParser.SetTableSerDeContext ctx) {

    }

    @Override
    public void exitSetTableSerDe(SqlBaseParser.SetTableSerDeContext ctx) {

    }

    @Override
    public void enterAddTablePartition(SqlBaseParser.AddTablePartitionContext ctx) {

    }

    @Override
    public void exitAddTablePartition(SqlBaseParser.AddTablePartitionContext ctx) {

    }

    @Override
    public void enterRenameTablePartition(SqlBaseParser.RenameTablePartitionContext ctx) {

    }

    @Override
    public void exitRenameTablePartition(SqlBaseParser.RenameTablePartitionContext ctx) {

    }

    @Override
    public void enterDropTablePartitions(SqlBaseParser.DropTablePartitionsContext ctx) {

    }

    @Override
    public void exitDropTablePartitions(SqlBaseParser.DropTablePartitionsContext ctx) {

    }

    @Override
    public void enterSetTableLocation(SqlBaseParser.SetTableLocationContext ctx) {

    }

    @Override
    public void exitSetTableLocation(SqlBaseParser.SetTableLocationContext ctx) {

    }

    @Override
    public void enterRecoverPartitions(SqlBaseParser.RecoverPartitionsContext ctx) {

    }

    @Override
    public void exitRecoverPartitions(SqlBaseParser.RecoverPartitionsContext ctx) {

    }

    @Override
    public void enterDropTable(SqlBaseParser.DropTableContext ctx) {

    }

    @Override
    public void exitDropTable(SqlBaseParser.DropTableContext ctx) {

    }

    @Override
    public void enterDropView(SqlBaseParser.DropViewContext ctx) {

    }

    @Override
    public void exitDropView(SqlBaseParser.DropViewContext ctx) {

    }

    @Override
    public void enterCreateView(SqlBaseParser.CreateViewContext ctx) {

    }

    @Override
    public void exitCreateView(SqlBaseParser.CreateViewContext ctx) {

    }

    @Override
    public void enterCreateTempViewUsing(SqlBaseParser.CreateTempViewUsingContext ctx) {

    }

    @Override
    public void exitCreateTempViewUsing(SqlBaseParser.CreateTempViewUsingContext ctx) {

    }

    @Override
    public void enterAlterViewQuery(SqlBaseParser.AlterViewQueryContext ctx) {

    }

    @Override
    public void exitAlterViewQuery(SqlBaseParser.AlterViewQueryContext ctx) {

    }

    @Override
    public void enterCreateFunction(SqlBaseParser.CreateFunctionContext ctx) {

    }

    @Override
    public void exitCreateFunction(SqlBaseParser.CreateFunctionContext ctx) {

    }

    @Override
    public void enterDropFunction(SqlBaseParser.DropFunctionContext ctx) {

    }

    @Override
    public void exitDropFunction(SqlBaseParser.DropFunctionContext ctx) {

    }

    @Override
    public void enterExplain(SqlBaseParser.ExplainContext ctx) {

    }

    @Override
    public void exitExplain(SqlBaseParser.ExplainContext ctx) {

    }

    @Override
    public void enterShowTables(SqlBaseParser.ShowTablesContext ctx) {

    }

    @Override
    public void exitShowTables(SqlBaseParser.ShowTablesContext ctx) {

    }

    @Override
    public void enterShowTableExtended(SqlBaseParser.ShowTableExtendedContext ctx) {

    }

    @Override
    public void exitShowTableExtended(SqlBaseParser.ShowTableExtendedContext ctx) {

    }

    @Override
    public void enterShowTblProperties(SqlBaseParser.ShowTblPropertiesContext ctx) {

    }

    @Override
    public void exitShowTblProperties(SqlBaseParser.ShowTblPropertiesContext ctx) {

    }

    @Override
    public void enterShowColumns(SqlBaseParser.ShowColumnsContext ctx) {

    }

    @Override
    public void exitShowColumns(SqlBaseParser.ShowColumnsContext ctx) {

    }

    @Override
    public void enterShowViews(SqlBaseParser.ShowViewsContext ctx) {

    }

    @Override
    public void exitShowViews(SqlBaseParser.ShowViewsContext ctx) {

    }

    @Override
    public void enterShowPartitions(SqlBaseParser.ShowPartitionsContext ctx) {

    }

    @Override
    public void exitShowPartitions(SqlBaseParser.ShowPartitionsContext ctx) {

    }

    @Override
    public void enterShowFunctions(SqlBaseParser.ShowFunctionsContext ctx) {

    }

    @Override
    public void exitShowFunctions(SqlBaseParser.ShowFunctionsContext ctx) {

    }

    @Override
    public void enterShowCreateTable(SqlBaseParser.ShowCreateTableContext ctx) {

    }

    @Override
    public void exitShowCreateTable(SqlBaseParser.ShowCreateTableContext ctx) {

    }

    @Override
    public void enterShowCurrentNamespace(SqlBaseParser.ShowCurrentNamespaceContext ctx) {

    }

    @Override
    public void exitShowCurrentNamespace(SqlBaseParser.ShowCurrentNamespaceContext ctx) {

    }

    @Override
    public void enterShowCatalogs(SqlBaseParser.ShowCatalogsContext ctx) {

    }

    @Override
    public void exitShowCatalogs(SqlBaseParser.ShowCatalogsContext ctx) {

    }

    @Override
    public void enterDescribeFunction(SqlBaseParser.DescribeFunctionContext ctx) {

    }

    @Override
    public void exitDescribeFunction(SqlBaseParser.DescribeFunctionContext ctx) {

    }

    @Override
    public void enterDescribeNamespace(SqlBaseParser.DescribeNamespaceContext ctx) {

    }

    @Override
    public void exitDescribeNamespace(SqlBaseParser.DescribeNamespaceContext ctx) {

    }

    @Override
    public void enterDescribeRelation(SqlBaseParser.DescribeRelationContext ctx) {

    }

    @Override
    public void exitDescribeRelation(SqlBaseParser.DescribeRelationContext ctx) {

    }

    @Override
    public void enterDescribeQuery(SqlBaseParser.DescribeQueryContext ctx) {

    }

    @Override
    public void exitDescribeQuery(SqlBaseParser.DescribeQueryContext ctx) {

    }

    @Override
    public void enterCommentNamespace(SqlBaseParser.CommentNamespaceContext ctx) {

    }

    @Override
    public void exitCommentNamespace(SqlBaseParser.CommentNamespaceContext ctx) {

    }

    @Override
    public void enterCommentTable(SqlBaseParser.CommentTableContext ctx) {

    }

    @Override
    public void exitCommentTable(SqlBaseParser.CommentTableContext ctx) {

    }

    @Override
    public void enterRefreshTable(SqlBaseParser.RefreshTableContext ctx) {

    }

    @Override
    public void exitRefreshTable(SqlBaseParser.RefreshTableContext ctx) {

    }

    @Override
    public void enterRefreshFunction(SqlBaseParser.RefreshFunctionContext ctx) {

    }

    @Override
    public void exitRefreshFunction(SqlBaseParser.RefreshFunctionContext ctx) {

    }

    @Override
    public void enterRefreshResource(SqlBaseParser.RefreshResourceContext ctx) {

    }

    @Override
    public void exitRefreshResource(SqlBaseParser.RefreshResourceContext ctx) {

    }

    @Override
    public void enterCacheTable(SqlBaseParser.CacheTableContext ctx) {

    }

    @Override
    public void exitCacheTable(SqlBaseParser.CacheTableContext ctx) {

    }

    @Override
    public void enterUncacheTable(SqlBaseParser.UncacheTableContext ctx) {

    }

    @Override
    public void exitUncacheTable(SqlBaseParser.UncacheTableContext ctx) {

    }

    @Override
    public void enterClearCache(SqlBaseParser.ClearCacheContext ctx) {

    }

    @Override
    public void exitClearCache(SqlBaseParser.ClearCacheContext ctx) {

    }

    @Override
    public void enterLoadData(SqlBaseParser.LoadDataContext ctx) {

    }

    @Override
    public void exitLoadData(SqlBaseParser.LoadDataContext ctx) {

    }

    @Override
    public void enterTruncateTable(SqlBaseParser.TruncateTableContext ctx) {

    }

    @Override
    public void exitTruncateTable(SqlBaseParser.TruncateTableContext ctx) {

    }

    @Override
    public void enterRepairTable(SqlBaseParser.RepairTableContext ctx) {

    }

    @Override
    public void exitRepairTable(SqlBaseParser.RepairTableContext ctx) {

    }

    @Override
    public void enterManageResource(SqlBaseParser.ManageResourceContext ctx) {

    }

    @Override
    public void exitManageResource(SqlBaseParser.ManageResourceContext ctx) {

    }

    @Override
    public void enterFailNativeCommand(SqlBaseParser.FailNativeCommandContext ctx) {

    }

    @Override
    public void exitFailNativeCommand(SqlBaseParser.FailNativeCommandContext ctx) {

    }

    @Override
    public void enterSetTimeZone(SqlBaseParser.SetTimeZoneContext ctx) {

    }

    @Override
    public void exitSetTimeZone(SqlBaseParser.SetTimeZoneContext ctx) {

    }

    @Override
    public void enterSetQuotedConfiguration(SqlBaseParser.SetQuotedConfigurationContext ctx) {

    }

    @Override
    public void exitSetQuotedConfiguration(SqlBaseParser.SetQuotedConfigurationContext ctx) {

    }

    @Override
    public void enterSetConfiguration(SqlBaseParser.SetConfigurationContext ctx) {

    }

    @Override
    public void exitSetConfiguration(SqlBaseParser.SetConfigurationContext ctx) {

    }

    @Override
    public void enterResetQuotedConfiguration(SqlBaseParser.ResetQuotedConfigurationContext ctx) {

    }

    @Override
    public void exitResetQuotedConfiguration(SqlBaseParser.ResetQuotedConfigurationContext ctx) {

    }

    @Override
    public void enterResetConfiguration(SqlBaseParser.ResetConfigurationContext ctx) {

    }

    @Override
    public void exitResetConfiguration(SqlBaseParser.ResetConfigurationContext ctx) {

    }

    @Override
    public void enterCreateIndex(SqlBaseParser.CreateIndexContext ctx) {

    }

    @Override
    public void exitCreateIndex(SqlBaseParser.CreateIndexContext ctx) {

    }

    @Override
    public void enterDropIndex(SqlBaseParser.DropIndexContext ctx) {

    }

    @Override
    public void exitDropIndex(SqlBaseParser.DropIndexContext ctx) {

    }

    @Override
    public void enterTimezone(SqlBaseParser.TimezoneContext ctx) {

    }

    @Override
    public void exitTimezone(SqlBaseParser.TimezoneContext ctx) {

    }

    @Override
    public void enterConfigKey(SqlBaseParser.ConfigKeyContext ctx) {

    }

    @Override
    public void exitConfigKey(SqlBaseParser.ConfigKeyContext ctx) {

    }

    @Override
    public void enterConfigValue(SqlBaseParser.ConfigValueContext ctx) {

    }

    @Override
    public void exitConfigValue(SqlBaseParser.ConfigValueContext ctx) {

    }

    @Override
    public void enterUnsupportedHiveNativeCommands(SqlBaseParser.UnsupportedHiveNativeCommandsContext ctx) {

    }

    @Override
    public void exitUnsupportedHiveNativeCommands(SqlBaseParser.UnsupportedHiveNativeCommandsContext ctx) {

    }

    @Override
    public void enterCreateTableHeader(SqlBaseParser.CreateTableHeaderContext ctx) {

    }

    @Override
    public void exitCreateTableHeader(SqlBaseParser.CreateTableHeaderContext ctx) {

    }

    @Override
    public void enterReplaceTableHeader(SqlBaseParser.ReplaceTableHeaderContext ctx) {

    }

    @Override
    public void exitReplaceTableHeader(SqlBaseParser.ReplaceTableHeaderContext ctx) {

    }

    @Override
    public void enterBucketSpec(SqlBaseParser.BucketSpecContext ctx) {

    }

    @Override
    public void exitBucketSpec(SqlBaseParser.BucketSpecContext ctx) {

    }

    @Override
    public void enterSkewSpec(SqlBaseParser.SkewSpecContext ctx) {

    }

    @Override
    public void exitSkewSpec(SqlBaseParser.SkewSpecContext ctx) {

    }

    @Override
    public void enterLocationSpec(SqlBaseParser.LocationSpecContext ctx) {

    }

    @Override
    public void exitLocationSpec(SqlBaseParser.LocationSpecContext ctx) {

    }

    @Override
    public void enterCommentSpec(SqlBaseParser.CommentSpecContext ctx) {

    }

    @Override
    public void exitCommentSpec(SqlBaseParser.CommentSpecContext ctx) {

    }

    @Override
    public void enterQuery(SqlBaseParser.QueryContext ctx) {
        System.out.println("enter query");


    }

    @Override
    public void exitQuery(SqlBaseParser.QueryContext ctx) {

    }

    @Override
    public void enterInsertOverwriteTable(SqlBaseParser.InsertOverwriteTableContext ctx) {

    }

    @Override
    public void exitInsertOverwriteTable(SqlBaseParser.InsertOverwriteTableContext ctx) {

    }

    @Override
    public void enterInsertIntoTable(SqlBaseParser.InsertIntoTableContext ctx) {

    }

    @Override
    public void exitInsertIntoTable(SqlBaseParser.InsertIntoTableContext ctx) {

    }

    @Override
    public void enterInsertIntoReplaceWhere(SqlBaseParser.InsertIntoReplaceWhereContext ctx) {

    }

    @Override
    public void exitInsertIntoReplaceWhere(SqlBaseParser.InsertIntoReplaceWhereContext ctx) {

    }

    @Override
    public void enterInsertOverwriteHiveDir(SqlBaseParser.InsertOverwriteHiveDirContext ctx) {

    }

    @Override
    public void exitInsertOverwriteHiveDir(SqlBaseParser.InsertOverwriteHiveDirContext ctx) {

    }

    @Override
    public void enterInsertOverwriteDir(SqlBaseParser.InsertOverwriteDirContext ctx) {

    }

    @Override
    public void exitInsertOverwriteDir(SqlBaseParser.InsertOverwriteDirContext ctx) {

    }

    @Override
    public void enterPartitionSpecLocation(SqlBaseParser.PartitionSpecLocationContext ctx) {

    }

    @Override
    public void exitPartitionSpecLocation(SqlBaseParser.PartitionSpecLocationContext ctx) {

    }

    @Override
    public void enterPartitionSpec(SqlBaseParser.PartitionSpecContext ctx) {

    }

    @Override
    public void exitPartitionSpec(SqlBaseParser.PartitionSpecContext ctx) {

    }

    @Override
    public void enterPartitionVal(SqlBaseParser.PartitionValContext ctx) {

    }

    @Override
    public void exitPartitionVal(SqlBaseParser.PartitionValContext ctx) {

    }

    @Override
    public void enterNamespace(SqlBaseParser.NamespaceContext ctx) {

    }

    @Override
    public void exitNamespace(SqlBaseParser.NamespaceContext ctx) {

    }

    @Override
    public void enterNamespaces(SqlBaseParser.NamespacesContext ctx) {

    }

    @Override
    public void exitNamespaces(SqlBaseParser.NamespacesContext ctx) {

    }

    @Override
    public void enterDescribeFuncName(SqlBaseParser.DescribeFuncNameContext ctx) {

    }

    @Override
    public void exitDescribeFuncName(SqlBaseParser.DescribeFuncNameContext ctx) {

    }

    @Override
    public void enterDescribeColName(SqlBaseParser.DescribeColNameContext ctx) {

    }

    @Override
    public void exitDescribeColName(SqlBaseParser.DescribeColNameContext ctx) {

    }

    @Override
    public void enterCtes(SqlBaseParser.CtesContext ctx) {

    }

    @Override
    public void exitCtes(SqlBaseParser.CtesContext ctx) {

    }

    @Override
    public void enterNamedQuery(SqlBaseParser.NamedQueryContext ctx) {

    }

    @Override
    public void exitNamedQuery(SqlBaseParser.NamedQueryContext ctx) {

    }

    @Override
    public void enterTableProvider(SqlBaseParser.TableProviderContext ctx) {

    }

    @Override
    public void exitTableProvider(SqlBaseParser.TableProviderContext ctx) {

    }

    @Override
    public void enterCreateTableClauses(SqlBaseParser.CreateTableClausesContext ctx) {

    }

    @Override
    public void exitCreateTableClauses(SqlBaseParser.CreateTableClausesContext ctx) {

    }

    @Override
    public void enterPropertyList(SqlBaseParser.PropertyListContext ctx) {

    }

    @Override
    public void exitPropertyList(SqlBaseParser.PropertyListContext ctx) {

    }

    @Override
    public void enterProperty(SqlBaseParser.PropertyContext ctx) {

    }

    @Override
    public void exitProperty(SqlBaseParser.PropertyContext ctx) {

    }

    @Override
    public void enterPropertyKey(SqlBaseParser.PropertyKeyContext ctx) {

    }

    @Override
    public void exitPropertyKey(SqlBaseParser.PropertyKeyContext ctx) {

    }

    @Override
    public void enterPropertyValue(SqlBaseParser.PropertyValueContext ctx) {

    }

    @Override
    public void exitPropertyValue(SqlBaseParser.PropertyValueContext ctx) {

    }

    @Override
    public void enterExpressionPropertyList(SqlBaseParser.ExpressionPropertyListContext ctx) {

    }

    @Override
    public void exitExpressionPropertyList(SqlBaseParser.ExpressionPropertyListContext ctx) {

    }

    @Override
    public void enterExpressionProperty(SqlBaseParser.ExpressionPropertyContext ctx) {

    }

    @Override
    public void exitExpressionProperty(SqlBaseParser.ExpressionPropertyContext ctx) {

    }

    @Override
    public void enterConstantList(SqlBaseParser.ConstantListContext ctx) {

    }

    @Override
    public void exitConstantList(SqlBaseParser.ConstantListContext ctx) {

    }

    @Override
    public void enterNestedConstantList(SqlBaseParser.NestedConstantListContext ctx) {

    }

    @Override
    public void exitNestedConstantList(SqlBaseParser.NestedConstantListContext ctx) {

    }

    @Override
    public void enterCreateFileFormat(SqlBaseParser.CreateFileFormatContext ctx) {

    }

    @Override
    public void exitCreateFileFormat(SqlBaseParser.CreateFileFormatContext ctx) {

    }

    @Override
    public void enterTableFileFormat(SqlBaseParser.TableFileFormatContext ctx) {

    }

    @Override
    public void exitTableFileFormat(SqlBaseParser.TableFileFormatContext ctx) {

    }

    @Override
    public void enterGenericFileFormat(SqlBaseParser.GenericFileFormatContext ctx) {

    }

    @Override
    public void exitGenericFileFormat(SqlBaseParser.GenericFileFormatContext ctx) {

    }

    @Override
    public void enterStorageHandler(SqlBaseParser.StorageHandlerContext ctx) {

    }

    @Override
    public void exitStorageHandler(SqlBaseParser.StorageHandlerContext ctx) {

    }

    @Override
    public void enterResource(SqlBaseParser.ResourceContext ctx) {

    }

    @Override
    public void exitResource(SqlBaseParser.ResourceContext ctx) {

    }

    @Override
    public void enterSingleInsertQuery(SqlBaseParser.SingleInsertQueryContext ctx) {

    }

    @Override
    public void exitSingleInsertQuery(SqlBaseParser.SingleInsertQueryContext ctx) {

    }

    @Override
    public void enterMultiInsertQuery(SqlBaseParser.MultiInsertQueryContext ctx) {

    }

    @Override
    public void exitMultiInsertQuery(SqlBaseParser.MultiInsertQueryContext ctx) {

    }

    @Override
    public void enterDeleteFromTable(SqlBaseParser.DeleteFromTableContext ctx) {

    }

    @Override
    public void exitDeleteFromTable(SqlBaseParser.DeleteFromTableContext ctx) {

    }

    @Override
    public void enterUpdateTable(SqlBaseParser.UpdateTableContext ctx) {

    }

    @Override
    public void exitUpdateTable(SqlBaseParser.UpdateTableContext ctx) {

    }

    @Override
    public void enterMergeIntoTable(SqlBaseParser.MergeIntoTableContext ctx) {

    }

    @Override
    public void exitMergeIntoTable(SqlBaseParser.MergeIntoTableContext ctx) {

    }

    @Override
    public void enterIdentifierReference(SqlBaseParser.IdentifierReferenceContext ctx) {

    }

    @Override
    public void exitIdentifierReference(SqlBaseParser.IdentifierReferenceContext ctx) {

    }

    @Override
    public void enterQueryOrganization(SqlBaseParser.QueryOrganizationContext ctx) {

    }

    @Override
    public void exitQueryOrganization(SqlBaseParser.QueryOrganizationContext ctx) {

    }

    @Override
    public void enterMultiInsertQueryBody(SqlBaseParser.MultiInsertQueryBodyContext ctx) {

    }

    @Override
    public void exitMultiInsertQueryBody(SqlBaseParser.MultiInsertQueryBodyContext ctx) {

    }

    @Override
    public void enterQueryTermDefault(SqlBaseParser.QueryTermDefaultContext ctx) {

    }

    @Override
    public void exitQueryTermDefault(SqlBaseParser.QueryTermDefaultContext ctx) {

    }

    @Override
    public void enterSetOperation(SqlBaseParser.SetOperationContext ctx) {

    }

    @Override
    public void exitSetOperation(SqlBaseParser.SetOperationContext ctx) {

    }

    @Override
    public void enterQueryPrimaryDefault(SqlBaseParser.QueryPrimaryDefaultContext ctx) {

    }

    @Override
    public void exitQueryPrimaryDefault(SqlBaseParser.QueryPrimaryDefaultContext ctx) {

    }

    @Override
    public void enterFromStmt(SqlBaseParser.FromStmtContext ctx) {

    }

    @Override
    public void exitFromStmt(SqlBaseParser.FromStmtContext ctx) {

    }

    @Override
    public void enterTable(SqlBaseParser.TableContext ctx) {

    }

    @Override
    public void exitTable(SqlBaseParser.TableContext ctx) {

    }

    @Override
    public void enterInlineTableDefault1(SqlBaseParser.InlineTableDefault1Context ctx) {

    }

    @Override
    public void exitInlineTableDefault1(SqlBaseParser.InlineTableDefault1Context ctx) {

    }

    @Override
    public void enterSubquery(SqlBaseParser.SubqueryContext ctx) {

    }

    @Override
    public void exitSubquery(SqlBaseParser.SubqueryContext ctx) {

    }

    @Override
    public void enterSortItem(SqlBaseParser.SortItemContext ctx) {

    }

    @Override
    public void exitSortItem(SqlBaseParser.SortItemContext ctx) {

    }

    @Override
    public void enterFromStatement(SqlBaseParser.FromStatementContext ctx) {

    }

    @Override
    public void exitFromStatement(SqlBaseParser.FromStatementContext ctx) {

    }

    @Override
    public void enterFromStatementBody(SqlBaseParser.FromStatementBodyContext ctx) {

    }

    @Override
    public void exitFromStatementBody(SqlBaseParser.FromStatementBodyContext ctx) {

    }

    @Override
    public void enterTransformQuerySpecification(SqlBaseParser.TransformQuerySpecificationContext ctx) {

    }

    @Override
    public void exitTransformQuerySpecification(SqlBaseParser.TransformQuerySpecificationContext ctx) {

    }

    @Override
    public void enterRegularQuerySpecification(SqlBaseParser.RegularQuerySpecificationContext ctx) {

    }

    @Override
    public void exitRegularQuerySpecification(SqlBaseParser.RegularQuerySpecificationContext ctx) {

    }

    @Override
    public void enterTransformClause(SqlBaseParser.TransformClauseContext ctx) {

    }

    @Override
    public void exitTransformClause(SqlBaseParser.TransformClauseContext ctx) {

    }

    @Override
    public void enterSelectClause(SqlBaseParser.SelectClauseContext ctx) {

    }

    @Override
    public void exitSelectClause(SqlBaseParser.SelectClauseContext ctx) {

    }

    @Override
    public void enterSetClause(SqlBaseParser.SetClauseContext ctx) {

    }

    @Override
    public void exitSetClause(SqlBaseParser.SetClauseContext ctx) {

    }

    @Override
    public void enterMatchedClause(SqlBaseParser.MatchedClauseContext ctx) {

    }

    @Override
    public void exitMatchedClause(SqlBaseParser.MatchedClauseContext ctx) {

    }

    @Override
    public void enterNotMatchedClause(SqlBaseParser.NotMatchedClauseContext ctx) {

    }

    @Override
    public void exitNotMatchedClause(SqlBaseParser.NotMatchedClauseContext ctx) {

    }

    @Override
    public void enterNotMatchedBySourceClause(SqlBaseParser.NotMatchedBySourceClauseContext ctx) {

    }

    @Override
    public void exitNotMatchedBySourceClause(SqlBaseParser.NotMatchedBySourceClauseContext ctx) {

    }

    @Override
    public void enterMatchedAction(SqlBaseParser.MatchedActionContext ctx) {

    }

    @Override
    public void exitMatchedAction(SqlBaseParser.MatchedActionContext ctx) {

    }

    @Override
    public void enterNotMatchedAction(SqlBaseParser.NotMatchedActionContext ctx) {

    }

    @Override
    public void exitNotMatchedAction(SqlBaseParser.NotMatchedActionContext ctx) {

    }

    @Override
    public void enterNotMatchedBySourceAction(SqlBaseParser.NotMatchedBySourceActionContext ctx) {

    }

    @Override
    public void exitNotMatchedBySourceAction(SqlBaseParser.NotMatchedBySourceActionContext ctx) {

    }

    @Override
    public void enterAssignmentList(SqlBaseParser.AssignmentListContext ctx) {

    }

    @Override
    public void exitAssignmentList(SqlBaseParser.AssignmentListContext ctx) {

    }

    @Override
    public void enterAssignment(SqlBaseParser.AssignmentContext ctx) {

    }

    @Override
    public void exitAssignment(SqlBaseParser.AssignmentContext ctx) {

    }

    @Override
    public void enterWhereClause(SqlBaseParser.WhereClauseContext ctx) {

    }

    @Override
    public void exitWhereClause(SqlBaseParser.WhereClauseContext ctx) {

    }

    @Override
    public void enterHavingClause(SqlBaseParser.HavingClauseContext ctx) {

    }

    @Override
    public void exitHavingClause(SqlBaseParser.HavingClauseContext ctx) {

    }

    @Override
    public void enterHint(SqlBaseParser.HintContext ctx) {

    }

    @Override
    public void exitHint(SqlBaseParser.HintContext ctx) {

    }

    @Override
    public void enterHintStatement(SqlBaseParser.HintStatementContext ctx) {

    }

    @Override
    public void exitHintStatement(SqlBaseParser.HintStatementContext ctx) {

    }

    @Override
    public void enterFromClause(SqlBaseParser.FromClauseContext ctx) {

    }

    @Override
    public void exitFromClause(SqlBaseParser.FromClauseContext ctx) {

    }

    @Override
    public void enterTemporalClause(SqlBaseParser.TemporalClauseContext ctx) {

    }

    @Override
    public void exitTemporalClause(SqlBaseParser.TemporalClauseContext ctx) {

    }

    @Override
    public void enterAggregationClause(SqlBaseParser.AggregationClauseContext ctx) {

    }

    @Override
    public void exitAggregationClause(SqlBaseParser.AggregationClauseContext ctx) {

    }

    @Override
    public void enterGroupByClause(SqlBaseParser.GroupByClauseContext ctx) {

    }

    @Override
    public void exitGroupByClause(SqlBaseParser.GroupByClauseContext ctx) {

    }

    @Override
    public void enterGroupingAnalytics(SqlBaseParser.GroupingAnalyticsContext ctx) {

    }

    @Override
    public void exitGroupingAnalytics(SqlBaseParser.GroupingAnalyticsContext ctx) {

    }

    @Override
    public void enterGroupingElement(SqlBaseParser.GroupingElementContext ctx) {

    }

    @Override
    public void exitGroupingElement(SqlBaseParser.GroupingElementContext ctx) {

    }

    @Override
    public void enterGroupingSet(SqlBaseParser.GroupingSetContext ctx) {

    }

    @Override
    public void exitGroupingSet(SqlBaseParser.GroupingSetContext ctx) {

    }

    @Override
    public void enterPivotClause(SqlBaseParser.PivotClauseContext ctx) {

    }

    @Override
    public void exitPivotClause(SqlBaseParser.PivotClauseContext ctx) {

    }

    @Override
    public void enterPivotColumn(SqlBaseParser.PivotColumnContext ctx) {

    }

    @Override
    public void exitPivotColumn(SqlBaseParser.PivotColumnContext ctx) {

    }

    @Override
    public void enterPivotValue(SqlBaseParser.PivotValueContext ctx) {

    }

    @Override
    public void exitPivotValue(SqlBaseParser.PivotValueContext ctx) {

    }

    @Override
    public void enterUnpivotClause(SqlBaseParser.UnpivotClauseContext ctx) {

    }

    @Override
    public void exitUnpivotClause(SqlBaseParser.UnpivotClauseContext ctx) {

    }

    @Override
    public void enterUnpivotNullClause(SqlBaseParser.UnpivotNullClauseContext ctx) {

    }

    @Override
    public void exitUnpivotNullClause(SqlBaseParser.UnpivotNullClauseContext ctx) {

    }

    @Override
    public void enterUnpivotOperator(SqlBaseParser.UnpivotOperatorContext ctx) {

    }

    @Override
    public void exitUnpivotOperator(SqlBaseParser.UnpivotOperatorContext ctx) {

    }

    @Override
    public void enterUnpivotSingleValueColumnClause(SqlBaseParser.UnpivotSingleValueColumnClauseContext ctx) {

    }

    @Override
    public void exitUnpivotSingleValueColumnClause(SqlBaseParser.UnpivotSingleValueColumnClauseContext ctx) {

    }

    @Override
    public void enterUnpivotMultiValueColumnClause(SqlBaseParser.UnpivotMultiValueColumnClauseContext ctx) {

    }

    @Override
    public void exitUnpivotMultiValueColumnClause(SqlBaseParser.UnpivotMultiValueColumnClauseContext ctx) {

    }

    @Override
    public void enterUnpivotColumnSet(SqlBaseParser.UnpivotColumnSetContext ctx) {

    }

    @Override
    public void exitUnpivotColumnSet(SqlBaseParser.UnpivotColumnSetContext ctx) {

    }

    @Override
    public void enterUnpivotValueColumn(SqlBaseParser.UnpivotValueColumnContext ctx) {

    }

    @Override
    public void exitUnpivotValueColumn(SqlBaseParser.UnpivotValueColumnContext ctx) {

    }

    @Override
    public void enterUnpivotNameColumn(SqlBaseParser.UnpivotNameColumnContext ctx) {

    }

    @Override
    public void exitUnpivotNameColumn(SqlBaseParser.UnpivotNameColumnContext ctx) {

    }

    @Override
    public void enterUnpivotColumnAndAlias(SqlBaseParser.UnpivotColumnAndAliasContext ctx) {

    }

    @Override
    public void exitUnpivotColumnAndAlias(SqlBaseParser.UnpivotColumnAndAliasContext ctx) {

    }

    @Override
    public void enterUnpivotColumn(SqlBaseParser.UnpivotColumnContext ctx) {

    }

    @Override
    public void exitUnpivotColumn(SqlBaseParser.UnpivotColumnContext ctx) {

    }

    @Override
    public void enterUnpivotAlias(SqlBaseParser.UnpivotAliasContext ctx) {

    }

    @Override
    public void exitUnpivotAlias(SqlBaseParser.UnpivotAliasContext ctx) {

    }

    @Override
    public void enterLateralView(SqlBaseParser.LateralViewContext ctx) {

    }

    @Override
    public void exitLateralView(SqlBaseParser.LateralViewContext ctx) {

    }

    @Override
    public void enterSetQuantifier(SqlBaseParser.SetQuantifierContext ctx) {

    }

    @Override
    public void exitSetQuantifier(SqlBaseParser.SetQuantifierContext ctx) {

    }

    @Override
    public void enterRelation(SqlBaseParser.RelationContext ctx) {

    }

    @Override
    public void exitRelation(SqlBaseParser.RelationContext ctx) {

    }

    @Override
    public void enterRelationExtension(SqlBaseParser.RelationExtensionContext ctx) {

    }

    @Override
    public void exitRelationExtension(SqlBaseParser.RelationExtensionContext ctx) {

    }

    @Override
    public void enterJoinRelation(SqlBaseParser.JoinRelationContext ctx) {

    }

    @Override
    public void exitJoinRelation(SqlBaseParser.JoinRelationContext ctx) {

    }

    @Override
    public void enterJoinType(SqlBaseParser.JoinTypeContext ctx) {

    }

    @Override
    public void exitJoinType(SqlBaseParser.JoinTypeContext ctx) {

    }

    @Override
    public void enterJoinCriteria(SqlBaseParser.JoinCriteriaContext ctx) {

    }

    @Override
    public void exitJoinCriteria(SqlBaseParser.JoinCriteriaContext ctx) {

    }

    @Override
    public void enterSample(SqlBaseParser.SampleContext ctx) {

    }

    @Override
    public void exitSample(SqlBaseParser.SampleContext ctx) {

    }

    @Override
    public void enterSampleByPercentile(SqlBaseParser.SampleByPercentileContext ctx) {

    }

    @Override
    public void exitSampleByPercentile(SqlBaseParser.SampleByPercentileContext ctx) {

    }

    @Override
    public void enterSampleByRows(SqlBaseParser.SampleByRowsContext ctx) {

    }

    @Override
    public void exitSampleByRows(SqlBaseParser.SampleByRowsContext ctx) {

    }

    @Override
    public void enterSampleByBucket(SqlBaseParser.SampleByBucketContext ctx) {

    }

    @Override
    public void exitSampleByBucket(SqlBaseParser.SampleByBucketContext ctx) {

    }

    @Override
    public void enterSampleByBytes(SqlBaseParser.SampleByBytesContext ctx) {

    }

    @Override
    public void exitSampleByBytes(SqlBaseParser.SampleByBytesContext ctx) {

    }

    @Override
    public void enterIdentifierList(SqlBaseParser.IdentifierListContext ctx) {

    }

    @Override
    public void exitIdentifierList(SqlBaseParser.IdentifierListContext ctx) {

    }

    @Override
    public void enterIdentifierSeq(SqlBaseParser.IdentifierSeqContext ctx) {

    }

    @Override
    public void exitIdentifierSeq(SqlBaseParser.IdentifierSeqContext ctx) {

    }

    @Override
    public void enterOrderedIdentifierList(SqlBaseParser.OrderedIdentifierListContext ctx) {

    }

    @Override
    public void exitOrderedIdentifierList(SqlBaseParser.OrderedIdentifierListContext ctx) {

    }

    @Override
    public void enterOrderedIdentifier(SqlBaseParser.OrderedIdentifierContext ctx) {

    }

    @Override
    public void exitOrderedIdentifier(SqlBaseParser.OrderedIdentifierContext ctx) {

    }

    @Override
    public void enterIdentifierCommentList(SqlBaseParser.IdentifierCommentListContext ctx) {

    }

    @Override
    public void exitIdentifierCommentList(SqlBaseParser.IdentifierCommentListContext ctx) {

    }

    @Override
    public void enterIdentifierComment(SqlBaseParser.IdentifierCommentContext ctx) {

    }

    @Override
    public void exitIdentifierComment(SqlBaseParser.IdentifierCommentContext ctx) {

    }

    @Override
    public void enterTableName(SqlBaseParser.TableNameContext ctx) {

    }

    @Override
    public void exitTableName(SqlBaseParser.TableNameContext ctx) {

    }

    @Override
    public void enterAliasedQuery(SqlBaseParser.AliasedQueryContext ctx) {

    }

    @Override
    public void exitAliasedQuery(SqlBaseParser.AliasedQueryContext ctx) {

    }

    @Override
    public void enterAliasedRelation(SqlBaseParser.AliasedRelationContext ctx) {

    }

    @Override
    public void exitAliasedRelation(SqlBaseParser.AliasedRelationContext ctx) {

    }

    @Override
    public void enterInlineTableDefault2(SqlBaseParser.InlineTableDefault2Context ctx) {

    }

    @Override
    public void exitInlineTableDefault2(SqlBaseParser.InlineTableDefault2Context ctx) {

    }

    @Override
    public void enterTableValuedFunction(SqlBaseParser.TableValuedFunctionContext ctx) {

    }

    @Override
    public void exitTableValuedFunction(SqlBaseParser.TableValuedFunctionContext ctx) {

    }

    @Override
    public void enterInlineTable(SqlBaseParser.InlineTableContext ctx) {

    }

    @Override
    public void exitInlineTable(SqlBaseParser.InlineTableContext ctx) {

    }

    @Override
    public void enterFunctionTableSubqueryArgument(SqlBaseParser.FunctionTableSubqueryArgumentContext ctx) {

    }

    @Override
    public void exitFunctionTableSubqueryArgument(SqlBaseParser.FunctionTableSubqueryArgumentContext ctx) {

    }

    @Override
    public void enterFunctionTableNamedArgumentExpression(SqlBaseParser.FunctionTableNamedArgumentExpressionContext ctx) {

    }

    @Override
    public void exitFunctionTableNamedArgumentExpression(SqlBaseParser.FunctionTableNamedArgumentExpressionContext ctx) {

    }

    @Override
    public void enterFunctionTableReferenceArgument(SqlBaseParser.FunctionTableReferenceArgumentContext ctx) {

    }

    @Override
    public void exitFunctionTableReferenceArgument(SqlBaseParser.FunctionTableReferenceArgumentContext ctx) {

    }

    @Override
    public void enterFunctionTableArgument(SqlBaseParser.FunctionTableArgumentContext ctx) {

    }

    @Override
    public void exitFunctionTableArgument(SqlBaseParser.FunctionTableArgumentContext ctx) {

    }

    @Override
    public void enterFunctionTable(SqlBaseParser.FunctionTableContext ctx) {

    }

    @Override
    public void exitFunctionTable(SqlBaseParser.FunctionTableContext ctx) {

    }

    @Override
    public void enterTableAlias(SqlBaseParser.TableAliasContext ctx) {

    }

    @Override
    public void exitTableAlias(SqlBaseParser.TableAliasContext ctx) {

    }

    @Override
    public void enterRowFormatSerde(SqlBaseParser.RowFormatSerdeContext ctx) {

    }

    @Override
    public void exitRowFormatSerde(SqlBaseParser.RowFormatSerdeContext ctx) {

    }

    @Override
    public void enterRowFormatDelimited(SqlBaseParser.RowFormatDelimitedContext ctx) {

    }

    @Override
    public void exitRowFormatDelimited(SqlBaseParser.RowFormatDelimitedContext ctx) {

    }

    @Override
    public void enterMultipartIdentifierList(SqlBaseParser.MultipartIdentifierListContext ctx) {

    }

    @Override
    public void exitMultipartIdentifierList(SqlBaseParser.MultipartIdentifierListContext ctx) {

    }

    @Override
    public void enterMultipartIdentifier(SqlBaseParser.MultipartIdentifierContext ctx) {

    }

    @Override
    public void exitMultipartIdentifier(SqlBaseParser.MultipartIdentifierContext ctx) {

    }

    @Override
    public void enterMultipartIdentifierPropertyList(SqlBaseParser.MultipartIdentifierPropertyListContext ctx) {

    }

    @Override
    public void exitMultipartIdentifierPropertyList(SqlBaseParser.MultipartIdentifierPropertyListContext ctx) {

    }

    @Override
    public void enterMultipartIdentifierProperty(SqlBaseParser.MultipartIdentifierPropertyContext ctx) {

    }

    @Override
    public void exitMultipartIdentifierProperty(SqlBaseParser.MultipartIdentifierPropertyContext ctx) {

    }

    @Override
    public void enterTableIdentifier(SqlBaseParser.TableIdentifierContext ctx) {

    }

    @Override
    public void exitTableIdentifier(SqlBaseParser.TableIdentifierContext ctx) {

    }

    @Override
    public void enterFunctionIdentifier(SqlBaseParser.FunctionIdentifierContext ctx) {

    }

    @Override
    public void exitFunctionIdentifier(SqlBaseParser.FunctionIdentifierContext ctx) {

    }

    @Override
    public void enterNamedExpression(SqlBaseParser.NamedExpressionContext ctx) {

    }

    @Override
    public void exitNamedExpression(SqlBaseParser.NamedExpressionContext ctx) {

    }

    @Override
    public void enterNamedExpressionSeq(SqlBaseParser.NamedExpressionSeqContext ctx) {

    }

    @Override
    public void exitNamedExpressionSeq(SqlBaseParser.NamedExpressionSeqContext ctx) {

    }

    @Override
    public void enterPartitionFieldList(SqlBaseParser.PartitionFieldListContext ctx) {

    }

    @Override
    public void exitPartitionFieldList(SqlBaseParser.PartitionFieldListContext ctx) {

    }

    @Override
    public void enterPartitionTransform(SqlBaseParser.PartitionTransformContext ctx) {

    }

    @Override
    public void exitPartitionTransform(SqlBaseParser.PartitionTransformContext ctx) {

    }

    @Override
    public void enterPartitionColumn(SqlBaseParser.PartitionColumnContext ctx) {

    }

    @Override
    public void exitPartitionColumn(SqlBaseParser.PartitionColumnContext ctx) {

    }

    @Override
    public void enterIdentityTransform(SqlBaseParser.IdentityTransformContext ctx) {

    }

    @Override
    public void exitIdentityTransform(SqlBaseParser.IdentityTransformContext ctx) {

    }

    @Override
    public void enterApplyTransform(SqlBaseParser.ApplyTransformContext ctx) {

    }

    @Override
    public void exitApplyTransform(SqlBaseParser.ApplyTransformContext ctx) {

    }

    @Override
    public void enterTransformArgument(SqlBaseParser.TransformArgumentContext ctx) {

    }

    @Override
    public void exitTransformArgument(SqlBaseParser.TransformArgumentContext ctx) {

    }

    @Override
    public void enterExpression(SqlBaseParser.ExpressionContext ctx) {

    }

    @Override
    public void exitExpression(SqlBaseParser.ExpressionContext ctx) {

    }

    @Override
    public void enterNamedArgumentExpression(SqlBaseParser.NamedArgumentExpressionContext ctx) {

    }

    @Override
    public void exitNamedArgumentExpression(SqlBaseParser.NamedArgumentExpressionContext ctx) {

    }

    @Override
    public void enterFunctionArgument(SqlBaseParser.FunctionArgumentContext ctx) {

    }

    @Override
    public void exitFunctionArgument(SqlBaseParser.FunctionArgumentContext ctx) {

    }

    @Override
    public void enterExpressionSeq(SqlBaseParser.ExpressionSeqContext ctx) {

    }

    @Override
    public void exitExpressionSeq(SqlBaseParser.ExpressionSeqContext ctx) {

    }

    @Override
    public void enterLogicalNot(SqlBaseParser.LogicalNotContext ctx) {

    }

    @Override
    public void exitLogicalNot(SqlBaseParser.LogicalNotContext ctx) {

    }

    @Override
    public void enterPredicated(SqlBaseParser.PredicatedContext ctx) {

    }

    @Override
    public void exitPredicated(SqlBaseParser.PredicatedContext ctx) {

    }

    @Override
    public void enterExists(SqlBaseParser.ExistsContext ctx) {

    }

    @Override
    public void exitExists(SqlBaseParser.ExistsContext ctx) {

    }

    @Override
    public void enterLogicalBinary(SqlBaseParser.LogicalBinaryContext ctx) {

    }

    @Override
    public void exitLogicalBinary(SqlBaseParser.LogicalBinaryContext ctx) {

    }

    @Override
    public void enterPredicate(SqlBaseParser.PredicateContext ctx) {

    }

    @Override
    public void exitPredicate(SqlBaseParser.PredicateContext ctx) {

    }

    @Override
    public void enterValueExpressionDefault(SqlBaseParser.ValueExpressionDefaultContext ctx) {

    }

    @Override
    public void exitValueExpressionDefault(SqlBaseParser.ValueExpressionDefaultContext ctx) {

    }

    @Override
    public void enterComparison(SqlBaseParser.ComparisonContext ctx) {

    }

    @Override
    public void exitComparison(SqlBaseParser.ComparisonContext ctx) {

    }

    @Override
    public void enterArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext ctx) {

    }

    @Override
    public void exitArithmeticBinary(SqlBaseParser.ArithmeticBinaryContext ctx) {

    }

    @Override
    public void enterArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext ctx) {

    }

    @Override
    public void exitArithmeticUnary(SqlBaseParser.ArithmeticUnaryContext ctx) {

    }

    @Override
    public void enterDatetimeUnit(SqlBaseParser.DatetimeUnitContext ctx) {

    }

    @Override
    public void exitDatetimeUnit(SqlBaseParser.DatetimeUnitContext ctx) {

    }

    @Override
    public void enterStruct(SqlBaseParser.StructContext ctx) {

    }

    @Override
    public void exitStruct(SqlBaseParser.StructContext ctx) {

    }

    @Override
    public void enterDereference(SqlBaseParser.DereferenceContext ctx) {

    }

    @Override
    public void exitDereference(SqlBaseParser.DereferenceContext ctx) {

    }

    @Override
    public void enterTimestampadd(SqlBaseParser.TimestampaddContext ctx) {

    }

    @Override
    public void exitTimestampadd(SqlBaseParser.TimestampaddContext ctx) {

    }

    @Override
    public void enterSubstring(SqlBaseParser.SubstringContext ctx) {

    }

    @Override
    public void exitSubstring(SqlBaseParser.SubstringContext ctx) {

    }

    @Override
    public void enterCast(SqlBaseParser.CastContext ctx) {

    }

    @Override
    public void exitCast(SqlBaseParser.CastContext ctx) {

    }

    @Override
    public void enterLambda(SqlBaseParser.LambdaContext ctx) {

    }

    @Override
    public void exitLambda(SqlBaseParser.LambdaContext ctx) {

    }

    @Override
    public void enterParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext ctx) {

    }

    @Override
    public void exitParenthesizedExpression(SqlBaseParser.ParenthesizedExpressionContext ctx) {

    }

    @Override
    public void enterAny_value(SqlBaseParser.Any_valueContext ctx) {

    }

    @Override
    public void exitAny_value(SqlBaseParser.Any_valueContext ctx) {

    }

    @Override
    public void enterTrim(SqlBaseParser.TrimContext ctx) {

    }

    @Override
    public void exitTrim(SqlBaseParser.TrimContext ctx) {

    }

    @Override
    public void enterSimpleCase(SqlBaseParser.SimpleCaseContext ctx) {

    }

    @Override
    public void exitSimpleCase(SqlBaseParser.SimpleCaseContext ctx) {

    }

    @Override
    public void enterCurrentLike(SqlBaseParser.CurrentLikeContext ctx) {

    }

    @Override
    public void exitCurrentLike(SqlBaseParser.CurrentLikeContext ctx) {

    }

    @Override
    public void enterColumnReference(SqlBaseParser.ColumnReferenceContext ctx) {

    }

    @Override
    public void exitColumnReference(SqlBaseParser.ColumnReferenceContext ctx) {

    }

    @Override
    public void enterRowConstructor(SqlBaseParser.RowConstructorContext ctx) {

    }

    @Override
    public void exitRowConstructor(SqlBaseParser.RowConstructorContext ctx) {

    }

    @Override
    public void enterLast(SqlBaseParser.LastContext ctx) {

    }

    @Override
    public void exitLast(SqlBaseParser.LastContext ctx) {

    }

    @Override
    public void enterStar(SqlBaseParser.StarContext ctx) {

    }

    @Override
    public void exitStar(SqlBaseParser.StarContext ctx) {

    }

    @Override
    public void enterOverlay(SqlBaseParser.OverlayContext ctx) {

    }

    @Override
    public void exitOverlay(SqlBaseParser.OverlayContext ctx) {

    }

    @Override
    public void enterSubscript(SqlBaseParser.SubscriptContext ctx) {

    }

    @Override
    public void exitSubscript(SqlBaseParser.SubscriptContext ctx) {

    }

    @Override
    public void enterTimestampdiff(SqlBaseParser.TimestampdiffContext ctx) {

    }

    @Override
    public void exitTimestampdiff(SqlBaseParser.TimestampdiffContext ctx) {

    }

    @Override
    public void enterSubqueryExpression(SqlBaseParser.SubqueryExpressionContext ctx) {

    }

    @Override
    public void exitSubqueryExpression(SqlBaseParser.SubqueryExpressionContext ctx) {

    }

    @Override
    public void enterIdentifierClause(SqlBaseParser.IdentifierClauseContext ctx) {

    }

    @Override
    public void exitIdentifierClause(SqlBaseParser.IdentifierClauseContext ctx) {

    }

    @Override
    public void enterConstantDefault(SqlBaseParser.ConstantDefaultContext ctx) {

    }

    @Override
    public void exitConstantDefault(SqlBaseParser.ConstantDefaultContext ctx) {

    }

    @Override
    public void enterExtract(SqlBaseParser.ExtractContext ctx) {

    }

    @Override
    public void exitExtract(SqlBaseParser.ExtractContext ctx) {

    }

    @Override
    public void enterPercentile(SqlBaseParser.PercentileContext ctx) {

    }

    @Override
    public void exitPercentile(SqlBaseParser.PercentileContext ctx) {

    }

    @Override
    public void enterFunctionCall(SqlBaseParser.FunctionCallContext ctx) {

    }

    @Override
    public void exitFunctionCall(SqlBaseParser.FunctionCallContext ctx) {

    }

    @Override
    public void enterSearchedCase(SqlBaseParser.SearchedCaseContext ctx) {

    }

    @Override
    public void exitSearchedCase(SqlBaseParser.SearchedCaseContext ctx) {

    }

    @Override
    public void enterPosition(SqlBaseParser.PositionContext ctx) {

    }

    @Override
    public void exitPosition(SqlBaseParser.PositionContext ctx) {

    }

    @Override
    public void enterFirst(SqlBaseParser.FirstContext ctx) {

    }

    @Override
    public void exitFirst(SqlBaseParser.FirstContext ctx) {

    }

    @Override
    public void enterLiteralType(SqlBaseParser.LiteralTypeContext ctx) {

    }

    @Override
    public void exitLiteralType(SqlBaseParser.LiteralTypeContext ctx) {

    }

    @Override
    public void enterNullLiteral(SqlBaseParser.NullLiteralContext ctx) {

    }

    @Override
    public void exitNullLiteral(SqlBaseParser.NullLiteralContext ctx) {

    }

    @Override
    public void enterPosParameterLiteral(SqlBaseParser.PosParameterLiteralContext ctx) {

    }

    @Override
    public void exitPosParameterLiteral(SqlBaseParser.PosParameterLiteralContext ctx) {

    }

    @Override
    public void enterNamedParameterLiteral(SqlBaseParser.NamedParameterLiteralContext ctx) {

    }

    @Override
    public void exitNamedParameterLiteral(SqlBaseParser.NamedParameterLiteralContext ctx) {

    }

    @Override
    public void enterIntervalLiteral(SqlBaseParser.IntervalLiteralContext ctx) {

    }

    @Override
    public void exitIntervalLiteral(SqlBaseParser.IntervalLiteralContext ctx) {

    }

    @Override
    public void enterTypeConstructor(SqlBaseParser.TypeConstructorContext ctx) {

    }

    @Override
    public void exitTypeConstructor(SqlBaseParser.TypeConstructorContext ctx) {

    }

    @Override
    public void enterNumericLiteral(SqlBaseParser.NumericLiteralContext ctx) {

    }

    @Override
    public void exitNumericLiteral(SqlBaseParser.NumericLiteralContext ctx) {

    }

    @Override
    public void enterBooleanLiteral(SqlBaseParser.BooleanLiteralContext ctx) {

    }

    @Override
    public void exitBooleanLiteral(SqlBaseParser.BooleanLiteralContext ctx) {

    }

    @Override
    public void enterStringLiteral(SqlBaseParser.StringLiteralContext ctx) {

    }

    @Override
    public void exitStringLiteral(SqlBaseParser.StringLiteralContext ctx) {

    }

    @Override
    public void enterComparisonOperator(SqlBaseParser.ComparisonOperatorContext ctx) {

    }

    @Override
    public void exitComparisonOperator(SqlBaseParser.ComparisonOperatorContext ctx) {

    }

    @Override
    public void enterArithmeticOperator(SqlBaseParser.ArithmeticOperatorContext ctx) {

    }

    @Override
    public void exitArithmeticOperator(SqlBaseParser.ArithmeticOperatorContext ctx) {

    }

    @Override
    public void enterPredicateOperator(SqlBaseParser.PredicateOperatorContext ctx) {

    }

    @Override
    public void exitPredicateOperator(SqlBaseParser.PredicateOperatorContext ctx) {

    }

    @Override
    public void enterBooleanValue(SqlBaseParser.BooleanValueContext ctx) {

    }

    @Override
    public void exitBooleanValue(SqlBaseParser.BooleanValueContext ctx) {

    }

    @Override
    public void enterInterval(SqlBaseParser.IntervalContext ctx) {

    }

    @Override
    public void exitInterval(SqlBaseParser.IntervalContext ctx) {

    }

    @Override
    public void enterErrorCapturingMultiUnitsInterval(SqlBaseParser.ErrorCapturingMultiUnitsIntervalContext ctx) {

    }

    @Override
    public void exitErrorCapturingMultiUnitsInterval(SqlBaseParser.ErrorCapturingMultiUnitsIntervalContext ctx) {

    }

    @Override
    public void enterMultiUnitsInterval(SqlBaseParser.MultiUnitsIntervalContext ctx) {

    }

    @Override
    public void exitMultiUnitsInterval(SqlBaseParser.MultiUnitsIntervalContext ctx) {

    }

    @Override
    public void enterErrorCapturingUnitToUnitInterval(SqlBaseParser.ErrorCapturingUnitToUnitIntervalContext ctx) {

    }

    @Override
    public void exitErrorCapturingUnitToUnitInterval(SqlBaseParser.ErrorCapturingUnitToUnitIntervalContext ctx) {

    }

    @Override
    public void enterUnitToUnitInterval(SqlBaseParser.UnitToUnitIntervalContext ctx) {

    }

    @Override
    public void exitUnitToUnitInterval(SqlBaseParser.UnitToUnitIntervalContext ctx) {

    }

    @Override
    public void enterIntervalValue(SqlBaseParser.IntervalValueContext ctx) {

    }

    @Override
    public void exitIntervalValue(SqlBaseParser.IntervalValueContext ctx) {

    }

    @Override
    public void enterUnitInMultiUnits(SqlBaseParser.UnitInMultiUnitsContext ctx) {

    }

    @Override
    public void exitUnitInMultiUnits(SqlBaseParser.UnitInMultiUnitsContext ctx) {

    }

    @Override
    public void enterUnitInUnitToUnit(SqlBaseParser.UnitInUnitToUnitContext ctx) {

    }

    @Override
    public void exitUnitInUnitToUnit(SqlBaseParser.UnitInUnitToUnitContext ctx) {

    }

    @Override
    public void enterColPosition(SqlBaseParser.ColPositionContext ctx) {

    }

    @Override
    public void exitColPosition(SqlBaseParser.ColPositionContext ctx) {

    }

    @Override
    public void enterType(SqlBaseParser.TypeContext ctx) {

    }

    @Override
    public void exitType(SqlBaseParser.TypeContext ctx) {

    }

    @Override
    public void enterComplexDataType(SqlBaseParser.ComplexDataTypeContext ctx) {

    }

    @Override
    public void exitComplexDataType(SqlBaseParser.ComplexDataTypeContext ctx) {

    }

    @Override
    public void enterYearMonthIntervalDataType(SqlBaseParser.YearMonthIntervalDataTypeContext ctx) {

    }

    @Override
    public void exitYearMonthIntervalDataType(SqlBaseParser.YearMonthIntervalDataTypeContext ctx) {

    }

    @Override
    public void enterDayTimeIntervalDataType(SqlBaseParser.DayTimeIntervalDataTypeContext ctx) {

    }

    @Override
    public void exitDayTimeIntervalDataType(SqlBaseParser.DayTimeIntervalDataTypeContext ctx) {

    }

    @Override
    public void enterPrimitiveDataType(SqlBaseParser.PrimitiveDataTypeContext ctx) {

    }

    @Override
    public void exitPrimitiveDataType(SqlBaseParser.PrimitiveDataTypeContext ctx) {

    }

    @Override
    public void enterQualifiedColTypeWithPositionList(SqlBaseParser.QualifiedColTypeWithPositionListContext ctx) {

    }

    @Override
    public void exitQualifiedColTypeWithPositionList(SqlBaseParser.QualifiedColTypeWithPositionListContext ctx) {

    }

    @Override
    public void enterQualifiedColTypeWithPosition(SqlBaseParser.QualifiedColTypeWithPositionContext ctx) {

    }

    @Override
    public void exitQualifiedColTypeWithPosition(SqlBaseParser.QualifiedColTypeWithPositionContext ctx) {

    }

    @Override
    public void enterColDefinitionDescriptorWithPosition(SqlBaseParser.ColDefinitionDescriptorWithPositionContext ctx) {

    }

    @Override
    public void exitColDefinitionDescriptorWithPosition(SqlBaseParser.ColDefinitionDescriptorWithPositionContext ctx) {

    }

    @Override
    public void enterDefaultExpression(SqlBaseParser.DefaultExpressionContext ctx) {

    }

    @Override
    public void exitDefaultExpression(SqlBaseParser.DefaultExpressionContext ctx) {

    }

    @Override
    public void enterColTypeList(SqlBaseParser.ColTypeListContext ctx) {

    }

    @Override
    public void exitColTypeList(SqlBaseParser.ColTypeListContext ctx) {

    }

    @Override
    public void enterColType(SqlBaseParser.ColTypeContext ctx) {

    }

    @Override
    public void exitColType(SqlBaseParser.ColTypeContext ctx) {

    }

    @Override
    public void enterCreateOrReplaceTableColTypeList(SqlBaseParser.CreateOrReplaceTableColTypeListContext ctx) {

    }

    @Override
    public void exitCreateOrReplaceTableColTypeList(SqlBaseParser.CreateOrReplaceTableColTypeListContext ctx) {

    }

    @Override
    public void enterCreateOrReplaceTableColType(SqlBaseParser.CreateOrReplaceTableColTypeContext ctx) {

    }

    @Override
    public void exitCreateOrReplaceTableColType(SqlBaseParser.CreateOrReplaceTableColTypeContext ctx) {

    }

    @Override
    public void enterColDefinitionOption(SqlBaseParser.ColDefinitionOptionContext ctx) {

    }

    @Override
    public void exitColDefinitionOption(SqlBaseParser.ColDefinitionOptionContext ctx) {

    }

    @Override
    public void enterGenerationExpression(SqlBaseParser.GenerationExpressionContext ctx) {

    }

    @Override
    public void exitGenerationExpression(SqlBaseParser.GenerationExpressionContext ctx) {

    }

    @Override
    public void enterComplexColTypeList(SqlBaseParser.ComplexColTypeListContext ctx) {

    }

    @Override
    public void exitComplexColTypeList(SqlBaseParser.ComplexColTypeListContext ctx) {

    }

    @Override
    public void enterComplexColType(SqlBaseParser.ComplexColTypeContext ctx) {

    }

    @Override
    public void exitComplexColType(SqlBaseParser.ComplexColTypeContext ctx) {

    }

    @Override
    public void enterWhenClause(SqlBaseParser.WhenClauseContext ctx) {

    }

    @Override
    public void exitWhenClause(SqlBaseParser.WhenClauseContext ctx) {

    }

    @Override
    public void enterWindowClause(SqlBaseParser.WindowClauseContext ctx) {

    }

    @Override
    public void exitWindowClause(SqlBaseParser.WindowClauseContext ctx) {

    }

    @Override
    public void enterNamedWindow(SqlBaseParser.NamedWindowContext ctx) {

    }

    @Override
    public void exitNamedWindow(SqlBaseParser.NamedWindowContext ctx) {

    }

    @Override
    public void enterWindowRef(SqlBaseParser.WindowRefContext ctx) {

    }

    @Override
    public void exitWindowRef(SqlBaseParser.WindowRefContext ctx) {

    }

    @Override
    public void enterWindowDef(SqlBaseParser.WindowDefContext ctx) {

    }

    @Override
    public void exitWindowDef(SqlBaseParser.WindowDefContext ctx) {

    }

    @Override
    public void enterWindowFrame(SqlBaseParser.WindowFrameContext ctx) {

    }

    @Override
    public void exitWindowFrame(SqlBaseParser.WindowFrameContext ctx) {

    }

    @Override
    public void enterFrameBound(SqlBaseParser.FrameBoundContext ctx) {

    }

    @Override
    public void exitFrameBound(SqlBaseParser.FrameBoundContext ctx) {

    }

    @Override
    public void enterQualifiedNameList(SqlBaseParser.QualifiedNameListContext ctx) {

    }

    @Override
    public void exitQualifiedNameList(SqlBaseParser.QualifiedNameListContext ctx) {

    }

    @Override
    public void enterFunctionName(SqlBaseParser.FunctionNameContext ctx) {

    }

    @Override
    public void exitFunctionName(SqlBaseParser.FunctionNameContext ctx) {

    }

    @Override
    public void enterQualifiedName(SqlBaseParser.QualifiedNameContext ctx) {

    }

    @Override
    public void exitQualifiedName(SqlBaseParser.QualifiedNameContext ctx) {

    }

    @Override
    public void enterErrorCapturingIdentifier(SqlBaseParser.ErrorCapturingIdentifierContext ctx) {

    }

    @Override
    public void exitErrorCapturingIdentifier(SqlBaseParser.ErrorCapturingIdentifierContext ctx) {

    }

    @Override
    public void enterErrorIdent(SqlBaseParser.ErrorIdentContext ctx) {

    }

    @Override
    public void exitErrorIdent(SqlBaseParser.ErrorIdentContext ctx) {

    }

    @Override
    public void enterRealIdent(SqlBaseParser.RealIdentContext ctx) {

    }

    @Override
    public void exitRealIdent(SqlBaseParser.RealIdentContext ctx) {

    }

    @Override
    public void enterIdentifier(SqlBaseParser.IdentifierContext ctx) {

    }

    @Override
    public void exitIdentifier(SqlBaseParser.IdentifierContext ctx) {

    }

    @Override
    public void enterUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext ctx) {

    }

    @Override
    public void exitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext ctx) {

    }

    @Override
    public void enterQuotedIdentifierAlternative(SqlBaseParser.QuotedIdentifierAlternativeContext ctx) {

    }

    @Override
    public void exitQuotedIdentifierAlternative(SqlBaseParser.QuotedIdentifierAlternativeContext ctx) {

    }

    @Override
    public void enterQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext ctx) {

    }

    @Override
    public void exitQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext ctx) {

    }

    @Override
    public void enterBackQuotedIdentifier(SqlBaseParser.BackQuotedIdentifierContext ctx) {

    }

    @Override
    public void exitBackQuotedIdentifier(SqlBaseParser.BackQuotedIdentifierContext ctx) {

    }

    @Override
    public void enterExponentLiteral(SqlBaseParser.ExponentLiteralContext ctx) {

    }

    @Override
    public void exitExponentLiteral(SqlBaseParser.ExponentLiteralContext ctx) {

    }

    @Override
    public void enterDecimalLiteral(SqlBaseParser.DecimalLiteralContext ctx) {

    }

    @Override
    public void exitDecimalLiteral(SqlBaseParser.DecimalLiteralContext ctx) {

    }

    @Override
    public void enterLegacyDecimalLiteral(SqlBaseParser.LegacyDecimalLiteralContext ctx) {

    }

    @Override
    public void exitLegacyDecimalLiteral(SqlBaseParser.LegacyDecimalLiteralContext ctx) {

    }

    @Override
    public void enterIntegerLiteral(SqlBaseParser.IntegerLiteralContext ctx) {

    }

    @Override
    public void exitIntegerLiteral(SqlBaseParser.IntegerLiteralContext ctx) {

    }

    @Override
    public void enterBigIntLiteral(SqlBaseParser.BigIntLiteralContext ctx) {

    }

    @Override
    public void exitBigIntLiteral(SqlBaseParser.BigIntLiteralContext ctx) {

    }

    @Override
    public void enterSmallIntLiteral(SqlBaseParser.SmallIntLiteralContext ctx) {

    }

    @Override
    public void exitSmallIntLiteral(SqlBaseParser.SmallIntLiteralContext ctx) {

    }

    @Override
    public void enterTinyIntLiteral(SqlBaseParser.TinyIntLiteralContext ctx) {

    }

    @Override
    public void exitTinyIntLiteral(SqlBaseParser.TinyIntLiteralContext ctx) {

    }

    @Override
    public void enterDoubleLiteral(SqlBaseParser.DoubleLiteralContext ctx) {

    }

    @Override
    public void exitDoubleLiteral(SqlBaseParser.DoubleLiteralContext ctx) {

    }

    @Override
    public void enterFloatLiteral(SqlBaseParser.FloatLiteralContext ctx) {

    }

    @Override
    public void exitFloatLiteral(SqlBaseParser.FloatLiteralContext ctx) {

    }

    @Override
    public void enterBigDecimalLiteral(SqlBaseParser.BigDecimalLiteralContext ctx) {

    }

    @Override
    public void exitBigDecimalLiteral(SqlBaseParser.BigDecimalLiteralContext ctx) {

    }

    @Override
    public void enterAlterColumnAction(SqlBaseParser.AlterColumnActionContext ctx) {

    }

    @Override
    public void exitAlterColumnAction(SqlBaseParser.AlterColumnActionContext ctx) {

    }

    @Override
    public void enterStringLit(SqlBaseParser.StringLitContext ctx) {

    }

    @Override
    public void exitStringLit(SqlBaseParser.StringLitContext ctx) {

    }

    @Override
    public void enterComment(SqlBaseParser.CommentContext ctx) {

    }

    @Override
    public void exitComment(SqlBaseParser.CommentContext ctx) {

    }

    @Override
    public void enterVersion(SqlBaseParser.VersionContext ctx) {

    }

    @Override
    public void exitVersion(SqlBaseParser.VersionContext ctx) {

    }

    @Override
    public void enterAnsiNonReserved(SqlBaseParser.AnsiNonReservedContext ctx) {

    }

    @Override
    public void exitAnsiNonReserved(SqlBaseParser.AnsiNonReservedContext ctx) {

    }

    @Override
    public void enterStrictNonReserved(SqlBaseParser.StrictNonReservedContext ctx) {

    }

    @Override
    public void exitStrictNonReserved(SqlBaseParser.StrictNonReservedContext ctx) {

    }

    @Override
    public void enterNonReserved(SqlBaseParser.NonReservedContext ctx) {

    }

    @Override
    public void exitNonReserved(SqlBaseParser.NonReservedContext ctx) {

    }

    @Override
    public void visitTerminal(TerminalNode terminalNode) {

    }

    @Override
    public void visitErrorNode(ErrorNode errorNode) {

    }

    @Override
    public void enterEveryRule(ParserRuleContext parserRuleContext) {

    }

    @Override
    public void exitEveryRule(ParserRuleContext parserRuleContext) {

    }
}
