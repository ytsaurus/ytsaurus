#include "table_functions.h"

#include "query_context.h"
#include "storages_yt_nodes.h"
#include "function_helpers.h"

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/queue_client/config.h>

#include <yt/yt/ytlib/api/native/client.h>

#include <DataTypes/IDataType.h>

#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>

#include <Storages/IStorage.h>

#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>

namespace DB::ErrorCodes {

////////////////////////////////////////////////////////////////////////////////

extern const int BAD_ARGUMENTS;

////////////////////////////////////////////////////////////////////////////////

} // namespace DB::ErrorCodes

namespace NYT::NClickHouseServer {

////////////////////////////////////////////////////////////////////////////////

class TTableFunctionYtListsQueueExports
    : public DB::ITableFunction
{
public:
    static constexpr auto name = "ytListQueueExports";

    TTableFunctionYtListsQueueExports()
    { }

    std::string getName() const override
    {
        return name;
    }

    void parseArguments(const DB::ASTPtr& functionAst, DB::ContextPtr context) override
    {
        const auto& function = functionAst->as<DB::ASTFunction&>();
        if (!function.arguments) {
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "Table function {} must have arguments",
                getName());
        }

        DB::ASTs& args = function.arguments->children;
        if (args.size() < 1 || args.size() > 3) {
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "Table function {} must have 1, 2 or 3 arguments",
                getName());
        }

        ExportDirectory_ = EvaluateStringExpression(args[0], context);

        auto* queryContext = GetQueryContext(context);

        auto destinationYsonOrError = NConcurrency::WaitFor(
            queryContext->Client()->GetNode(Format("%v/@queue_static_export_destination", ExportDirectory_)));
        if (!destinationYsonOrError.IsOK()) {
            THROW_ERROR_EXCEPTION("Path %Qv does not correspond to a queue static export directory", ExportDirectory_)
                .With(destinationYsonOrError);
        }

        auto destinationConfig = NYTree::ConvertTo<NQueueClient::TQueueStaticExportDestinationConfig>(
            destinationYsonOrError.Value());

        QueueId_ = destinationConfig.OriginatingQueueId;

        auto exportConfigsYsonOrError = NConcurrency::WaitFor(
            queryContext->Client()->GetNode(Format("%s/@static_export_config", NObjectClient::FromObjectId(QueueId_))));
        if (!exportConfigsYsonOrError.IsOK()) {
            THROW_ERROR_EXCEPTION("Cannot get static export configs for queue %v (originating queue of %Qv)",
                QueueId_,
                ExportDirectory_)
                .With(exportConfigsYsonOrError);
        }

        auto exportConfigs = NYTree::ConvertTo<THashMap<std::string, NQueueClient::TQueueStaticExportConfigPtr>>(
            exportConfigsYsonOrError.Value());

        NQueueClient::TQueueStaticExportConfigPtr matchedExportConfig;
        for (const auto& [exportName, exportConfig] : exportConfigs) {
            if (exportConfig->ExportDirectory == ExportDirectory_) {
                matchedExportConfig = exportConfig;
                break;
            }
        }
        if (!matchedExportConfig) {
            THROW_ERROR_EXCEPTION("No static export configured with export directory %Qv for queue %v",
                ExportDirectory_,
                QueueId_);
        }

        if (!matchedExportConfig->ExportPeriod) {
            THROW_ERROR_EXCEPTION(
                "Static export %Qv uses a CRON schedule, which is not supported by %Qv",
                ExportDirectory_,
                getName());
        }

        Options_.UseUpperBoundForTableNames = matchedExportConfig->UseUpperBoundForTableNames;
        Options_.Period = *matchedExportConfig->ExportPeriod;

        auto pattern = matchedExportConfig->OutputTableNamePattern;

        if (pattern.contains("%PERIOD")) {
            SubstGlobal(pattern, "%PERIOD", ToString(Options_.Period.Seconds()));
        }

        auto placeholderPos = pattern.find("%UNIX_TS");
        if (placeholderPos == std::string::npos) {
            THROW_ERROR_EXCEPTION(
                "Cannot filter table by time: OutputTableNamePattern %Qv does not contain %%UNIX_TS",
                pattern);
        }

        Options_.OutputTableNamePatternPrefix = pattern.substr(0, placeholderPos);
        Options_.OutputTableNamePatternSuffix = pattern.substr(placeholderPos + std::string_view("%UNIX_TS").size());

        if (Options_.OutputTableNamePatternPrefix.contains('%') || Options_.OutputTableNamePatternSuffix.contains('%')) {
            THROW_ERROR_EXCEPTION(
                "Cannot filter table by time: OutputTableNamePattern %Qv combines %%UNIX_TS with other "
                "specifiers, which is not supported",
                pattern);
        }

        if (args.size() >= 2) {
            Options_.From = ParseDateTimeArg(args[1], context);
        }
        if (args.size() >= 3) {
            Options_.To = ParseDateTimeArg(args[2], context);
        }
    }

    DB::ColumnsDescription getActualTableStructure(DB::ContextPtr context, bool /*isInsertQuery*/) const override
    {
        // It's ok, creating StorageYtLogTables is not expensive.
        auto storage = Execute(context);
        return storage->getInMemoryMetadata().getColumns();
    }

private:
    std::string ExportDirectory_;
    NObjectClient::TObjectId QueueId_;
    TStorageYtQueueExportsOptions Options_;

    DB::StoragePtr executeImpl(
        const DB::ASTPtr& /*functionAst*/,
        DB::ContextPtr context,
        const std::string& /*tableName*/,
        DB::ColumnsDescription /*cachedColumns*/,
        bool /*isInsertQuery*/) const override
    {
        return Execute(context);
    }

    DB::StoragePtr Execute(DB::ContextPtr /*context*/) const
    {
        return CreateStorageYtQueueExports(ExportDirectory_, Options_);
    }

    const char* getStorageTypeName() const override
    {
        return "YtNodes";
    }
};

////////////////////////////////////////////////////////////////////////////////

void RegisterTableFunctionYtQueueExports()
{
    auto& factory = DB::TableFunctionFactory::instance();

    factory.registerFunction<TTableFunctionYtListsQueueExports>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
