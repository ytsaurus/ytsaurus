#pragma once

#ifndef DYNAMIC_TABLE_MULTIPLEXER_PROCESS_FUNCTION_INL_H_
    #error "Direct inclusion of this file is not allowed, include dynamic_table_multiplexer_process_function.h"
    #include "dynamic_table_multiplexer_process_function.h"
#endif

#include <yt/yt/flow/library/cpp/common/runtime_init_context.h>

#include <yt/yt/flow/library/cpp/misc/retryable_client.h>
#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/cache/cache.h>

#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TUserState>
TDynamicTableMultiplexerProcessFunction<TUserState>::TDynamicTableMultiplexerProcessFunction(
    const TProcessFunctionContextPtr& context)
    : TMultiplexerProcessFunction<TUserState>(context)
    , TablePath_(GetTablePath(context))
    , Client_(CreateClient(context, TablePath_))
{ }

template <class TUserState>
std::string TDynamicTableMultiplexerProcessFunction<TUserState>::BuildAdditionalWhereCondition(
    const TKey& /*key*/,
    TStateAccessor<TUserState>& /*userState*/,
    const IRuntimeContextPtr& /*context*/)
{
    return {};
}

template <class TUserState>
NTableClient::TTableSchemaPtr TDynamicTableMultiplexerProcessFunction<TUserState>::GetCurrentOffsetSchema(
    const IRuntimeContextPtr& context)
{
    EnsureInitialized(context);
    return SecondaryKeySchema_;
}

template <class TUserState>
NYPath::TRichYPath TDynamicTableMultiplexerProcessFunction<TUserState>::GetTablePath(
    const TProcessFunctionContextPtr& context)
{
    auto parameters =
        context->InitContext->GetParameters<TDynamicTableMultiplexerParameters>();
    auto tablePath = parameters->TablePath;
    THROW_ERROR_EXCEPTION_IF(tablePath.GetPath().empty(),
        "\"table_path\" must be set and have a non-empty path");
    THROW_ERROR_EXCEPTION_UNLESS(tablePath.GetCluster(),
        "\"table_path\" must specify a cluster");
    return tablePath;
}

template <class TUserState>
IRetryableClientPtr TDynamicTableMultiplexerProcessFunction<TUserState>::CreateClient(
    const TProcessFunctionContextPtr& context,
    const NYPath::TRichYPath& tablePath)
{
    THROW_ERROR_EXCEPTION_UNLESS(
        context->ClientsCache,
        "YT clients cache is not available in this process-function host");
    THROW_ERROR_EXCEPTION_UNLESS(
        context->Invoker,
        "Invoker is not available in this process-function host");
    THROW_ERROR_EXCEPTION_UNLESS(
        context->StatusProfiler,
        "Status profiler is not available in this process-function host");

    return CreateRetryableClient(
        context->ClientsCache->GetClient(*tablePath.GetCluster()),
        context->Invoker,
        context->StatusProfiler->WithPrefix("/dynamic_table_multiplexer/retryable_client"),
        context->Logger.WithTag("Component", "DynamicTableMultiplexer"));
}

template <class TUserState>
void TDynamicTableMultiplexerProcessFunction<TUserState>::EnsureInitialized(
    const IRuntimeContextPtr& context)
{
    if (RowSchema_) {
        return;
    }

    NApi::TGetNodeOptions options;
    options.Attributes = {"schema"};
    options.ReadFrom = NApi::EMasterChannelKind::Cache;
    auto node = NYTree::ConvertTo<NYTree::INodePtr>(
        NConcurrency::WaitFor(Client_->GetNode(TablePath_.GetPath(), options))
            .ValueOrThrow());
    auto tableSchema = node->Attributes().template Get<NTableClient::TTableSchemaPtr>("schema");

    auto [rowSchema, secondaryKeyColumns] =
        NDynamicTableMultiplexer::SplitTableSchemaByGroupBy(*tableSchema, *context->GetKeySchema());
    RowSchema_ = std::move(rowSchema);
    SecondaryKeyColumns_ = std::move(secondaryKeyColumns);

    std::vector<NTableClient::TColumnSchema> secondaryKeyColumnSchemas;
    secondaryKeyColumnSchemas.reserve(SecondaryKeyColumns_.size());
    for (int index = 0; index < std::ssize(SecondaryKeyColumns_); ++index) {
        secondaryKeyColumnSchemas.push_back(RowSchema_->Columns()[index]);
    }
    SecondaryKeySchema_ = New<NTableClient::TTableSchema>(std::move(secondaryKeyColumnSchemas));
}

template <class TUserState>
std::optional<TKey> TDynamicTableMultiplexerProcessFunction<TUserState>::FetchBatch(
    const TKey& key,
    const std::optional<TKey>& startOffsetExclusive,
    const std::optional<TKey>& endOffsetInclusive,
    i64 limit,
    TStateAccessor<TUserState>& userState,
    const IOutputCollectorPtr& output,
    const IRuntimeContextPtr& context)
{
    EnsureInitialized(context);

    auto query = NDynamicTableMultiplexer::BuildSelectQuery(
        *context->GetKeySchema(),
        key,
        SecondaryKeyColumns_,
        startOffsetExclusive,
        endOffsetInclusive,
        *RowSchema_,
        TablePath_.GetPath(),
        limit,
        BuildAdditionalWhereCondition(key, userState, context));

    auto result = NConcurrency::WaitFor(Client_->SelectRows(query.Query, NApi::TSelectRowsOptions{}))
        .ValueOrThrow();
    auto rows = result.Rowset->GetRows();
    if (rows.empty()) {
        return std::nullopt;
    }

    int rowColumnCount = std::ssize(RowSchema_->Columns());
    for (auto row : rows) {
        TPayloadBuilder rowBuilder(RowSchema_);
        for (int index = 0; index < rowColumnCount; ++index) {
            rowBuilder.SetValue(row[index], index);
        }
        BuildOutputForRow(
            key,
            rowBuilder.Finish(),
            RowSchema_,
            userState,
            output,
            context);
    }

    auto lastRow = rows[rows.Size() - 1];
    auto nextOffset = NTableClient::GetKeyPrefix(lastRow, std::ssize(SecondaryKeyColumns_));
    return TKey(TKey::TUnderlying(std::move(nextOffset)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
