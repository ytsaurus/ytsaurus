#pragma once

#ifndef DYNAMIC_TABLE_MULTIPLEXER_COMPUTATION_INL_H_
    #error "Direct inclusion of this file is not allowed, include dynamic_table_multiplexer_computation.h"
    // For sane code completion.
    #include "dynamic_table_multiplexer_computation.h"
#endif

#include <yt/yt/flow/library/cpp/misc/retryable_client.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/node.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

template <class TUserState>
void TDynamicTableMultiplexerComputation<TUserState>::DoInit(IJobInitContextPtr initContext)
{
    TMultiplexerComputation<TUserState>::DoInit(initContext);

    auto params = this->GetParameters();
    auto groupBySchema = this->GetKeySchema();

    NApi::TGetNodeOptions options;
    options.Attributes = {"schema"};
    options.ReadFrom = NApi::EMasterChannelKind::Cache;
    auto node = NYTree::ConvertTo<NYTree::INodePtr>(
        NConcurrency::WaitFor(this->GetRetryableClient()->GetNode(params->TablePath.GetPath(), options))
            .ValueOrThrow());
    auto tableSchema = node->Attributes().template Get<NTableClient::TTableSchemaPtr>("schema");

    auto [rowSchema, secondaryKeyColumns] =
        NDynamicTableMultiplexer::SplitTableSchemaByGroupBy(*tableSchema, *groupBySchema);

    RowSchema_ = std::move(rowSchema);
    SecondaryKeyColumns_ = std::move(secondaryKeyColumns);

    std::vector<NTableClient::TColumnSchema> secondaryKeyColumnSchemas;
    secondaryKeyColumnSchemas.reserve(SecondaryKeyColumns_.size());
    for (int i = 0; i < std::ssize(SecondaryKeyColumns_); ++i) {
        secondaryKeyColumnSchemas.push_back(RowSchema_->Columns()[i]);
    }
    SecondaryKeySchema_ = New<NTableClient::TTableSchema>(std::move(secondaryKeyColumnSchemas));
}

////////////////////////////////////////////////////////////////////////////////

template <class TUserState>
std::optional<TKey> TDynamicTableMultiplexerComputation<TUserState>::DoFetchBatch(
    const TKey& key,
    const std::optional<TKey>& startOffsetExclusive,
    const std::optional<TKey>& endOffsetInclusive,
    i64 limit,
    TStateAccessor<TUserState>& userState,
    IOutputCollectorPtr output)
{
    auto params = this->GetParameters();
    auto groupBySchema = this->GetKeySchema();

    auto additionalWhere = this->BuildAdditionalWhereCondition(key, userState);

    auto parameterizedQuery = NDynamicTableMultiplexer::BuildSelectQuery(
        *groupBySchema,
        key,
        SecondaryKeyColumns_,
        startOffsetExclusive,
        endOffsetInclusive,
        *RowSchema_,
        params->TablePath.GetPath(),
        limit,
        additionalWhere);

    auto result = NConcurrency::WaitFor(
        this->GetRetryableClient()->SelectRows(parameterizedQuery.Query, NApi::TSelectRowsOptions{}))
        .ValueOrThrow();

    auto rows = result.Rowset->GetRows();
    if (rows.empty()) {
        return std::nullopt;
    }

    int rowColumnCount = std::ssize(RowSchema_->Columns());
    for (auto row : rows) {
        TPayloadBuilder rowBuilder(RowSchema_);
        for (int i = 0; i < rowColumnCount; ++i) {
            rowBuilder.SetValue(row[i], i);
        }
        auto rowPayload = rowBuilder.Finish();

        this->DoBuildOutputForRow(
            key,
            rowPayload,
            RowSchema_,
            userState,
            output);
    }

    auto lastRow = rows[rows.Size() - 1];
    auto nextOffset = NTableClient::GetKeyPrefix(lastRow, std::ssize(SecondaryKeyColumns_));
    return TKey(TKey::TUnderlying(std::move(nextOffset)));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
