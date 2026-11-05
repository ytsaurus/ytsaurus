#pragma once

#include "multiplexer_process_function.h"

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct TDynamicTableMultiplexerParameters
    : public NYTree::TYsonStruct
{
    //! Path to the pre-built sorted dynamic table to read rows from.
    //! Must include cluster (e.g. "<cluster=primary>//tmp/lookup").
    //! The table's leading key columns must match the computation's group-by schema.
    NYPath::TRichYPath TablePath;

    REGISTER_YSON_STRUCT(TDynamicTableMultiplexerParameters);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

namespace NDynamicTableMultiplexer {

//! A self-contained SELECT query string with values inlined as literals.
struct TParameterizedSelectQuery
{
    std::string Query;
};

//! Builds a SELECT query for one key, optionally bounded by secondary-key offsets.
TParameterizedSelectQuery BuildSelectQuery(
    const NTableClient::TTableSchema& groupBySchema,
    const TKey& key,
    const std::vector<std::string>& secondaryKeyColumns,
    const std::optional<TKey>& startOffsetExclusive,
    const std::optional<TKey>& endOffsetInclusive,
    const NTableClient::TTableSchema& rowSchema,
    const NYPath::TYPath& tablePath,
    i64 limit,
    const std::string& additionalWhere = {});

//! Validates the table's leading key columns and splits off its row schema and secondary keys.
std::pair<NTableClient::TTableSchemaPtr, std::vector<std::string>> SplitTableSchemaByGroupBy(
    const NTableClient::TTableSchema& tableSchema,
    const NTableClient::TTableSchema& groupBySchema);

} // namespace NDynamicTableMultiplexer

////////////////////////////////////////////////////////////////////////////////

//! Multiplexer process function backed by a sorted dynamic table whose leading key columns match
//! the computation's group-by schema.
template <class TUserState = TEmptyMultiplexerUserState>
class TDynamicTableMultiplexerProcessFunction
    : public TMultiplexerProcessFunction<TUserState>
{
public:
    explicit TDynamicTableMultiplexerProcessFunction(const TProcessFunctionContextPtr& context);

protected:
    //! Converts one selected row into zero or more output messages.
    virtual void BuildOutputForRow(
        const TKey& key,
        const TPayload& row,
        const NTableClient::TTableSchemaPtr& rowSchema,
        TStateAccessor<TUserState>& userState,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) = 0;

    //! Returns an optional extra predicate appended to the generated SELECT.
    virtual std::string BuildAdditionalWhereCondition(
        const TKey& key,
        TStateAccessor<TUserState>& userState,
        const IRuntimeContextPtr& context);

    NTableClient::TTableSchemaPtr GetCurrentOffsetSchema(
        const IRuntimeContextPtr& context) final;

    std::optional<TKey> FetchBatch(
        const TKey& key,
        const std::optional<TKey>& startOffsetExclusive,
        const std::optional<TKey>& endOffsetInclusive,
        i64 limit,
        TStateAccessor<TUserState>& userState,
        const IOutputCollectorPtr& output,
        const IRuntimeContextPtr& context) final;

private:
    const NYPath::TRichYPath TablePath_;
    const IRetryableClientPtr Client_;
    std::vector<std::string> SecondaryKeyColumns_;
    NTableClient::TTableSchemaPtr SecondaryKeySchema_;
    NTableClient::TTableSchemaPtr RowSchema_;

    void EnsureInitialized(const IRuntimeContextPtr& context);

    static NYPath::TRichYPath GetTablePath(const TProcessFunctionContextPtr& context);
    static IRetryableClientPtr CreateClient(
        const TProcessFunctionContextPtr& context,
        const NYPath::TRichYPath& tablePath);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define DYNAMIC_TABLE_MULTIPLEXER_PROCESS_FUNCTION_INL_H_
#include "dynamic_table_multiplexer_process_function-inl.h"
#undef DYNAMIC_TABLE_MULTIPLEXER_PROCESS_FUNCTION_INL_H_
