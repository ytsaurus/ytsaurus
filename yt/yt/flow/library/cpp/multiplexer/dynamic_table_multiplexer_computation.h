#pragma once

#include "multiplexer_computation.h"
#include "public.h"

#include <yt/yt/client/ypath/rich.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Static parameters for the dynamic-table multiplexer.
struct TDynamicTableMultiplexerParameters
    : public virtual TTransformComputation::TParameters
{
    //! Path to the pre-built sorted dynamic table to read rows from.
    //! Must include cluster (e.g. "<cluster=primary>//tmp/lookup").
    //! The table's leading key columns must match the computation's group_by_schema.
    NYPath::TRichYPath TablePath;

    REGISTER_YSON_STRUCT(TDynamicTableMultiplexerParameters);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

//! Multiplexer computation that fetches rows from a pre-built sorted dynamic table.
//!
//! For each input message the computation iterates over rows in the table whose
//! leading key matches the input's group_by_schema key, emitting one output message
//! per fetched row via DoBuildOutputForRow.
//!
//! The set of secondary key columns (those after group_by) and the projected
//! columns are derived from the table schema in DoInit. If the schema changes
//! between pipeline runs, iteration restarts from scratch automatically (the
//! base class drops per-key offset on schema mismatch).
//!
//! TUserState is persisted between input/timer firings and lets the user forward
//! payload from the input message into the output rows. Defaults to an empty
//! state for the simple case where nothing needs to be forwarded.
template <class TUserState = TEmptyMultiplexerUserState>
class TDynamicTableMultiplexerComputation
    : public TMultiplexerComputation<TUserState>
{
public:
    using TMultiplexerComputation<TUserState>::TMultiplexerComputation;

    YT_FLOW_EXTEND_PARAMETERS(TDynamicTableMultiplexerParameters, TMultiplexerComputation<TUserState>);

    //! Build an output message for a single row fetched from the lookup table.
    //!
    //! \param key        The key from group_by_schema.
    //! \param row        Values of all table columns except group_by ones, in
    //!                   the order they appear in the table schema.
    //! \param rowSchema  Schema describing |row| (column names + types).
    //! \param userState  User-defined state populated by DoOnInputMessage.
    //! \param output     Output collector for emitted messages.
    virtual void DoBuildOutputForRow(
        const TKey& key,
        const TPayload& row,
        const NTableClient::TTableSchemaPtr& rowSchema,
        TStateAccessor<TUserState>& userState,
        IOutputCollectorPtr output) = 0;

    NTableClient::TTableSchemaPtr GetCurrentOffsetSchema() const final
    {
        return SecondaryKeySchema_;
    }

    //! Optional extra predicate appended to the SELECT's WHERE clause.
    //! Default: empty (no extra predicate).
    //!
    //! Use this hook to filter rows on the YT side based on per-key state —
    //! e.g. drop rows that don't satisfy a condition derived from the input
    //! payload. The returned fragment is ANDed into WHERE as-is, so it is
    //! the subclass's responsibility to escape values it injects.
    virtual std::string BuildAdditionalWhereCondition(
        const TKey& /*key*/,
        TStateAccessor<TUserState>& /*userState*/)
    {
        return {};
    }

protected:
    void DoInit(IJobInitContextPtr initContext) override;

    std::optional<TKey> DoFetchBatch(
        const TKey& key,
        const std::optional<TKey>& startOffsetExclusive,
        const std::optional<TKey>& endOffsetInclusive,
        i64 limit,
        TStateAccessor<TUserState>& userState,
        IOutputCollectorPtr output) final;

private:
    //! Names of secondary-key columns (table key columns after group_by_schema).
    std::vector<std::string> SecondaryKeyColumns_;
    //! Sub-schema describing offsets (just the secondary-key columns).
    NTableClient::TTableSchemaPtr SecondaryKeySchema_;
    //! Sub-schema of the SELECT projection passed to DoBuildOutputForRow
    //! (everything except group_by columns).
    NTableClient::TTableSchemaPtr RowSchema_;
};

////////////////////////////////////////////////////////////////////////////////

namespace NDynamicTableMultiplexer {

//! A self-contained SELECT query string. Values are inlined as literals
//! (with Any-typed values wrapped in `yson_string_to_any(...)`) — no
//! placeholders are used, so this can be passed to SelectRows as-is.
struct TParameterizedSelectQuery
{
    std::string Query;
};

//! Builds a SELECT query that fetches one batch of rows for a given key,
//! optionally bounded by start/end offsets on the secondary key. If
//! |additionalWhere| is non-empty it is ANDed into the WHERE clause as-is.
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

//! Validates that the leading key columns of |tableSchema| match |groupBySchema|
//! and returns a sub-schema with everything except group_by columns plus the
//! list of secondary-key column names (those after group_by that have a sort order).
std::pair<NTableClient::TTableSchemaPtr, std::vector<std::string>> SplitTableSchemaByGroupBy(
    const NTableClient::TTableSchema& tableSchema,
    const NTableClient::TTableSchema& groupBySchema);

} // namespace NDynamicTableMultiplexer

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define DYNAMIC_TABLE_MULTIPLEXER_COMPUTATION_INL_H_
#include "dynamic_table_multiplexer_computation-inl.h"
#undef DYNAMIC_TABLE_MULTIPLEXER_COMPUTATION_INL_H_
