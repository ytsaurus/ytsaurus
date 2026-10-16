#include "tablet_index_evaluator.h"

#include <yt/yt/flow/library/cpp/common/column_evaluator_cache.h>
#include <yt/yt/flow/library/cpp/common/message.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <yt/yt/library/query/base/query.h>

#include <yt/yt/core/misc/finally.h>

#include <limits>

namespace NYT::NFlow {

using namespace NTableClient;
using namespace NQueryClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

// The $ prefix marks this as a system-like column to minimize collision with real payload columns.
inline const std::string TabletIndexEvalColumnName = "$tablet_index_expression_result";

} // namespace

////////////////////////////////////////////////////////////////////////////////

i64 ReduceHashToTabletIndex(ui64 hash, i64 tabletCount, EQueueTabletIndexRoutingHashPolicy policy)
{
    YT_VERIFY(tabletCount > 0);
    auto count = static_cast<ui64>(tabletCount);
    if (policy == EQueueTabletIndexRoutingHashPolicy::Modulo) {
        // Reduce the hash modulo the tablet count.
        return static_cast<i64>(hash % count);
    }
    // Range: reduce the hash to contiguous equal-width ranges (rangeSize = 2^64 / count, rounded up).
    // This is a hand-maintained copy of the canonical range sharder; a conformance test pins the two together.
    if (count < 2) {
        return 0;
    }
    auto rangeSize = std::numeric_limits<ui64>::max() / count + 1;
    return static_cast<i64>(hash / rangeSize);
}

////////////////////////////////////////////////////////////////////////////////

TTabletIndexEvaluator::TTabletIndexEvaluator(
    const TTableSchemaPtr& streamSchema,
    const std::string& expression,
    std::optional<EQueueTabletIndexRoutingHashPolicy> policy)
    : Policy_(policy)
    , Expression_(expression)
    , RowBuffer_(New<TRowBuffer>())
    , EvalBuffer_(New<TRowBuffer>())
{
    // The computed key column carries the expression; the payload columns follow as
    // value columns it can reference. The column evaluator resolves the referenced
    // columns; unreferenced ones stay null and are never read.
    std::vector<TColumnSchema> columns;
    columns.reserve(streamSchema->GetColumnCount() + 1);
    columns.push_back(TColumnSchema(TabletIndexEvalColumnName, EValueType::Uint64, ESortOrder::Ascending)
            .SetExpression(expression));
    for (const auto& column : streamSchema->Columns()) {
        columns.push_back(TColumnSchema(column.Name(), column.LogicalType()));
    }
    TabletIndexColumnId_ = 0;
    auto evalSchema = New<TTableSchema>(std::move(columns));

    try {
        // NB: the process-wide evaluator cache interns one entry per distinct schema and grows
        // unboundedly; the fix (a bounded schema-interning pool) is tracked by YTFLOW-765.
        Evaluator_ = CreateFastColumnEvaluatorCache()->Find(evalSchema);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Invalid tablet routing expression %Qv", expression)
            .With(ex);
    }
    YT_VERIFY(Evaluator_);

    // The result is written to the Int64 $tablet_index; require a uint64 so that a string- or
    // null-typed expression fails at construction rather than silently misrouting every row.
    auto resultType = Evaluator_->GetExpression(TabletIndexColumnId_)->GetWireType();
    THROW_ERROR_EXCEPTION_UNLESS(resultType == EValueType::Uint64,
        "Tablet routing expression %Qv must produce a uint64, but produces %Qlv",
        expression,
        resultType);

    for (int evalSchemaId : Evaluator_->GetReferenceIds(TabletIndexColumnId_)) {
        const auto& name = evalSchema->Columns()[evalSchemaId].Name();
        References_.push_back(TReference{
            .EvalSchemaId = evalSchemaId,
            .PayloadColumnId = streamSchema->GetColumnIndexOrThrow(name),
        });
    }

    Row_ = RowBuffer_->AllocateUnversioned(evalSchema->GetColumnCount());
    for (int i = 0; i < evalSchema->GetColumnCount(); ++i) {
        Row_[i] = MakeUnversionedNullValue(i);
    }
}

i64 TTabletIndexEvaluator::GetTabletIndex(const TPayload& payload, i64 tabletCount)
{
    YT_VERIFY(tabletCount > 0);

    // Only the referenced values change per message; the rest stay null. EvaluateKeys captures
    // string-typed computed outputs into the buffer; ours is a uint64 so it stays empty, but
    // EvaluateKeys still requires the argument — clear it per call to keep memory bounded.
    auto finally = Finally([&] {
        EvalBuffer_->Clear();
    });

    for (const auto& reference : References_) {
        auto value = GetColumn(payload, reference.PayloadColumnId);
        value.Id = reference.EvalSchemaId;
        Row_[reference.EvalSchemaId] = value;
    }

    Evaluator_->EvaluateKeys(Row_, EvalBuffer_, /*preserveColumnsIds*/ false);
    const auto& result = Row_[TabletIndexColumnId_];
    // Runtime guard against null (Type == Null) and any other non-uint64 result.
    THROW_ERROR_EXCEPTION_UNLESS(result.Type == EValueType::Uint64,
        "Tablet routing expression %Qv produced a %Qlv value, expected uint64",
        Expression_,
        result.Type);
    auto evaluatedValue = result.Data.Uint64;

    i64 tabletIndex;
    if (Policy_) {
        // Hash mode: the value is a hash reduced to a tablet index.
        tabletIndex = ReduceHashToTabletIndex(evaluatedValue, tabletCount, *Policy_);
    } else {
        // Verbatim mode: the value is the tablet index itself.
        THROW_ERROR_EXCEPTION_IF(
            evaluatedValue > static_cast<ui64>(std::numeric_limits<i64>::max()),
            "Tablet index expression %Qv produced value %v that does not fit into an int64 $tablet_index",
            Expression_,
            evaluatedValue);
        tabletIndex = static_cast<i64>(evaluatedValue);
    }

    THROW_ERROR_EXCEPTION_UNLESS(0 <= tabletIndex && tabletIndex < tabletCount,
        "Tablet routing expression %Qv produced tablet index %v outside the valid range [0, %v)",
        Expression_,
        tabletIndex,
        tabletCount);
    return tabletIndex;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
