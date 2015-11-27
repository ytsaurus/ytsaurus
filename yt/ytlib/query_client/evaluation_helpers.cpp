#include "evaluation_helpers.h"
#include "column_evaluator.h"
#include "private.h"
#include "helpers.h"
#include "plan_fragment.h"
#include "plan_helpers.h"
#include "query_statistics.h"

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/common.h>

#include <yt/core/profiling/scoped_timer.h>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;
using NTableClient::GetUnversionedRowDataSize;

static const auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

const i64 PoolChunkSize = 32 * 1024;
const i64 BufferLimit = 32 * PoolChunkSize;

TTopCollector::TTopCollector(i64 limit, TComparerFunction* comparer)
    : Comparer_(comparer)
{
    Rows_.reserve(limit);
}

std::pair<TRow, int> TTopCollector::Capture(TRow row)
{
    if (EmptyBufferIds_.empty()) {
        if (GarbageMemorySize_ > TotalMemorySize_ / 2) {
            // Collect garbage.

            std::vector<std::vector<size_t>> buffersToRows(Buffers_.size());
            for (size_t rowId = 0; rowId < Rows_.size(); ++rowId) {
                buffersToRows[Rows_[rowId].second].push_back(rowId);
            }

            auto buffer = New<TRowBuffer>(PoolChunkSize);

            TotalMemorySize_ = 0;
            AllocatedMemorySize_ = 0;
            GarbageMemorySize_ = 0;

            for (size_t bufferId = 0; bufferId < buffersToRows.size(); ++bufferId) {
                for (auto rowId : buffersToRows[bufferId]) {
                    auto& row = Rows_[rowId].first;
                    
                    auto savedSize = buffer->GetSize();
                    row = buffer->Capture(row);
                    AllocatedMemorySize_ += buffer->GetSize() - savedSize;
                }

                TotalMemorySize_ += buffer->GetCapacity();

                if (buffer->GetSize() < BufferLimit) {
                    EmptyBufferIds_.push_back(bufferId);
                }

                std::swap(buffer, Buffers_[bufferId]);
                buffer->Clear();
            }
        } else {
            // Allocate buffer and add to emptyBufferIds.
            EmptyBufferIds_.push_back(Buffers_.size());
            Buffers_.push_back(New<TRowBuffer>(PoolChunkSize));
        }
    }

    YCHECK(!EmptyBufferIds_.empty());

    auto bufferId = EmptyBufferIds_.back();
    auto buffer = Buffers_[bufferId];

    auto savedSize = buffer->GetSize();
    auto savedCapacity = buffer->GetCapacity();

    auto capturedRow = buffer->Capture(row);

    AllocatedMemorySize_ += buffer->GetSize() - savedSize;
    TotalMemorySize_ += buffer->GetCapacity() - savedCapacity;

    if (buffer->GetSize() >= BufferLimit) {
        EmptyBufferIds_.pop_back();
    }

    return std::make_pair(capturedRow, bufferId);
}

void TTopCollector::AccountGarbage(TRow row)
{
    GarbageMemorySize_ += GetUnversionedRowDataSize(row.GetCount());
    for (int index = 0; index < row.GetCount(); ++index) {
        const auto& value = row[index];

        if (IsStringLikeType(EValueType(value.Type))) {
            GarbageMemorySize_ += value.Length;
        }
    }
}

void TTopCollector::AddRow(TRow row)
{
    if (Rows_.size() < Rows_.capacity()) {
        auto capturedRow = Capture(row);
        Rows_.emplace_back(capturedRow);
        std::push_heap(Rows_.begin(), Rows_.end(), Comparer_);
    } else if (!Comparer_(Rows_.front().first, row)) {
        auto capturedRow = Capture(row);
        std::pop_heap(Rows_.begin(), Rows_.end(), Comparer_);
        AccountGarbage(Rows_.back().first);
        Rows_.back() = capturedRow;
        std::push_heap(Rows_.begin(), Rows_.end(), Comparer_);
    }
}

////////////////////////////////////////////////////////////////////////////////

bool UpdateAndCheckRowLimit(i64* limit, char* flag)
{
    if (*limit > 0) {
        --(*limit);
        return true;
    } else {
        *flag = true;
        return false;
    }
}

TJoinEvaluator GetJoinEvaluator(
    const TJoinClause& joinClause,
    TConstExpressionPtr predicate,
    const TTableSchema& selfTableSchema,
    const TColumnEvaluatorCachePtr evaluatorCache)
{
    const auto& equations = joinClause.Equations;
    auto isLeft = joinClause.IsLeft;
    auto& foreignTableSchema = joinClause.ForeignTableSchema;
    auto& foreignKeyColumnsCount = joinClause.ForeignKeyColumnsCount;
    auto& renamedTableSchema = joinClause.RenamedTableSchema;
    auto& joinedTableSchema = joinClause.JoinedTableSchema;
    auto& foreignDataId = joinClause.ForeignDataId;
    auto foreignPredicate = ExtractPredicateForColumnSubset(predicate, renamedTableSchema);

    // Create subquery TQuery{ForeignDataSplit, foreign predicate and (join columns) in (keys)}.
    auto subquery = New<TQuery>(std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max());

    subquery->TableSchema = foreignTableSchema;
    subquery->KeyColumnsCount = foreignKeyColumnsCount;
    subquery->RenamedTableSchema = renamedTableSchema;
    subquery->WhereClause = foreignPredicate;

    // (join key... , other columns...)
    auto projectClause = New<TProjectClause>();
    std::vector<TConstExpressionPtr> joinKeyExprs;
    for (const auto& column : equations) {
        projectClause->AddProjection(column.second, InferName(column.second));
        joinKeyExprs.push_back(column.second);
    }

    // Check if equations form primary key, make foreign subquery without IN clause
    //

    bool canUseSourceRanges = true;
    std::vector<int> equationByIndex(foreignKeyColumnsCount, -1);
    for (size_t equationIndex = 0; equationIndex < equations.size(); ++equationIndex) {
        const auto& column = equations[equationIndex];
        const auto& expr = column.second;

        if (const auto* refExpr = expr->As<TReferenceExpression>()) {
            auto index = renamedTableSchema.GetColumnIndexOrThrow(refExpr->ColumnName);
            if (index < foreignKeyColumnsCount) {
                equationByIndex[index] = equationIndex;
                continue;
            }
        }
        break;
    }

    std::vector<bool> usedJoinKeyEquations(equations.size(), false);

    auto evaluator = evaluatorCache->Find(foreignTableSchema, foreignKeyColumnsCount);
    size_t keyPrefix = 0;
    for (; keyPrefix < foreignKeyColumnsCount; ++keyPrefix) {
        if (equationByIndex[keyPrefix] >= 0) {
            usedJoinKeyEquations[equationByIndex[keyPrefix]] = true;
            continue;
        }

        if (foreignTableSchema.Columns()[keyPrefix].Expression) {
            const auto& references = evaluator->GetReferenceIds(keyPrefix);
            auto canEvaluate = true;
            for (int referenceIndex : references) {
                if (equationByIndex[referenceIndex] < 0) {
                    canEvaluate = false;
                }
            }
            if (canEvaluate) {
                continue;
            }
        }
        break;
    }

    for (auto flag : usedJoinKeyEquations) {
        if (!flag) {
            canUseSourceRanges = false;
        }
    }

    for (const auto& column : renamedTableSchema.Columns()) {
        projectClause->AddProjection(New<TReferenceExpression>(
            column.Type,
            column.Name),
            column.Name);
    }

    subquery->ProjectClause = projectClause;

    auto subqueryTableSchema = subquery->GetTableSchema();

    auto joinKeySize = equations.size();

    std::vector<std::pair<bool, int>> columnMapping;
    for (const auto& column : joinedTableSchema.Columns()) {
        if (auto self = selfTableSchema.FindColumn(column.Name)) {
            columnMapping.emplace_back(true, selfTableSchema.GetColumnIndex(*self));
        } else if (auto foreign = subqueryTableSchema.FindColumn(column.Name)) {
            columnMapping.emplace_back(false, subqueryTableSchema.GetColumnIndex(*foreign));
        } else {
            YUNREACHABLE();
        }        
    }

    return [=] (
        TExecutionContext* context,
        THasherFunction* groupHasher,
        TComparerFunction* groupComparer,
        TComparerFunction* keyComparer,
        std::vector<TRow> keys,
        std::vector<TRow> allRows,
        TRowBufferPtr permanentBuffer,
        std::vector<TRow>* joinedRows)
    {
        // TODO: keys should be joined with allRows: [(key, sourceRow)]

        auto rowBuffer = permanentBuffer;
        TRowRanges ranges;

        if (canUseSourceRanges) {
            LOG_DEBUG("Using join via source ranges");
            for (auto key : keys) {
                TRow lowerBound = TRow::Allocate(rowBuffer->GetPool(), keyPrefix);
                for (int column = 0; column < keyPrefix; ++column) {
                    if (equationByIndex[column] >= 0) {
                        lowerBound[column] = key[equationByIndex[column]];
                    }
                }

                for (int column = 0; column < keyPrefix; ++column) {
                    if (foreignTableSchema.Columns()[column].Expression) {
                        evaluator->EvaluateKey(lowerBound, rowBuffer, column);
                    }
                }

                TRow upperBound = TRow::Allocate(rowBuffer->GetPool(), keyPrefix + 1);
                for (int column = 0; column < keyPrefix; ++column) {
                    upperBound[column] = lowerBound[column];
                }

                upperBound[keyPrefix] = MakeUnversionedSentinelValue(EValueType::Max);
                ranges.emplace_back(lowerBound, upperBound);
            }

            LOG_DEBUG("Sorting %v ranges",
                ranges.size());

            std::sort(ranges.begin(), ranges.end(), [] (const TRowRange& lhs, const TRowRange& rhs) {
                return lhs.first < rhs.first;
            });
        } else {
            LOG_DEBUG("Using join via IN clause");
            ranges.emplace_back(
                rowBuffer->Capture(NTableClient::MinKey().Get()),
                rowBuffer->Capture(NTableClient::MaxKey().Get()));

            LOG_DEBUG("Sorting %v join keys",
                keys.size());

            std::sort(keys.begin(), keys.end(), keyComparer);

            auto inClause = New<TInOpExpression>(joinKeyExprs, MakeSharedRange(std::move(keys), permanentBuffer));

            subquery->WhereClause = subquery->WhereClause
                ? MakeAndExpression(inClause, subquery->WhereClause)
                : inClause;
        }

        LOG_DEBUG("Executing subquery");
        NApi::IRowsetPtr rowset;

        {
            ISchemafulWriterPtr writer;
            TFuture<NApi::IRowsetPtr> rowsetFuture;
            std::tie(writer, rowsetFuture) = NApi::CreateSchemafulRowsetWriter(subquery->GetTableSchema());
            NProfiling::TAggregatingTimingGuard timingGuard(&context->Statistics->AsyncTime);

            auto statistics = context->ExecuteCallback(
                subquery,
                foreignDataId,
                std::move(rowBuffer),
                std::move(ranges),
                writer);

            LOG_DEBUG("Remote subquery statistics %v", statistics);
            *context->Statistics += statistics;

            rowset = WaitFor(rowsetFuture).ValueOrThrow();
        }

        const auto& foreignRows = rowset->GetRows();

        LOG_DEBUG("Got %v foreign rows", foreignRows.size());

        TJoinLookupRows foreignLookup(
            InitialGroupOpHashtableCapacity,
            groupHasher,
            groupComparer);

        for (auto row : foreignRows) {
            foreignLookup.insert(row);
        }

        // Join rowsets.
        TRowBuilder rowBuilder;
        // allRows have format (join key... , other columns...)

        auto addRow = [&] (TRow joinedRow) -> bool {
            if (!UpdateAndCheckRowLimit(&context->JoinRowLimit, &context->StopFlag)) {
                context->Statistics->IncompleteOutput = true;
                return true;
            }

            joinedRows->push_back(context->PermanentBuffer->Capture(joinedRow));
            return false;
        };

        LOG_DEBUG("Joining started");

        for (auto row : allRows) {
            auto equalRange = foreignLookup.equal_range(row);
            for (auto it = equalRange.first; it != equalRange.second; ++it) {
                rowBuilder.Reset();
                auto foreignRow = *it;
                for (auto columnIndex : columnMapping) {
                    rowBuilder.AddValue(columnIndex.first ? row[joinKeySize + columnIndex.second] : foreignRow[columnIndex.second]);
                }

                if (addRow(rowBuilder.GetRow())) {
                    return;
                }
            }

            if (isLeft && equalRange.first == equalRange.second) {
                rowBuilder.Reset();
                for (auto columnIndex : columnMapping) {
                    rowBuilder.AddValue(columnIndex.first
                        ? row[joinKeySize + columnIndex.second]
                        : MakeUnversionedSentinelValue(EValueType::Null));
                }

                if (addRow(rowBuilder.GetRow())) {
                    return;
                }
            }
        }

        LOG_DEBUG("Joining finished");
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
