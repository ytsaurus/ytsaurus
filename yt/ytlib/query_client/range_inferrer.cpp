#include "stdafx.h"

#include "private.h"
#include "range_inferrer.h"
#include "plan_helpers.h"
#include "key_trie.h"
#include "folding_profiler.h"

#include <yt/core/misc/ref_counted.h>
#include <yt/core/misc/variant.h>
#include <yt/core/misc/small_vector.h>

#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/schema.h>
#include <yt/ytlib/table_client/unversioned_row.h>

#include <cstdlib>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

using TDivisors = SmallVector<TUnversionedValue, 1>;

class TModuloRangeGenerator
{
public:
    explicit TModuloRangeGenerator(TUnversionedValue modulo)
        : Value_(modulo)
    {
        Modulo_ = (Value_.Type == EValueType::Uint64)
            ? modulo.Data.Uint64
            : std::abs(modulo.Data.Int64);
        Reset();
    }

    ui64 Count() const
    {
        return (Value_.Type == EValueType::Uint64)
            ? Modulo_
            : (Modulo_ * 2) - 1;
    }

    bool Finished() const
    {
        return Value_.Data.Uint64 == Modulo_;
    }

    TUnversionedValue Next()
    {
        YCHECK(!Finished());
        auto result = Value_;
        ++Value_.Data.Uint64;
        return result;
    }

    void Reset()
    {
        Value_.Data.Uint64 = (Value_.Type == EValueType::Uint64)
            ? 0
            : (-Modulo_ + 1);
    }
private:
    TUnversionedValue Value_;
    ui64 Modulo_;
};

template <class T>
class TQuotientEnumerationGenerator
{
public:
    TQuotientEnumerationGenerator(T lower, T upper, const SmallVector<T, 1>& divisors)
        : DivisorQueue_(CreateDivisorQueue(lower, divisors))
        , Estimate_(Estimate(lower, upper, divisors))
        , Lower_(lower)
        , Upper_(upper)
    {
        Reset();
    }

    void Reset()
    {
        CurrentQueue_ = DivisorQueue_;
        Current_ = Lower_;
        Delta_ = 0;
    }

    T Next()
    {
        YCHECK(!Finished());

        auto result = Current_;

        while (!CurrentQueue_.empty() && CurrentQueue_.top().first == Delta_) {
            auto top = CurrentQueue_.top();
            CurrentQueue_.pop();
            if (top.second + Delta_ >= Delta_ ) {
                CurrentQueue_.emplace(top.second + Delta_, top.second);
            }
        }

        if (!CurrentQueue_.empty()) {
            auto top = CurrentQueue_.top();
            if (top.first - Delta_ > static_cast<ui64>(Upper_ - Current_)) {
                while (!CurrentQueue_.empty()) {
                    CurrentQueue_.pop();
                }
            } else {
                Current_ += top.first - Delta_;
                Delta_ = top.first;
            }
        }

        return result;
    }

    bool Finished() const
    {
        return CurrentQueue_.empty();
    }

    ui64 Estimation() const
    {
        return Estimate_;
    }

    static ui64 Estimate(T lower, T upper, const SmallVector<T, 1>& divisors)
    {
        ui64 estimate = 1;
        for (auto divisor : divisors) {
            estimate += static_cast<ui64>(upper - lower) / Abs(divisor) + 1;
        }
        return std::min(estimate, static_cast<ui64>(upper - lower + 1));
    }

private:
    using TPriorityQueue = std::priority_queue<
        std::pair<ui64, ui64>,
        std::vector<std::pair<ui64, ui64>>,
        std::greater<std::pair<ui64, ui64>>>;

    const TPriorityQueue DivisorQueue_;
    const ui64 Estimate_;
    const T Lower_;
    const T Upper_;

    TPriorityQueue CurrentQueue_;
    T Current_ = T();
    ui64 Delta_ = 0;

    static TPriorityQueue CreateDivisorQueue(T lower, const SmallVector<T, 1>& divisors)
    {
        TPriorityQueue queue;
        for (auto divisor : divisors) {
            ui64 step = lower >= 0 ? Abs(divisor) - (lower % divisor) : - (lower % divisor);
            queue.emplace(step, Abs(divisor));
        }
        return queue;
    }



    static ui64 Abs(T value)
    {
        return value >= 0 ? value : -value;
    }
};

struct IGenerator
{
    virtual ~IGenerator() = default;

    virtual void Reset() = 0;
    virtual TUnversionedValue Next() = 0;

    virtual bool Finished() const = 0;
    virtual ui64 Estimation() const = 0;
};

template <class TGenerator, class TLift>
class TLiftedGenerator
    : public IGenerator
{
public:
    TLiftedGenerator(TGenerator underlying, TLift lift)
        : Underlying_(std::move(underlying))
        , Lift_(std::move(lift))
    { }

    virtual void Reset() override
    {
        Underlying_.Reset();
    }

    virtual TUnversionedValue Next() override
    {
        return Lift_(Underlying_.Next());
    }

    virtual bool Finished() const override
    {
        return Underlying_.Finished();
    }

    virtual ui64 Estimation() const override
    {
        return Underlying_.Estimation();
    }

private:
    TGenerator Underlying_;
    TLift Lift_;
};

template <class TPrimitive, class TLift, class TUnlift>
std::unique_ptr<IGenerator> CreateLiftedGenerator(
    TLift lift,
    TUnlift unlift,
    TUnversionedValue lower,
    TUnversionedValue upper,
    const TDivisors& divisors)
{
    auto unliftedDivisors = SmallVector<TPrimitive, 1>();
    for (const auto& divisor : divisors) {
        unliftedDivisors.push_back(unlift(divisor));
    }
    auto underlying = TQuotientEnumerationGenerator<TPrimitive>(
        unlift(lower),
        unlift(upper),
        unliftedDivisors);
    return std::make_unique<TLiftedGenerator<decltype(underlying), TLift>>(
        std::move(underlying),
        std::move(lift));
}

std::unique_ptr<IGenerator> CreateQuotientEnumerationGenerator(
    TUnversionedValue lower,
    TUnversionedValue upper,
    const TDivisors& divisors)
{
    std::unique_ptr<IGenerator> generator;
    switch (lower.Type) {
        case EValueType::Int64:
            generator = CreateLiftedGenerator<i64>(
                [] (i64 value) { return MakeUnversionedInt64Value(value); },
                [] (const TUnversionedValue& value) { return value.Data.Int64; },
                lower, upper, divisors);
            break;
        case EValueType::Uint64:
            generator = CreateLiftedGenerator<ui64>(
                [] (ui64 value) { return MakeUnversionedUint64Value(value); },
                [] (const TUnversionedValue& value) { return value.Data.Uint64; },
                lower, upper, divisors);
        default:
            break;
    }
    return generator;
}

ui64 Estimate(TUnversionedValue lower, TUnversionedValue upper, TDivisors partial)
{
    YCHECK(partial.empty() || lower.Type == upper.Type);

    switch (lower.Type) {
        case EValueType::Int64: {
            auto unliftedDivisors = SmallVector<i64, 1>();
            for (const auto& divisor : partial) {
                unliftedDivisors.push_back(divisor.Data.Int64);
            }

            return TQuotientEnumerationGenerator<i64>::Estimate(
                lower.Data.Int64,
                upper.Data.Int64,
                unliftedDivisors);
            break;
        }
        case EValueType::Uint64: {
            auto unliftedDivisors = SmallVector<ui64, 1>();
            for (const auto& divisor : partial) {
                unliftedDivisors.push_back(divisor.Data.Uint64);
            }

            return TQuotientEnumerationGenerator<ui64>::Estimate(
                lower.Data.Uint64,
                upper.Data.Uint64,
                unliftedDivisors);
        }
        default:
            break;
    }
    return 1;
}

// Extract ranges from a predicate and enrich them with computed column values.
class TRangeInferrerHeavy
    : public TRefCounted
{
public:
    TRangeInferrerHeavy(
        TConstExpressionPtr predicate,
        const TTableSchema& schema,
        const TKeyColumns& renamedKeyColumns,
        const TColumnEvaluatorCachePtr& evaluatorCache,
        const IFunctionRegistryPtr functionRegistry,
        ui64 rangeExpansionLimit,
        bool verboseLogging)
        : Schema_(schema)
        , KeySize_(renamedKeyColumns.size())
        , VerboseLogging_(verboseLogging)
    {
        Evaluator_ = evaluatorCache->Find(Schema_, KeySize_);
        yhash_set<Stroka> references;
        if (predicate) {
            Profile(predicate, schema, nullptr, nullptr, &references, functionRegistry);
        }

        // TODO(savrus): use enriched key columns here.
        KeyTrie_ = ExtractMultipleConstraints(
            predicate,
            renamedKeyColumns,
            Buffer_,
            functionRegistry);

        LOG_DEBUG_IF(
            VerboseLogging_,
            "Predicate %Qv defines key constraints %Qv",
            InferName(predicate),
            KeyTrie_);


        auto ranges = GetRangesFromTrieWithinRange(
            TRowRange(Buffer_->Capture(MinKey().Get()), Buffer_->Capture(MaxKey().Get())),
            KeyTrie_,
            Buffer_,
            true);

        LOG_DEBUG_IF(
            VerboseLogging_,
            "Got %v from key trie",
            ranges.size());

        RangeExpansionLeft_ = (rangeExpansionLimit > ranges.size())
            ? rangeExpansionLimit - ranges.size()
            : 0;

        for (int index = 0; index < ranges.size(); ++index) {
            EnrichKeyRange(ranges[index], EnrichedRanges_);
        }
        EnrichedRanges_ = MergeOverlappingRanges(std::move(EnrichedRanges_));
    }

    virtual TRowRanges GetRangesWithinRange(const TRowRange& keyRange, TRowBufferPtr rowBuffer)
    {
        auto startIt = std::lower_bound(
            EnrichedRanges_.begin(),
            EnrichedRanges_.end(),
            keyRange,
            [] (const TRowRange& it, const TRowRange& value) {
                return it.second <= value.first;
            });

        std::vector<TRowRange> result;
        while (startIt < EnrichedRanges_.end() && startIt->first < keyRange.second) {
            auto lower = std::max(startIt->first, keyRange.first);
            auto upper = std::min(startIt->second, keyRange.second);
            result.emplace_back(rowBuffer->Capture(lower), rowBuffer->Capture(upper));
            ++startIt;
        }

        return result;
    }

private:
    const TTableSchema Schema_;
    size_t KeySize_;
    const bool VerboseLogging_;

    TColumnEvaluatorPtr Evaluator_;
    TKeyTriePtr KeyTrie_ = TKeyTrie::Universal();

    // TODO(babenko): rename this
    const TRowBufferPtr Buffer_ = New<TRowBuffer>();

    ui64 RangeExpansionLeft_;
    std::vector<TRowRange> EnrichedRanges_;

    TDivisors GetDivisors(int keyIndex, TConstExpressionPtr expr)
    {
        auto name = Schema_.Columns()[keyIndex].Name;

        TUnversionedValue one;
        one.Id = 0;
        one.Type = Schema_.Columns()[keyIndex].Type;
        one.Data.Int64 = 1;

        if (auto referenceExpr = expr->As<TReferenceExpression>()) {
            return (referenceExpr->ColumnName == name) ? TDivisors{one} : TDivisors();
        } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
            TDivisors result;
            for (const auto& argument : functionExpr->Arguments) {
                const auto arg = GetDivisors(keyIndex, argument);
                result.append(arg.begin(), arg.end());
            }
            return result;
        } else if (auto unaryOp = expr->As<TUnaryOpExpression>()) {
            return GetDivisors(keyIndex, unaryOp->Operand);
        } else if (auto binaryOp = expr->As<TBinaryOpExpression>()) {
            auto reference = binaryOp->Lhs->As<TReferenceExpression>();
            auto literal = binaryOp->Rhs->As<TLiteralExpression>();
            if (binaryOp->Opcode == EBinaryOp::Divide
                && reference
                && literal
                && IsIntegralType(static_cast<TUnversionedValue>(literal->Value).Type)
                && reference->ColumnName == name)
            {
                TUnversionedValue value = literal->Value;
                value.Id = 0;
                return TDivisors{value};
            }
            auto lhs = GetDivisors(keyIndex, binaryOp->Lhs);
            auto rhs = GetDivisors(keyIndex, binaryOp->Rhs);
            lhs.append(rhs.begin(), rhs.end());
            return lhs;
        } else if (auto inOp = expr->As<TInOpExpression>()) {
            TDivisors result;
            for (const auto& argument : inOp->Arguments) {
                const auto arg = GetDivisors(keyIndex, argument);
                result.append(arg.begin(), arg.end());
            }
            return result;
        } else if (expr->As<TLiteralExpression>()) {
            return TDivisors();
        } else {
            YUNREACHABLE();
        }
    }

    TNullable<TModuloRangeGenerator> GetModuloGeneratorForColumn(int index)
    {
        if (!Schema_.Columns()[index].Expression) {
            return Null;
        }
        auto expr = Evaluator_->GetExpression(index)->As<TBinaryOpExpression>();
        if (expr && expr->Opcode == EBinaryOp::Modulo) {
            if (auto literalExpr = expr->Rhs->As<TLiteralExpression>()) {
                TUnversionedValue value = literalExpr->Value;
                if (IsIntegralType(value.Type)) {
                    value.Id = index;
                    return TModuloRangeGenerator(value);
                }
            }
        }
        return Null;
    }

    TNullable<TUnversionedValue> TrimSentinel(TRow row)
    {
        TNullable<TUnversionedValue> result;
        for (int index = row.GetCount() - 1; index >= 0 && IsSentinelType(row[index].Type); --index) {
            result = row[index];
            row.SetCount(index);
        }
        return result;
    }

    void AppendSentinel(TRow row, TNullable<TUnversionedValue> sentinel)
    {
        if (sentinel) {
            row[row.GetCount()] = sentinel.Get();
            row.SetCount(row.GetCount() + 1);
        }
    }

    TRow Copy(TRow source, int size = std::numeric_limits<int>::max())
    {
        size = std::min(size, source.GetCount());
        auto row = TUnversionedRow::Allocate(Buffer_->GetPool(), size);
        for (int index = 0; index < size; ++index) {
            row[index] = source[index];
        }
        return row;
    }

    void Copy(TRow source, TRow dest, int count)
    {
        count = std::min(count, source.GetCount());
        for (int index = 0; index < count; ++index) {
            dest[index] = source[index];
        }
    }

    void EnrichKeyRange(std::pair<TRow, TRow>& range, std::vector<std::pair<TRow, TRow>>& ranges)
    {
        TRow lower = range.first;
        TRow upper = range.second;

        auto lowerSentinel = TrimSentinel(lower);
        auto upperSentinel = TrimSentinel(upper);

        bool canEnumerate = false;
        size_t prefixSize = 0;
        for (; prefixSize < std::min(range.first.GetCount(), range.second.GetCount()); ++prefixSize) {
            if (lower[prefixSize].Type == EValueType::TheBottom) {
                YCHECK(upper[prefixSize].Type == EValueType::TheBottom);
                continue;
            }

            YCHECK(!IsSentinelType(lower[prefixSize].Type) && !IsSentinelType(upper[prefixSize].Type));

            if (lower[prefixSize] != upper[prefixSize]) {
                if (IsIntegralType(lower[prefixSize].Type) && IsIntegralType(upper[prefixSize].Type)) {
                    canEnumerate = true;
                }
                break;
            }

            YCHECK(lower[prefixSize] == upper[prefixSize]);
        }

        // In prefix there are only Fixed, computed columns and Bottom (undefined).

        // Collect here evalutable columns.
        std::vector<std::pair<size_t, TNullable<TModuloRangeGenerator>>> computedColumns;
        std::vector<ui64> estimations;

        std::set<TUnversionedValue> divisorsSet;
        ui64 rangeCount = 1;

        size_t shrinkSize;
        for (shrinkSize = 0; shrinkSize < KeySize_; ++shrinkSize) {
            if (shrinkSize < prefixSize && lower[shrinkSize].Type != EValueType::TheBottom) {
                // Is fixed.
                continue;
            }

            // Is computed or Bottom.
            if (!Schema_.Columns()[shrinkSize].Expression) {
                // Is Bottom (undefined)
                break;
            }

            const auto& references = Evaluator_->GetReferenceIds(shrinkSize);
            auto isEvaluatable = true;
            for (int referenceIndex : references) {
                if (referenceIndex >= (canEnumerate ? prefixSize + 1 : prefixSize)) {
                    isEvaluatable = false;
                }
            }

            auto moduloGenerator = GetModuloGeneratorForColumn(shrinkSize);
            TDivisors partial;
            ui64 estimation = moduloGenerator ? moduloGenerator->Count() : std::numeric_limits<ui64>::max();

            if (isEvaluatable) {
                if (canEnumerate) {
                    partial = GetDivisors(prefixSize, Evaluator_->GetExpression(shrinkSize));
                    partial.erase(
                        std::remove_if(partial.begin(), partial.end(), [&] (TUnversionedValue value) {
                            return divisorsSet.count(value);
                        }),
                        partial.end());

                    auto enumEstimation = Estimate(lower[prefixSize], upper[prefixSize], partial);

                    // Here we solve whether create modulo generator or collect divisors.
                    if (enumEstimation < estimation) {
                        estimation = enumEstimation;
                        moduloGenerator.Reset();
                    } else {
                        partial.clear();
                    }
                } else {
                    estimation = 1;
                    moduloGenerator.Reset();
                }
            } else if (!moduloGenerator) {
                break;
            }

            if (estimation <= RangeExpansionLeft_ &&
                rangeCount * estimation <= RangeExpansionLeft_)
            {
                rangeCount *= estimation;
            } else {
                break;
            }

            divisorsSet.insert(partial.begin(), partial.end());
            computedColumns.emplace_back(shrinkSize, moduloGenerator);
            estimations.push_back(estimation);
        }

        // Trim trailing modulo columns
        if (shrinkSize != prefixSize) { // or !canEnumerate || shrinkSize < prefixSize ?
            while (!computedColumns.empty() &&
                computedColumns.back().second &&
                computedColumns.back().first + 1 == shrinkSize)
            {
                --shrinkSize;
                rangeCount /= estimations.back();

                estimations.pop_back();
                computedColumns.pop_back();
            }
        }

        // Update range expansion limit utilization.
        RangeExpansionLeft_ -= std::min(rangeCount, RangeExpansionLeft_);

        TDivisors divisors(divisorsSet.begin(), divisorsSet.end());

        auto enumerateModulo = [&] (TUnversionedRow& prefixRow, auto yield) {
            for (auto& column : computedColumns) {
                auto columnIndex = column.first;
                if (column.second) {
                    column.second->Reset();
                    prefixRow[columnIndex] = MakeUnversionedSentinelValue(EValueType::Null, columnIndex);
                } else {
                    Evaluator_->EvaluateKey(prefixRow, Buffer_, columnIndex);
                }
            }

            yield();

            size_t evalIndex = computedColumns.size();
            while (evalIndex > 0) {
                auto columnIndex = computedColumns[evalIndex - 1].first;
                auto generator = computedColumns[evalIndex - 1].second.GetPtr();
                if (!generator || generator->Finished()) {
                    --evalIndex;
                } else {
                    YASSERT(generator);
                    prefixRow[columnIndex] = generator->Next();
                    while (evalIndex < computedColumns.size()) {
                        ++evalIndex;
                        computedColumns[evalIndex - 1].second->Reset();
                        auto columnIndex = computedColumns[evalIndex - 1].first;
                        prefixRow[columnIndex] = MakeUnversionedSentinelValue(EValueType::Null, columnIndex);
                    }
                    yield();
                }
            }
        };

        auto lowerRow = TUnversionedRow::Allocate(Buffer_->GetPool(), KeySize_ + 1);
        auto upperRow = TUnversionedRow::Allocate(Buffer_->GetPool(), KeySize_ + 1);

        size_t lowerSize = shrinkSize;
        while (lowerSize < range.first.GetCount() && !IsSentinelType(range.first[lowerSize].Type)) {
            ++lowerSize;
        }

        size_t upperSize = shrinkSize;
        while (upperSize < range.second.GetCount() && !IsSentinelType(range.second[upperSize].Type)) {
            ++upperSize;
        }

        Copy(range.first, lowerRow, KeySize_ + 1);
        Copy(range.second, upperRow, KeySize_ + 1);

        std::function<void(TUnversionedRow& row,
            size_t,
            TNullable<TUnversionedValue> finalizeSentinel,
            TNullable<TUnversionedValue> sentinel)> finalizeRow;
        if (shrinkSize < prefixSize) {
            // Shrinked.
            // If is shrinked, then we append fixed sentinels: No sentinel for lower bound and Max sentinel for
            // upper bound
            finalizeRow = [&] (TUnversionedRow& row,
                size_t size,
                TNullable<TUnversionedValue> finalizeSentinel,
                TNullable<TUnversionedValue> sentinel)
            {
                row.SetCount(shrinkSize);
                AppendSentinel(row, finalizeSentinel);
            };
        } else {
            finalizeRow = [&] (TUnversionedRow& row,
                size_t size,
                TNullable<TUnversionedValue> finalizeSentinel,
                TNullable<TUnversionedValue> sentinel)
            {
                row.SetCount(size);
                AppendSentinel(row, sentinel);
            };
        }

        // Add range to result.
        auto yieldRange = [&] (
            TUnversionedRow lowerRow,
            TUnversionedRow upperRow,
            TNullable<TUnversionedValue> lowerSentinel,
            TNullable<TUnversionedValue> upperSentinel)
        {
            finalizeRow(lowerRow, lowerSize, Null, lowerSentinel);
            finalizeRow(upperRow, upperSize, MakeUnversionedSentinelValue(EValueType::Max), upperSentinel);
            YCHECK(lowerRow <= upperRow);
            ranges.push_back(std::make_pair(Copy(lowerRow), Copy(upperRow)));
        };

        TRow prefixRow = TUnversionedRow::Allocate(Buffer_->GetPool(), KeySize_ + 1);
        Copy(range.first, prefixRow, prefixSize);

        if (canEnumerate && !divisors.empty()) {
            auto generator = CreateQuotientEnumerationGenerator(
                range.first[prefixSize],
                range.second[prefixSize],
                divisors);

            generator->Reset();

            auto upperBound = upperRow[prefixSize];

            auto step = generator->Next();
            YCHECK(step == lowerRow[prefixSize]);

            while (!generator->Finished()) {
                prefixRow[prefixSize] = step;

                lowerRow[prefixSize] = step;
                step = generator->Next();
                upperRow[prefixSize] = step;

                enumerateModulo(prefixRow, [&] () {
                    Copy(prefixRow, lowerRow, prefixSize);
                    Copy(prefixRow, upperRow, prefixSize);
                    yieldRange(lowerRow, upperRow, lowerSentinel, Null);
                });

                lowerSentinel.Reset();
            }

            prefixRow[prefixSize] = step;
            lowerRow[prefixSize] = step;
            upperRow[prefixSize] = upperBound;
            enumerateModulo(prefixRow, [&] () {
                Copy(prefixRow, lowerRow, prefixSize);
                Copy(prefixRow, upperRow, prefixSize);
                yieldRange(lowerRow, upperRow, Null, upperSentinel);
            });
        } else {
            enumerateModulo(prefixRow, [&] () {
                Copy(prefixRow, lowerRow, shrinkSize);
                Copy(prefixRow, upperRow, shrinkSize);
                yieldRange(lowerRow, upperRow, lowerSentinel, upperSentinel);
            });
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TRangeInferrer CreateHeavyRangeInferrer(
    TConstExpressionPtr predicate,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const IFunctionRegistryPtr functionRegistry,
    ui64 rangeExpansionLimit,
    bool verboseLogging)
{
    auto heavyInferrer = New<TRangeInferrerHeavy>(
        predicate,
        schema,
        keyColumns,
        evaluatorCache,
        functionRegistry,
        rangeExpansionLimit,
        verboseLogging);

    return [MOVE(heavyInferrer)] (const TRowRange& keyRange, const TRowBufferPtr& rowBuffer) mutable {
        return heavyInferrer->GetRangesWithinRange(keyRange, rowBuffer);
    };
}

TRangeInferrer CreateLightRangeInferrer(
    TConstExpressionPtr predicate,
    const TKeyColumns& keyColumns,
    const IFunctionRegistryPtr functionRegistry,
    bool verboseLogging)
{
    auto keyTrieBuffer = New<TRowBuffer>();
    auto keyTrie = ExtractMultipleConstraints(
        predicate,
        keyColumns,
        keyTrieBuffer,
        functionRegistry);

    LOG_DEBUG_IF(
        verboseLogging,
        "Predicate %Qv defines key constraints %Qv",
        InferName(predicate),
        keyTrie);

    return [
        MOVE(keyTrieBuffer),
        MOVE(keyTrie)
    ] (const TRowRange& keyRange, const TRowBufferPtr& rowBuffer) {
        return GetRangesFromTrieWithinRange(keyRange, keyTrie, rowBuffer);
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TRangeInferrer CreateRangeInferrer(
    TConstExpressionPtr predicate,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const IFunctionRegistryPtr& functionRegistry,
    ui64 rangeExpansionLimit,
    bool verboseLogging)
{
    if (!schema.HasComputedColumns()) {
        return CreateLightRangeInferrer(
            predicate,
            keyColumns,
            functionRegistry,
            verboseLogging);
    }

    return CreateHeavyRangeInferrer(
        predicate,
        schema,
        keyColumns,
        evaluatorCache,
        functionRegistry,
        rangeExpansionLimit,
        verboseLogging);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

