#include "stdafx.h"

#include "private.h"
#include "range_inferrer.h"
#include "plan_helpers.h"
#include "key_trie.h"
#include "folding_profiler.h"

#include <yt/core/misc/ref_counted.h>
#include <yt/core/misc/variant.h>
#include <yt/core/misc/small_vector.h>

#include <yt/ytlib/new_table_client/row_buffer.h>
#include <yt/ytlib/new_table_client/schema.h>
#include <yt/ytlib/new_table_client/unversioned_row.h>

#include <cstdlib>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;
using namespace NVersionedTableClient;

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

    static ui64 Estimate(T lower, T upper, const SmallVector<T, 1>& divisors)
    {
        ui64 estimate = 1;
        for (auto divisor : divisors) {
            estimate += static_cast<ui64>(upper - lower) / Abs(divisor) + 1;
        }
        return std::min(estimate, static_cast<ui64>(upper - lower + 1));
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

// Extract ranges from a predicate and enrich them with computed column values.
class TRangeInferrerHeavy
    : public TRefCounted
{
public:
    TRangeInferrerHeavy(
        TConstExpressionPtr predicate,
        const TTableSchema& schema,
        const TKeyColumns& keyColumns,
        const TColumnEvaluatorCachePtr& evaluatorCache,
        const IFunctionRegistryPtr functionRegistry,
        ui64 rangeExpansionLimit,
        bool verboseLogging)
        : Schema_(schema)
        , KeySize_(keyColumns.size())
        , RangeExpansionLimit_(rangeExpansionLimit)
        , VerboseLogging_(verboseLogging)
    {
        Evaluator_ = evaluatorCache->Find(Schema_, KeySize_);
        yhash_set<Stroka> references;
        if (predicate) {
            Profile(predicate, schema, nullptr, nullptr, &references, functionRegistry);
        }
        auto depletedKeyColumns = BuildDepletedIdMapping(references);

        // TODO(savrus): use enriched key columns here.
        KeyTrie_ = ExtractMultipleConstraints(
            predicate,
            depletedKeyColumns,
            Buffer_,
            functionRegistry);

        LOG_DEBUG_IF(
            VerboseLogging_,
            "Predicate %Qv defines key constraints %Qv",
            InferName(predicate),
            KeyTrie_);

        //TODO(savrus): this is a hotfix for YT-2836. Further discussion in YT-2842.
        auto rangeCountLimit = GetRangeCountLimit();

        auto ranges = GetExtendedRangesFromTrieWithinRange(
            TRowRange(Buffer_->Capture(MinKey().Get()), Buffer_->Capture(MaxKey().Get())),
            KeyTrie_,
            Buffer_,
            rangeCountLimit);
        YCHECK(ranges.first.size() == ranges.second.size());

        RangeExpansionLeft_ = (RangeExpansionLimit_ > ranges.first.size())
            ? RangeExpansionLimit_ - ranges.first.size()
            : 0;

        for (int index = 0; index < ranges.first.size(); ++index) {
            EnrichKeyRange(ranges.first[index], ranges.second[index], EnrichedRanges_);
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
    const int KeySize_;
    const ui64 RangeExpansionLimit_;
    const bool VerboseLogging_;

    TColumnEvaluatorPtr Evaluator_;
    TKeyTriePtr KeyTrie_ = TKeyTrie::Universal();

    // TODO(babenko): rename this
    const TRowBufferPtr Buffer_ = New<TRowBuffer>();

    std::vector<int> DepletedToSchemaMapping_;
    std::vector<int> ComputedColumnIndexes_;
    std::vector<int> SchemaToDepletedMapping_;

    ui64 RangeExpansionLeft_;
    std::vector<TRowRange> EnrichedRanges_;

    TKeyColumns BuildDepletedIdMapping(const yhash_set<Stroka>& references)
    {
        TKeyColumns depletedKeyColumns;
        SchemaToDepletedMapping_.resize(KeySize_ + 1, -1);

        auto addIndexToMapping = [&] (int index) {
            SchemaToDepletedMapping_[index] = DepletedToSchemaMapping_.size();
            DepletedToSchemaMapping_.push_back(index);
        };

        for (int index = 0; index < KeySize_; ++index) {
            auto column = Schema_.Columns()[index];
            if (!column.Expression || references.find(column.Name) != references.end()) {
                addIndexToMapping(index);
                depletedKeyColumns.push_back(column.Name);
            } else {
                ComputedColumnIndexes_.push_back(index);
            }
        }
        addIndexToMapping(KeySize_);

        return depletedKeyColumns;
    }

    int SchemaToDepletedIndex(int schemaIndex)
    {
        YCHECK(schemaIndex >= 0 && schemaIndex < SchemaToDepletedMapping_.size());
        return SchemaToDepletedMapping_[schemaIndex];
    }

    int DepletedToSchemaIndex(int depletedIndex)
    {
        YCHECK(depletedIndex >= 0 && depletedIndex < DepletedToSchemaMapping_.size());
        return DepletedToSchemaMapping_[depletedIndex];
    }

    bool IsUnboundedColumn(int depletedIndex, ui32 unboundedColumnMask)
    {
        YCHECK(depletedIndex < sizeof(unboundedColumnMask) * 8);
        return unboundedColumnMask & (1 << depletedIndex);
    }

    ui64 GetRangeCountLimit() {
        ui64 moduloExpansion = 1;
        for (int index = 0; index < KeySize_; ++index) {
            if (Schema_.Columns()[index].Expression) {
                auto expr = Evaluator_->GetExpression(index)->As<TBinaryOpExpression>();
                if (expr && expr->Opcode == EBinaryOp::Modulo) {
                    if (auto literalExpr = expr->Rhs->As<TLiteralExpression>()) {
                        TUnversionedValue value = literalExpr->Value;
                        switch (value.Type) {
                            case EValueType::Int64:
                                moduloExpansion *= value.Data.Int64 * 2;
                                break;

                            case EValueType::Uint64:
                                moduloExpansion *= value.Data.Uint64 + 1;
                                break;

                            default:
                                break;
                        }
                    }
                }
            }
        }

        return moduloExpansion == 1
            ? std::numeric_limits<ui64>::max()
            : RangeExpansionLimit_ / moduloExpansion;
    }

    TDivisors GetDivisors(int keyIndex, std::vector<int> referries)
    {
        auto name = Schema_.Columns()[keyIndex].Name;
        TUnversionedValue one;
        one.Id = 0;
        one.Type = Schema_.Columns()[keyIndex].Type;
        one.Data.Int64 = 1;

        std::function<TDivisors(TConstExpressionPtr expr)> getDivisors =
            [&] (TConstExpressionPtr expr)
        {
            if (auto referenceExpr = expr->As<TReferenceExpression>()) {
                return (referenceExpr->ColumnName == name) ? TDivisors{one} : TDivisors();
            } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
                TDivisors result;
                for (const auto& argument : functionExpr->Arguments) {
                    const auto arg = getDivisors(argument);
                    result.append(arg.begin(), arg.end());
                }
                return result;
            } else if (auto unaryOp = expr->As<TUnaryOpExpression>()) {
                return getDivisors(unaryOp->Operand);
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
                auto lhs = getDivisors(binaryOp->Lhs);
                auto rhs = getDivisors(binaryOp->Rhs);
                lhs.append(rhs.begin(), rhs.end());
                return lhs;
            } else if (auto inOp = expr->As<TInOpExpression>()) {
                TDivisors result;
                for (const auto& argument : inOp->Arguments) {
                    const auto arg = getDivisors(argument);
                    result.append(arg.begin(), arg.end());
                }
                return result;
            } else if (expr->As<TLiteralExpression>()) {
                return TDivisors();
            } else {
                YUNREACHABLE();
            }
        };

        TDivisors result;
        for (int index : referries) {
            auto partial = getDivisors(Evaluator_->GetExpression(index));
            result.append(partial.begin(), partial.end());
        }
        std::sort(result.begin(), result.end());
        result.erase(std::unique(result.begin(), result.end()), result.end());
        return result;
    }

    bool IsUserColumn(int index, const std::pair<TRow, TRow>& range, ui32 unboundedColumnMask, int depletedPrefixSize)
    {
        int depletedIndex = SchemaToDepletedIndex(index);
        return !(depletedIndex >= depletedPrefixSize ||
            depletedIndex == -1 ||
            IsUnboundedColumn(depletedIndex, unboundedColumnMask) ||
            IsSentinelType(range.first[depletedIndex].Type) ||
            IsSentinelType(range.second[depletedIndex].Type) ||
            range.first[depletedIndex] != range.second[depletedIndex]);
    }

    TNullable<int> IsExactColumn(int index, const std::pair<TRow, TRow>& range, ui32 unboundedColumnMask, int depletedPrefixSize)
    {
        if (!Schema_.Columns()[index].Expression) {
            return Null;
        }
        const auto& references = Evaluator_->GetReferenceIds(index);
        for (int referenceIndex : references) {
            if (!IsUserColumn(referenceIndex, range, unboundedColumnMask, depletedPrefixSize)) {
                return Null;
            }
        }
        return references.empty() ? -1 : references.back();
    }

    bool CanEnumerate(int index, const std::pair<TRow, TRow>& range, ui32 unboundedColumnMask, int depletedPrefixSize)
    {
        if (!Schema_.Columns()[index].Expression) {
            return false;
        }
        const auto& references = Evaluator_->GetReferenceIds(index);
        for (int referenceIndex : references) {
            int depletedIndex = SchemaToDepletedIndex(referenceIndex);
            if (depletedIndex >= depletedPrefixSize + 1 ||
                depletedIndex == -1 ||
                IsUnboundedColumn(depletedIndex, unboundedColumnMask) ||
                IsSentinelType(range.first[depletedIndex].Type) ||
                IsSentinelType(range.second[depletedIndex].Type) ||
                (depletedIndex < depletedPrefixSize &&
                    range.first[depletedIndex] != range.second[depletedIndex]))
            {
                return false;
            }
        }
        return true;
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

    int ExpandKey(TRow destination, TRow source, int size, ui32 unboundedColumnMask)
    {
        for (int index = 0; index < size; ++index) {
            int depletedIndex = SchemaToDepletedIndex(index);
            if (depletedIndex != -1 && !IsUnboundedColumn(depletedIndex, unboundedColumnMask)) {
                if (depletedIndex < source.GetCount()) {
                    destination[index] = source[depletedIndex];
                } else {
                    return index;
                }
            }
        }
        return size;
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

    int GetMaxReferenceIndex(const std::vector<int>& columns)
    {
        int maxReferenceIndex = -1;
        for (int index : columns) {
            const auto& references = Evaluator_->GetReferenceIds(index);
            if (!references.empty() && references.back() > maxReferenceIndex) {
                maxReferenceIndex = references.back();
            }
        }
        return maxReferenceIndex;
    }

    void EnrichKeyRange(std::pair<TRow, TRow>& range, ui32 unboundedColumnMask, std::vector<std::pair<TRow, TRow>>& ranges)
    {
        auto lowerSentinel = TrimSentinel(range.first);
        auto upperSentinel = TrimSentinel(range.second);

        // Find the longest common prefix for depleted bounds.
        int depletedPrefixSize = 0;
        while (depletedPrefixSize < range.first.GetCount() &&
            depletedPrefixSize < range.second.GetCount() &&
            (range.first[depletedPrefixSize] == range.second[depletedPrefixSize] ||
                IsUnboundedColumn(depletedPrefixSize, unboundedColumnMask)))
        {
            ++depletedPrefixSize;
        }

        // Check if we need to iterate over a first column after the longest common prefix.
        bool canEnumerate =
            depletedPrefixSize < range.first.GetCount() &&
            depletedPrefixSize < range.second.GetCount() &&
            !IsUnboundedColumn(depletedPrefixSize, unboundedColumnMask) &&
            IsIntegralType(range.first[depletedPrefixSize].Type) &&
            IsIntegralType(range.second[depletedPrefixSize].Type);

        int prefixSize = DepletedToSchemaIndex(depletedPrefixSize);
        int shrinkSize = KeySize_;
        ui64 rangeCount = 1;
        std::vector<std::pair<int, TModuloRangeGenerator>> moduloComputedColumns;
        std::vector<int> exactlyComputedColumns;
        std::vector<int> enumerableColumns;
        std::vector<int> enumeratorDependentColumns;
        std::unique_ptr<IGenerator> enumeratorGenerator;

        // Add modulo computed column if we still fit into range expansion limit.
        auto addModuloColumn = [&] (int index, const TModuloRangeGenerator& generator) {
            auto count = generator.Count();
            if (count < RangeExpansionLeft_ && rangeCount * count < RangeExpansionLeft_) {
                rangeCount *= count;
                moduloComputedColumns.push_back(std::make_pair(index, generator));
                return true;
            }
            return false;
        };

        // For each column check that we can use existing value, copmpute it or generate.
        for (int index = 0; index < KeySize_; ++index) {
            if (IsUserColumn(index, range, unboundedColumnMask, depletedPrefixSize)) {
                continue;
            } else if (auto lastReference = IsExactColumn(index, range, unboundedColumnMask, depletedPrefixSize)) {
                exactlyComputedColumns.push_back(index);
                continue;
            } else if (canEnumerate && CanEnumerate(index, range, unboundedColumnMask, depletedPrefixSize)) {
                if (index < prefixSize) {
                    enumerableColumns.push_back(index);
                } else {
                    enumeratorDependentColumns.push_back(index);
                }
                continue;
            } else if (auto generator = GetModuloGeneratorForColumn(index)) {
                if (addModuloColumn(index, generator.Get())) {
                    continue;
                }
            }
            shrinkSize = index;
            break;
        }

        // Shrink bounds up to the first column dependent on range iteration. Try to use modulo generators when appropriate.
        auto shrinkEnumerableColumns = [&] () {
            for (int index : enumerableColumns) {
                if (auto generator = GetModuloGeneratorForColumn(index)) {
                    if (addModuloColumn(index, generator.Get())) {
                        continue;
                    }
                }
                shrinkSize = index;
                break;
            }
            enumerableColumns.clear();
            std::sort(moduloComputedColumns.begin(), moduloComputedColumns.end(), [] (
                    const std::pair<int, TModuloRangeGenerator>& left,
                    const std::pair<int, TModuloRangeGenerator>& right)
                {
                    return left.first < right.first;
                });
            while (!exactlyComputedColumns.empty() && exactlyComputedColumns.back() >= shrinkSize) {
                exactlyComputedColumns.pop_back();
            }
            while (!moduloComputedColumns.empty() && moduloComputedColumns.back().first >= shrinkSize) {
                moduloComputedColumns.pop_back();
            }
        };

        // Check if we can use iteration.
        if (canEnumerate && !enumerableColumns.empty()) {
            auto generator = CreateQuotientEnumerationGenerator(
                range.first[depletedPrefixSize],
                range.second[depletedPrefixSize],
                GetDivisors(prefixSize, enumerableColumns));

            if (generator &&
                generator->Estimation() < RangeExpansionLeft_ &&
                rangeCount * generator->Estimation() < RangeExpansionLeft_)
            {
                rangeCount *= generator->Estimation();
                enumeratorGenerator = std::move(generator);
            } else {
                shrinkEnumerableColumns();
            }
        }

        // Update range expansion limit utilization.
        RangeExpansionLeft_ -= std::min(rangeCount, RangeExpansionLeft_);

        // Map depleted key onto enriched key.
        auto lowerRow = TUnversionedRow::Allocate(Buffer_->GetPool(), KeySize_ + 1);
        auto upperRow = TUnversionedRow::Allocate(Buffer_->GetPool(), KeySize_ + 1);
        int lowerSize = ExpandKey(lowerRow, range.first, KeySize_, unboundedColumnMask);
        int upperSize = ExpandKey(upperRow, range.second, KeySize_, unboundedColumnMask);
        bool isShrinked = shrinkSize < DepletedToSchemaIndex(depletedPrefixSize);

        // Trim trailing modulo computed columns.
        while (!moduloComputedColumns.empty() &&
            (isShrinked || (lowerSize == upperSize && lowerSize == shrinkSize)) &&
            moduloComputedColumns.back().first == shrinkSize - 1)
        {
            if (lowerSize == upperSize && lowerSize == shrinkSize) {
                --lowerSize;
                --upperSize;
            }
            --shrinkSize;
            moduloComputedColumns.pop_back();
        }

        // Evaluate computed columns with exact value.
        for (int index : exactlyComputedColumns) {
            Evaluator_->EvaluateKey(lowerRow, Buffer_, index);
            upperRow[index] = lowerRow[index];
        }

        // Shrink bound and append sentinel.
        auto shrinkRow = [&] (TUnversionedRow& row, int size, TNullable<TUnversionedValue> shrinkSentinel, TNullable<TUnversionedValue> sentinel) {
            if (isShrinked) {
                row.SetCount(shrinkSize);
                AppendSentinel(row, shrinkSentinel);
            } else {
                row.SetCount(size);
                AppendSentinel(row, sentinel);
            }
        };

        // Add range to result.
        auto yieldRange = [&] (
            TUnversionedRow lowerRow,
            TUnversionedRow upperRow,
            TNullable<TUnversionedValue> lowerSentinel,
            TNullable<TUnversionedValue> upperSentinel)
        {
            shrinkRow(lowerRow, lowerSize, Null, lowerSentinel);
            shrinkRow(upperRow, upperSize, MakeUnversionedSentinelValue(EValueType::Max), upperSentinel);
            ranges.push_back(std::make_pair(lowerRow, upperRow));
        };

        // Evaluate specified columns.
        auto evaluateColumns = [&] (TUnversionedRow& row, const std::vector<int>& columns) {
            for (int index : columns) {
                Evaluator_->EvaluateKey(row, Buffer_, index);
            }
        };

        // Iterate over a range and generate corresponding bounds.
        auto generateEnumerableColumns = [&] (TUnversionedRow lowerRow, TUnversionedRow upperRow) {
            if (!enumeratorGenerator) {
                yieldRange(lowerRow, upperRow, lowerSentinel, upperSentinel);
            } else {
                auto& generator = *enumeratorGenerator;
                generator.Reset();
                YCHECK(generator.Next() == lowerRow[prefixSize]);
                evaluateColumns(lowerRow, enumerableColumns);
                evaluateColumns(lowerRow, enumeratorDependentColumns);
                auto left = lowerRow;
                auto right = Copy(left, prefixSize + 1);
                auto buffer = Copy(left, prefixSize + 1);
                auto leftSentinel = lowerSentinel;
                while (!generator.Finished()) {
                    auto step = generator.Next();
                    for (int index : enumerableColumns) {
                        right[index] = left[index];
                    }
                    right[prefixSize] = step;
                    yieldRange(Copy(left), Copy(right), leftSentinel, Null);
                    leftSentinel.Reset();
                    left = buffer;
                    left[prefixSize] = step;
                    evaluateColumns(left, enumerableColumns);
                }
                evaluateColumns(upperRow, enumerableColumns);
                evaluateColumns(upperRow, enumeratorDependentColumns);
                yieldRange(left, upperRow, Null, upperSentinel);
            }
        };

        // Multiply generators.
        if (moduloComputedColumns.empty()) {
            generateEnumerableColumns(lowerRow, upperRow);
        } else {
            auto yield = [&] () {
                generateEnumerableColumns(Copy(lowerRow), Copy(upperRow));
            };
            auto setValue = [&] (TUnversionedValue value) {
                lowerRow[value.Id] = value;
                upperRow[value.Id] = value;
            };

            for (auto& generator : moduloComputedColumns) {
                setValue(MakeUnversionedSentinelValue(EValueType::Null, generator.first));
            }
            yield();

            int generatorIndex = moduloComputedColumns.size() - 1;
            while (generatorIndex >= 0) {
                if (moduloComputedColumns[generatorIndex].second.Finished()) {
                    --generatorIndex;
                } else {
                    setValue(moduloComputedColumns[generatorIndex].second.Next());
                    while (generatorIndex + 1 < moduloComputedColumns.size()) {
                        ++generatorIndex;
                        moduloComputedColumns[generatorIndex].second.Reset();
                        setValue(MakeUnversionedSentinelValue(EValueType::Null, moduloComputedColumns[generatorIndex].first));
                    }
                    yield();
                }
            }
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
    if (!predicate || !schema.HasComputedColumns()) {
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

