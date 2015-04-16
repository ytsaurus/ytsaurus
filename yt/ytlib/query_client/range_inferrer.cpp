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
using TInt64Divisors = SmallVector<i64, 1>;
using TUint64Divisors = SmallVector<ui64, 1>;

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
            : (Modulo_ - 1) * 2 + 1;
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
    TQuotientEnumerationGenerator(T lower, T upper, SmallVector<T, 1>& divisors)
        : Estimation_(Estimate(lower, upper, divisors))
        , Lower_(lower)
        , Upper_(upper)
    {
        for (auto divisor : divisors) {
            ui64 step = lower >= 0 ? Abs(divisor) - (lower % divisor) : - (lower % divisor);
            PermanentQueue_.emplace(step, Abs(divisor));
        }
        Reset();
    }

    void Reset()
    {
        Queue_ = PermanentQueue_;
        Current_ = Lower_;
        Delta_ = 0;
    }

    bool Finished()
    {
        return Queue_.empty();
    }

    T Next()
    {
        YCHECK(!Finished());
        auto result = Current_;
        Shift();
        if (!Queue_.empty()) {
            auto top = Queue_.top();
            if (top.first - Delta_ > static_cast<ui64>(Upper_ - Current_)) {
                while (!Queue_.empty()) {
                    Queue_.pop();
                }
            } else {
                Current_ += top.first - Delta_;
                Delta_ = top.first;
            }
        }
        return result;
    }

    ui64 Estimation() {
        return Estimation_;
    }

private:
    using TPriorityQueue = std::priority_queue<
        std::pair<ui64, ui64>,
        std::vector<std::pair<ui64, ui64>>,
        std::greater<std::pair<ui64, ui64>>>;
    TPriorityQueue PermanentQueue_;
    TPriorityQueue Queue_;
    const ui64 Estimation_;
    const T Lower_;
    const T Upper_;
    T Current_;
    ui64 Delta_ = 0;

    void Shift()
    {
        while (!Queue_.empty() && Queue_.top().first == Delta_) {
            auto top = Queue_.top();
            Queue_.pop();
            if (top.second + Delta_ >= Delta_ ) {
                Queue_.emplace(top.second + Delta_, top.second);
            }
        }
    }

    static ui64 Estimate(T lower, T upper, SmallVector<T, 1>& divisors)
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

template <>
class TQuotientEnumerationGenerator<TUnversionedValue>
{
public:
    TQuotientEnumerationGenerator(TUnversionedValue lower, TUnversionedValue upper, TDivisors divisors)
        : Type_(lower.Type)
        , Generator_(Create(lower, upper, divisors))
    { }

    void Reset()
    {
        if (Type_ == EValueType::Int64) {
            Generator_.As<TQuotientEnumerationGenerator<i64>>().Reset();
        } else {
            Generator_.As<TQuotientEnumerationGenerator<ui64>>().Reset();
        }
    }

    bool Finished()
    {
        if (Type_ == EValueType::Int64) {
            return Generator_.As<TQuotientEnumerationGenerator<i64>>().Finished();
        } else {
            return Generator_.As<TQuotientEnumerationGenerator<ui64>>().Finished();
        }
    }

    TUnversionedValue Next()
    {
        if (Type_ == EValueType::Int64) {
            return MakeUnversionedInt64Value(Generator_.As<TQuotientEnumerationGenerator<i64>>().Next());
        } else {
            return MakeUnversionedUint64Value(Generator_.As<TQuotientEnumerationGenerator<ui64>>().Next());
        }
    }

    ui64 Estimation()
    {
        if (Type_ == EValueType::Int64) {
            return Generator_.As<TQuotientEnumerationGenerator<i64>>().Estimation();
        } else {
            return Generator_.As<TQuotientEnumerationGenerator<ui64>>().Estimation();
        }
    }

private:
    using TGenerator = TVariant<TQuotientEnumerationGenerator<i64>, TQuotientEnumerationGenerator<ui64>>;

    EValueType Type_;
    TGenerator Generator_;

    TGenerator Create(TUnversionedValue lower, TUnversionedValue upper, TDivisors unversionedDivisors)
    {
        YCHECK(lower.Type == upper.Type);
        if (lower.Type == EValueType::Int64) {
            auto divisors = GetInt64Vector(unversionedDivisors);
            return TQuotientEnumerationGenerator<i64>(lower.Data.Int64, upper.Data.Int64, divisors);

        } else if (lower.Type == EValueType::Uint64) {
            auto divisors = GetUint64Vector(unversionedDivisors);
            return TQuotientEnumerationGenerator<ui64>(lower.Data.Uint64, upper.Data.Uint64, divisors);
        }
        YUNREACHABLE();
    }

    TInt64Divisors GetInt64Vector(TDivisors unversionedVector)
    {
        TInt64Divisors result;
        for (const auto& element : unversionedVector) {
            YCHECK(element.Type == EValueType::Int64);
            result.push_back(element.Data.Int64);
        }
        return result;
    }

    TUint64Divisors GetUint64Vector(TDivisors unversionedVector)
    {
        TUint64Divisors result;
        for (const auto& element : unversionedVector) {
            YCHECK(element.Type == EValueType::Uint64);
            result.push_back(element.Data.Uint64);
        }
        return result;
    }
};

// Extract ranges from a predicate and enrich them with computed column values.
class TRangeInferrerHeavy
    : public TRefCounted
{
public:
    TRangeInferrerHeavy(
        const TConstExpressionPtr& predicate,
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

        KeyTrie_ = ExtractMultipleConstraints(
            predicate,
            depletedKeyColumns,
            &KeyTrieBuffer_,
            functionRegistry);

        LOG_DEBUG_IF(
            VerboseLogging_,
            "Predicate %Qv defines key constraints %Qv",
            InferName(predicate),
            KeyTrie_);
    }

    virtual std::vector<TKeyRange> GetRangesWithinRange(const TKeyRange& keyRange)
    {
        auto ranges = GetRangesFromTrieWithinRange(keyRange, KeyTrie_, &Buffer_);
        std::vector<std::pair<TRow, TRow>> enrichedRanges;
        RangeExpansionLeft_ = (RangeExpansionLimit_ > ranges.size())
            ? RangeExpansionLimit_ - ranges.size()
            : 0;

        for (auto& range : ranges) {
            EnrichKeyRange(range, enrichedRanges);
        }
        enrichedRanges = MergeOverlappingRanges(std::move(enrichedRanges));

        std::vector<TKeyRange> owningRanges;
        for (auto range : enrichedRanges) {
            owningRanges.emplace_back(TKey(range.first), TKey(range.second));
        }

        Buffer_.Clear();
        return owningRanges;
    }

private:
    TColumnEvaluatorPtr Evaluator_;
    TKeyTriePtr KeyTrie_ = TKeyTrie::Universal();
    TRowBuffer KeyTrieBuffer_;
    TRowBuffer Buffer_;
    std::vector<int> DepletedToSchemaMapping_;
    std::vector<int> ComputedColumnIndexes_;
    std::vector<int> SchemaToDepletedMapping_;
    TTableSchema Schema_;
    int KeySize_;
    ui64 RangeExpansionLimit_;
    ui64 RangeExpansionLeft_;
    bool VerboseLogging_;

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

    TDivisors GetDivisors(int keyIndex, std::vector<int> referries)
    {
        auto name = Schema_.Columns()[keyIndex].Name;
        TUnversionedValue one;
        one.Id = 0;
        one.Type = Schema_.Columns()[keyIndex].Type;
        one.Data.Int64 = 1;

        std::function<TDivisors(const TConstExpressionPtr& expr)> getDivisors =
            [&] (const TConstExpressionPtr& expr)
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

    bool IsUserColumn(int index, const std::pair<TRow, TRow>& range)
    {
        return SchemaToDepletedMapping_[index] != -1;
    }

    TNullable<int> IsExactColumn(int index, const std::pair<TRow, TRow>& range, int depletedPrefixSize)
    {
        const auto& references = Evaluator_->GetReferenceIds(index);
        for (int referenceIndex : references) {
            int depletedIndex = SchemaToDepletedMapping_[referenceIndex];
            if (depletedIndex >= depletedPrefixSize
                || depletedIndex == -1
                || IsSentinelType(range.first[depletedIndex].Type)
                || IsSentinelType(range.second[depletedIndex].Type)
                || range.first[depletedIndex] != range.second[depletedIndex])
            {
                return TNullable<int>();
            }
        }
        return references.empty() ? -1 : references.back();
    }

    bool CanEnumerate(int index, const std::pair<TRow, TRow>& range, int depletedPrefixSize)
    {
        const auto& references = Evaluator_->GetReferenceIds(index);
        for (int referenceIndex : references) {
            int depletedIndex = SchemaToDepletedMapping_[referenceIndex];
            if (depletedIndex >= depletedPrefixSize + 1
                || depletedIndex == -1
                || IsSentinelType(range.first[depletedIndex].Type)
                || IsSentinelType(range.second[depletedIndex].Type)
                || (depletedIndex < depletedPrefixSize &&
                    range.first[depletedIndex] != range.second[depletedIndex]))
            {
                return false;
            }
        }
        return true;
    }

    TNullable<TModuloRangeGenerator> GetModuloGeneratorForColumn(int index)
    {
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
        return TNullable<TModuloRangeGenerator>();
    }

    int ExpandKey(TRow destination, TRow source, int size)
    {
        for (int index = 0; index < size; ++index) {
            int depletedIndex = SchemaToDepletedMapping_[index];
            if (depletedIndex != -1) {
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
        auto row = TUnversionedRow::Allocate(Buffer_.GetAlignedPool(), size);
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

    bool EnrichKeyRange(std::pair<TRow, TRow>& range, std::vector<std::pair<TRow, TRow>>& ranges)
    {
        auto lowerSentinel = TrimSentinel(range.first);
        auto upperSentinel = TrimSentinel(range.second);

        // Find the longest common prefix for depleted bounds.
        int depletedPrefixSize = 0;
        while (depletedPrefixSize < range.first.GetCount() &&
            depletedPrefixSize < range.second.GetCount() &&
            range.first[depletedPrefixSize] == range.second[depletedPrefixSize])
        {
            ++depletedPrefixSize;
        }

        // Check if we need to iterate over a first column after the longest common prefix.
        bool canEnumerate = depletedPrefixSize < range.first.GetCount() &&
            depletedPrefixSize < range.second.GetCount() &&
            IsIntegralType(range.first[depletedPrefixSize].Type) &&
            IsIntegralType(range.second[depletedPrefixSize].Type);

        int prefixSize = DepletedToSchemaMapping_[depletedPrefixSize];
        int shrinkSize = KeySize_;
        ui64 rangeCount = 1;
        std::vector<std::pair<int, TModuloRangeGenerator>> moduloComputedColumns;
        std::vector<int> exactlyComputedColumns;
        std::vector<int> enumerableColumns;
        std::vector<int> enumeratorDependentColumns;
        TNullable<TQuotientEnumerationGenerator<TUnversionedValue>> enumeratorGenerator;

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
            if (IsUserColumn(index, range)) {
                continue;
            } else if (auto lastReference = IsExactColumn(index, range, depletedPrefixSize)) {
                exactlyComputedColumns.push_back(index);
                continue;
            } else if (canEnumerate && CanEnumerate(index, range, depletedPrefixSize)) {
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
        auto shrinkDivisionComputedColumns = [&] () {
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
            while (!exactlyComputedColumns.empty() && exactlyComputedColumns.back() >= shrinkSize) {
                exactlyComputedColumns.pop_back();
            }
            while (!moduloComputedColumns.empty() && moduloComputedColumns.back().first >= shrinkSize) {
                moduloComputedColumns.pop_back();
            }
        };

        // Check if we can use iteration.
        if (canEnumerate && !enumerableColumns.empty()) {
            if (shrinkSize <= prefixSize) {
                shrinkDivisionComputedColumns();
            } else {
                auto generator = TQuotientEnumerationGenerator<TUnversionedValue>(
                    range.first[depletedPrefixSize],
                    range.second[depletedPrefixSize],
                    GetDivisors(prefixSize, enumerableColumns));
                if (generator.Estimation() < RangeExpansionLeft_ && rangeCount * generator.Estimation() < RangeExpansionLeft_) {
                    rangeCount *= generator.Estimation();
                    enumeratorGenerator = generator;
                } else {
                    shrinkDivisionComputedColumns();
                }
            }
        }

        // Update range expansion limit utilization.
        RangeExpansionLeft_ -= std::min(rangeCount, RangeExpansionLeft_);

        // Map depleted key onto enriched key.
        int maxReferenceIndex = std::max(
            GetMaxReferenceIndex(exactlyComputedColumns),
            GetMaxReferenceIndex(enumerableColumns));
        auto lowerRow = TUnversionedRow::Allocate(Buffer_.GetAlignedPool(), KeySize_ + 1);
        auto upperRow = TUnversionedRow::Allocate(Buffer_.GetAlignedPool(), KeySize_ + 1);
        int lowerSize = ExpandKey(lowerRow, range.first, std::max(shrinkSize, maxReferenceIndex + 1));
        int upperSize = ExpandKey(upperRow, range.second, std::max(shrinkSize, maxReferenceIndex + 1));

        // Trim trailing modulo computed columns.
        while (!moduloComputedColumns.empty() &&
            moduloComputedColumns.back().first == lowerSize - 1 &&
            lowerSize == upperSize)
        {
            --lowerSize;
            --upperSize;
            moduloComputedColumns.pop_back();
        }

        // Evaluate computed columns with exact value.
        for (int index : exactlyComputedColumns) {
            Evaluator_->EvaluateKey(lowerRow, Buffer_, index);
            upperRow[index] = lowerRow[index];
        }

        // Check whther our bound is shrinked and append appropriate sentinels.
        bool shrinked;
        if (shrinkSize < DepletedToSchemaMapping_[range.second.GetCount()]) {
            upperRow[shrinkSize].Type = EValueType::Max;
            upperRow.SetCount(shrinkSize + 1);
            lowerRow.SetCount(shrinkSize);
            shrinked = true;
        } else {
            upperRow.SetCount(upperSize);
            lowerRow.SetCount(lowerSize);
            AppendSentinel(upperRow, upperSentinel);
            AppendSentinel(lowerRow, lowerSentinel);
            shrinked = false;
        }

        // Evaluate specified columns.
        auto evaluateColumns = [&] (TUnversionedRow& row, const std::vector<int>& columns) {
            for (int index : columns) {
                Evaluator_->EvaluateKey(row, Buffer_, index);
            }
        };

        // Iterate over a range and generate corresponding bounds.
        auto generateEnumerableColumns = [&] (TUnversionedRow lowerRow, TUnversionedRow upperRow) {
            if (!enumeratorGenerator) {
                ranges.push_back(std::make_pair(lowerRow, upperRow));
            } else {
                auto& generator = enumeratorGenerator.Get();
                generator.Reset();
                YCHECK(generator.Next() == lowerRow[prefixSize]);
                evaluateColumns(lowerRow, enumerableColumns);
                evaluateColumns(lowerRow, enumeratorDependentColumns);
                auto left = lowerRow;
                while (!generator.Finished()) {
                    auto step = generator.Next();
                    auto right = Copy(left, prefixSize + 1);
                    right[prefixSize] = step;
                    ranges.push_back(std::make_pair(left, right));
                    left = Copy(right, prefixSize + 1);
                    evaluateColumns(left, enumerableColumns);
                }
                evaluateColumns(upperRow, enumerableColumns);
                evaluateColumns(upperRow, enumeratorDependentColumns);
                ranges.push_back(std::make_pair(left, upperRow));
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
        return shrinked;
    }
};

struct TRefCountedRowBuffer
    : public TRefCounted
{
    TRowBuffer RowBuffer;
};

////////////////////////////////////////////////////////////////////////////////

TRangeInferrer CreateHeavyRangeInferrer(
    const TConstExpressionPtr& predicate,
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

    return [MOVE(heavyInferrer)] (const TKeyRange& keyRange) mutable {
        return heavyInferrer->GetRangesWithinRange(keyRange);
    };
}

TRangeInferrer CreateLightRangeInferrer(
    const TConstExpressionPtr& predicate,
    const TKeyColumns& keyColumns,
    const IFunctionRegistryPtr functionRegistry,
    bool verboseLogging)
{
    auto keyTrieBuffer = New<TRefCountedRowBuffer>();
    auto keyTrie = ExtractMultipleConstraints(
        predicate,
        keyColumns,
        &keyTrieBuffer->RowBuffer,
        functionRegistry);

    LOG_DEBUG_IF(
        verboseLogging,
        "Predicate %Qv defines key constraints %Qv",
        InferName(predicate),
        keyTrie);

    return [
        MOVE(keyTrieBuffer),
        MOVE(keyTrie)
    ] (const TKeyRange& keyRange) {
        TRowBuffer rowBuffer;
        auto unversionedRanges = GetRangesFromTrieWithinRange(keyRange, keyTrie, &rowBuffer);
        std::vector<TKeyRange> ranges;
        for (auto range : unversionedRanges) {
            ranges.emplace_back(TKey(range.first), TKey(range.second));
        }
        return ranges;
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TRangeInferrer CreateRangeInferrer(
    const TConstExpressionPtr& predicate,
    const TTableSchema& schema,
    const TKeyColumns& keyColumns,
    const TColumnEvaluatorCachePtr& evaluatorCache,
    const IFunctionRegistryPtr functionRegistry,
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

