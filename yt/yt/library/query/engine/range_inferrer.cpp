#include <yt/yt/library/query/base/private.h>
#include <yt/yt/library/query/base/key_trie.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h>
#include <yt/yt/library/query/base/coordination_helpers.h>

#include <yt/yt/library/query/engine_api/range_inferrer.h>
#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <library/cpp/yt/memory/ref_counted.h>

#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <cstdlib>

namespace NYT::NQueryClient {

using namespace NConcurrency;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

using TDivisors = TCompactVector<TUnversionedValue, 1>;

struct TRangeInferrerBufferTag
{ };

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
        YT_VERIFY(!Finished());
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
    TQuotientEnumerationGenerator(T lower, T upper, const TCompactVector<T, 1>& divisors)
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
        YT_VERIFY(!Finished());

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

    static ui64 Estimate(T lower, T upper, const TCompactVector<T, 1>& divisors)
    {
        ui64 estimate = 1;
        for (auto divisor : divisors) {
            estimate += static_cast<ui64>(upper - lower) / Abs(divisor) + 1;
        }
        return std::min(estimate, static_cast<ui64>(upper - lower + 1));
    }

private:
    using TPriorityQueueType = std::priority_queue<
        std::pair<ui64, ui64>,
        std::vector<std::pair<ui64, ui64>>,
        std::greater<std::pair<ui64, ui64>>>;

    const TPriorityQueueType DivisorQueue_;
    const ui64 Estimate_;
    const T Lower_;
    const T Upper_;

    TPriorityQueueType CurrentQueue_;
    T Current_ = T();
    ui64 Delta_ = 0;

    static TPriorityQueueType CreateDivisorQueue(T lower, const TCompactVector<T, 1>& divisors)
    {
        TPriorityQueueType queue;
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

    void Reset() override
    {
        Underlying_.Reset();
    }

    TUnversionedValue Next() override
    {
        return Lift_(Underlying_.Next());
    }

    bool Finished() const override
    {
        return Underlying_.Finished();
    }

    ui64 Estimation() const override
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
    auto unliftedDivisors = TCompactVector<TPrimitive, 1>();
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
            break;
        default:
            break;
    }
    return generator;
}

ui64 Estimate(TUnversionedValue lower, TUnversionedValue upper, TDivisors partial)
{
    YT_VERIFY(partial.empty() || lower.Type == upper.Type);

    switch (lower.Type) {
        case EValueType::Int64: {
            auto unliftedDivisors = TCompactVector<i64, 1>();
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
            auto unliftedDivisors = TCompactVector<ui64, 1>();
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

static std::optional<TUnversionedValue> TrimSentinel(TMutableRow row)
{
    std::optional<TUnversionedValue> result;
    for (int index = row.GetCount() - 1; index >= 0 && IsSentinelType(row[index].Type); --index) {
        result = row[index];
        row.SetCount(index);
    }
    return result;
}

void Copy(TRow source, TMutableRow dest, ui32 count)
{
    count = std::min(count, source.GetCount());
    for (int index = 0; index < static_cast<int>(count); ++index) {
        dest[index] = source[index];
    }
}

TMutableRow CaptureRowWithSentinel(TRowBuffer* buffer, TRow src, int size, std::optional<TUnversionedValue> sentinel)
{
    int rowSize = size + static_cast<bool>(sentinel);
    auto row = buffer->AllocateUnversioned(rowSize);
    Copy(src, row, size);
    if (sentinel) {
        row[size] = *sentinel;
    }
    return row;
}

TDivisors GetDivisors(const TSchemaColumns& columns, int keyIndex, TConstExpressionPtr expr)
{
    auto name = columns[keyIndex].Name();

    TUnversionedValue one;
    one.Id = 0;
    one.Type = columns[keyIndex].GetWireType();
    one.Data.Int64 = 1;

    if (auto referenceExpr = expr->As<TReferenceExpression>()) {
        return referenceExpr->ColumnName == name
            ? TDivisors{one}
            : TDivisors();
    } else if (auto functionExpr = expr->As<TFunctionExpression>()) {
        TDivisors result;
        for (const auto& argument : functionExpr->Arguments) {
            const auto arg = GetDivisors(columns, keyIndex, argument);
            result.insert(result.end(), arg.begin(), arg.end());
        }
        return result;
    } else if (auto unaryOp = expr->As<TUnaryOpExpression>()) {
        return GetDivisors(columns, keyIndex, unaryOp->Operand);
    } else if (auto binaryOp = expr->As<TBinaryOpExpression>()) {
        auto reference = binaryOp->Lhs->As<TReferenceExpression>();
        auto literal = binaryOp->Rhs->As<TLiteralExpression>();

        if (reference
            && literal
            && IsIntegralType(static_cast<TUnversionedValue>(literal->Value).Type)
            && reference->ColumnName == name)
        {
            if (binaryOp->Opcode == EBinaryOp::Divide) {
                TUnversionedValue value = literal->Value;
                value.Id = 0;
                return TDivisors{value};
            } else if (binaryOp->Opcode == EBinaryOp::RightShift) {
                TUnversionedValue value = literal->Value;
                value.Data.Uint64 = static_cast<ui64>(1) << value.Data.Uint64;
                value.Id = 0;
                return TDivisors{value};
            }
        }

        auto lhs = GetDivisors(columns, keyIndex, binaryOp->Lhs);
        auto rhs = GetDivisors(columns, keyIndex, binaryOp->Rhs);
        lhs.insert(lhs.end(), rhs.begin(), rhs.end());
        return lhs;
    } else if (auto inExpr = expr->As<TInExpression>()) {
        TDivisors result;
        for (const auto& argument : inExpr->Arguments) {
            const auto arg = GetDivisors(columns, keyIndex, argument);
            result.insert(result.end(), arg.begin(), arg.end());
        }
        return result;
    } else if (expr->As<TLiteralExpression>()) {
        return TDivisors();
    } else {
        YT_ABORT();
    }
}

std::optional<TModuloRangeGenerator> GetModuloGeneratorForColumn(
    const TColumnEvaluator& evaluator,
    const TSchemaColumns& columns,
    int index)
{
    if (!columns[index].Expression()) {
        return std::nullopt;
    }
    auto expr = evaluator.GetExpression(index)->As<TBinaryOpExpression>();
    if (expr && expr->Opcode == EBinaryOp::Modulo) {
        if (auto literalExpr = expr->Rhs->As<TLiteralExpression>()) {
            TUnversionedValue value = literalExpr->Value;
            if (IsIntegralType(value.Type)) {
                value.Id = index;
                return TModuloRangeGenerator(value);
            }
        }
    }
    return std::nullopt;
}

void EnrichKeyRange(
    const TColumnEvaluator& evaluator,
    const TSchemaColumns& columns,
    TRowBuffer* buffer,
    TMutableRowRange range,
    std::vector<TRowRange>& ranges,
    size_t keySize,
    ui64* rangeExpansionLeft)
{
    auto lower = range.first;
    auto upper = range.second;

    auto lowerSentinel = TrimSentinel(lower);
    auto upperSentinel = TrimSentinel(upper);

    bool canEnumerate = false;
    size_t prefixSize = 0;
    for (; prefixSize < std::min(range.first.GetCount(), range.second.GetCount()); ++prefixSize) {
        if (lower[prefixSize].Type == EValueType::TheBottom) {
            YT_VERIFY(upper[prefixSize].Type == EValueType::TheBottom);
            continue;
        }

        YT_VERIFY(!IsSentinelType(lower[prefixSize].Type) && !IsSentinelType(upper[prefixSize].Type));

        if (lower[prefixSize] != upper[prefixSize]) {
            if (IsIntegralType(lower[prefixSize].Type) && IsIntegralType(upper[prefixSize].Type)) {
                canEnumerate = true;
            }
            break;
        }

        YT_VERIFY(lower[prefixSize] == upper[prefixSize]);
    }

    // In prefix there are only Fixed, computed columns and Bottom (undefined).

    // Collect here evalutable columns.
    std::vector<std::pair<size_t, std::optional<TModuloRangeGenerator>>> computedColumns;
    std::vector<ui64> estimations;

    std::set<TUnversionedValue> divisorsSet;
    ui64 rangeCount = 1;

    size_t shrinkSize;
    for (shrinkSize = 0; shrinkSize < keySize; ++shrinkSize) {
        if (shrinkSize < prefixSize && lower[shrinkSize].Type != EValueType::TheBottom) {
            // Is fixed.
            continue;
        }

        // Is computed or Bottom.
        if (!columns[shrinkSize].Expression()) {
            // Is Bottom (undefined)
            break;
        }

        const auto& references = evaluator.GetReferenceIds(shrinkSize);
        auto canEvaluate = true;
        for (int referenceIndex : references) {
            if (referenceIndex >= static_cast<int>(canEnumerate ? prefixSize + 1 : prefixSize)) {
                canEvaluate = false;
                break;
            } else if (
                lower[referenceIndex].Type == EValueType::TheBottom ||
                upper[referenceIndex].Type == EValueType::TheBottom)
            {
                canEvaluate = false;
                break;
            }
        }

        auto moduloGenerator = GetModuloGeneratorForColumn(evaluator, columns, shrinkSize);
        TDivisors partial;
        ui64 estimation = moduloGenerator ? moduloGenerator->Count() : std::numeric_limits<ui64>::max();

        if (canEvaluate) {
            if (canEnumerate) {
                partial = GetDivisors(columns, prefixSize, evaluator.GetExpression(shrinkSize));
                partial.erase(
                    std::remove_if(partial.begin(), partial.end(), [&] (TUnversionedValue value) {
                        return divisorsSet.count(value);
                    }),
                    partial.end());

                auto enumEstimation = Estimate(lower[prefixSize], upper[prefixSize], partial);

                // Here we solve whether create modulo generator or collect divisors.
                if (enumEstimation < estimation) {
                    estimation = enumEstimation;
                    moduloGenerator.reset();
                } else {
                    partial.clear();
                }
            } else {
                estimation = 1;
                moduloGenerator.reset();
            }
        } else if (!moduloGenerator) {
            break;
        }

        if (estimation <= *rangeExpansionLeft &&
            rangeCount * estimation <= *rangeExpansionLeft)
        {
            rangeCount *= estimation;
        } else if (estimation > 1) {
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
    *rangeExpansionLeft -= std::min(rangeCount, *rangeExpansionLeft);

    TDivisors divisors(divisorsSet.begin(), divisorsSet.end());

    auto enumerateModulo = [&] (TMutableUnversionedRow prefixRow, std::function<void()> yield) {
        for (auto& column : computedColumns) {
            auto columnIndex = column.first;
            if (column.second) {
                column.second->Reset();
                prefixRow[columnIndex] = MakeUnversionedSentinelValue(EValueType::Null, columnIndex);
            } else {
                evaluator.EvaluateKey(prefixRow, buffer, columnIndex, /*preserveColumnId*/ false);
            }
        }

        yield();

        size_t evalIndex = computedColumns.size();
        while (evalIndex-- > 0) {
            auto& [columnIndex, generator] = computedColumns[evalIndex];
            if (!generator || generator->Finished()) {
                continue;
            } else {
                YT_ASSERT(generator);
                prefixRow[columnIndex] = generator->Next();
                while (++evalIndex < computedColumns.size()) {
                    auto& [columnIndex, generator] = computedColumns[evalIndex];
                    if (!generator) {
                        continue;
                    }
                    generator->Reset();
                    prefixRow[columnIndex] = MakeUnversionedSentinelValue(EValueType::Null, columnIndex);
                }
                yield();
            }
        }
    };

    auto lowerRow = buffer->AllocateUnversioned(keySize + 1);
    auto upperRow = buffer->AllocateUnversioned(keySize + 1);

    size_t lowerSize = shrinkSize;
    while (lowerSize < range.first.GetCount() && !IsSentinelType(range.first[lowerSize].Type)) {
        ++lowerSize;
    }

    size_t upperSize = shrinkSize;
    while (upperSize < range.second.GetCount() && !IsSentinelType(range.second[upperSize].Type)) {
        ++upperSize;
    }

    Copy(range.first, lowerRow, keySize + 1);
    Copy(range.second, upperRow, keySize + 1);

    std::function<TMutableRow(
        TUnversionedRow row,
        size_t size,
        const std::optional<TUnversionedValue>& finalizeSentinel,
        const std::optional<TUnversionedValue>& sentinel)> finalizeRow;
    if (shrinkSize < prefixSize) {
        // Shrunk.
        // If is shrunk, then we append fixed sentinels: No sentinel for lower bound and Max sentinel for
        // upper bound
        finalizeRow = [&] (
            TUnversionedRow src,
            size_t /*size*/,
            const std::optional<TUnversionedValue>& finalizeSentinel,
            const std::optional<TUnversionedValue>& /*sentinel*/)
        {
            return CaptureRowWithSentinel(buffer, src, shrinkSize, finalizeSentinel);
        };
    } else {
        finalizeRow = [&] (
            TUnversionedRow src,
            size_t size,
            const std::optional<TUnversionedValue>& /*finalizeSentinel*/,
            const std::optional<TUnversionedValue>& sentinel)
        {
            return CaptureRowWithSentinel(buffer, src, size, sentinel);
        };
    }

    // Add range to result.
    auto yieldRange = [&] (
        TMutableRow lowerRow,
        TMutableRow upperRow,
        const std::optional<TUnversionedValue>& lowerSentinel,
        const std::optional<TUnversionedValue>& upperSentinel)
    {
        auto lower = finalizeRow(lowerRow, lowerSize, std::nullopt, lowerSentinel);
        auto upper = finalizeRow(upperRow, upperSize, MakeUnversionedSentinelValue(EValueType::Max), upperSentinel);
        YT_ASSERT(lower <= upper);
        if (lower < upper) {
            ranges.push_back(std::pair(lower, upper));
        }
    };

    auto prefixRow = buffer->AllocateUnversioned(keySize + 1);
    for (auto& item : prefixRow) {
        item = MakeUnversionedNullValue();
    }

    Copy(range.first, prefixRow, prefixSize);

    if (canEnumerate && !divisors.empty()) {
        auto generator = CreateQuotientEnumerationGenerator(
            range.first[prefixSize],
            range.second[prefixSize],
            divisors);

        generator->Reset();

        auto upperBound = upperRow[prefixSize];

        auto step = generator->Next();
        YT_VERIFY(step == lowerRow[prefixSize]);

        while (!generator->Finished()) {
            prefixRow[prefixSize] = step;

            lowerRow[prefixSize] = step;
            step = generator->Next();
            upperRow[prefixSize] = step;

            enumerateModulo(prefixRow, [&] {
                Copy(prefixRow, lowerRow, prefixSize);
                Copy(prefixRow, upperRow, prefixSize);
                yieldRange(lowerRow, upperRow, lowerSentinel, std::nullopt);
            });

            lowerSentinel.reset();
        }

        prefixRow[prefixSize] = step;
        lowerRow[prefixSize] = step;
        upperRow[prefixSize] = upperBound;
        enumerateModulo(prefixRow, [&] {
            Copy(prefixRow, lowerRow, prefixSize);
            Copy(prefixRow, upperRow, prefixSize);
            yieldRange(lowerRow, upperRow, std::nullopt, upperSentinel);
        });
    } else {
        enumerateModulo(prefixRow, [&] {
            Copy(prefixRow, lowerRow, shrinkSize);
            Copy(prefixRow, upperRow, shrinkSize);
            yieldRange(lowerRow, upperRow, lowerSentinel, upperSentinel);
        });
    }
}

////////////////////////////////////////////////////////////////////////////////

TSharedRange<TRowRange> CreateHeavyRangeInferrer(
    TConstExpressionPtr predicate,
    const TTableSchemaPtr& schema,
    const TKeyColumns& keyColumns,
    const IColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    const TQueryOptions& options)
{
    auto buffer = New<TRowBuffer>(TRangeInferrerBufferTag());
    auto keySize = schema->GetKeyColumnCount();

    auto evaluator = evaluatorCache->Find(schema);
    auto keyTrie = ExtractMultipleConstraints(
        predicate,
        keyColumns,
        buffer,
        rangeExtractors);

    YT_LOG_DEBUG_IF(
        options.VerboseLogging,
        "Predicate %Qv defines key constraints %Qv",
        InferName(predicate),
        keyTrie);

    // TODO(savrus): this is a hotfix for YT-2836. Further discussion in YT-2842.
    ui64 moduloExpansion = 1;
    for (int index = 0; index < static_cast<int>(keySize); ++index) {
        if (schema->Columns()[index].Expression()) {
            moduloExpansion *= GetEvaluatedColumnModulo(evaluator->GetExpression(index));
        }
    }

    auto rangeCountLimit = options.RangeExpansionLimit / moduloExpansion;

    auto ranges = GetRangesFromTrieWithinRange(
        TRowRange(buffer->CaptureRow(MinKey()), buffer->CaptureRow(MaxKey())),
        keyTrie,
        buffer,
        true,
        rangeCountLimit);

    YT_LOG_DEBUG_IF(
        options.VerboseLogging,
        "Got %v ranges from key trie",
        ranges.size());

    auto rangeExpansionLeft = options.RangeExpansionLimit > ranges.size()
        ? options.RangeExpansionLimit - ranges.size()
        : 0;

    TRowRanges enrichedRanges;
    for (auto range : ranges) {
        EnrichKeyRange(
            *evaluator,
            schema->Columns(),
            buffer.Get(),
            range,
            enrichedRanges,
            keySize,
            &rangeExpansionLeft);
    }
    std::sort(enrichedRanges.begin(), enrichedRanges.end());
    enrichedRanges.erase(
        MergeOverlappingRanges(enrichedRanges.begin(), enrichedRanges.end()),
        enrichedRanges.end());

    return MakeSharedRange(enrichedRanges, buffer);
}

TSharedRange<TRowRange> CreateLightRangeInferrer(
    TConstExpressionPtr predicate,
    const TKeyColumns& keyColumns,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    const TQueryOptions& options)
{
    auto rowBuffer = New<TRowBuffer>(TRangeInferrerBufferTag());
    auto keyTrie = ExtractMultipleConstraints(
        predicate,
        keyColumns,
        rowBuffer,
        rangeExtractors);

    YT_LOG_DEBUG_IF(
        options.VerboseLogging,
        "Predicate %Qv defines key constraints %Qv",
        InferName(predicate),
        keyTrie);

    auto mutableRanges = GetRangesFromTrieWithinRange(
        TRowRange(MinKey(), MaxKey()),
        keyTrie,
        rowBuffer,
        /*insertUndefined*/ false,
        options.RangeExpansionLimit);

    // TODO(sabdenovch): make a hard cast here.

    TRowRanges resultRanges(mutableRanges.begin(), mutableRanges.end());

    return MakeSharedRange(std::move(resultRanges), rowBuffer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TSharedRange<TRowRange> CreateRangeInferrer(
    TConstExpressionPtr predicate,
    const TTableSchemaPtr& schema,
    const TKeyColumns& keyColumns,
    const IColumnEvaluatorCachePtr& evaluatorCache,
    const TConstRangeExtractorMapPtr& rangeExtractors,
    const TQueryOptions& options)
{
    return schema->HasMaterializedComputedColumns()
        ? CreateHeavyRangeInferrer(
            predicate,
            schema,
            keyColumns,
            evaluatorCache,
            rangeExtractors,
            options)
        : CreateLightRangeInferrer(
            predicate,
            keyColumns,
            rangeExtractors,
            options);

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
