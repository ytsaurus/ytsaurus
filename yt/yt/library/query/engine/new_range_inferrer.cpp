#include <yt/yt/library/query/engine_api/new_range_inferrer.h>
#include <yt/yt/library/query/engine_api/column_evaluator.h>

#include <yt/yt/library/query/base/private.h>
#include <yt/yt/library/query/base/constraints.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_helpers.h> // MergeOverlappingRanges
#include <yt/yt/library/query/base/coordination_helpers.h>

#include <yt/yt/core/misc/heap.h>

namespace NYT::NQueryClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

ui64 SaturationArithmeticSum(ui64 lhs, ui64 rhs)
{
    ui64 result;
    if (__builtin_add_overflow(lhs, rhs, &result)) {
        return std::numeric_limits<ui64>::max();
    } else {
        return result;
    }
}

ui64 SaturationArithmeticMultiply(ui64 lhs, ui64 rhs)
{
    YT_VERIFY(lhs != 0 && rhs != 0);

    ui64 result;
    if (__builtin_mul_overflow(lhs, rhs, &result)) {
        return std::numeric_limits<ui64>::max();
    } else {
        return result;
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TRangeInferrerBufferTag
{ };

class TModuloRangeGenerator
{
public:
    explicit TModuloRangeGenerator(TUnversionedValue modulo)
        : Value_(modulo)
        , Modulo_(Value_.Type == EValueType::Uint64
            ? modulo.Data.Uint64
            : std::abs(modulo.Data.Int64))
    {
        Reset();
    }

    bool IsFinished() const
    {
        return Value_.Data.Uint64 == Modulo_;
    }

    TUnversionedValue Next()
    {
        YT_VERIFY(!IsFinished());
        auto result = Value_;
        ++Value_.Data.Uint64;
        return result;
    }

    TUnversionedValue Reset()
    {
        Value_.Data.Uint64 = Value_.Type == EValueType::Uint64
            ? 0
            : -Modulo_ + 1;

        return MakeUnversionedSentinelValue(EValueType::Null);
    }
private:
    TUnversionedValue Value_;
    ui64 Modulo_;
};

class TQuotientGenerator
{
public:
    struct TQueueItem
    {
        ui64 NextValue = 0;
        ui64 Divisor;

        bool operator < (const TQueueItem& other) const
        {
            return NextValue < other.NextValue;
        }
    };

    TQuotientGenerator(ui64 limit, TRange<ui64> divisors)
        : Limit_(limit)
    {
        DivisorQueue_.reserve(std::ssize(divisors));
        for (auto divisor : divisors) {
            DivisorQueue_.push_back({0, divisor});
        }
    }

    void ResetUnsigned(ui64 lower)
    {
        YT_VERIFY(IsFinished());
        Next_ = 0;
        for (auto& [nextValue, divisor] : DivisorQueue_) {
            nextValue = divisor - (lower % divisor);
            YT_VERIFY(nextValue != 0);
        }

        MakeHeap(DivisorQueue_.begin(), DivisorQueue_.end());
        Finished_ = false;
    }

    void ResetSigned(i64 lower)
    {
        YT_VERIFY(IsFinished());
        Next_ = 0;
        for (auto& [nextValue, divisor] : DivisorQueue_) {
            nextValue = lower >= 0
                ? divisor - (lower % static_cast<i64>(divisor))
                : - (lower % static_cast<i64>(divisor));

            if (nextValue == 0) {
                nextValue = divisor;
            }
        }

        MakeHeap(DivisorQueue_.begin(), DivisorQueue_.end());
        Finished_ = false;
    }

    bool IsFinished() const
    {
        return Finished_;
    }

    ui64 Next()
    {
        YT_VERIFY(!IsFinished());

        auto& top = DivisorQueue_.front();
        auto result = Next_;

        Next_ = top.NextValue;

        // Finish if current value reaches limit or next value is greater than limit.
        if (result >= Limit_ || Next_ > Limit_) {
            Finished_ = true;
            return result;
        }

        if (Next_ != std::numeric_limits<ui64>::max()) {
            do {
                top.NextValue = SaturationArithmeticSum(Next_, top.Divisor);
                AdjustHeapFront(DivisorQueue_.begin(), DivisorQueue_.end());
            } while (top.NextValue == Next_);
        }

        return result;
    }

private:
    const ui64 Limit_;
    ui64 Next_ = 0;
    bool Finished_ = true;

    std::vector<TQueueItem> DivisorQueue_;
};

TUnversionedValue LowerBoundToValue(TValueBound lower, bool signedType)
{
    auto value = lower.Value;
    YT_VERIFY(IsIntegralType(value.Type) || value.Type == EValueType::Null || value.Type == EValueType::Min);

    if (value.Type == EValueType::Min) {
        value.Type = EValueType::Null;
        lower.Flag = false;
    }

    if (lower.Flag) {
        if (value.Type == EValueType::Null) {
            if (signedType) {
                value.Type = EValueType::Int64;
                value.Data.Int64 = std::numeric_limits<i64>::min();
            } else {
                value.Type = EValueType::Uint64;
                value.Data.Uint64 = std::numeric_limits<ui64>::min();
            }
        } else {
            if (signedType) {
                YT_VERIFY(value.Data.Int64 < std::numeric_limits<i64>::max());
                ++value.Data.Int64;
            } else {
                YT_VERIFY(value.Data.Uint64 < std::numeric_limits<ui64>::max());
                ++value.Data.Uint64;
            }
        }
    }

    YT_VERIFY(IsIntegralType(value.Type) || value.Type == EValueType::Null);

    return value;
}

TUnversionedValue UpperBoundToValue(TValueBound upper, bool signedType)
{
    auto value = upper.Value;
    YT_VERIFY(IsIntegralType(value.Type) || value.Type == EValueType::Null || value.Type == EValueType::Max);

    if (value.Type == EValueType::Max) {
        if (signedType) {
            value.Type = EValueType::Int64;
            value.Data.Int64 = std::numeric_limits<i64>::max();
        } else {
            value.Type = EValueType::Uint64;
            value.Data.Uint64 = std::numeric_limits<ui64>::max();
        }
    } else if (IsIntegralType(value.Type)) {
        if (!upper.Flag) {
            if (signedType) {
                YT_VERIFY(value.Data.Int64 > std::numeric_limits<i64>::min());
                --value.Data.Int64;
            } else {
                YT_VERIFY(value.Data.Uint64 > std::numeric_limits<ui64>::min());
                --value.Data.Uint64;
            }
        }
    } else {
        YT_VERIFY(value.Type == EValueType::Null);
    }

    return value;
}

ui64 ValueToUint64(TUnversionedValue value, bool signedType)
{
    if (value.Type == EValueType::Null) {
        return signedType ? std::numeric_limits<i64>::min() : 0;
    }

    return value.Data.Uint64;
}

class TQuotientValueGenerator
    : public TQuotientGenerator
{
public:
    TQuotientValueGenerator(bool signedType, TValueBound lower, TValueBound upper, TRange<ui64> divisors)
        : TQuotientValueGenerator(
            signedType,
            LowerBoundToValue(lower, signedType),
            UpperBoundToValue(upper, signedType),
            divisors)
    {
        YT_VERIFY(lower < upper);
    }

    TQuotientValueGenerator(
        bool signedType,
        TUnversionedValue lower,
        TUnversionedValue upper,
        TRange<ui64> divisors)
        : TQuotientGenerator(
            ValueToUint64(upper, signedType) - ValueToUint64(lower, signedType),
            divisors)
        , Start_(ValueToUint64(lower, signedType))
        , LowerIsNull_(lower.Type == EValueType::Null)
        , UpperIsNull_(upper.Type == EValueType::Null)
        , SignedType_(signedType)
    {
        YT_VERIFY(!UpperIsNull_ || LowerIsNull_);
    }

    TUnversionedValue Reset()
    {
        YT_VERIFY(IsFinished());
        if (!UpperIsNull_) {
            // Start value is used to evaluate initial value for each divisor.
            if (SignedType_) {
                TQuotientGenerator::ResetSigned(Start_);
            } else {
                TQuotientGenerator::ResetUnsigned(Start_);
            }
        }

        if (LowerIsNull_) {
            return MakeUnversionedSentinelValue(EValueType::Null);
        }

        return DoNext();
    }

    TUnversionedValue Next()
    {
        YT_VERIFY(!IsFinished());
        return DoNext();
    }

private:
    const ui64 Start_;
    const bool LowerIsNull_;
    const bool UpperIsNull_;
    const bool SignedType_;

    TUnversionedValue DoNext()
    {
        auto current = Start_ + TQuotientGenerator::Next();
        return SignedType_
            ? MakeUnversionedInt64Value(current)
            : MakeUnversionedUint64Value(current);
    }
};

static ui64 Estimate(ui64 cardinalityMinusOne, TRange<ui64> divisors)
{
    ui64 estimate = 1;
    for (auto divisor : divisors) {
        // Ceil division.
        // Use cardinalityMinusOne + divisor instead of cardinality + divisor - 1.
        estimate = SaturationArithmeticMultiply(
            estimate,
            SaturationArithmeticSum(cardinalityMinusOne, divisor) / divisor);
    }

    return estimate;
}

ui64 GetModuloCardinality(TUnversionedValue value)
{
    YT_VERIFY(IsIntegralType(value.Type));
    return value.Type == EValueType::Uint64
        ? value.Data.Uint64
        : SaturationArithmeticMultiply(std::abs(value.Data.Int64), 2) - 1;
}

template <class TGenerator>
class TRowGenerator
{
public:
    TRowGenerator(TMutableRange<TGenerator> generators, TRange<int> columnIds, TMutableRange<TValue> prefixRow)
        : Generators_(generators)
        , ColumnIds_(columnIds)
        , PrefixRow_(prefixRow)
    {
        Init();
    }

    void Init()
    {
        while (EvalIndex_ < std::ssize(Generators_)) {
            PrefixRow_[ColumnIds_[EvalIndex_]] = Generators_[EvalIndex_].Reset();
            ++EvalIndex_;
        }
    }

    bool Next()
    {
        while (EvalIndex_ > 0) {
            --EvalIndex_;
            auto columnId = ColumnIds_[EvalIndex_];
            auto& generator = Generators_[EvalIndex_];

            if (!generator.IsFinished()) {
                PrefixRow_[columnId] = generator.Next();
                // Suffix is reset here.
                EvalIndex_ = std::ssize(Generators_);
                return true;
            } else {
                PrefixRow_[columnId] = generator.Reset();
            }
        }

        EvalIndex_ = std::ssize(Generators_);
        return false;
    }

private:
    TMutableRange<TGenerator> Generators_;
    TRange<int> ColumnIds_;
    TMutableRange<TValue> PrefixRow_;
    int EvalIndex_ = 0;
};

static ui64 Abs(i64 value)
{
    return value >= 0 ? value : -value;
}

class TDivisorsCollector
{
public:
    TDivisorsCollector(const TKeyColumns& keyColumns, TMutableRange<std::vector<ui64>> referenceIdToDivisors)
        : KeyColumns_(keyColumns)
        , ReferenceIdToDivisors_(referenceIdToDivisors)
    { }

    std::vector<int> GetCounts() const
    {
        std::vector<int> result(std::ssize(ReferenceIdToDivisors_));
        for (int index = 0; index < std::ssize(result); ++index) {
            result[index] = std::ssize(ReferenceIdToDivisors_[index]);
        }
        return result;
    }

    void Collect(TConstExpressionPtr expr)
    {
        if (const auto* referenceExpr = expr->As<TReferenceExpression>()) {
            int keyPartIndex = ColumnNameToKeyPartIndex(KeyColumns_, referenceExpr->ColumnName);

            if (IsIntegralType(referenceExpr->GetWireType()) && keyPartIndex >= 0) {
                ReferenceIdToDivisors_[keyPartIndex].push_back(1);
            }
        } else if (const auto* functionExpr = expr->As<TFunctionExpression>()) {
            for (const auto& argument : functionExpr->Arguments) {
                Collect(argument);
            }
        } else if (const auto* unaryOp = expr->As<TUnaryOpExpression>()) {
            return Collect(unaryOp->Operand);
        } else if (const auto* binaryOp = expr->As<TBinaryOpExpression>()) {
            const auto* referenceExpr = binaryOp->Lhs->As<TReferenceExpression>();
            const auto* literalExpr = binaryOp->Rhs->As<TLiteralExpression>();

            if (referenceExpr &&
                literalExpr &&
                IsIntegralType(binaryOp->GetWireType()))
            {
                int keyPartIndex = ColumnNameToKeyPartIndex(KeyColumns_, referenceExpr->ColumnName);

                if (keyPartIndex >= 0){
                    if (binaryOp->Opcode == EBinaryOp::Divide) {
                        const TUnversionedValue& value = literalExpr->Value;
                        ui64 divisor = binaryOp->GetWireType() == EValueType::Int64
                            ? Abs(value.Data.Int64)
                            : value.Data.Uint64;
                        ReferenceIdToDivisors_[keyPartIndex].push_back(divisor);
                    } else if (binaryOp->Opcode == EBinaryOp::RightShift) {
                        const TUnversionedValue& value = literalExpr->Value;
                        ReferenceIdToDivisors_[keyPartIndex].push_back(1 << value.Data.Uint64);
                    }
                    return;
                }
            }

            Collect(binaryOp->Lhs);
            Collect(binaryOp->Rhs);
        } else if (const auto* inExpr = expr->As<TInExpression>()) {
            for (const auto& argument : inExpr->Arguments) {
                Collect(argument);
            }
        } else if (const auto* betweenExpr = expr->As<TBetweenExpression>()) {
            for (const auto& argument : betweenExpr->Arguments) {
                Collect(argument);
            }
        } else if (const auto* transformExpr = expr->As<TTransformExpression>()) {
            for (const auto& argument : transformExpr->Arguments) {
                Collect(argument);
            }
        } else if (expr->As<TLiteralExpression>()) {
        } else {
            YT_ABORT();
        }
    }

private:
    const TKeyColumns& KeyColumns_;
    // Size equal to key column count.
    TMutableRange<std::vector<ui64>> ReferenceIdToDivisors_;
};

class TComputedColumnsInfo
{
public:
    void Build(const TColumnEvaluator& evaluator,  const TKeyColumns& keyColumns)
    {
        int keyColumnCount = std::ssize(keyColumns);

        ReferenceIdToDivisors_.resize(keyColumnCount);
        TDivisorsCollector collector{keyColumns, ReferenceIdToDivisors_};

        // Collect divisors and modules for each evaluated column.
        for (int columnId = 0; columnId < keyColumnCount; ++columnId) {
            const auto& expression = evaluator.GetExpression(columnId);

            if (!expression) {
                continue;
            }

            EvaluatedExpressionToColumnId_.push_back(columnId);
            collector.Collect(expression);
            DivisorCountsPerEvaluatedColumn_.push_back(collector.GetCounts());

            TUnversionedValue modulo = MakeUnversionedSentinelValue(EValueType::Null);

            auto expr = expression->As<TBinaryOpExpression>();
            if (expr && expr->Opcode == EBinaryOp::Modulo) {
                if (auto literalExpr = expr->Rhs->As<TLiteralExpression>()) {
                    TUnversionedValue value = literalExpr->Value;
                    if (IsIntegralType(value.Type)) {
                        value.Id = columnId;
                        modulo = value;
                    }
                }
            }

            ModuloPerEvaluatedColumn_.push_back(modulo);
        }
    }

protected:
    // Size equal to key column count.
    std::vector<std::vector<ui64>> ReferenceIdToDivisors_;
    // Size equal to evaluated column count in schema.
    std::vector<std::vector<int>> DivisorCountsPerEvaluatedColumn_;
    std::vector<TUnversionedValue> ModuloPerEvaluatedColumn_;
    std::vector<int> EvaluatedExpressionToColumnId_;
};

class TComputedColumnsEvaluator
    : public TComputedColumnsInfo
{
public:
    // Enumeration schema to get key count not greater than desired estimation.

    // Collect according to max estimation.
    // - dividable simple columns and divisors.
    // - modulo evaluatated columns.
    // - computed columns (evaluated but not modulo columns).
    // Calculate estimation during collection.

    TComputedColumnsEvaluator(
        TComputedColumnsInfo computedColumnsInfo,
        const TCompactVector<EValueType, 16> keyTypes,
        bool verboseLogging)
        : TComputedColumnsInfo(std::move(computedColumnsInfo))
        , KeyTypes_(std::move(keyTypes))
        , VerboseLogging_(verboseLogging)
        , LastDivisorCounts_(KeyTypes_.size(), 0)
    { }

    int GetKeyColumnCount() const
    {
        return std::ssize(ReferenceIdToDivisors_);
    }

    ui64 GetEstimation(
        int evaluatedColumnIndex,
        TRange<int> referenceIds,
        TRange<TColumnConstraint> constraints)
    {
        ui64 expressionEstimation = 1;
        for (auto refColumnId : referenceIds) {
            // If column is integer and not divided, then there is single divisor: 1.

            YT_VERIFY(refColumnId < std::ssize(constraints));

            const auto& constraint = constraints[refColumnId];

            if (constraint.IsExact()) {
                continue;
            }

            int divisorsStartIndex = evaluatedColumnIndex > 0
                // Size of DivisorCountsPerEvaluatedColumn[evaluatedColumnIndex - 1] is equal to key column count.
                ? DivisorCountsPerEvaluatedColumn_[evaluatedColumnIndex - 1][refColumnId]
                : 0;
            int divisorsEndIndex = DivisorCountsPerEvaluatedColumn_[evaluatedColumnIndex][refColumnId];

            if (divisorsEndIndex == 0) {
                // Constraint is not integer and not exact. Can not enumerate, skip column.
                // If value is integer it has at least `one` in divisors.
                expressionEstimation = std::numeric_limits<ui64>::max();
                break;
            }

            // IsRange and has divisors.
            YT_VERIFY(divisorsEndIndex > 0 && constraint.IsRange());

            auto divisors = MakeRange(ReferenceIdToDivisors_[refColumnId])
                .Slice(divisorsStartIndex, divisorsEndIndex);

            // Collect unique divisors.
            auto& mergedDivisors = ReferenceIdToDivisorsMerged_[refColumnId];

            auto savedDivisorsCount = mergedDivisors.size();
            LastDivisorCounts_[refColumnId] = savedDivisorsCount;

            for (auto divisor : divisors) {
                auto foundIt = std::find(
                    mergedDivisors.data(),
                    mergedDivisors.data() + savedDivisorsCount,
                    divisor);
                if (foundIt == mergedDivisors.data() + savedDivisorsCount) {
                    mergedDivisors.push_back(divisor);
                }
            }

            YT_VERIFY(IsIntegralType(KeyTypes_[refColumnId]));
            auto signedType = KeyTypes_[refColumnId] == EValueType::Int64;


            auto lowerBoundValue = LowerBoundToValue(constraint.Lower, signedType);
            auto upperBoundValue = UpperBoundToValue(constraint.Upper, signedType);

            // It happens when lower bound is `> N` and upper is `< N + 1`.
            // Also we have to consider converted to integers range with inclusive bounds
            // because otherwise upper bound `<= MAX_INT` could not be expressed.
            if (lowerBoundValue > upperBoundValue) {
                return 0;
            }

            // For full range 0 .. MAX_INT cardinality is MAX_INT + 1 but it can not be represented.
            // For range N .. N cardinality is 1.
            // Cardinality always greater than zero. So we can keep it as cardinalityMinusOne.
            ui64 cardinalityMinusOne =
                ValueToUint64(upperBoundValue, signedType) - ValueToUint64(lowerBoundValue, signedType);

            auto uniqueDivisors = MakeRange(mergedDivisors).Slice(savedDivisorsCount, mergedDivisors.size());
            expressionEstimation = SaturationArithmeticMultiply(
                expressionEstimation,
                Estimate(cardinalityMinusOne, uniqueDivisors));
            YT_VERIFY(expressionEstimation != 0);
        }
        return expressionEstimation;
    }

    void RevertLastDivisors(TRange<int> referenceIds)
    {
        for (auto refColumnId : referenceIds) {
            // If constraint is exact counts are equal.
            ReferenceIdToDivisorsMerged_[refColumnId].resize(LastDivisorCounts_[refColumnId]);
        }
    }

    // Returns estimation and generatable fixed key prefix size.
    // Prefix columns are exact or evaluated (evaluated exactly or by enumerating divisors or modulo).
    // No need to compute columns after prefix.
    // However columns in suffix are considered when computing evaluated columns.
    std::pair<ui64, int> GetEstimationAndEvaluatedColumns(
        const TColumnEvaluator& evaluator,
        TRange<TColumnConstraint> constraints,
        ui64 maxEstimation)
    {
        YT_VERIFY(maxEstimation > 0);

        int keyColumnCount = GetKeyColumnCount();
        YT_VERIFY(std::ssize(constraints) == keyColumnCount);

        ModuloColumnIds_.clear();
        ComputedColumnIds_.clear();

        ReferenceIdToDivisorsMerged_.assign(keyColumnCount, {});

        int columnId = 0;
        int evaluatedColumnIndex = 0;
        ui64 estimation = 1;

        while (columnId < keyColumnCount) {
            // Check if column is evaluated. Evaluate column even if column constraint is exact.
            if (evaluatedColumnIndex < std::ssize(DivisorCountsPerEvaluatedColumn_) &&
                EvaluatedExpressionToColumnId_[evaluatedColumnIndex] == columnId)
            {
                const auto& referenceIds = evaluator.GetReferenceIds(columnId);
                auto expressionEstimation = GetEstimation(evaluatedColumnIndex, referenceIds, constraints);

                // Sometimes estimation can be zero. Skip such constraints.
                // We can not determine if constraint is empty at previous stages because
                // in general case range (a, b) is not empty when a < b. But for integers range (1, 2) is empty.
                if (expressionEstimation == 0) {
                     return {0, 0};
                }

                bool moduloEnumeration = false;
                if (ModuloPerEvaluatedColumn_[evaluatedColumnIndex].Type != EValueType::Null && !constraints[columnId].IsExact()) {
                    auto cardinality = GetModuloCardinality(ModuloPerEvaluatedColumn_[evaluatedColumnIndex]);
                    if (cardinality < expressionEstimation) {
                        YT_VERIFY(cardinality != 0);
                        expressionEstimation = cardinality;
                        moduloEnumeration = true;
                    }
                }

                if (expressionEstimation <= maxEstimation / estimation) {
                    if (moduloEnumeration) {
                        RevertLastDivisors(referenceIds);
                        ModuloColumnIds_.push_back(evaluatedColumnIndex);
                    } else {
                        ComputedColumnIds_.push_back(columnId);
                    }

                    estimation = SaturationArithmeticMultiply(estimation, expressionEstimation);
                    YT_VERIFY(estimation <= maxEstimation);
                } else {
                    RevertLastDivisors(referenceIds);

                    // Continue if column is exact. No need to evaluate computed column in this case.
                    if (!constraints[columnId].IsExact()) {
                        // Column cannot be evaluated and constraint is not exact.
                        break;
                    }
                }

                ++evaluatedColumnIndex;
            } else if (!constraints[columnId].IsExact()) {
                break;
            }
            ++columnId;
        }

        int keyPrefixSize = columnId;

        return {estimation, keyPrefixSize};
    }

    std::vector<TRowRange> EnrichKeyRange(
        const TColumnEvaluator& evaluator,
        TRowBuffer* buffer,
        TRange<TColumnConstraint> constraintRow,
        ui64 rangeExpansionLeft)
    {
        auto [estimation, prefixSize] = GetEstimationAndEvaluatedColumns(
            evaluator,
            constraintRow,
            rangeExpansionLeft);

        if (estimation == 0) {
            return {};
        }

        const int keyColumnCount = GetKeyColumnCount();
        YT_VERIFY(keyColumnCount == std::ssize(constraintRow));

        std::vector<TQuotientValueGenerator> quotientGenerators;
        std::vector<int> quotientColumnIds;

        std::vector<TModuloRangeGenerator> moduloGenerators;
        std::vector<int> moduloColumnIds;

        // Key columns outside of prefix are used to compute evaluated columns.
        auto boundRow = buffer->AllocateUnversioned(keyColumnCount);

        // Fill with sentinel to fail early when reading uninitialized memory.
        for (auto& value : boundRow) {
            value = MakeUnversionedSentinelValue(EValueType::TheBottom);
        }

        for (int columnId = 0; columnId < keyColumnCount; ++columnId) {
            if (constraintRow[columnId].IsExact()) {
                boundRow[columnId] = constraintRow[columnId].GetValue();
            } else if (!ReferenceIdToDivisorsMerged_[columnId].empty()) {
                auto signedType = KeyTypes_[columnId] == EValueType::Int64;

                quotientGenerators.push_back(TQuotientValueGenerator(
                    signedType,
                    constraintRow[columnId].Lower,
                    constraintRow[columnId].Upper,
                    ReferenceIdToDivisorsMerged_[columnId]));
                quotientColumnIds.push_back(columnId);
            }
        }

        for (auto evaluatedColumnIndex : ModuloColumnIds_) {
            auto columnId = EvaluatedExpressionToColumnId_[evaluatedColumnIndex];
            moduloGenerators.push_back(TModuloRangeGenerator(ModuloPerEvaluatedColumn_[evaluatedColumnIndex]));
            moduloColumnIds.push_back(columnId);
        }

        // No range after prefix.
        if (prefixSize < keyColumnCount && constraintRow[prefixSize].IsUniversal()) {
            while (!moduloColumnIds.empty() && moduloColumnIds.back() + 1 == prefixSize) {
                // Skip modulo column if it is last in range.
                --prefixSize;
                moduloGenerators.pop_back();
                moduloColumnIds.pop_back();
            }
        }

        TRowGenerator<TQuotientValueGenerator> rowQuotientGenerator(
            quotientGenerators,
            quotientColumnIds,
            MakeMutableRange(boundRow.Begin(), boundRow.End()));

        TRowGenerator<TModuloRangeGenerator> rowModuloGenerator(
            moduloGenerators,
            moduloColumnIds,
            MakeMutableRange(boundRow.Begin(), boundRow.End()));

        std::vector<TRowRange> resultRanges;
        do {
            bool evaluatedColumnViolatesConstraints = false;

            for (auto columnId : ComputedColumnIds_) {
                evaluator.EvaluateKey(boundRow, buffer, columnId);

                // Check if evaluated value outside constraints.
                if (!TestValue(boundRow[columnId], constraintRow[columnId].Lower, constraintRow[columnId].Upper)) {
                    evaluatedColumnViolatesConstraints = true;
                    break;
                }
            }

            if (evaluatedColumnViolatesConstraints) {
                YT_LOG_DEBUG_IF(VerboseLogging_, "Skipping range (BoundRow: %kv)", boundRow);
                continue;
            }

            do {
                TRowRange rowRange;
                if (prefixSize < keyColumnCount) {
                    // Included/excluded bounds are also considered inside TQuotientValueGenerator.
                    auto lowerBound = MakeLowerBound(
                        buffer,
                        MakeRange(boundRow.Begin(), prefixSize),
                        constraintRow[prefixSize].Lower);
                    auto upperBound = MakeUpperBound(
                        buffer,
                        MakeRange(boundRow.Begin(), prefixSize),
                        constraintRow[prefixSize].Upper);
                    rowRange = std::make_pair(lowerBound, upperBound);
                } else {
                    rowRange = RowRangeFromPrefix(buffer, MakeRange(boundRow.Begin(), prefixSize));
                }

                YT_LOG_DEBUG_IF(VerboseLogging_, "Producing range [%kv .. %kv]", rowRange.first, rowRange.second);

                resultRanges.push_back(rowRange);
            } while (rowModuloGenerator.Next());
        } while (rowQuotientGenerator.Next());

        return resultRanges;
    }

private:
    const TCompactVector<EValueType, 16> KeyTypes_;
    bool VerboseLogging_;

    std::vector<int> ModuloColumnIds_;
    std::vector<int> ComputedColumnIds_;

    // Merge divisors during estimation because estimation depends on divisors.
    std::vector<std::vector<ui64>> ReferenceIdToDivisorsMerged_;

    std::vector<int> LastDivisorCounts_;
};

std::vector<TMutableRowRange> CropRanges(
    const TRowRange& keyRange,
    TRange<TRowRange> ranges,
    const TRowBufferPtr& rowBuffer)
{
    auto subrange = CropItems(
        ranges,
        [&] (auto it) {
            return !(keyRange.first < it->second);
        },
        [&] (auto it) {
            return it->first < keyRange.second;
        });

    std::vector<TMutableRowRange> result;
    if (!subrange.Empty()) {
        auto lower = std::max(subrange.Front().first, keyRange.first);
        auto upper = std::min(subrange.Back().second, keyRange.second);

        ForEachRange(subrange, TRowRange(lower, upper), [&] (auto item) {
            auto [lower, upper] = item;
            result.emplace_back(rowBuffer->CaptureRow(lower), rowBuffer->CaptureRow(upper));
        });
    }

    return result;
}

TCompactVector<EValueType, 16> GetKeyTypes(const TTableSchemaPtr& tableSchema)
{
    auto keyColumnCount = tableSchema->GetKeyColumnCount();
    const auto& schemaColumns = tableSchema->Columns();

    TCompactVector<EValueType, 16> keyTypes;
    keyTypes.resize(keyColumnCount);
    // Use raw data pointer because TCompactVector has branch in index operator.
    auto* keyTypesData = keyTypes.data();
    for (int keyColumnIndex = 0; keyColumnIndex < keyColumnCount; ++keyColumnIndex) {
        auto type = schemaColumns[keyColumnIndex].GetWireType();
        keyTypesData[keyColumnIndex] = type;
    }

    return keyTypes;
}

TRangeInferrer CreateNewHeavyRangeInferrer(
    TConstExpressionPtr predicate,
    const TTableSchemaPtr& schema,
    const TKeyColumns& keyColumns,
    const IColumnEvaluatorCachePtr& evaluatorCache,
    const TConstConstraintExtractorMapPtr& constraintExtractors,
    const TQueryOptions& options)
{
    auto buffer = New<TRowBuffer>(TRangeInferrerBufferTag());
    auto keySize = schema->GetKeyColumnCount();

    auto evaluator = evaluatorCache->Find(schema);

    // TODO(savrus): this is a hotfix for YT-2836. Further discussion in YT-2842.
    ui64 moduloExpansion = 1;
    for (int index = 0; index < static_cast<int>(keySize); ++index) {
        if (schema->Columns()[index].Expression()) {
            moduloExpansion *= GetEvaluatedColumnModulo(evaluator->GetExpression(index));
        }
    }

    auto rangeCountLimit = options.RangeExpansionLimit / moduloExpansion;

    TComputedColumnsInfo computedColumnInfos;
    computedColumnInfos.Build(*evaluator, schema->GetKeyColumnNames());

    auto keyTypes = GetKeyTypes(schema);
    TComputedColumnsEvaluator keyEvaluator(std::move(computedColumnInfos), keyTypes, options.VerboseLogging);

    TConstraintsHolder constraints(keyColumns.size());
    auto constraintRef = constraints.ExtractFromExpression(predicate, keyColumns, buffer, constraintExtractors);

    YT_LOG_DEBUG_IF(
        options.VerboseLogging,
        "Predicate %Qv defines key constraints %Qv",
        InferName(predicate),
        ToString(constraints, constraintRef));

    std::vector<TRowRange> enrichedRanges;

    TReadRangesGenerator rangesGenerator(constraints);
    rangesGenerator.GenerateReadRanges(
        constraintRef,
        [&] (TRange<TColumnConstraint> constraintRow, ui64 /*rangeExpansionLimit*/) {
            YT_LOG_DEBUG_IF(
                options.VerboseLogging,
                "ConstraintRow: %v",
                MakeFormattableView(
                    constraintRow,
                    [] (TStringBuilderBase* builder, const TColumnConstraint& constraint) {
                        builder->AppendString("[");
                        FormatValue(builder, constraint.Lower.Value, "%k");
                        builder->AppendString(", ");
                        FormatValue(builder, constraint.Upper.Value, "%k");
                        builder->AppendString("]");
                    }));

            // Use original range expansion limit as workaround for YT-2836, YT-2842.
            auto ranges = keyEvaluator.EnrichKeyRange(
                *evaluator,
                buffer.Get(),
                constraintRow,
                options.RangeExpansionLimit);

            enrichedRanges.insert(enrichedRanges.end(), ranges.begin(), ranges.end());
        },
        rangeCountLimit);

    std::sort(enrichedRanges.begin(), enrichedRanges.end());
    enrichedRanges.erase(
        MergeOverlappingRanges(enrichedRanges.begin(), enrichedRanges.end()),
        enrichedRanges.end());

    for (int index = 0; index + 1 < std::ssize(enrichedRanges); ++index) {
        YT_VERIFY(enrichedRanges[index].second <= enrichedRanges[index + 1].first);
    }

    return [
        enrichedRanges,
        buffer
    ] (const TRowRange& keyRange, const TRowBufferPtr& rowBuffer) mutable {
        return CropRanges(keyRange, enrichedRanges, rowBuffer);
    };
}

TRangeInferrer CreateNewLightRangeInferrer(
    TConstExpressionPtr predicate,
    const TKeyColumns& keyColumns,
    const TConstConstraintExtractorMapPtr& constraintExtractors,
    const TQueryOptions& options)
{
    return [
        predicate,
        keyColumns,
        constraintExtractors,
        options
    ] (const TRowRange& keyRange, const TRowBufferPtr& buffer) {
        TConstraintsHolder constraints(keyColumns.size());
        auto constraintRef = constraints.ExtractFromExpression(predicate, keyColumns, buffer, constraintExtractors);

        YT_LOG_DEBUG_IF(
            options.VerboseLogging,
            "Predicate %Qv defines key constraints %Qv",
            InferName(predicate),
            ToString(constraints, constraintRef));

        std::vector<TRowRange> resultRanges;

        TReadRangesGenerator rangesGenerator(constraints);
        rangesGenerator.GenerateReadRanges(
            constraintRef,
            [&] (TRange<TColumnConstraint> constraintRow, ui64 /*rangeExpansionLimit*/) {
                YT_LOG_DEBUG_IF(
                    options.VerboseLogging,
                    "ConstraintRow: %v",
                    MakeFormattableView(
                        constraintRow,
                        [] (TStringBuilderBase* builder, const TColumnConstraint& constraint) {
                            builder->AppendString("[");
                            FormatValue(builder, constraint.Lower.Value, "%k");
                            builder->AppendString(", ");
                            FormatValue(builder, constraint.Upper.Value, "%k");
                            builder->AppendString("]");
                        }));

                auto boundRow = buffer->AllocateUnversioned(std::ssize(keyColumns));

                int columnId = 0;
                while (columnId < std::ssize(constraintRow) && constraintRow[columnId].IsExact()) {
                    boundRow[columnId] = constraintRow[columnId].GetValue();
                    ++columnId;
                }

                auto prefixSize = columnId;
                auto keyColumnCount = std::ssize(constraintRow);

                TRowRange rowRange;
                if (prefixSize < keyColumnCount) {
                    auto lowerBound = MakeLowerBound(
                        buffer.Get(),
                        MakeRange(boundRow.Begin(), prefixSize),
                        constraintRow[prefixSize].Lower);
                    auto upperBound = MakeUpperBound(
                        buffer.Get(),
                        MakeRange(boundRow.Begin(), prefixSize),
                        constraintRow[prefixSize].Upper);

                    rowRange = std::make_pair(lowerBound, upperBound);
                } else {
                    rowRange = RowRangeFromPrefix(buffer.Get(), MakeRange(boundRow.Begin(), prefixSize));
                }

                if (resultRanges.empty()) {
                    resultRanges.push_back(rowRange);
                    return;
                }

                if (resultRanges.back().second == rowRange.first) {
                    resultRanges.back().second = rowRange.second;
                } else if (resultRanges.back() == rowRange) {
                    // Skip.
                } else if (resultRanges.back().second == rowRange.second) {
                    YT_VERIFY(resultRanges.back().first <= rowRange.first);
                } else {
                    YT_VERIFY(resultRanges.back().second < rowRange.first);
                    resultRanges.push_back(rowRange);
                }
            },
            options.RangeExpansionLimit);

        return CropRanges(keyRange, resultRanges, buffer);
    };
}

TRangeInferrer CreateNewRangeInferrer(
    TConstExpressionPtr predicate,
    const TTableSchemaPtr& schema,
    const TKeyColumns& keyColumns,
    const IColumnEvaluatorCachePtr& evaluatorCache,
    const TConstConstraintExtractorMapPtr& constraintExtractors,
    const TQueryOptions& options)
{
    return schema->HasComputedColumns()
        ? CreateNewHeavyRangeInferrer(
            predicate,
            schema,
            keyColumns,
            evaluatorCache,
            constraintExtractors,
            options)
        : CreateNewLightRangeInferrer(
            predicate,
            keyColumns,
            constraintExtractors,
            options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
