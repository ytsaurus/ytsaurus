#include "stdafx.h"

#include "private.h"
#include "range_inferrer.h"
#include "plan_helpers.h"
#include "key_trie.h"

#ifdef YT_USE_LLVM
#include "folding_profiler.h"
#endif

#include <yt/core/misc/ref_counted.h>

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

class TModuloRangeGenerator
{
public:
    TModuloRangeGenerator(TUnversionedValue modulo)
        : Value_(modulo)
    {
        Modulo_ = (Value_.Type == EValueType::Uint64)
            ? modulo.Data.Uint64
            : std::abs(modulo.Data.Int64);
        Reset();
    }

    ui64 Count()
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
        Profile(predicate, schema, nullptr, nullptr, &references, functionRegistry);
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
        bool rebuildRanges = false;
        RangeExpansionLeft_ = (RangeExpansionLimit_ > ranges.size())
            ? RangeExpansionLimit_ - ranges.size()
            : 0;

        for (auto& range : ranges) {
            rebuildRanges |= EnrichKeyRange(range, enrichedRanges);
        }

        if (rebuildRanges) {
            std::sort(enrichedRanges.begin(), enrichedRanges.end());
            int last = 0;
            for (int index = 1; index < enrichedRanges.size(); ++index) {
                if (enrichedRanges[index].first < enrichedRanges[last].second) {
                    enrichedRanges[last].second = enrichedRanges[index].second;
                } else {
                    ++last;
                    if (last < index) {
                        enrichedRanges[last] = enrichedRanges[index];
                    }
                }
            }
            enrichedRanges.resize(last + 1);
        }

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

    bool IsUserColumn(int index, const std::pair<TRow, TRow>& range)
    {
        return SchemaToDepletedMapping_[index] != -1;
    }

    TNullable<int> IsExactColumn(int index, const std::pair<TRow, TRow>& range, int depletedPrefixSize)
    {
        int maxReferenceIndex = 0;

        for (int referenceIndex : Evaluator_->GetReferenceIds(index)) {
            int depletedIndex = SchemaToDepletedMapping_[referenceIndex];
            maxReferenceIndex = std::max(maxReferenceIndex, referenceIndex);

            if (depletedIndex >= depletedPrefixSize
                || depletedIndex == -1
                || IsSentinelType(range.first[depletedIndex].Type)
                || IsSentinelType(range.second[depletedIndex].Type)
                || range.first[depletedIndex] != range.second[depletedIndex])
            {
                return TNullable<int>();
            }
        }
        return maxReferenceIndex;
    }

    TNullable<TModuloRangeGenerator> IsModuloColumn(int index)
    {
        auto expr = Evaluator_->GetExpression(index)->As<TBinaryOpExpression>();
        if (expr && expr->Opcode == EBinaryOp::Modulo) {
            if (auto literalExpr = expr->Rhs->As<TLiteralExpression>()) {
                TUnversionedValue value = literalExpr->Value;
                if (value.Type == EValueType::Int64 || value.Type == EValueType::Uint64) {
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

    TRow Copy(TRow source)
    {
        auto row = TUnversionedRow::Allocate(Buffer_.GetAlignedPool(), source.GetCount());
        for (int index = 0; index < source.GetCount(); ++index) {
            row[index] = source[index];
        }
        return row;
    }

    bool EnrichKeyRange(std::pair<TRow, TRow>& range, std::vector<std::pair<TRow, TRow>>& ranges)
    {
        auto lowerSentinel = TrimSentinel(range.first);
        auto upperSentinel = TrimSentinel(range.second);

        int depletedPrefixSize = 0;
        while (depletedPrefixSize < range.first.GetCount()
            && depletedPrefixSize < range.second.GetCount()
            && range.first[depletedPrefixSize] == range.second[depletedPrefixSize])
        {
            ++depletedPrefixSize;
        }

        int shrinkSize = KeySize_;
        int maxReferenceIndex = 0;
        int rangeCount = 1;
        std::vector<TModuloRangeGenerator> moduloGenerators;
        std::vector<int> exactGenerators;

        for (int index = 0; index < KeySize_; ++index) {
            if (IsUserColumn(index, range)) {
                continue;
            } else if (auto lastReference = IsExactColumn(index, range, depletedPrefixSize)) {
                maxReferenceIndex = std::max(maxReferenceIndex, lastReference.Get());
                exactGenerators.push_back(index);
                continue;
            } else if (auto generator = IsModuloColumn(index)) {
                auto count = generator.Get().Count();
                if (count < RangeExpansionLeft_ && rangeCount * count < RangeExpansionLeft_) {
                    rangeCount *= count;
                    moduloGenerators.push_back(generator.Get());
                    continue;
                }
            }
            shrinkSize = index;
            break;
        }

        RangeExpansionLeft_ -= rangeCount;

        auto lowerRow = TUnversionedRow::Allocate(Buffer_.GetAlignedPool(), KeySize_ + 1);
        auto upperRow = TUnversionedRow::Allocate(Buffer_.GetAlignedPool(), KeySize_ + 1);

        int lowerSize = ExpandKey(lowerRow, range.first, std::max(shrinkSize, maxReferenceIndex + 1));
        int upperSize = ExpandKey(upperRow, range.second, std::max(shrinkSize, maxReferenceIndex + 1));

        for (int index : exactGenerators) {
            Evaluator_->EvaluateKey(lowerRow, Buffer_, index);
            upperRow[index] = lowerRow[index];
        }

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

        if (moduloGenerators.empty()) {
            ranges.push_back(std::make_pair(lowerRow, upperRow));
        } else {
            auto yield = [&] () {
                ranges.push_back(std::make_pair(Copy(lowerRow), Copy(upperRow)));
            };
            auto setValue = [&] (TUnversionedValue value) {
                lowerRow[value.Id] = value;
                upperRow[value.Id] = value;
            };

            for (auto& generator : moduloGenerators) {
                setValue(generator.Next());
            }
            yield();

            int generatorIndex = moduloGenerators.size() - 1;
            while (generatorIndex >= 0) {
                if (moduloGenerators[generatorIndex].Finished()) {
                    --generatorIndex;
                } else {
                    setValue(moduloGenerators[generatorIndex].Next());
                    while (generatorIndex + 1 < moduloGenerators.size()) {
                        ++generatorIndex;
                        moduloGenerators[generatorIndex].Reset();
                        setValue(moduloGenerators[generatorIndex].Next());
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
    if (!predicate) {
        return CreateLightRangeInferrer(
            predicate,
            TKeyColumns(),
            functionRegistry,
            verboseLogging);
    }

#ifdef YT_USE_LLVM
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
#else
    return CreateLightRangeInferrer(
        predicate,
        keyColumns,
        functionRegistry,
        verboseLogging);
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

