#include "stdafx.h"

#include "column_evaluator.h"
#include "config.h"

#ifdef YT_USE_LLVM
#include "cg_fragment_compiler.h"
#include "query_statistics.h"
#include "folding_profiler.h"
#endif

#include <core/misc/sync_cache.h>

namespace NYT {
namespace NQueryClient {

using namespace NVersionedTableClient;

////////////////////////////////////////////////////////////////////////////////

TColumnEvaluator::TColumnEvaluator(
    const TTableSchema& schema,
    int keySize,
    const IFunctionRegistryPtr functionRegistry)
    : Schema_(schema)
    , KeySize_(keySize)
    , FunctionRegistry_(functionRegistry)
#ifdef YT_USE_LLVM
    , Evaluators_(keySize)
    , Variables_(keySize)
    , ReferenceIds_(keySize)
    , Expressions_(keySize)
#endif
{ }

void TColumnEvaluator::PrepareEvaluator(int index)
{
#ifdef YT_USE_LLVM
    YCHECK(index < KeySize_);
    YCHECK(Schema_.Columns()[index].Expression);

    if (!Evaluators_[index]) {
        yhash_set<Stroka> references;
        Expressions_[index] = PrepareExpression(
            Schema_.Columns()[index].Expression.Get(),
            Schema_,
            FunctionRegistry_.Get());
        Evaluators_[index] = Profile(
            Expressions_[index],
            Schema_,
            nullptr,
            &Variables_[index],
            &references,
            FunctionRegistry_)();

        for (const auto& reference : references) {
            ReferenceIds_[index].push_back(Schema_.GetColumnIndexOrThrow(reference));
        }
        std::sort(ReferenceIds_[index].begin(), ReferenceIds_[index].end());
    }
#else
    THROW_ERROR_EXCEPTION("Computed colums require LLVM enabled in build");
#endif
}

void TColumnEvaluator::EvaluateKey(TRow fullRow, TRowBuffer& buffer, int index)
{
    YCHECK(index < fullRow.GetCount());

#ifdef YT_USE_LLVM
    PrepareEvaluator(index);

    TQueryStatistics statistics;
    TExecutionContext executionContext;
    executionContext.Schema = &Schema_;
    executionContext.LiteralRows = &Variables_[index].LiteralRows;
    executionContext.PermanentBuffer = &buffer;
    executionContext.OutputBuffer = &buffer;
    executionContext.IntermediateBuffer = &buffer;
    executionContext.Statistics = &statistics;
#ifndef NDEBUG
    int dummy;
    executionContext.StackSizeGuardHelper = reinterpret_cast<size_t>(&dummy);
#endif

    Evaluators_[index](
        &fullRow[index],
        fullRow,
        Variables_[index].ConstantsRowBuilder.GetRow(),
        &executionContext);

    fullRow[index].Id = index;
#else
    THROW_ERROR_EXCEPTION("Computed colums require LLVM enabled in build");
#endif
}

void TColumnEvaluator::EvaluateKeys(TRow fullRow, TRowBuffer& buffer)
{
    for (int index = 0; index < KeySize_; ++index) {
        if (Schema_.Columns()[index].Expression) {
            EvaluateKey(fullRow, buffer, index);
        }
    }
}

TRow TColumnEvaluator::EvaluateKeys(
    TRowBuffer& buffer,
    const TRow partialRow,
    const TNameTableToSchemaIdMapping& idMapping)
{
    bool keyColumnSeen[MaxKeyColumnCount] {};
    int columnCount = 0;

    for (int index = 0; index < partialRow.GetCount(); ++index) {
        int id = partialRow[index].Id;

        if (id >= idMapping.size()) {
            THROW_ERROR_EXCEPTION("Invalid column id %v, expected in range [0,%v]",
                id,
                idMapping.size() - 1);
        }

        int schemaId = idMapping[id];
        YCHECK(schemaId < Schema_.Columns().size());
        const auto& column = Schema_.Columns()[schemaId];

        if (column.Expression) {
            THROW_ERROR_EXCEPTION(
                "Column %Qv is computed automatically and should not be provided by user",
                column.Name);
        }

        if (schemaId < KeySize_) {
            if (keyColumnSeen[schemaId]) {
                THROW_ERROR_EXCEPTION("Duplicate key component %Qv",
                    column.Name);
            }

            keyColumnSeen[schemaId] = true;
        } else {
            ++columnCount;
        }
    }

    columnCount += KeySize_;
    auto fullRow = TUnversionedRow::Allocate(buffer.GetAlignedPool(), columnCount);

    for (int index = 0; index < KeySize_; ++index) {
        fullRow[index].Type = EValueType::Null;
    }

    int dataColumnId = KeySize_;
    for (int index = 0; index < partialRow.GetCount(); ++index) {
        int id = partialRow[index].Id;
        int schemaId = idMapping[id];

        if (schemaId < KeySize_) {
            fullRow[schemaId] = partialRow[index];
        } else {
            fullRow[dataColumnId] = partialRow[index];
            fullRow[dataColumnId].Id = schemaId;
            ++dataColumnId;
        }
    }

    EvaluateKeys(fullRow, buffer);
    return fullRow;
}

const std::vector<int>& TColumnEvaluator::GetReferenceIds(int index)
{
#ifdef YT_USE_LLVM
    PrepareEvaluator(index);
    return ReferenceIds_[index];
#else
    THROW_ERROR_EXCEPTION("Computed colums require LLVM enabled in build");
#endif
}

TConstExpressionPtr TColumnEvaluator::GetExpression(int index)
{
#ifdef YT_USE_LLVM
    PrepareEvaluator(index);
    return Expressions_[index];
#else
    THROW_ERROR_EXCEPTION("Computed colums require LLVM enabled in build");
#endif
}

////////////////////////////////////////////////////////////////////////////////

#ifdef YT_USE_LLVM

class TCachedColumnEvaluator
    : public TSyncCacheValueBase<llvm::FoldingSetNodeID, TCachedColumnEvaluator>
{
public:
    TCachedColumnEvaluator(
        const llvm::FoldingSetNodeID& id,
        TColumnEvaluatorPtr evaluator)
        : TSyncCacheValueBase(id)
        , Evaluator_(std::move(evaluator))
    { }

    TColumnEvaluatorPtr GetColumnEvaluator()
    {
        return Evaluator_;
    }

private:
    const TColumnEvaluatorPtr Evaluator_;
};

class TColumnEvaluatorCache::TImpl
    : public TSyncSlruCacheBase<llvm::FoldingSetNodeID, TCachedColumnEvaluator>
{
public:
    explicit TImpl(
        TColumnEvaluatorCacheConfigPtr config,
        const IFunctionRegistryPtr functionRegistry)
        : TSyncSlruCacheBase(config->CGCache)
        , FunctionRegistry_(functionRegistry)
    { }

    TColumnEvaluatorPtr Get(const TTableSchema& schema, int keySize)
    {
        llvm::FoldingSetNodeID id;
        Profile(schema, keySize, &id, FunctionRegistry_);

        auto cachedEvaluator = Find(id);
        if (!cachedEvaluator) {
            auto evaluator = New<TColumnEvaluator>(schema, keySize, FunctionRegistry_);
            cachedEvaluator = New<TCachedColumnEvaluator>(id, evaluator);
            TryInsert(cachedEvaluator, &cachedEvaluator);
        }

        return cachedEvaluator->GetColumnEvaluator();
    }

private:
    const IFunctionRegistryPtr FunctionRegistry_;
};

#endif

////////////////////////////////////////////////////////////////////////////////

TColumnEvaluatorCache::TColumnEvaluatorCache(
    TColumnEvaluatorCacheConfigPtr config,
    const IFunctionRegistryPtr functionRegistry)
#ifdef YT_USE_LLVM
    : Impl_(New<TImpl>(std::move(config), functionRegistry))
#endif
{ }

TColumnEvaluatorCache::~TColumnEvaluatorCache() = default;

TColumnEvaluatorPtr TColumnEvaluatorCache::Find(
    const TTableSchema& schema,
    int keySize)
{
#ifdef YT_USE_LLVM
    return Impl_->Get(schema, keySize);
#else
    THROW_ERROR_EXCEPTION("Computed columns are not supported in this build");
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
