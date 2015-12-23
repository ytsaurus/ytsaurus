#include "column_evaluator.h"
#include "cg_fragment_compiler.h"
#include "config.h"
#include "folding_profiler.h"
#include "query_preparer.h"
#include "query_statistics.h"

#include <yt/core/misc/common.h>
#include <yt/core/misc/sync_cache.h>

namespace NYT {
namespace NQueryClient {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

TColumnEvaluator::TColumnEvaluator(
    const TTableSchema& schema,
    int keySize,
    const IFunctionRegistryPtr functionRegistry)
    : Schema_(schema)
    , KeySize_(keySize)
    , FunctionRegistry_(functionRegistry)
    , Evaluators_(keySize)
    , Variables_(keySize)
    , ReferenceIds_(keySize)
    , Expressions_(keySize)
    , AllLiteralArgs_(keySize)
{ }

void TColumnEvaluator::PrepareEvaluator()
{
    for (size_t index = 0; index < KeySize_; ++index) {
        if (!Schema_.Columns()[index].Expression) {
            continue;
        }
        if (!Evaluators_[index]) {
            yhash_set<Stroka> references;
            Expressions_[index] = PrepareExpression(
                Schema_.Columns()[index].Expression.Get(),
                Schema_,
                FunctionRegistry_);
            Evaluators_[index] = Profile(
                Expressions_[index],
                Schema_,
                nullptr,
                &Variables_[index],
                &references,
                &AllLiteralArgs_[index],
                FunctionRegistry_)();

            for (const auto& reference : references) {
                ReferenceIds_[index].push_back(Schema_.GetColumnIndexOrThrow(reference));
            }
            std::sort(ReferenceIds_[index].begin(), ReferenceIds_[index].end());
        }
    }
}

void TColumnEvaluator::EvaluateKey(TRow fullRow, const TRowBufferPtr& buffer, int index) const
{
    YCHECK(index < fullRow.GetCount());

    TQueryStatistics statistics;
    TExecutionContext executionContext;
    executionContext.Schema = &Schema_;
    executionContext.LiteralRows = &Variables_[index].LiteralRows;
    executionContext.PermanentBuffer = buffer;
    executionContext.OutputBuffer = buffer;
    executionContext.IntermediateBuffer = buffer;
    executionContext.Statistics = &statistics;
#ifndef NDEBUG
    int dummy;
    executionContext.StackSizeGuardHelper = reinterpret_cast<size_t>(&dummy);
#endif

    std::vector<TFunctionContext*> functionContexts;
    for (auto& literalArgs : AllLiteralArgs_[index]) {
        executionContext.FunctionContexts.emplace_back(std::move(literalArgs));
    }
    for (auto& functionContext : executionContext.FunctionContexts) {
        functionContexts.push_back(&functionContext);
    }

    Evaluators_[index](
        &fullRow[index],
        fullRow,
        const_cast<TRowBuilder&>(Variables_[index].ConstantsRowBuilder).GetRow(),
        &executionContext,
        &functionContexts[0]);

    fullRow[index].Id = index;
}

void TColumnEvaluator::EvaluateKeys(TRow fullRow, const TRowBufferPtr& buffer) const
{
    for (int index = 0; index < KeySize_; ++index) {
        if (Schema_.Columns()[index].Expression) {
            EvaluateKey(fullRow, buffer, index);
        }
    }
}

TRow TColumnEvaluator::EvaluateKeys(
    TRow partialRow,
    const TRowBufferPtr& buffer,
    const TNameTableToSchemaIdMapping& idMapping) const
{
    bool keyColumnSeen[MaxKeyColumnCount] {};
    int columnCount = 0;

    for (int index = 0; index < partialRow.GetCount(); ++index) {
        int id = partialRow[index].Id;

        if (id < 0 || id >= idMapping.size()) {
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
    auto fullRow = TUnversionedRow::Allocate(buffer->GetPool(), columnCount);

    for (int index = 0; index < KeySize_; ++index) {
        fullRow[index].Type = EValueType::Null;
        fullRow[index].Id = index;
    }

    int dataColumnId = KeySize_;
    for (int index = 0; index < partialRow.GetCount(); ++index) {
        int id = partialRow[index].Id;
        int schemaId = idMapping[id];
        int place = id < KeySize_ ? id : dataColumnId++;
        fullRow[place] = partialRow[index];
        fullRow[place].Id = schemaId;
    }

    EvaluateKeys(fullRow, buffer);
    return fullRow;
}

const std::vector<int>& TColumnEvaluator::GetReferenceIds(int index) const
{
    return ReferenceIds_[index];
}

TConstExpressionPtr TColumnEvaluator::GetExpression(int index) const
{
    return Expressions_[index];
}

////////////////////////////////////////////////////////////////////////////////

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
            evaluator->PrepareEvaluator();

            TryInsert(cachedEvaluator, &cachedEvaluator);
        }

        return cachedEvaluator->GetColumnEvaluator();
    }

private:
    const IFunctionRegistryPtr FunctionRegistry_;
};

////////////////////////////////////////////////////////////////////////////////

TColumnEvaluatorCache::TColumnEvaluatorCache(
    TColumnEvaluatorCacheConfigPtr config,
    const IFunctionRegistryPtr functionRegistry)
    : Impl_(New<TImpl>(std::move(config), functionRegistry))
{ }

TColumnEvaluatorCache::~TColumnEvaluatorCache() = default;

TColumnEvaluatorPtr TColumnEvaluatorCache::Find(
    const TTableSchema& schema,
    int keySize)
{
    return Impl_->Get(schema, keySize);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
