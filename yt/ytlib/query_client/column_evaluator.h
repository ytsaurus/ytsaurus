#pragma once

#include "public.h"

#ifdef YT_USE_LLVM
#include "evaluation_helpers.h"
#endif

#include <ytlib/new_table_client/unversioned_row.h>

#include <ytlib/query_client/function_registry.h>

#include <util/generic/hash_set.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TColumnEvaluator
    : public TRefCounted
{
public:
    TColumnEvaluator(
        const TTableSchema& schema,
        int keySize,
        const TFunctionRegistryPtr functionRegistry);

    void EvaluateKey(
        TRow fullRow,
        NVersionedTableClient::TRowBuffer& buffer,
        int index);

    void EvaluateKeys(
        TRow fullRow,
        NVersionedTableClient::TRowBuffer& buffer);

    void EvaluateKeys(
        TRow fullRow,
        NVersionedTableClient::TRowBuffer& buffer,
        const TRow partialRow,
        const NVersionedTableClient::TNameTableToSchemaIdMapping& idMapping);

    const yhash_set<Stroka>& GetReferences(int index);

private:
    void PrepareEvaluator(int index);

    const TTableSchema Schema_;
    const int KeySize_;
    const TFunctionRegistryPtr FunctionRegistry_;

#ifdef YT_USE_LLVM
    std::vector<TCGExpressionCallback> Evaluators_;
    std::vector<TCGVariables> Variables_;
    std::vector<yhash_set<Stroka>> References_;
#endif
};

DEFINE_REFCOUNTED_TYPE(TColumnEvaluator);

////////////////////////////////////////////////////////////////////////////////

class TColumnEvaluatorCache
    : public TRefCounted
{
public:
    explicit TColumnEvaluatorCache(TColumnEvaluatorCacheConfigPtr config);
    ~TColumnEvaluatorCache();

    TColumnEvaluatorPtr Find(const TTableSchema& schema, int keySize);

private:
    class TImpl;

    DECLARE_NEW_FRIEND();

#ifdef YT_USE_LLVM
    const TIntrusivePtr<TImpl> Impl_;
#endif
};

DEFINE_REFCOUNTED_TYPE(TColumnEvaluatorCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

