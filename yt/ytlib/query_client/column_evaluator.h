#pragma once

#include "public.h"

#ifdef YT_USE_LLVM
#include "cg_types.h"
#endif

#include <ytlib/new_table_client/public.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TColumnEvaluator
    : public TRefCounted
{
public:
    TColumnEvaluator(const TTableSchema& schema, int keySize);

    void EvaluateKey(
        TRow fullRow,
        NVersionedTableClient::TRowBuffer& buffer,
        int keyIndex);

    void EvaluateKeys(
        TRow fullRow,
        NVersionedTableClient::TRowBuffer& buffer);

    void EvaluateKeys(
        TRow fullRow,
        NVersionedTableClient::TRowBuffer& buffer,
        const TRow partialRow,
        const NVersionedTableClient::TNameTableToSchemaIdMapping& idMapping);

private:
    const TTableSchema Schema_;
    const int KeySize_;

#ifdef YT_USE_LLVM
    std::vector<TCGExpressionCallback> Evaluators_;
    std::vector<TCGVariables> Variables_;
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

