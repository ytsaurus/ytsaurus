#pragma once

#include "public.h"

#ifdef YT_USE_LLVM
#include "cg_types.h"
#endif

#include <ytlib/new_table_client/public.h>
#include <ytlib/new_table_client/unversioned_row.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TColumnEvaluator
    : public TRefCounted
{
public:
    TColumnEvaluator(const TTableSchema& schema, int keySize);

    void EvaluateKey(
        NVersionedTableClient::TUnversionedRow fullRow,
        NVersionedTableClient::TRowBuffer& buffer,
        int keyIndex);

    void EvaluateKeys(
        NVersionedTableClient::TUnversionedRow fullRow,
        NVersionedTableClient::TRowBuffer& buffer);

    void EvaluateKeys(
        NVersionedTableClient::TUnversionedRow fullRow,
        NVersionedTableClient::TRowBuffer& buffer,
        const NVersionedTableClient::TUnversionedRow partialRow,
        const NVersionedTableClient::TNameTableToSchemaIdMapping& idMapping);

private:
    const NVersionedTableClient::TTableSchema Schema_;
    const int KeySize_;

#ifdef YT_USE_LLVM
    std::vector<NQueryClient::TCGExpressionCallback> Evaluators_;
    std::vector<NQueryClient::TCGVariables> Variables_;
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

