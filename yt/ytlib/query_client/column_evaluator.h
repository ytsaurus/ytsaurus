#pragma once

#include "public.h"
#include "evaluation_helpers.h"

#include <yt/ytlib/query_client/function_registry.h>

#include <yt/ytlib/table_client/unversioned_row.h>

namespace NYT {
namespace NQueryClient {

////////////////////////////////////////////////////////////////////////////////

class TColumnEvaluator
    : public TRefCounted
{
public:
    static TColumnEvaluatorPtr Create(
        const TTableSchema& schema,
        int keyColumnCount,
        IFunctionRegistryPtr functionRegistry);

    void EvaluateKey(
        TMutableRow fullRow,
        const TRowBufferPtr& buffer,
        int index) const;

    void EvaluateKeys(
        TMutableRow fullRow,
        const TRowBufferPtr& buffer) const;

    TMutableRow EvaluateKeys(
        TMutableRow partialRow,
        const TRowBufferPtr& buffer,
        const NTableClient::TNameTableToSchemaIdMapping& idMapping) const;

    const std::vector<int>& GetReferenceIds(int index) const;
    TConstExpressionPtr GetExpression(int index) const;

    void InitAggregate(
        int schemaId,
        NTableClient::TUnversionedValue* state,
        const TRowBufferPtr& buffer);

    void UpdateAggregate(
        int schemaId,
        NTableClient::TUnversionedValue* result,
        const NTableClient::TUnversionedValue& state,
        const NTableClient::TUnversionedValue& update,
        const TRowBufferPtr& buffer);

    void MergeAggregate(
        int index,
        NTableClient::TUnversionedValue* result,
        const NTableClient::TUnversionedValue& state,
        const NTableClient::TUnversionedValue& mergeeState,
        const TRowBufferPtr& buffer);

    void FinalizeAggregate(
        int index,
        NTableClient::TUnversionedValue* result,
        const NTableClient::TUnversionedValue& state,
        const TRowBufferPtr& buffer);

    DEFINE_BYREF_RO_PROPERTY(TTableSchema, TableSchema);
    DEFINE_BYVAL_RO_PROPERTY(int, KeyColumnCount);

private:
    const IFunctionRegistryPtr FunctionRegistry_;

    std::vector<TCGExpressionCallback> Evaluators_;
    std::vector<TCGVariables> Variables_;
    std::vector<std::vector<int>> ReferenceIds_;
    std::vector<TConstExpressionPtr> Expressions_;
    std::vector<std::vector<std::vector<bool>>> AllLiteralArgs_;
    std::unordered_map<int, TCGAggregateCallbacks> Aggregates_;

    TColumnEvaluator(
        const TTableSchema& schema,
        int keyColumnCount,
        IFunctionRegistryPtr functionRegistry);

    void Prepare();
    void VerifyAggregate(int index);

    DECLARE_NEW_FRIEND();
};

DEFINE_REFCOUNTED_TYPE(TColumnEvaluator);

////////////////////////////////////////////////////////////////////////////////

class TColumnEvaluatorCache
    : public TRefCounted
{
public:
    explicit TColumnEvaluatorCache(
        TColumnEvaluatorCacheConfigPtr config,
        IFunctionRegistryPtr functionRegistry);
    ~TColumnEvaluatorCache();

    TColumnEvaluatorPtr Find(const TTableSchema& schema, int keyColumnCount);

private:
    class TImpl;

    DECLARE_NEW_FRIEND();

    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TColumnEvaluatorCache);

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

