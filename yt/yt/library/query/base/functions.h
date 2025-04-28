#pragma once

#include "public.h"

#include "functions_common.h"
#include "typing.h"

#include <yt/yt/client/table_client/logical_type.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

struct ITypeInferrer
    : public virtual TRefCounted
{
    template <class TDerived>
    const TDerived* As() const
    {
        return dynamic_cast<const TDerived*>(this);
    }

    template <class TDerived>
    TDerived* As()
    {
        return dynamic_cast<TDerived*>(this);
    }
};

DEFINE_REFCOUNTED_TYPE(ITypeInferrer)

////////////////////////////////////////////////////////////////////////////////

class TFunctionTypeInferrer
    : public ITypeInferrer
{
public:
    TFunctionTypeInferrer(
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgumentType,
        TType resultType);

    TFunctionTypeInferrer(
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType resultType);

    TFunctionTypeInferrer(
        std::vector<TType> argumentTypes,
        TType resultType);

    int GetNormalizedConstraints(
        std::vector<TTypeSet>* typeConstraints,
        std::vector<int>* formalArguments,
        std::optional<std::pair<int, bool>>* repeatedType) const;

    std::vector<TTypeId> InferTypes(TTypingCtx* typingCtx, TRange<TLogicalTypePtr> argumentTypes, TStringBuf name) const;

private:
    const std::unordered_map<TTypeParameter, TUnionType> TypeParameterConstraints_;
    const std::vector<TType> ArgumentTypes_;
    const TType RepeatedArgumentType_;
    const TType ResultType_;

    TTypingCtx::TFunctionSignature GetSignature(TTypingCtx* typingCtx, int argumentCount) const;
};

class TAggregateFunctionTypeInferrer
    : public ITypeInferrer
{
public:
    TAggregateFunctionTypeInferrer() = default;

    TAggregateFunctionTypeInferrer(
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType stateType,
        TType resultType);

    TAggregateFunctionTypeInferrer(
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        TType argumentType,
        TType stateType,
        TType resultType)
        : TAggregateFunctionTypeInferrer(
            std::move(typeParameterConstraints),
            std::vector<TType>{argumentType},
            stateType,
            resultType)
    { }

    std::pair<int, int> GetNormalizedConstraints(
        std::vector<TTypeSet>* typeConstraints,
        std::vector<int>* argumentConstraintIndexes) const;

    virtual std::vector<TTypeId> InferTypes(TTypingCtx* typingCtx, TRange<TLogicalTypePtr> argumentTypes, TStringBuf name) const;

private:
    const std::unordered_map<TTypeParameter, TUnionType> TypeParameterConstraints_;
    const std::vector<TType> ArgumentTypes_;
    const TType StateType_;
    const TType ResultType_;

    TTypingCtx::TFunctionSignature GetSignature(TTypingCtx* typingCtx) const;
};

class TArrayAggTypeInferrer
    : public TAggregateFunctionTypeInferrer
{
public:
    using TAggregateFunctionTypeInferrer::TAggregateFunctionTypeInferrer;

    std::vector<TTypeId> InferTypes(TTypingCtx* typingCtx, TRange<TLogicalTypePtr> argumentTypes, TStringBuf /*name*/) const override;
};

////////////////////////////////////////////////////////////////////////////////

struct TTypeInferrerMap
    : public TRefCounted
    , public std::unordered_map<std::string, ITypeInferrerPtr>
{
    const ITypeInferrerPtr& GetFunction(const std::string& functionName) const;
};

DEFINE_REFCOUNTED_TYPE(TTypeInferrerMap)

////////////////////////////////////////////////////////////////////////////////

bool IsUserCastFunction(const std::string& name);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
