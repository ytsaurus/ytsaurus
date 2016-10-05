#pragma once

#include "public.h"

#include "key_trie.h"
#include "functions_common.h"

namespace NYT {
namespace NQueryClient {

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

struct TFunctionTypeInferrer
    : public ITypeInferrer
{
public:
    TFunctionTypeInferrer(
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgumentType,
        TType resultType)
        : TypeArgumentConstraints_(typeArgumentConstraints)
        , ArgumentTypes_(argumentTypes)
        , RepeatedArgumentType_(repeatedArgumentType)
        , ResultType_(resultType)
    { }

    TFunctionTypeInferrer(
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        std::vector<TType> argumentTypes,
        TType resultType)
        : TFunctionTypeInferrer(typeArgumentConstraints, argumentTypes, EValueType::Null, resultType)
    { }

    TFunctionTypeInferrer(
        std::vector<TType> argumentTypes,
        TType resultType)
        : TFunctionTypeInferrer(std::unordered_map<TTypeArgument, TUnionType>(), argumentTypes, resultType)
    { }

    EValueType InferResultType(
        const std::vector<EValueType>& argumentTypes,
        const Stroka& name,
        const TStringBuf& source) const;

private:
    std::unordered_map<TTypeArgument, TUnionType> TypeArgumentConstraints_;
    std::vector<TType> ArgumentTypes_;
    TType RepeatedArgumentType_;
    TType ResultType_;

};

struct TAggregateTypeInferrer
    : public ITypeInferrer
{
public:
    TAggregateTypeInferrer(
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        TType argumentType,
        TType resultType,
        TType stateType)
        : TypeArgumentConstraints_(typeArgumentConstraints)
        , ArgumentType_(argumentType)
        , ResultType_(resultType)
        , StateType_(stateType)
    { }

    EValueType InferStateType(
        EValueType type,
        const Stroka& aggregateName,
        const TStringBuf& source) const;

    EValueType InferResultType(
        EValueType argumentType,
        const Stroka& aggregateName,
        const TStringBuf& source) const;

private:
    std::unordered_map<TTypeArgument, TUnionType> TypeArgumentConstraints_;
    TType ArgumentType_;
    TType ResultType_;
    TType StateType_;

};

////////////////////////////////////////////////////////////////////////////////

typedef std::function<TKeyTriePtr(
    const TConstFunctionExpressionPtr& expr,
    const TKeyColumns& keyColumns,
    const TRowBufferPtr& rowBuffer)> TRangeExtractor;

////////////////////////////////////////////////////////////////////////////////

struct TTypeInferrerMap
    : public TRefCounted
    , public std::unordered_map<Stroka, ITypeInferrerPtr>
{
    const ITypeInferrerPtr& GetFunction(const Stroka& functionName) const;

};

DEFINE_REFCOUNTED_TYPE(TTypeInferrerMap)

struct TRangeExtractorMap
    : public TRefCounted
    , public std::unordered_map<Stroka, TRangeExtractor>
{ };

DEFINE_REFCOUNTED_TYPE(TRangeExtractorMap)

struct TFunctionProfilerMap
    : public TRefCounted
    , public std::unordered_map<Stroka, IFunctionCodegenPtr>
{
    const IFunctionCodegenPtr& GetFunction(const Stroka& functionName) const;

};

DEFINE_REFCOUNTED_TYPE(TFunctionProfilerMap)

struct TAggregateProfilerMap
    : public TRefCounted
    , public std::unordered_map<Stroka, IAggregateCodegenPtr>
{
    const IAggregateCodegenPtr& GetAggregate(const Stroka& functionName) const;

};

DEFINE_REFCOUNTED_TYPE(TAggregateProfilerMap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT