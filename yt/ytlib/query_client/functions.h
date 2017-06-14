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

    size_t GetNormalizedConstraints(
        std::vector<TTypeSet>* typeConstraints,
        std::vector<size_t>* formalArguments,
        TNullable<std::pair<size_t, bool>>* repeatedType) const;

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

    void GetNormalizedConstraints(
        TTypeSet* constraint,
        TNullable<EValueType>* stateType,
        TNullable<EValueType>* resultType,
        const TStringBuf& name) const;

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
    , public std::unordered_map<TString, ITypeInferrerPtr>
{
    const ITypeInferrerPtr& GetFunction(const TString& functionName) const;

};

DEFINE_REFCOUNTED_TYPE(TTypeInferrerMap)

struct TRangeExtractorMap
    : public TRefCounted
    , public std::unordered_map<TString, TRangeExtractor>
{ };

DEFINE_REFCOUNTED_TYPE(TRangeExtractorMap)

struct TFunctionProfilerMap
    : public TRefCounted
    , public std::unordered_map<TString, IFunctionCodegenPtr>
{
    const IFunctionCodegenPtr& GetFunction(const TString& functionName) const;

};

DEFINE_REFCOUNTED_TYPE(TFunctionProfilerMap)

struct TAggregateProfilerMap
    : public TRefCounted
    , public std::unordered_map<TString, IAggregateCodegenPtr>
{
    const IAggregateCodegenPtr& GetAggregate(const TString& functionName) const;

};

DEFINE_REFCOUNTED_TYPE(TAggregateProfilerMap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
