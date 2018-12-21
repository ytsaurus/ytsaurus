#pragma once

#include "public.h"

#include "key_trie.h"
#include "functions_common.h"

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

struct TFunctionTypeInferrer
    : public ITypeInferrer
{
public:
    TFunctionTypeInferrer(
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgumentType,
        TType resultType)
        : TypeArgumentConstraints_(std::move(typeArgumentConstraints))
        , ArgumentTypes_(std::move(argumentTypes))
        , RepeatedArgumentType_(repeatedArgumentType)
        , ResultType_(resultType)
    { }

    TFunctionTypeInferrer(
        std::unordered_map<TTypeArgument, TUnionType> typeArgumentConstraints,
        std::vector<TType> argumentTypes,
        TType resultType)
        : TFunctionTypeInferrer(
            std::move(typeArgumentConstraints),
            std::move(argumentTypes),
            EValueType::Null,
            resultType)
    { }

    TFunctionTypeInferrer(
        std::vector<TType> argumentTypes,
        TType resultType)
        : TFunctionTypeInferrer(
            std::unordered_map<TTypeArgument, TUnionType>(),
            std::move(argumentTypes),
            resultType)
    { }

    size_t GetNormalizedConstraints(
        std::vector<TTypeSet>* typeConstraints,
        std::vector<size_t>* formalArguments,
        std::optional<std::pair<size_t, bool>>* repeatedType) const;

private:
    const std::unordered_map<TTypeArgument, TUnionType> TypeArgumentConstraints_;
    const std::vector<TType> ArgumentTypes_;
    const TType RepeatedArgumentType_;
    const TType ResultType_;
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
        : TypeArgumentConstraints_(std::move(typeArgumentConstraints))
        , ArgumentType_(argumentType)
        , ResultType_(resultType)
        , StateType_(stateType)
    { }

    void GetNormalizedConstraints(
        TTypeSet* constraint,
        std::optional<EValueType>* stateType,
        std::optional<EValueType>* resultType,
        TStringBuf name) const;

private:
    const std::unordered_map<TTypeArgument, TUnionType> TypeArgumentConstraints_;
    const TType ArgumentType_;
    const TType ResultType_;
    const TType StateType_;
};

////////////////////////////////////////////////////////////////////////////////

using TRangeExtractor = std::function<TKeyTriePtr(
    const TConstFunctionExpressionPtr& expr,
    const TKeyColumns& keyColumns,
    const TRowBufferPtr& rowBuffer)>;

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

bool IsUserCastFunction(const TString& name);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
