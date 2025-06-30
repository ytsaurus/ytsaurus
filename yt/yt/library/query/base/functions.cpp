#include "functions.h"

#include <yt/yt/client/table_client/logical_type.h>

#include <library/cpp/yt/misc/variant.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TFunctionTypeInferrer)

class TFunctionTypeInferrer
    : public ITypeInferrer
{
public:
    TFunctionTypeInferrer(
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType repeatedArgumentType,
        TType resultType)
        : TypeParameterConstraints_(std::move(typeParameterConstraints))
        , ArgumentTypes_(std::move(argumentTypes))
        , RepeatedArgumentType_(repeatedArgumentType)
        , ResultType_(resultType)
    { }

    bool IsAggregate() const override
    {
        return false;
    }

    int GetNormalizedConstraints(
        std::vector<TTypeSet>* typeConstraints,
        std::vector<int>* formalArguments,
        std::optional<std::pair<int, bool>>* repeatedType) const override
    {
        std::unordered_map<TTypeParameter, int> idToIndex;

        auto getIndex = [&] (const TType& type) -> int {
            return Visit(type,
                [&] (TTypeParameter genericId) -> int {
                    auto itIndex = idToIndex.find(genericId);
                    if (itIndex != idToIndex.end()) {
                        return itIndex->second;
                    } else {
                        int index = typeConstraints->size();
                        auto it = TypeParameterConstraints_.find(genericId);
                        if (it == TypeParameterConstraints_.end()) {
                            typeConstraints->push_back(TTypeSet({
                                EValueType::Null,
                                EValueType::Int64,
                                EValueType::Uint64,
                                EValueType::Double,
                                EValueType::Boolean,
                                EValueType::String,
                                EValueType::Any,
                            }));
                        } else {
                            typeConstraints->push_back(TTypeSet(it->second.begin(), it->second.end()));
                        }
                        idToIndex.emplace(genericId, index);
                        return index;
                    }
                },
                [&] (EValueType fixedType) -> int {
                    int index = typeConstraints->size();
                    typeConstraints->push_back(TTypeSet({fixedType}));
                    return index;
                },
                [&] (const TUnionType& unionType) -> int {
                    int index = typeConstraints->size();
                    typeConstraints->push_back(TTypeSet(unionType.begin(), unionType.end()));
                    return index;
                });
        };

        for (const auto& argumentType : ArgumentTypes_) {
            formalArguments->push_back(getIndex(argumentType));
        }

        if (!(std::holds_alternative<EValueType>(RepeatedArgumentType_) &&
            std::get<EValueType>(RepeatedArgumentType_) == EValueType::Null))
        {
            *repeatedType = std::pair(
                getIndex(RepeatedArgumentType_),
                std::get_if<TUnionType>(&RepeatedArgumentType_));
        }

        return getIndex(ResultType_);
    }

    std::vector<TTypeId> InferTypes(
        TTypingCtx* typingCtx,
        TRange<TLogicalTypePtr> argumentTypes,
        TStringBuf name) const override
    {
        std::vector<TTypeId> argumentTypeIds;
        for (auto type : argumentTypes) {
            argumentTypeIds.push_back(typingCtx->GetTypeId(GetWireType(type)));
        }

        auto signature = GetSignature(typingCtx, std::ssize(argumentTypes));

        return typingCtx->InferFunctionType(name, {signature}, argumentTypeIds);
    }

    std::pair<int, int> GetNormalizedConstraints(
        std::vector<TTypeSet>* /*typeConstraints*/,
        std::vector<int>* /*argumentConstraintIndexes*/) const override
    {
        YT_ABORT();
    }

private:
    const std::unordered_map<TTypeParameter, TUnionType> TypeParameterConstraints_;
    const std::vector<TType> ArgumentTypes_;
    const TType RepeatedArgumentType_;
    const TType ResultType_;

    TTypingCtx::TFunctionSignature GetSignature(TTypingCtx* typingCtx, int argumentCount) const
    {
        TTypingCtx::TFunctionSignature signature({});
        int nextGenericId = 0;

        auto registerConstraints = [&] (int id, const TUnionType& unionType) {
            if (id >= std::ssize(signature.Constraints)) {
                signature.Constraints.resize(id + 1);
            }
            for (auto type : unionType) {
                signature.Constraints[id].push_back(typingCtx->GetTypeId(type));
            }
        };

        Visit(ResultType_,
            [&] (TTypeParameter genericId) {
                signature.Types.push_back(-(1 + genericId));
                nextGenericId = std::min(nextGenericId, genericId + 1);
            },
            [&] (EValueType fixedType) {
                signature.Types.push_back(typingCtx->GetTypeId(fixedType));
            },
            [&] (const TUnionType& /*unionType*/) {
                THROW_ERROR_EXCEPTION("Result type cannot be union");
            });

        for (const auto& formalArgument : ArgumentTypes_) {
            Visit(formalArgument,
                [&] (TTypeParameter genericId) {
                    signature.Types.push_back(-(1 + genericId));
                    nextGenericId = std::min(nextGenericId, genericId + 1);
                },
                [&] (EValueType fixedType) {
                    signature.Types.push_back(typingCtx->GetTypeId(fixedType));
                },
                [&] (const TUnionType& unionType) {
                    signature.Types.push_back(-(1 + nextGenericId));
                    registerConstraints(nextGenericId, unionType);
                    ++nextGenericId;
                });
        }

        if (!(std::holds_alternative<EValueType>(RepeatedArgumentType_) &&
            std::get<EValueType>(RepeatedArgumentType_) == EValueType::Null))
        {
            Visit(RepeatedArgumentType_,
                [&] (TTypeParameter genericId) {
                    for (int i = std::ssize(ArgumentTypes_); i < argumentCount; ++i) {
                        signature.Types.push_back(-(1 + genericId));
                    }
                    nextGenericId = std::min(nextGenericId, genericId + 1);
                },
                [&] (EValueType fixedType) {
                    for (int i = std::ssize(ArgumentTypes_); i < argumentCount; ++i) {
                        signature.Types.push_back(typingCtx->GetTypeId(fixedType));
                    }
                },
                [&] (const TUnionType& unionType) {
                    for (int i = std::ssize(ArgumentTypes_); i < argumentCount; ++i) {
                        signature.Types.push_back(-(1 + nextGenericId));
                        registerConstraints(nextGenericId, unionType);
                        ++nextGenericId;
                    }
                });
        }

        for (const auto& [parameterId, unionType] : TypeParameterConstraints_) {
            registerConstraints(parameterId, unionType);
        }

        return signature;
    }
};

DEFINE_REFCOUNTED_TYPE(TFunctionTypeInferrer)

////////////////////////////////////////////////////////////////////////////////

ITypeInferrerPtr CreateFunctionTypeInferrer(
    TType resultType,
    std::vector<TType> argumentTypes,
    std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
    TType repeatedArgumentType)
{
    return New<TFunctionTypeInferrer>(
        std::move(typeParameterConstraints),
        std::move(argumentTypes),
        std::move(repeatedArgumentType),
        std::move(resultType));
}

////////////////////////////////////////////////////////////////////////////////

class TAggregateFunctionTypeInferrer
    : public ITypeInferrer
{
public:
    TAggregateFunctionTypeInferrer(
        std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints,
        std::vector<TType> argumentTypes,
        TType stateType,
        TType resultType)
        : TypeParameterConstraints_(std::move(typeParameterConstraints))
        , ArgumentTypes_(std::move(argumentTypes))
        , StateType_(stateType)
        , ResultType_(resultType)
    { }

    bool IsAggregate() const override
    {
        return true;
    }

    std::pair<int, int> GetNormalizedConstraints(
        std::vector<TTypeSet>* typeConstraints,
        std::vector<int>* argumentConstraintIndexes) const override
    {
        std::unordered_map<TTypeParameter, int> idToIndex;

        auto getIndex = [&] (const TType& type) -> int {
            return Visit(type,
                [&] (EValueType fixedType) -> int {
                    typeConstraints->push_back(TTypeSet({fixedType}));
                    return typeConstraints->size() - 1;
                },
                [&] (TTypeParameter genericId) -> int {
                    auto itIndex = idToIndex.find(genericId);
                    if (itIndex != idToIndex.end()) {
                        return itIndex->second;
                    } else {
                        int index = typeConstraints->size();
                        auto it = TypeParameterConstraints_.find(genericId);
                        if (it == TypeParameterConstraints_.end()) {
                            typeConstraints->push_back(TTypeSet({
                                EValueType::Null,
                                EValueType::Int64,
                                EValueType::Uint64,
                                EValueType::Double,
                                EValueType::Boolean,
                                EValueType::String,
                                EValueType::Any,
                            }));
                        } else {
                            typeConstraints->push_back(TTypeSet(it->second.begin(), it->second.end()));
                        }
                        idToIndex.emplace(genericId, index);
                        return index;
                    }
                },
                [&] (const TUnionType& unionType) -> int {
                    typeConstraints->push_back(TTypeSet(unionType.begin(), unionType.end()));
                    return typeConstraints->size() - 1;
                });
        };

        for (const auto& argumentType : ArgumentTypes_) {
            argumentConstraintIndexes->push_back(getIndex(argumentType));
        }

        return std::pair(getIndex(StateType_), getIndex(ResultType_));
    }

    std::vector<TTypeId> InferTypes(
        TTypingCtx* typingCtx,
        TRange<TLogicalTypePtr> argumentTypes,
        TStringBuf name) const override
    {
        std::vector<TTypeId> argumentTypeIds;
        for (auto type : argumentTypes) {
            argumentTypeIds.push_back(typingCtx->GetTypeId(GetWireType(type)));
        }

        auto signature = GetSignature(typingCtx);

        // TODO: Argument types and additional types (result, state)
        // Return two vectors?
        return typingCtx->InferFunctionType(name, {signature}, argumentTypeIds, 2);
    }

    int GetNormalizedConstraints(
        std::vector<TTypeSet>* /*typeConstraints*/,
        std::vector<int>* /*formalArguments*/,
        std::optional<std::pair<int, bool>>* /*repeatedType*/) const override
    {
        YT_ABORT();
    }

private:
    const std::unordered_map<TTypeParameter, TUnionType> TypeParameterConstraints_;
    const std::vector<TType> ArgumentTypes_;
    const TType StateType_;
    const TType ResultType_;

    TTypingCtx::TFunctionSignature GetSignature(TTypingCtx* typingCtx) const
    {
        TTypingCtx::TFunctionSignature signature({});
        int nextGenericId = 0;

        auto registerConstraints = [&] (int id, const TUnionType& unionType) {
            if (id >= std::ssize(signature.Constraints)) {
                signature.Constraints.resize(id + 1);
            }
            for (auto type : unionType) {
                signature.Constraints[id].push_back(typingCtx->GetTypeId(type));
            }
        };

        Visit(ResultType_,
            [&] (TTypeParameter genericId) {
                signature.Types.push_back(-(1 + genericId));
                nextGenericId = std::min(nextGenericId, genericId + 1);
            },
            [&] (EValueType fixedType) {
                signature.Types.push_back(typingCtx->GetTypeId(fixedType));
            },
            [&] (const TUnionType& /*unionType*/) {
                THROW_ERROR_EXCEPTION("Result type cannot be union");
            });

        Visit(StateType_,
            [&] (TTypeParameter genericId) {
                signature.Types.push_back(-(1 + genericId));
                nextGenericId = std::min(nextGenericId, genericId + 1);
            },
            [&] (EValueType fixedType) {
                signature.Types.push_back(typingCtx->GetTypeId(fixedType));
            },
            [&] (const TUnionType& /*unionType*/) {
                THROW_ERROR_EXCEPTION("State type cannot be union");
            });

        for (const auto& formalArgument : ArgumentTypes_) {
            Visit(formalArgument,
                [&] (TTypeParameter genericId) {
                    signature.Types.push_back(-(1 + genericId));
                    nextGenericId = std::min(nextGenericId, genericId + 1);
                },
                [&] (EValueType fixedType) {
                    signature.Types.push_back(typingCtx->GetTypeId(fixedType));
                },
                [&] (const TUnionType& unionType) {
                    signature.Types.push_back(-(1 + nextGenericId));
                    registerConstraints(nextGenericId, unionType);
                    ++nextGenericId;
                });
        }

        for (const auto& [parameterId, unionType] : TypeParameterConstraints_) {
            registerConstraints(parameterId, unionType);
        }

        return signature;
    }
};

////////////////////////////////////////////////////////////////////////////////

ITypeInferrerPtr CreateAggregateTypeInferrer(
    TType resultType,
    std::vector<TType> argumentTypes,
    TType stateType,
    std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints)
{
    return New<TAggregateFunctionTypeInferrer>(
        std::move(typeParameterConstraints),
        std::move(argumentTypes),
        std::move(stateType),
        std::move(resultType));
}

ITypeInferrerPtr CreateAggregateTypeInferrer(
    TType resultType,
    TType argumentType,
    TType stateType,
    std::unordered_map<TTypeParameter, TUnionType> typeParameterConstraints)
{
    return New<TAggregateFunctionTypeInferrer>(
        std::move(typeParameterConstraints),
        std::vector<TType>{std::move(argumentType)},
        std::move(stateType),
        std::move(resultType));
}

////////////////////////////////////////////////////////////////////////////////

class TArrayAggTypeInferrer
    : public TAggregateFunctionTypeInferrer
{
public:
    TArrayAggTypeInferrer()
        : TAggregateFunctionTypeInferrer(
            /*typeParameterConstraints*/ {},
            {
                TUnionType{
                    EValueType::String,
                    EValueType::Uint64,
                    EValueType::Int64,
                    EValueType::Double,
                    EValueType::Boolean,
                    EValueType::Any,
                    EValueType::Composite,
                },
                EValueType::Boolean,
            },
            EValueType::String,
            EValueType::Any)
    { }

    std::vector<TTypeId> InferTypes(
        TTypingCtx* typingCtx,
        TRange<TLogicalTypePtr> argumentTypes,
        TStringBuf name) const override
    {
        THROW_ERROR_EXCEPTION_UNLESS(argumentTypes.size() == 2,
            "Expected two arguments for %Qv function, got %v",
            name,
            argumentTypes.size());

        return {
            typingCtx->GetTypeId(ListLogicalType(argumentTypes[0])),
            typingCtx->GetTypeId(EValueType::String),
            typingCtx->GetTypeId(argumentTypes[0]),
            typingCtx->GetTypeId(EValueType::Boolean),
        };
    }

    int GetNormalizedConstraints(
        std::vector<TTypeSet>* /*typeConstraints*/,
        std::vector<int>* /*formalArguments*/,
        std::optional<std::pair<int, bool>>* /*repeatedType*/) const override
    {
        YT_ABORT();
    }
};


////////////////////////////////////////////////////////////////////////////////

class TDummyTypeInferrer
    : public ITypeInferrer
{
public:
    TDummyTypeInferrer(std::string name, bool aggregate, bool supportedInV1, bool supportedInV2)
        : Name_(std::move(name))
        , Aggregate_(aggregate)
        , SupportedInV1_(supportedInV1)
        , SupportedInV2_(supportedInV2)
    { }

    bool IsAggregate() const override
    {
        return Aggregate_;
    }

    [[noreturn]] int GetNormalizedConstraints(
        std::vector<TTypeSet>* /*typeConstraints*/,
        std::vector<int>* /*formalArguments*/,
        std::optional<std::pair<int, bool>>* /*repeatedType*/) const override
    {
        THROW_ERROR_EXCEPTION_UNLESS(SupportedInV1_, "Function %Qv is not supported in expression builder v1",
            Name_);
        YT_ABORT();
    }

    [[noreturn]] std::pair<int, int> GetNormalizedConstraints(
        std::vector<TTypeSet>* /*typeConstraints*/,
        std::vector<int>* /*argumentConstraintIndexes*/) const override
    {
        THROW_ERROR_EXCEPTION_UNLESS(SupportedInV1_, "Function %Qv is not supported in expression builder v1",
            Name_);
        YT_ABORT();
    }

    [[noreturn]] std::vector<TTypeId> InferTypes(
        TTypingCtx* /*typingCtx*/,
        TRange<TLogicalTypePtr> /*argumentTypes*/,
        TStringBuf /*name*/) const override
    {
        THROW_ERROR_EXCEPTION_UNLESS(SupportedInV2_, "Function %Qv is not supported in expression builder v2",
            Name_);
        YT_ABORT();
    }

private:
    const std::string Name_;
    const bool Aggregate_;
    const bool SupportedInV1_;
    const bool SupportedInV2_;
};

////////////////////////////////////////////////////////////////////////////////

ITypeInferrerPtr CreateArrayAggTypeInferrer()
{
    return New<TArrayAggTypeInferrer>();
}

ITypeInferrerPtr CreateDummyTypeInferrer(
    std::string name,
    bool aggregate,
    bool supportedInV1,
    bool supportedInV2)
{
    return New<TDummyTypeInferrer>(std::move(name), aggregate, supportedInV1, supportedInV2);
}

////////////////////////////////////////////////////////////////////////////////

const ITypeInferrerPtr& TTypeInferrerMap::GetFunction(const std::string& functionName) const
{
    auto found = this->find(functionName);
    if (found == this->end()) {
        THROW_ERROR_EXCEPTION("Undefined function %Qv",
            functionName);
    }
    return found->second;
}

////////////////////////////////////////////////////////////////////////////////

bool IsUserCastFunction(const std::string& name)
{
    return
        name == "int64" ||
        name == "uint64" ||
        name == "double" ||
        name == "boolean" ||
        name == "to_any" ||
        name == "cast_operator";
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
