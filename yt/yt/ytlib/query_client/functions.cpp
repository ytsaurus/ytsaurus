#include "functions.h"

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

size_t TFunctionTypeInferrer::GetNormalizedConstraints(
    std::vector<TTypeSet>* typeConstraints,
    std::vector<size_t>* formalArguments,
    std::optional<std::pair<size_t, bool>>* repeatedType) const
{
    std::unordered_map<TTypeArgument, size_t> idToIndex;

    auto getIndex = [&] (const TType& type) -> size_t {
        return Visit(type,
            [&] (TTypeArgument genericId) -> size_t {
                auto itIndex = idToIndex.find(genericId);
                if (itIndex != idToIndex.end()) {
                    return itIndex->second;
                } else {
                    size_t index = typeConstraints->size();
                    auto it = TypeArgumentConstraints_.find(genericId);
                    if (it == TypeArgumentConstraints_.end()) {
                        typeConstraints->push_back(TTypeSet({
                            EValueType::Null,
                            EValueType::Int64,
                            EValueType::Uint64,
                            EValueType::Double,
                            EValueType::Boolean,
                            EValueType::String,
                            EValueType::Any}));
                    } else {
                        typeConstraints->push_back(TTypeSet(it->second.begin(), it->second.end()));
                    }
                    idToIndex.emplace(genericId, index);
                    return index;
                }
            },
            [&] (EValueType fixedType) -> size_t {
                size_t index = typeConstraints->size();
                typeConstraints->push_back(TTypeSet({fixedType}));
                return index;
            },
            [&] (const TUnionType& unionType) -> size_t {
                size_t index = typeConstraints->size();
                typeConstraints->push_back(TTypeSet(unionType.begin(), unionType.end()));
                return index;
            });
    };

    for (const auto& argumentType : ArgumentTypes_) {
        formalArguments->push_back(getIndex(argumentType));
    }

    if (!(std::holds_alternative<EValueType>(RepeatedArgumentType_) && std::get<EValueType>(RepeatedArgumentType_) == EValueType::Null)) {
        *repeatedType = std::make_pair(getIndex(RepeatedArgumentType_), std::get_if<TUnionType>(&RepeatedArgumentType_));
    }

    return getIndex(ResultType_);
}

void TAggregateTypeInferrer::GetNormalizedConstraints(
    TTypeSet* constraint,
    std::optional<EValueType>* stateType,
    std::optional<EValueType>* resultType,
    TStringBuf name) const
{
    if (TypeArgumentConstraints_.size() > 1) {
        THROW_ERROR_EXCEPTION("Too many constraints for aggregate function");
    }

    auto setType = [&] (const TType& targetType, bool allowGeneric) -> std::optional<EValueType> {
        if (auto* fixedType = std::get_if<EValueType>(&targetType)) {
            return *fixedType;
        }
        if (allowGeneric) {
            if (auto* typeId = std::get_if<TTypeArgument>(&targetType)) {
                auto found = TypeArgumentConstraints_.find(*typeId);
                if (found != TypeArgumentConstraints_.end()) {
                    return std::nullopt;
                }
            }
        }
        THROW_ERROR_EXCEPTION("Invalid type constraints for aggregate function %Qv", name);
    };

    Visit(ArgumentType_,
        [&] (const TUnionType& unionType) {
            *constraint = TTypeSet(unionType.begin(), unionType.end());
            *resultType = setType(ResultType_, false);
            *stateType = setType(StateType_, false);
        },
        [&] (EValueType fixedType) {
            *constraint = TTypeSet({fixedType});
            *resultType = setType(ResultType_, false);
            *stateType = setType(StateType_, false);
        },
        [&] (TTypeArgument typeId) {
            auto found = TypeArgumentConstraints_.find(typeId);
            if (found == TypeArgumentConstraints_.end()) {
                THROW_ERROR_EXCEPTION("Invalid type constraints for aggregate function %Qv", name);
            }
            *constraint = TTypeSet(found->second.begin(), found->second.end());
            *resultType = setType(ResultType_, true);
            *stateType = setType(StateType_, true);
        });
}

////////////////////////////////////////////////////////////////////////////////

const ITypeInferrerPtr& TTypeInferrerMap::GetFunction(const TString& functionName) const
{
    auto found = this->find(functionName);
    if (found == this->end()) {
        THROW_ERROR_EXCEPTION("Undefined function %Qv",
            functionName);
    }
    return found->second;
}

const IFunctionCodegenPtr& TFunctionProfilerMap::GetFunction(const TString& functionName) const
{
    auto found = this->find(functionName);
    if (found == this->end()) {
        THROW_ERROR_EXCEPTION("Code generator not found for regular function %Qv",
            functionName);
    }
    return found->second;
}

const IAggregateCodegenPtr& TAggregateProfilerMap::GetAggregate(const TString& functionName) const
{
    auto found = this->find(functionName);
    if (found == this->end()) {
        THROW_ERROR_EXCEPTION("Code generator not found for aggregate function %Qv",
            functionName);
    }
    return found->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
