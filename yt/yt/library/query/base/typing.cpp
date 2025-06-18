#include "typing.h"

#include <yt/yt/client/table_client/logical_type.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

TTypingCtx::TTypingCtx()
{
    FillFunctionSignatures();
}

TTypeId TTypingCtx::GetTypeId(TString name, const TLogicalTypePtr& logicalType)
{
    auto [it, emplaced] = UniqueTypes_.emplace(name, std::ssize(UniqueTypes_));
    if (emplaced) {
        TypeNames_.emplace_back(name, logicalType);
    }
    return it->second;
}

TTypeId TTypingCtx::GetTypeId(EValueType wireType)
{
    if (wireType == EValueType::Composite) {
        // No conversion from EValueType::Composite to GetLogicalType.
        wireType = EValueType::Any;
    }

    return GetTypeId(NTableClient::GetLogicalType(wireType));
}

TTypeId TTypingCtx::GetTypeId(ESimpleLogicalValueType type)
{
    return GetTypeId(MakeLogicalType(type, /*required*/ false));
}

TTypeId TTypingCtx::GetTypeId(const TLogicalTypePtr& logicalType)
{
    const auto metatype = logicalType->GetMetatype();
    switch (metatype) {
        case ELogicalMetatype::Simple:
            return GetTypeId(ToString(logicalType->AsSimpleTypeRef().GetElement()), logicalType);
        case ELogicalMetatype::Optional: {
            auto elementTypeId = GetTypeId(logicalType->AsOptionalTypeRef().GetElement());
            return GetTypeId(Format("optional<%v>", elementTypeId), logicalType);
        }
        case ELogicalMetatype::List: {
            auto elementTypeId = GetTypeId(logicalType->AsListTypeRef().GetElement());
            return GetTypeId(Format("list<%v>", elementTypeId), logicalType);
        }
        case ELogicalMetatype::Struct: {
            TStringBuilder stringBuilder;

            stringBuilder.AppendString("struct<");

            bool first = true;
            for (const auto& structItem : logicalType->AsStructTypeRef().GetFields()) {
                if (first) {
                    first = false;
                } else {
                    stringBuilder.AppendChar(';');
                }

                auto elementTypeId = GetTypeId(structItem.Type);
                stringBuilder.AppendFormat("%v=%v", structItem.Name, elementTypeId);
            }

            stringBuilder.AppendChar('>');

            return GetTypeId(stringBuilder.Flush(), logicalType);
        }
        case ELogicalMetatype::Tuple: {
            TStringBuilder stringBuilder;

            stringBuilder.AppendString("tuple<");

            bool first = true;
            for (const auto& element : logicalType->AsTupleTypeRef().GetElements()) {
                if (first) {
                    first = false;
                } else {
                    stringBuilder.AppendChar(';');
                }

                auto elementTypeId = GetTypeId(element);
                stringBuilder.AppendFormat("%v", elementTypeId);
            }

            stringBuilder.AppendChar('>');

            return GetTypeId(stringBuilder.Flush(), logicalType);
        }
        default:
            THROW_ERROR_EXCEPTION("Unsupported logical type")
                << TErrorAttribute("type", ToString(*logicalType));
    }
    Y_UNREACHABLE();
}

EValueType TTypingCtx::GetWireType(TTypeId typeId)
{
    return NTableClient::GetWireType(GetLogicalType(typeId));
}

TLogicalTypePtr TTypingCtx::GetLogicalType(TTypeId typeId)
{
    return TypeNames_[typeId].second;
}

bool TTypingCtx::HasImplicitCast(TTypeId sourceType, TTypeId targetType)
{
    auto nullType = GetTypeId(EValueType::Null);
    auto int64Type = GetTypeId(EValueType::Int64);
    auto uint64Type = GetTypeId(EValueType::Uint64);
    auto doubleType = GetTypeId(EValueType::Double);
    auto anyType = GetTypeId(EValueType::Any);

    return
        sourceType == nullType ||
        targetType == anyType ||
        sourceType == int64Type && targetType == uint64Type ||
        sourceType == int64Type && targetType == doubleType ||
        sourceType == uint64Type && targetType == doubleType;
}

void TTypingCtx::RegisterFunction(std::string name, TFunctionSignatures signatures)
{
    auto [it, emplaced] = Functions_.emplace(name, TFunctionSignatures{});
    it->second.insert(it->second.end(), signatures.begin(), signatures.end());
}

void TTypingCtx::FillFunctionSignatures()
{
    auto nullType = GetTypeId(EValueType::Null);
    auto booleanType = GetTypeId(EValueType::Boolean);
    auto int64Type = GetTypeId(EValueType::Int64);
    auto uint64Type = GetTypeId(EValueType::Uint64);
    auto doubleType = GetTypeId(EValueType::Double);
    auto stringType = GetTypeId(EValueType::String);
    auto anyType = GetTypeId(EValueType::Any);

    // Rule. If function has overload for T1 and T2 and threre are casts from null to T1 and T2, then oveloads are ambiguous.
    // Overload for null must be addeed or implicit casts must be disabled via type paramenters and constraints.

    // Unary.
    for (const auto& name : {
        "+", "-"
    }) {
        TFunctionSignatures signatures;
        signatures.push_back(TFunctionSignature({-1, -1}, {{nullType, int64Type, uint64Type, doubleType}}));
        RegisterFunction(name, std::move(signatures));
    }

    for (const auto& name : {
        "~"
    }) {
        TFunctionSignatures signatures;
        signatures.push_back(TFunctionSignature({-1, -1}, {{nullType, int64Type, uint64Type}}));
        RegisterFunction(name, std::move(signatures));
    }

    for (const auto& name : {
        "NOT"
    }) {
        TFunctionSignatures signatures;
        signatures.push_back(TFunctionSignature({booleanType, booleanType}));
        RegisterFunction(name, std::move(signatures));
    }

    // Binary
    for (const auto& name : {
        "+", "-", "*", "/"
    }) {
        TFunctionSignatures signatures;
        signatures.push_back(TFunctionSignature({-1, -1, -1}, {{nullType, int64Type, uint64Type, doubleType}}));
        RegisterFunction(name, std::move(signatures));
    }

    for (const auto& name : {
        "%"
    }) {
        TFunctionSignatures signatures;
        signatures.push_back(TFunctionSignature({-1, -1, -1}, {{nullType, int64Type, uint64Type}}));
        RegisterFunction(name, std::move(signatures));
    }

    for (const auto& name : {
        "<<", ">>", "&", "|"
    }) {
        TFunctionSignatures signatures;
        signatures.push_back(TFunctionSignature({-1, -1, -1}, {{nullType, int64Type, uint64Type}}));
        RegisterFunction(name, std::move(signatures));
    }


    for (const auto& name : {
        "AND", "OR"
    }) {
        TFunctionSignatures signatures;
        signatures.push_back(TFunctionSignature({booleanType, booleanType, booleanType}));
        RegisterFunction(name, std::move(signatures));
    }

    for (const auto& name : {
        "=", "!=", "<", ">", "<=", ">=",
    }) {
        TFunctionSignatures signatures;
        signatures.push_back(TFunctionSignature({booleanType, -1, -1}, {{nullType, booleanType, int64Type, uint64Type, doubleType, stringType, anyType}}));

        RegisterFunction(name, std::move(signatures));
    }

    // Concatenate
    for (const auto& name : {
        "||", "concat"
    }) {
        TFunctionSignatures signatures;
        signatures.push_back(TFunctionSignature({stringType, stringType, stringType}));

        RegisterFunction(name, std::move(signatures));
    }
}

std::vector<TTypingCtx::TFunctionSignature> TTypingCtx::GetFunctionSignatures(TStringBuf name, int /*argCount*/)
{
    auto foundIt = Functions_.find(name);
    if (foundIt != Functions_.end()) {
        return foundIt->second;
    }

    THROW_ERROR_EXCEPTION("No known function")
        << TErrorAttribute("name", name);
}

// Additional types are 0 in case no result type.
// 1 for result type.
// 2 for result and state types.
std::vector<TTypeId> TTypingCtx::InferFunctionType(
    TStringBuf name,
    std::vector<TFunctionSignature> signatures,
    TRange<TTypeId> argumentTypes,
    int additionalTypes)
{
    int bestScore = std::numeric_limits<int>::max();
    std::vector<std::pair<int, std::vector<TTypeId>>> matches;

    auto matchSignature = [&] (int signatureIndex) {
        const auto& signature = signatures[signatureIndex];

        auto matchConstraint = [&] (int constraintId, int argumentType) {
            if (constraintId >= std::ssize(signature.Constraints)) {
                // No constraint.
                return true;
            }

            auto it = std::find(
                signature.Constraints[constraintId].begin(),
                signature.Constraints[constraintId].end(),
                argumentType);
            return it != signature.Constraints[constraintId].end();
        };

        int nonParametricCoercionCount = 0;

        std::vector<std::vector<TTypeId>> parametricAssignments;

        for (int argIndex = 0; argIndex < std::ssize(argumentTypes); ++argIndex) {
            auto formalTypeId = signature.Types[argIndex + additionalTypes];
            auto argumentType = argumentTypes[argIndex];

            // Parametric type.
            if (formalTypeId < 0) {
                if (std::ssize(parametricAssignments) < -formalTypeId) {
                    parametricAssignments.resize(-formalTypeId);
                }

                if (matchConstraint(-(formalTypeId + 1), argumentType)) {
                    parametricAssignments[-(formalTypeId + 1)].push_back(argumentType);
                }
                continue;
            }

            if (argumentType != formalTypeId) {
                if (HasImplicitCast(argumentType, formalTypeId)) {
                    ++nonParametricCoercionCount;
                } else {
                    return;
                }
            }
        }

        for (auto& assignments : parametricAssignments) {
            if (assignments.empty()) {
                return;
            }
            std::sort(assignments.begin(), assignments.end());
            assignments.erase(std::unique(assignments.begin(), assignments.end()), assignments.end());
        }

        std::vector<int> parameterIndexes(std::ssize(parametricAssignments), 0);

        auto matchParametricSignature = [&] () {
            int coercionCount = nonParametricCoercionCount;

            std::vector<TTypeId> parameters;
            for (int index = 0; index < std::ssize(parametricAssignments); ++index) {
                parameters.push_back(parametricAssignments[index][parameterIndexes[index]]);
            }

            for (int argIndex = 0; argIndex < std::ssize(argumentTypes); ++argIndex) {
                auto formalTypeId = signatures[signatureIndex].Types[argIndex + additionalTypes];

                if (formalTypeId >= 0) {
                    continue;
                }

                // Parametric type.
                formalTypeId = parameters[-(formalTypeId + 1)];

                if (argumentTypes[argIndex] != formalTypeId) {
                    if (HasImplicitCast(argumentTypes[argIndex], formalTypeId)) {
                        ++coercionCount;
                    } else {
                        return;
                    }
                }
            }

            if (coercionCount < bestScore) {
                matches.clear();
                matches.emplace_back(signatureIndex, std::move(parameters));
                bestScore = coercionCount;
            } else if (coercionCount == bestScore) {
                matches.emplace_back(signatureIndex, std::move(parameters));
            }
        };

        while (true) {
            matchParametricSignature();

            int index = 0;
            while (true) {
                if (index >= std::ssize(parameterIndexes)) {
                    return;
                }

                parameterIndexes[index]++;
                if (parameterIndexes[index] < std::ssize(parametricAssignments[index])) {
                    break;
                } else {
                    parameterIndexes[index] = 0;
                    ++index;
                }
            }
        }
    };

    for (int signatureIndex = 0; signatureIndex < std::ssize(signatures); ++signatureIndex) {
        if (std::ssize(argumentTypes) + additionalTypes != std::ssize(signatures[signatureIndex].Types)) {
            // Argument count does not match signature.
            continue;
        }
        matchSignature(signatureIndex);
    }

    auto typeFormatter = [&] (auto* builder, TTypeId typeId) {
        if (typeId >= 0) {
            builder->AppendFormat("%v", *GetLogicalType(typeId));
        } else {
            builder->AppendFormat("%v", -typeId - 1);
        }
    };

    auto signatureFormatter = [&] (auto* builder, const auto& signature) {
        auto arguments = TRange(signature.Types).Slice(additionalTypes, signature.Types.size());
        auto results = TRange(signature.Types).Slice(0, additionalTypes);

        builder->AppendFormat("Signature: %v -> %v, Constraints: %v",
            MakeFormattableView(arguments, typeFormatter),
            MakeFormattableView(results, typeFormatter),
            MakeFormattableView(signature.Constraints, [&] (auto* builder, const auto& typeIds) {
                builder->AppendFormat("%v", MakeFormattableView(typeIds, typeFormatter));
            }));
    };

    if (matches.empty()) {
        // Use %Qv to match errors in unittests.
        THROW_ERROR_EXCEPTION("No matching function %Qv", name)
            << TErrorAttribute("signatures", Format("%v", MakeFormattableView(signatures, signatureFormatter)))
            << TErrorAttribute("argument_types", Format("%v", MakeFormattableView(argumentTypes, [&] (auto* builder, const auto& typeId) {
                builder->AppendFormat("%v", *GetLogicalType(typeId));
            })));
    } else if (std::ssize(matches) == 1) {
        const auto& [signatureIndex, parameters] = matches.front();

        auto result = signatures[signatureIndex].Types;

        for (int index = 0; index < std::ssize(result); ++index) {
            auto formalTypeId = result[index];

            if (formalTypeId < 0) {
                result[index] = parameters[-(formalTypeId + 1)];
            }
        }

        return result;
    } else {
        THROW_ERROR_EXCEPTION("Ambiguous resolution for function %Qv", name)
            << TErrorAttribute("signatures", Format("%v", MakeFormattableView(signatures, signatureFormatter)))
            << TErrorAttribute("matches", Format("%v", MakeFormattableView(matches, [&] (auto* builder, const auto& match) {
                builder->AppendFormat("SignatureIndex: %v, Types: %v", match.first, MakeFormattableView(match.second, [&] (auto* builder, const auto& typeId) {
                    builder->AppendFormat("%v", *GetLogicalType(typeId));
                }));
            })))
            << TErrorAttribute("argument_types", Format("%v", MakeFormattableView(argumentTypes, [&] (auto* builder, const auto& typeId) {
                builder->AppendFormat("%v", *GetLogicalType(typeId));
            })));
    }
}

// Returns result types and argument types coercions.
std::vector<TTypeId> TTypingCtx::InferFunctionType(TStringBuf name, TRange<TTypeId> argumentTypes)
{
    auto signatures = GetFunctionSignatures(name, std::ssize(argumentTypes));
    return InferFunctionType(name, signatures, argumentTypes);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
