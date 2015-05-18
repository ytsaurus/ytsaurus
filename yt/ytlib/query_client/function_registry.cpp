#include "function_registry.h"
#include "functions.h"
#include "builtin_functions.h"
#include "user_defined_functions.h"

#include "udf/builtin_functions.h"
#include "udf/builtin_aggregates.h"

#include <ytlib/api/public.h>
#include <ytlib/api/client.h>
#include <ytlib/api/file_reader.h>
#include <ytlib/api/config.h>

#include <core/logging/log.h>

#include <core/ytree/convert.h>

#include <core/ypath/token.h>

#include <core/concurrency/scheduler.h>

#include <core/misc/error.h>

#include <core/misc/serialize.h>

#include <mutex>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

IFunctionDescriptorPtr IFunctionRegistry::GetFunction(const Stroka& functionName)
{
    auto function = FindFunction(functionName);
    YCHECK(function);
    return function;
}

IAggregateFunctionDescriptorPtr IFunctionRegistry::GetAggregateFunction(const Stroka& functionName)
{
    auto function = FindAggregateFunction(functionName);
    YCHECK(function);
    return function;
}

////////////////////////////////////////////////////////////////////////////////

void TFunctionRegistry::RegisterFunction(IFunctionDescriptorPtr descriptor)
{
    auto functionName = to_lower(descriptor->GetName());
    YCHECK(!FindAggregateFunction(functionName));
    YCHECK(RegisteredFunctions_.insert(std::make_pair(functionName, std::move(descriptor))).second);
}

IFunctionDescriptorPtr TFunctionRegistry::FindFunction(const Stroka& functionName)
{
    auto name = to_lower(functionName);
    if (RegisteredFunctions_.count(name) == 0) {
        return nullptr;
    } else {
        return RegisteredFunctions_.at(name);
    }
}

void TFunctionRegistry::RegisterAggregateFunction(IAggregateFunctionDescriptorPtr descriptor)
{
    auto aggregateName = to_lower(descriptor->GetName());
    YCHECK(!FindFunction(aggregateName));
    YCHECK(RegisteredAggregateFunctions_.insert(std::make_pair(aggregateName, std::move(descriptor))).second);
}

IAggregateFunctionDescriptorPtr TFunctionRegistry::FindAggregateFunction(const Stroka& aggregateName)
{
    auto name = to_lower(aggregateName);
    if (RegisteredAggregateFunctions_.count(name) == 0) {
        return nullptr;
    } else {
        return RegisteredAggregateFunctions_.at(name);
    }
}

////////////////////////////////////////////////////////////////////////////////

void RegisterBuiltinFunctions(TFunctionRegistryPtr registry)
{
    auto builtinImplementations = TSharedRef(
        builtin_functions_bc,
        builtin_functions_bc_len,
        nullptr);

    auto aggregatesImplementation = TSharedRef(
        builtin_aggregates_bc,
        builtin_aggregates_bc_len,
        nullptr);

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "is_substr",
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::Boolean,
        builtinImplementations,
        ECallingConvention::Simple));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "lower",
        std::vector<TType>{EValueType::String},
        EValueType::String,
        builtinImplementations,
        ECallingConvention::Simple));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "sleep",
        std::vector<TType>{EValueType::Int64},
        EValueType::Int64,
        builtinImplementations,
        ECallingConvention::Simple));

    TUnionType hashTypes = TUnionType{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::String};

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "simple_hash",
        std::vector<TType>{},
        hashTypes,
        EValueType::Uint64,
        builtinImplementations));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "farm_hash",
        std::vector<TType>{},
        hashTypes,
        EValueType::Uint64,
        builtinImplementations));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "is_null",
        std::vector<TType>{0},
        EValueType::Boolean,
        builtinImplementations,
        ECallingConvention::UnversionedValue));

    registry->RegisterFunction(New<TIfFunction>());
    registry->RegisterFunction(New<TIsPrefixFunction>());
    registry->RegisterFunction(New<TCastFunction>(
        EValueType::Int64,
        "int64"));
    registry->RegisterFunction(New<TCastFunction>(
        EValueType::Uint64,
        "uint64"));
    registry->RegisterFunction(New<TCastFunction>(
        EValueType::Double,
        "double"));

    registry->RegisterAggregateFunction(New<TAggregateFunction>("sum"));
    registry->RegisterAggregateFunction(New<TAggregateFunction>("min"));
    registry->RegisterAggregateFunction(New<TAggregateFunction>("max"));
    registry->RegisterAggregateFunction(New<TUserDefinedAggregateFunction>(
        "avg",
        EValueType::Int64,
        EValueType::Double,
        EValueType::String,
        aggregatesImplementation,
        ECallingConvention::UnversionedValue));
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ETypeCategory,
    ((TypeArgument) (TType::TagOf<TTypeArgument>()))
    ((UnionType)    (TType::TagOf<TUnionType>()))
    ((ConcreteType) (TType::TagOf<EValueType>()))
);

struct TDescriptorType
{
    TType Type = EValueType::Min;

    // NB(lukyan): For unknown reason Visual C++ does not create default constructor
    // for this class. Moreover it does not create it if TDescriptorType() = default is written
    TDescriptorType()
    { }
};

const Stroka TagKey = "tag";
const Stroka ValueKey = "value";

void Serialize(const TDescriptorType& value, NYson::IYsonConsumer* consumer)
{
    consumer->OnBeginMap();

    consumer->OnKeyedItem(TagKey);
    NYT::NYTree::Serialize(ETypeCategory(value.Type.Tag()), consumer);

    consumer->OnKeyedItem(ValueKey);
    if (auto typeArg = value.Type.TryAs<TTypeArgument>()) {
        NYT::NYTree::Serialize(*typeArg, consumer);
    } else if (auto unionType = value.Type.TryAs<TUnionType>()) {
        NYT::NYTree::Serialize(*unionType, consumer);
    } else {
        NYT::NYTree::Serialize(value.Type.As<EValueType>(), consumer);
    }

    consumer->OnEndMap();
}

void Deserialize(TDescriptorType& value, INodePtr node)
{
    auto mapNode = node->AsMap();

    auto tagNode = mapNode->GetChild(TagKey);
    ETypeCategory tag;
    Deserialize(tag, tagNode);

    auto valueNode = mapNode->GetChild(ValueKey);
    switch (tag) {
        case ETypeCategory::TypeArgument:
            {
                TTypeArgument type;
                Deserialize(type, valueNode);
                value.Type = type;
                break;
            }
        case ETypeCategory::UnionType:
            {
                TUnionType type;
                Deserialize(type, valueNode);
                value.Type = type;
                break;
            }
        case ETypeCategory::ConcreteType: 
            {
                EValueType type;
                Deserialize(type, valueNode);
                value.Type = type;
                break;
            }
        default:
            YUNREACHABLE();
    }
}

class TCypressFunctionDescriptor
    : public TYsonSerializable
{
public:
    Stroka Name;
    std::vector<TDescriptorType> ArgumentTypes;
    TNullable<TDescriptorType> RepeatedArgumentType;
    TDescriptorType ResultType;
    ECallingConvention CallingConvention;

    TCypressFunctionDescriptor()
    {
        RegisterParameter("name", Name)
            .NonEmpty();
        RegisterParameter("argument_types", ArgumentTypes);
        RegisterParameter("result_type", ResultType);
        RegisterParameter("calling_convention", CallingConvention);
        RegisterParameter("repeated_argument_type", RepeatedArgumentType)
            .Default();
    }

    std::vector<TType> GetArgumentsTypes()
    {
        std::vector<TType> argumentTypes;
        for (const auto& type: ArgumentTypes) {
            argumentTypes.push_back(type.Type);
        }
        return argumentTypes;
    }
};

DECLARE_REFCOUNTED_CLASS(TCypressFunctionDescriptor)
DEFINE_REFCOUNTED_TYPE(TCypressFunctionDescriptor)

class TCypressAggregateDescriptor
    : public TYsonSerializable
{
public:
    Stroka Name;
    TDescriptorType ArgumentType;
    EValueType StateType;
    TDescriptorType ResultType;
    ECallingConvention CallingConvention;

    TCypressAggregateDescriptor()
    {
        RegisterParameter("name", Name)
            .NonEmpty();
        RegisterParameter("argument_type", ArgumentType);
        RegisterParameter("state_type", StateType);
        RegisterParameter("result_type", ResultType);
        RegisterParameter("calling_convention", CallingConvention);
    }
};

DECLARE_REFCOUNTED_CLASS(TCypressAggregateDescriptor)
DEFINE_REFCOUNTED_TYPE(TCypressAggregateDescriptor)

TSharedRef ReadFile(const Stroka& fileName, NApi::IClientPtr client)
{
    auto reader = client->CreateFileReader(fileName);

    WaitFor(reader->Open())
        .ThrowOnError();

    std::vector<TSharedRef> blocks;
    while (true) {
        auto block = WaitFor(reader->Read())
            .ValueOrThrow();
        if (!block)
            break;
        blocks.push_back(block);
    }

    i64 size = GetByteSize(blocks);
    auto file = TSharedMutableRef::Allocate(size);
    auto memoryOutput = TMemoryOutput(
        file.Begin(),
        size);
    
    for (const auto& block : blocks) {
        memoryOutput.Write(block.Begin(), block.Size());
    }

    return file;
}

TCypressFunctionRegistry::TCypressFunctionRegistry(
    NApi::IClientPtr client,
    const NYPath::TYPath& registryPath,
    TFunctionRegistryPtr builtinRegistry)
    : Client_(client)
    , RegistryPath_(registryPath)
    , BuiltinRegistry_(std::move(builtinRegistry))
    , UdfRegistry_(New<TFunctionRegistry>())
{ }

IFunctionDescriptorPtr TCypressFunctionRegistry::FindFunction(const Stroka& functionName)
{
    if (auto function = BuiltinRegistry_->FindFunction(functionName)) {
        return function;
    } else if (auto function = UdfRegistry_->FindFunction(functionName)) {
        LOG_DEBUG("Found a cached implementation of function %Qv", functionName);
        return function;
    } else {
        LookupAndRegisterFunction(functionName);
        return UdfRegistry_->FindFunction(functionName);
    }
}

template <class TDescriptor>
TDescriptor LookupDescriptor(
    const Stroka& descriptorAttribute,
    const Stroka& functionName,
    const Stroka& functionPath,
    NApi::IClientPtr client)
{
    LOG_DEBUG("Looking for implementation of function %Qv in Cypress",
        functionName);
    
    auto getDescriptorOptions = NApi::TGetNodeOptions();
    getDescriptorOptions.AttributeFilter = TAttributeFilter(
        EAttributeFilterMode::MatchingOnly,
        std::vector<Stroka>{descriptorAttribute});

    auto cypressFunctionOrError = WaitFor(client->GetNode(
        functionPath,
        getDescriptorOptions));

    if (!cypressFunctionOrError.IsOK()) {
        LOG_DEBUG(cypressFunctionOrError, "Failed to find implementation of function %Qv in Cypress",
            functionName);
        return nullptr;
    }

    LOG_DEBUG("Found implementation of function %Qv in Cypress",
        functionName);

    TDescriptor cypressDescriptor;
    try {
        cypressDescriptor = ConvertToNode(cypressFunctionOrError.Value())
            ->Attributes()
            .Get<TDescriptor>(descriptorAttribute);
    } catch (const TErrorException& exception) {
        THROW_ERROR_EXCEPTION(
            "Error while deserializing UDF descriptor from Cypress")
            << exception;
    }

    return cypressDescriptor;
}

void TCypressFunctionRegistry::LookupAndRegisterFunction(const Stroka& functionName)
{
    const auto descriptorAttribute = "function_descriptor";

    auto functionPath = RegistryPath_ + "/" + ToYPathLiteral(to_lower(functionName));

    auto cypressDescriptor = LookupDescriptor<TCypressFunctionDescriptorPtr>(
        descriptorAttribute,
        functionName,
        functionPath,
        Client_);

    if (!cypressDescriptor) {
        return;
    }

    if (cypressDescriptor->CallingConvention == ECallingConvention::Simple &&
        cypressDescriptor->RepeatedArgumentType)
    {
        THROW_ERROR_EXCEPTION("Function using the simple calling convention may not have repeated arguments");
    }

    auto implementationFile = ReadFile(
        functionPath,
        Client_);

    if (!cypressDescriptor->RepeatedArgumentType) {
        UdfRegistry_->RegisterFunction(New<TUserDefinedFunction>(
            cypressDescriptor->Name,
            cypressDescriptor->GetArgumentsTypes(),
            cypressDescriptor->ResultType.Type,
            implementationFile,
            cypressDescriptor->CallingConvention));
    } else {
        UdfRegistry_->RegisterFunction(New<TUserDefinedFunction>(
            cypressDescriptor->Name,
            cypressDescriptor->GetArgumentsTypes(),
            cypressDescriptor->RepeatedArgumentType->Type,
            cypressDescriptor->ResultType.Type,
            implementationFile));
    }
}

IAggregateFunctionDescriptorPtr TCypressFunctionRegistry::FindAggregateFunction(const Stroka& aggregateName)
{
    if (auto aggregate = BuiltinRegistry_->FindAggregateFunction(aggregateName)) {
        return aggregate;
    } else if (auto aggregate = UdfRegistry_->FindAggregateFunction(aggregateName)) {
        LOG_DEBUG("Found a cached implementation of function %Qv", aggregateName);
        return aggregate;
    } else {
        LookupAndRegisterAggregate(aggregateName);
        return UdfRegistry_->FindAggregateFunction(aggregateName);
    }
}

void TCypressFunctionRegistry::LookupAndRegisterAggregate(const Stroka& aggregateName)
{
    const auto descriptorAttribute = "aggregate_descriptor";

    auto aggregatePath = RegistryPath_ + "/" + ToYPathLiteral(to_lower(aggregateName));

    auto cypressDescriptor = LookupDescriptor<TCypressAggregateDescriptorPtr>(
        descriptorAttribute,
        aggregateName,
        aggregatePath,
        Client_);

    if (!cypressDescriptor) {
        return;
    }

    auto implementationFile = ReadFile(
        aggregatePath,
        Client_);

    UdfRegistry_->RegisterAggregateFunction(New<TUserDefinedAggregateFunction>(
        aggregateName,
        cypressDescriptor->ArgumentType.Type,
        cypressDescriptor->ResultType.Type,
        cypressDescriptor->StateType,
        implementationFile,
        cypressDescriptor->CallingConvention));
}

////////////////////////////////////////////////////////////////////////////////

TFunctionRegistryPtr CreateBuiltinFunctionRegistryImpl()
{
    auto registry = New<TFunctionRegistry>();
    RegisterBuiltinFunctions(registry);
    return registry;
}

IFunctionRegistryPtr CreateBuiltinFunctionRegistry()
{
    return CreateBuiltinFunctionRegistryImpl();
}

IFunctionRegistryPtr CreateFunctionRegistry(NApi::IClientPtr client)
{
    auto config = client->GetConnection()->GetConfig();
    auto builtinRegistry = CreateBuiltinFunctionRegistryImpl();

    if (config->EnableUdf) {
        return New<TCypressFunctionRegistry>(
            client,
            config->UdfRegistryPath,
            builtinRegistry);
    } else {
        return builtinRegistry;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
