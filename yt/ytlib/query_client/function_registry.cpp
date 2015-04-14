#include "function_registry.h"
#include "functions.h"
#include "builtin_functions.h"
#include "user_defined_functions.h"

#include "udf/builtin_functions.h"

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

////////////////////////////////////////////////////////////////////////////////

void TFunctionRegistry::RegisterFunction(IFunctionDescriptorPtr function)
{
    Stroka functionName = to_lower(function->GetName());
    YCHECK(RegisteredFunctions_.insert(std::make_pair(functionName, std::move(function))).second);
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

////////////////////////////////////////////////////////////////////////////////

void RegisterBuiltinFunctions(TFunctionRegistryPtr registry)
{
    auto builtinImplementations = TSharedRef::FromRefNonOwning(TRef(
        builtin_functions_bc,
        builtin_functions_bc_len));

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

    TUnionType hashTypes = TUnionType{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::String};

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "simple_hash",
        std::vector<TType>{hashTypes},
        hashTypes,
        EValueType::Uint64,
        builtinImplementations));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "farm_hash",
        std::vector<TType>{hashTypes},
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
}

////////////////////////////////////////////////////////////////////////////////

struct TDescriptorType {
    TDescriptorType()
        : Type(EValueType::Min)
    { }

    TDescriptorType(TType type)
        : Type(type)
    { }

    TType Type;
};

const Stroka TagKey = "tag";
const Stroka ValueKey = "value";

void Serialize(const TDescriptorType& value, NYson::IYsonConsumer* consumer)
{
    consumer->OnBeginMap();

    consumer->OnKeyedItem(TagKey);
    NYT::NYTree::Serialize(value.Type.Tag(), consumer);

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

    auto tagNode = mapNode->FindChild(TagKey);
    int tag;
    Deserialize(tag, tagNode);

    auto valueNode = mapNode->FindChild(ValueKey);
    if (tag == TType::TagOf<TTypeArgument>()) {
        TTypeArgument type;
        Deserialize(type, valueNode);
        value.Type = type;
    } else if (tag == TType::TagOf<TUnionType>()) {
        TUnionType type;
        Deserialize(type, valueNode);
        value.Type = type;
    } else {
        EValueType type;
        Deserialize(type, valueNode);
        value.Type = type;
    }
}

class TCypressFunctionDescriptor
    : public TYsonSerializable
{
public:
    Stroka Name;
    std::vector<TDescriptorType> ArgumentTypes;
    TDescriptorType RepeatedArgumentType;
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
            .Default(TDescriptorType(EValueType::Null));
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

    i64 size = GetTotalSize(blocks);
    auto file = TSharedRef::Allocate(size);
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
        LookupAndRegister(functionName);
        return UdfRegistry_->FindFunction(functionName);
    }
}

void TCypressFunctionRegistry::LookupAndRegister(const Stroka& functionName)
{
    LOG_DEBUG("Looking for implementation of function %Qv in Cypress",
        functionName);
    
    const auto descriptorAttribute = "function_descriptor";

    auto functionPath = RegistryPath_ + "/" + ToYPathLiteral(to_lower(functionName));

    auto getDescriptorOptions = NApi::TGetNodeOptions();
    getDescriptorOptions.AttributeFilter = TAttributeFilter(
        EAttributeFilterMode::MatchingOnly,
        std::vector<Stroka>{descriptorAttribute});

    auto cypressFunctionOrError = WaitFor(Client_->GetNode(
        functionPath,
        getDescriptorOptions));

    if (!cypressFunctionOrError.IsOK()) {
        LOG_DEBUG(cypressFunctionOrError, "Failed to find implementation of function %Qv in Cypress",
            functionName);
        return;
    }

    LOG_DEBUG("Found implementation of function %Qv in Cypress",
        functionName);

    TCypressFunctionDescriptorPtr cypressFunction;
    bool hasNoRepeatedArgument;
    try {
        cypressFunction = ConvertToNode(cypressFunctionOrError.Value())
            ->Attributes()
            .Find<TCypressFunctionDescriptorPtr>(descriptorAttribute);
        
        hasNoRepeatedArgument =
            cypressFunction->RepeatedArgumentType.Type.Is<EValueType>() &&
            cypressFunction->RepeatedArgumentType.Type.As<EValueType>() == EValueType::Null;

        if (cypressFunction->CallingConvention == ECallingConvention::Simple && !hasNoRepeatedArgument) {
            THROW_ERROR_EXCEPTION("Function using the simple calling convention may not have repeated arguments");
        }
    } catch (const TErrorException& exception) {
        THROW_ERROR_EXCEPTION(
            "Error while deserializing UDF descriptor from Cypress")
            << exception;
    }
  
    auto implementationFile = ReadFile(
        functionPath,
        Client_);

    if (hasNoRepeatedArgument) {
        UdfRegistry_->RegisterFunction(New<TUserDefinedFunction>(
            cypressFunction->Name,
            cypressFunction->GetArgumentsTypes(),
            cypressFunction->ResultType.Type,
            implementationFile,
            cypressFunction->CallingConvention));
    } else {
        UdfRegistry_->RegisterFunction(New<TUserDefinedFunction>(
            cypressFunction->Name,
            cypressFunction->GetArgumentsTypes(),
            cypressFunction->RepeatedArgumentType.Type,
            cypressFunction->ResultType.Type,
            implementationFile));
    }
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
