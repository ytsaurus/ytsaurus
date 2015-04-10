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
    registry->RegisterFunction(New<TIfFunction>());
    registry->RegisterFunction(New<TIsPrefixFunction>());
    registry->RegisterFunction(New<TUserDefinedFunction>(
        "is_substr",
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::Boolean,
        TSharedRef::FromRefNonOwning(TRef(
            builtin_functions_bc,
            builtin_functions_bc_len)),
        ECallingConvention::Simple));
    registry->RegisterFunction(New<TUserDefinedFunction>(
        "lower",
        std::vector<TType>{EValueType::String},
        EValueType::String,
        TSharedRef::FromRefNonOwning(TRef(
            builtin_functions_bc,
            builtin_functions_bc_len)),
        ECallingConvention::Simple));
    registry->RegisterFunction(New<THashFunction>(
        "simple_hash",
        "SimpleHash"));
    registry->RegisterFunction(New<THashFunction>(
        "farm_hash",
        "FarmHash"));
    registry->RegisterFunction(New<TUserDefinedFunction>(
        "is_null",
        std::vector<TType>{0},
        EValueType::Boolean,
        TSharedRef::FromRefNonOwning(TRef(
            builtin_functions_bc,
            builtin_functions_bc_len)),
        ECallingConvention::UnversionedValue));
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

struct TTypeVector {
    TTypeVector()
        : Vector({})
    { }

    TTypeVector(std::vector<TType> vector)
        : Vector(vector)
    { }

    std::vector<TType> Vector;
};


void Deserialize(TType& value, INodePtr node)
{
    auto mapNode = node->AsMap();

    auto tagNode = mapNode->FindChild("tag");
    int tag;
    Deserialize(tag, tagNode);

    auto valueNode = mapNode->FindChild("value");
    if (tag == TType::TagOf<TTypeArgument>()) {
        TTypeArgument type;
        Deserialize(type, valueNode);
        value = TType(TVariantTypeTag<TTypeArgument>(), type);
    } else if (tag == TType::TagOf<TUnionType>()) {
        TUnionType type;
        Deserialize(type, valueNode);
        value = TType(TVariantTypeTag<TUnionType>(), type);
    } else {
        EValueType type;
        Deserialize(type, valueNode);
        value = TType(TVariantTypeTag<EValueType>(), type);
    }
}

void Deserialize(TTypeVector& value, INodePtr node)
{
    auto listNode = node->AsList();
    auto size = listNode->GetChildCount();
    for (int i = 0; i < size; ++i) {
        TType child = EValueType::Min;
        Deserialize(child, listNode->GetChild(i));
        value.Vector.push_back(child);
    }
}

void Serialize(const TTypeVector& value, NYson::IYsonConsumer* consumer)
{
    Serialize(value.Vector, consumer);
}

class TCypressFunctionDescriptor
    : public TYsonSerializable
{
public:
    Stroka Name;
    TTypeVector ArgumentTypes;
    TType ResultType = EValueType::Min;
    ECallingConvention CallingConvention;

    TCypressFunctionDescriptor()
    {
        RegisterParameter("name", Name)
            .NonEmpty();
        RegisterParameter("argument_types", ArgumentTypes);
        RegisterParameter("result_type", ResultType);
        RegisterParameter("calling_convention", CallingConvention);
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

    auto cypressFunction = ConvertToNode(cypressFunctionOrError.Value())
        ->Attributes()
        .Find<TCypressFunctionDescriptorPtr>(descriptorAttribute);

    auto implementationFile = ReadFile(
        functionPath,
        Client_);

    UdfRegistry_->RegisterFunction(New<TUserDefinedFunction>(
        cypressFunction->Name,
        cypressFunction->ArgumentTypes.Vector,
        cypressFunction->ResultType,
        implementationFile,
        cypressFunction->CallingConvention));
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

using namespace NQueryClient;

// Define these in the NYT namespace so that they're in the same namespace as TVariant
void Serialize(const TType& value, NYson::IYsonConsumer* consumer)
{
    consumer->OnBeginMap();

    consumer->OnKeyedItem("tag");
    Serialize(value.Tag(), consumer);

    consumer->OnKeyedItem("value");
    if (auto typeArg = value.TryAs<TTypeArgument>()) {
        Serialize(*typeArg, consumer);
    } else if (auto unionType = value.TryAs<TUnionType>()) {
        Serialize(*unionType, consumer);
    } else {
        Serialize(value.As<EValueType>(), consumer);
    }

    consumer->OnEndMap();
}

void Deserialize(TType& value, INodePtr node)
{
    NQueryClient::Deserialize(value, node);
}

} // namespace NYT
