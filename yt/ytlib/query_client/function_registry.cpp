#include "function_registry.h"
#include "functions.h"
#include "builtin_functions.h"

#include <ytlib/api/public.h>
#include <ytlib/api/client.h>
#include <ytlib/api/file_reader.h>

#include <core/ytree/convert.h>

#include <core/concurrency/scheduler.h>

#include <core/misc/error.h>

#include <mutex>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

IFunctionRegistry::~IFunctionRegistry()
{ }

////////////////////////////////////////////////////////////////////////////////

void TFunctionRegistry::RegisterFunction(TIntrusivePtr<IFunctionDescriptor> function)
{
    Stroka functionName = to_lower(function->GetName());
    YCHECK(RegisteredFunctions_.insert(std::make_pair(functionName, std::move(function))).second);
}

IFunctionDescriptor& TFunctionRegistry::GetFunction(const Stroka& functionName)
{
    return *RegisteredFunctions_.at(to_lower(functionName));
}

bool TFunctionRegistry::IsRegistered(const Stroka& functionName)
{
    return RegisteredFunctions_.count(to_lower(functionName)) != 0;
}

////////////////////////////////////////////////////////////////////////////////

void RegisterBuiltinFunctionsImpl(IFunctionRegistryPtr registry)
{
    registry->RegisterFunction(New<TIfFunction>());
    registry->RegisterFunction(New<TIsPrefixFunction>());
    registry->RegisterFunction(New<TIsSubstrFunction>());
    registry->RegisterFunction(New<TLowerFunction>());
    registry->RegisterFunction(New<THashFunction>(
        "simple_hash",
        "SimpleHash"));
    registry->RegisterFunction(New<THashFunction>(
        "farm_hash",
        "FarmHash"));
    registry->RegisterFunction(New<TIsNullFunction>());
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

TCypressFunctionRegistry::TCypressFunctionRegistry(
    NApi::IClientPtr client,
    TFunctionRegistryPtr builtinRegistry)
    : TFunctionRegistry()
    , Client_(std::move(client))
    , BuiltinRegistry_(std::move(builtinRegistry))
    , UDFRegistry_(New<TFunctionRegistry>())
{ }

TSharedRef ReadFile(NApi::IClientPtr client, const Stroka& fileName)
{
    auto reader = client->CreateFileReader(fileName);
    WaitFor(reader->Open());

    int size = 0;
    std::vector<TSharedRef> blocks;
    while (true) {
        auto blockOrError = reader->Read().Get();
        THROW_ERROR_EXCEPTION_IF_FAILED(blockOrError);
        auto block = blockOrError.Value();

        if (!block) {
            break;
        }

        blocks.push_back(block);
        size += block.Size();
    }
    
    auto file = TSharedRef::Allocate(size);
    auto memoryOutput = TMemoryOutput(
        file.Begin(),
        size);
    
    for (const auto& block : blocks) {
        memoryOutput.Write(block.Begin(), block.Size());
    }

    return file;
}

class TCypressFunctionDescriptor
    : public TYsonSerializable
{
public:
    Stroka Name;
    std::vector<EValueType> ArgumentTypes;
    EValueType ResultType;
    Stroka ImplementationPath;

    TCypressFunctionDescriptor()
    {
        RegisterParameter("name", Name)
            .NonEmpty();
        RegisterParameter("argument_types", ArgumentTypes);
        RegisterParameter("result_type", ResultType);
        RegisterParameter("implementation_path", ImplementationPath)
            .NonEmpty();
    }
};

DECLARE_REFCOUNTED_CLASS(TCypressFunctionDescriptor)
DEFINE_REFCOUNTED_TYPE(TCypressFunctionDescriptor)

void TCypressFunctionRegistry::LookupInCypress(const Stroka& functionName)
{
    Stroka registryPath = "//tmp/udfs";
    auto functionPath = registryPath + "/" + to_lower(functionName);

    auto cypressFunctionOrError = WaitFor(Client_->GetNode(functionPath));
    if (!cypressFunctionOrError.IsOK()) {
        return;
    }

    auto function = ConvertTo<TCypressFunctionDescriptorPtr>(
        cypressFunctionOrError.Value());

    auto implementationFile = ReadFile(Client_, function->ImplementationPath);

    UDFRegistry_->RegisterFunction(New<TUserDefinedFunction>(
        function->Name,
        function->ArgumentTypes,
        function->ResultType,
        implementationFile));
}

IFunctionDescriptor& TCypressFunctionRegistry::GetFunction(const Stroka& functionName)
{
    if (BuiltinRegistry_->IsRegistered(functionName)) {
        return BuiltinRegistry_->GetFunction(functionName);
    } else if (UDFRegistry_->IsRegistered(functionName)) {
        return UDFRegistry_->GetFunction(functionName);
    } else {
        LookupInCypress(functionName);
        return UDFRegistry_->GetFunction(functionName);
    }
}

bool TCypressFunctionRegistry::IsRegistered(const Stroka& functionName)
{
    if (BuiltinRegistry_->IsRegistered(functionName)
        || UDFRegistry_->IsRegistered(functionName))
    {
        return true;
    } else {
        LookupInCypress(functionName);
        return UDFRegistry_->IsRegistered(functionName);
    }
}

////////////////////////////////////////////////////////////////////////////////

TFunctionRegistryPtr CreateBuiltinFunctionRegistryImpl()
{
    auto registry = New<TFunctionRegistry>();
    RegisterBuiltinFunctionsImpl(registry);
    return registry;
}

IFunctionRegistryPtr CreateBuiltinFunctionRegistry()
{
    return CreateBuiltinFunctionRegistryImpl();
}

IFunctionRegistryPtr CreateFunctionRegistry(NApi::IClientPtr client)
{
    auto builtinRegistry = CreateBuiltinFunctionRegistryImpl();
    return New<TCypressFunctionRegistry>(client, builtinRegistry);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
