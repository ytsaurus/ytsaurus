#include "function_registry.h"
#include "functions.h"
#include "builtin_functions.h"
#include "user_defined_functions.h"
#include "udf_descriptor.h"

#include "udf/hyperloglog.h"
#include "udf/double_cast.h"
#include "udf/farm_hash.h"
#include "udf/int64.h"
#include "udf/is_null.h"
#include "udf/is_substr.h"
#include "udf/lower.h"
#include "udf/sleep.h"
#include "udf/uint64.h"
#include "udf/min.h"
#include "udf/max.h"
#include "udf/sum.h"
#include "udf/avg.h"

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

class TFunctionRegistry
    : public IFunctionRegistry
{
public:
    IFunctionDescriptorPtr RegisterFunction(IFunctionDescriptorPtr descriptor);

    virtual IFunctionDescriptorPtr FindFunction(const Stroka& functionName) override;

    IAggregateFunctionDescriptorPtr RegisterAggregateFunction(IAggregateFunctionDescriptorPtr descriptor);

    virtual IAggregateFunctionDescriptorPtr FindAggregateFunction(const Stroka& aggregateName) override;

private:
    std::unordered_map<Stroka, IFunctionDescriptorPtr> RegisteredFunctions_;
    std::unordered_map<Stroka, IAggregateFunctionDescriptorPtr> RegisteredAggregateFunctions_;
    TSpinLock Lock_;
};

////////////////////////////////////////////////////////////////////////////////

IFunctionDescriptorPtr TFunctionRegistry::RegisterFunction(IFunctionDescriptorPtr descriptor)
{
    auto functionName = to_lower(descriptor->GetName());
    TGuard<TSpinLock> guard(Lock_);
    return RegisteredFunctions_.emplace(functionName, std::move(descriptor)).first->second;
}

IFunctionDescriptorPtr TFunctionRegistry::FindFunction(const Stroka& functionName)
{
    auto name = to_lower(functionName);
    TGuard<TSpinLock> guard(Lock_);
    auto found = RegisteredFunctions_.find(name);
    return found != RegisteredFunctions_.end() ? found->second : nullptr;
}

IAggregateFunctionDescriptorPtr TFunctionRegistry::RegisterAggregateFunction(IAggregateFunctionDescriptorPtr descriptor)
{
    auto aggregateName = to_lower(descriptor->GetName());
    TGuard<TSpinLock> guard(Lock_);
    return RegisteredAggregateFunctions_.emplace(aggregateName, std::move(descriptor)).first->second;
}

IAggregateFunctionDescriptorPtr TFunctionRegistry::FindAggregateFunction(const Stroka& aggregateName)
{
    auto name = to_lower(aggregateName);
    TGuard<TSpinLock> guard(Lock_);
    auto found = RegisteredAggregateFunctions_.find(name);
    return found != RegisteredAggregateFunctions_.end() ? found->second : nullptr;
}

////////////////////////////////////////////////////////////////////////////////

void RegisterBuiltinFunctions(TIntrusivePtr<TFunctionRegistry>& registry)
{
    registry->RegisterFunction(New<TUserDefinedFunction>(
        "is_substr",
        std::vector<TType>{EValueType::String, EValueType::String},
        EValueType::Boolean,
        TSharedRef(
            is_substr_bc,
            is_substr_bc_len,
            nullptr),
        ECallingConvention::Simple));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "lower",
        std::vector<TType>{EValueType::String},
        EValueType::String,
        TSharedRef(
            lower_bc,
            lower_bc_len,
            nullptr),
        ECallingConvention::Simple));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "sleep",
        std::vector<TType>{EValueType::Int64},
        EValueType::Int64,
        TSharedRef(
            sleep_bc,
            sleep_bc_len,
            nullptr),
        ECallingConvention::Simple));

    TUnionType hashTypes = TUnionType{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Boolean,
        EValueType::String};

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "farm_hash",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<TType>{},
        hashTypes,
        EValueType::Uint64,
        TSharedRef(
            farm_hash_bc,
            farm_hash_bc_len,
            nullptr)));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "is_null",
        std::vector<TType>{0},
        EValueType::Boolean,
        TSharedRef(
            is_null_bc,
            is_null_bc_len,
            nullptr),
        ECallingConvention::UnversionedValue));

    auto typeArg = 0;
    auto castConstraints = std::unordered_map<TTypeArgument, TUnionType>();
    castConstraints[typeArg] = std::vector<EValueType>{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double};

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "int64",
        castConstraints,
        std::vector<TType>{typeArg},
        EValueType::Null,
        EValueType::Int64,
        TSharedRef(
            int64_bc,
            int64_bc_len,
            nullptr)));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "uint64",
        castConstraints,
        std::vector<TType>{typeArg},
        EValueType::Null,
        EValueType::Uint64,
        TSharedRef(
            uint64_bc,
            uint64_bc_len,
            nullptr)));

    registry->RegisterFunction(New<TUserDefinedFunction>(
        "double",
        "double_cast",
        castConstraints,
        std::vector<TType>{typeArg},
        EValueType::Null,
        EValueType::Double,
        TSharedRef(
            double_cast_bc,
            double_cast_bc_len,
            nullptr)));

    registry->RegisterFunction(New<TIfFunction>());
    registry->RegisterFunction(New<TIsPrefixFunction>());

    auto constraints = std::unordered_map<TTypeArgument, TUnionType>();
    constraints[typeArg] = std::vector<EValueType>{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double,
        EValueType::String};
    auto sumConstraints = std::unordered_map<TTypeArgument, TUnionType>();
    sumConstraints[typeArg] = std::vector<EValueType>{
        EValueType::Int64,
        EValueType::Uint64,
        EValueType::Double};

    registry->RegisterAggregateFunction(New<TUserDefinedAggregateFunction>(
        "sum",
        sumConstraints,
        typeArg,
        typeArg,
        typeArg,
        TSharedRef(
            sum_bc,
            sum_bc_len,
            nullptr),
        ECallingConvention::UnversionedValue));
    registry->RegisterAggregateFunction(New<TUserDefinedAggregateFunction>(
        "min",
        constraints,
        typeArg,
        typeArg,
        typeArg,
        TSharedRef(
            min_bc,
            min_bc_len,
            nullptr),
        ECallingConvention::UnversionedValue));
    registry->RegisterAggregateFunction(New<TUserDefinedAggregateFunction>(
        "max",
        constraints,
        typeArg,
        typeArg,
        typeArg,
        TSharedRef(
            max_bc,
            max_bc_len,
            nullptr),
        ECallingConvention::UnversionedValue));
    registry->RegisterAggregateFunction(New<TUserDefinedAggregateFunction>(
        "avg",
        std::unordered_map<TTypeArgument, TUnionType>(),
        EValueType::Int64,
        EValueType::Double,
        EValueType::String,
        TSharedRef(
            avg_bc,
            avg_bc_len,
            nullptr),
        ECallingConvention::UnversionedValue));
    registry->RegisterAggregateFunction(New<TUserDefinedAggregateFunction>(
        "cardinality",
        std::unordered_map<TTypeArgument, TUnionType>(),
        std::vector<EValueType>{
            EValueType::String,
            EValueType::Uint64,
            EValueType::Int64,
            EValueType::Double,
            EValueType::Boolean},
        EValueType::Uint64,
        EValueType::String,
        TSharedRef(
            hyperloglog_bc,
            hyperloglog_bc_len,
            nullptr),
        ECallingConvention::UnversionedValue));
}

////////////////////////////////////////////////////////////////////////////////

IFunctionRegistryPtr CreateBuiltinFunctionRegistry()
{
    auto registry = New<TFunctionRegistry>();
    RegisterBuiltinFunctions(registry);
    return registry;
}

////////////////////////////////////////////////////////////////////////////////

namespace {

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
            .Find<TDescriptor>(descriptorAttribute);
    } catch (const TErrorException& exception) {
        THROW_ERROR_EXCEPTION(
            "Error while deserializing UDF descriptor from Cypress")
            << exception;
    }

    return cypressDescriptor;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TCypressFunctionRegistry
    : public IFunctionRegistry
{
public:
    TCypressFunctionRegistry(
        NApi::IClientPtr client,
        const NYPath::TYPath& registryPath,
        IFunctionRegistryPtr builtinRegistry);

    virtual IFunctionDescriptorPtr FindFunction(const Stroka& functionName) override;

    virtual IAggregateFunctionDescriptorPtr FindAggregateFunction(const Stroka& aggregateName) override;

private:
    const NApi::IClientPtr Client_;
    const NYPath::TYPath RegistryPath_;
    const IFunctionRegistryPtr BuiltinRegistry_;
    const TIntrusivePtr<TFunctionRegistry> UdfRegistry_;

    IFunctionDescriptorPtr LookupFunction(const Stroka& functionName);
    IAggregateFunctionDescriptorPtr LookupAggregate(const Stroka& aggregateName);
};

////////////////////////////////////////////////////////////////////////////////

TCypressFunctionRegistry::TCypressFunctionRegistry(
    NApi::IClientPtr client,
    const NYPath::TYPath& registryPath,
    IFunctionRegistryPtr builtinRegistry)
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
        auto udf = LookupFunction(functionName);
        if (udf) {
            udf = UdfRegistry_->RegisterFunction(udf);
        }
        return udf;
    }
}

IFunctionDescriptorPtr TCypressFunctionRegistry::LookupFunction(const Stroka& functionName)
{
    auto functionPath = GetUdfDescriptorPath(RegistryPath_, functionName);

    auto cypressDescriptor = LookupDescriptor<TCypressFunctionDescriptorPtr>(
        FunctionDescriptorAttribute,
        functionName,
        functionPath,
        Client_);

    if (!cypressDescriptor) {
        return nullptr;
    }

    if (cypressDescriptor->CallingConvention == ECallingConvention::Simple &&
        cypressDescriptor->RepeatedArgumentType)
    {
        THROW_ERROR_EXCEPTION("Function using the simple calling convention may not have repeated arguments");
    }

    auto implementationFile = ReadFile(
        functionPath,
        Client_);

    return cypressDescriptor->RepeatedArgumentType
        ? New<TUserDefinedFunction>(
            cypressDescriptor->Name,
            std::unordered_map<TTypeArgument, TUnionType>(),
            cypressDescriptor->GetArgumentsTypes(),
            cypressDescriptor->RepeatedArgumentType->Type,
            cypressDescriptor->ResultType.Type,
            implementationFile)
        : New<TUserDefinedFunction>(
            cypressDescriptor->Name,
            cypressDescriptor->GetArgumentsTypes(),
            cypressDescriptor->ResultType.Type,
            implementationFile,
            cypressDescriptor->CallingConvention);
}

IAggregateFunctionDescriptorPtr TCypressFunctionRegistry::FindAggregateFunction(const Stroka& aggregateName)
{
    if (auto aggregate = BuiltinRegistry_->FindAggregateFunction(aggregateName)) {
        return aggregate;
    } else if (auto aggregate = UdfRegistry_->FindAggregateFunction(aggregateName)) {
        LOG_DEBUG("Found a cached implementation of function %Qv", aggregateName);
        return aggregate;
    } else {
        auto udf = LookupAggregate(aggregateName);
        if (udf) {
            udf = UdfRegistry_->RegisterAggregateFunction(udf);
        }
        return udf;
    }
}

IAggregateFunctionDescriptorPtr TCypressFunctionRegistry::LookupAggregate(const Stroka& aggregateName)
{
    auto aggregatePath = GetUdfDescriptorPath(RegistryPath_, aggregateName);

    auto cypressDescriptor = LookupDescriptor<TCypressAggregateDescriptorPtr>(
        AggregateDescriptorAttribute,
        aggregateName,
        aggregatePath,
        Client_);

    if (!cypressDescriptor) {
        return nullptr;
    }

    auto implementationFile = ReadFile(
        aggregatePath,
        Client_);

    return New<TUserDefinedAggregateFunction>(
        aggregateName,
        std::unordered_map<TTypeArgument, TUnionType>(),
        cypressDescriptor->ArgumentType.Type,
        cypressDescriptor->ResultType.Type,
        cypressDescriptor->StateType.Type,
        implementationFile,
        cypressDescriptor->CallingConvention);
}

////////////////////////////////////////////////////////////////////////////////

IFunctionRegistryPtr CreateClientFunctionRegistry(NApi::IClientPtr client)
{
    auto config = client->GetConnection()->GetConfig();
    auto builtinRegistry = CreateBuiltinFunctionRegistry();

    if (config->EnableUdf) {
        return New<TCypressFunctionRegistry>(
            client,
            config->UdfRegistryPath,
            std::move(builtinRegistry));
    } else {
        return builtinRegistry;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TCombiningFunctionRegistry
    : public IFunctionRegistry
{
public:
    explicit TCombiningFunctionRegistry(std::vector<IFunctionRegistryPtr> registries)
        : Registries_(std::move(registries))
    { }

    virtual IFunctionDescriptorPtr FindFunction(const Stroka& functionName) override
    {
        for (const auto& registry : Registries_) {
            if (auto function = registry->FindFunction(functionName)) {
                return function;
            }
        }
        return nullptr;
    }

    virtual IAggregateFunctionDescriptorPtr FindAggregateFunction(const Stroka& aggregateName) override
    {
        for (const auto& registry : Registries_) {
            if (auto function = registry->FindAggregateFunction(aggregateName)) {
                return function;
            }
        }
        return nullptr;
    }

private:
    std::vector<IFunctionRegistryPtr> Registries_;
};

////////////////////////////////////////////////////////////////////////////////

IFunctionRegistryPtr CreateJobFunctionRegistry(
    const std::vector<TUdfDescriptorPtr>& descriptors,
    TNullable<Stroka> implementationPath,
    IFunctionRegistryPtr builtinRegistry)
{
    auto udfRegistry = New<TFunctionRegistry>();

    auto readImplementation = [&] (const Stroka name) {
        if (implementationPath) {
            auto path = implementationPath.Get() + "/" + name;
            TFileInput file(path);
            return TSharedRef::FromString(file.ReadAll());
        } else {
            return TSharedRef();
        }
    };

    for (const auto& descriptor : descriptors) {
        const auto& name = descriptor->Name;
        auto implementationFile = readImplementation(name);

        if (descriptor->FunctionDescriptor) {
            auto& cypressDescriptor = descriptor->FunctionDescriptor;

            udfRegistry->RegisterFunction(
                cypressDescriptor->RepeatedArgumentType
                    ? New<TUserDefinedFunction>(
                        cypressDescriptor->Name,
                        std::unordered_map<TTypeArgument, TUnionType>(),
                        cypressDescriptor->GetArgumentsTypes(),
                        cypressDescriptor->RepeatedArgumentType->Type,
                        cypressDescriptor->ResultType.Type,
                        implementationFile)
                    : New<TUserDefinedFunction>(
                        cypressDescriptor->Name,
                        cypressDescriptor->GetArgumentsTypes(),
                        cypressDescriptor->ResultType.Type,
                        implementationFile,
                        cypressDescriptor->CallingConvention));
        }

        if (descriptor->AggregateDescriptor) {
            auto& cypressDescriptor = descriptor->AggregateDescriptor;

            udfRegistry->RegisterAggregateFunction(New<TUserDefinedAggregateFunction>(
                name,
                std::unordered_map<TTypeArgument, TUnionType>(),
                cypressDescriptor->ArgumentType.Type,
                cypressDescriptor->ResultType.Type,
                cypressDescriptor->StateType.Type,
                implementationFile,
                cypressDescriptor->CallingConvention));
        }
    }

    std::vector<IFunctionRegistryPtr> registries{builtinRegistry, udfRegistry};
    return New<TCombiningFunctionRegistry>(registries);
}

////////////////////////////////////////////////////////////////////////////////

Stroka GetUdfDescriptorPath(const TYPath& registryPath, const Stroka& functionName)
{
    return registryPath + "/" + ToYPathLiteral(to_lower(functionName));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
