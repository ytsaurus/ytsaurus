#include "driver.h"
#include "private.h"
#include "response.h"
#include "error.h"
#include "descriptor.h"

#include <yt/yt/python/common/buffered_stream.h>
#include <yt/yt/python/common/shutdown.h>

#include <yt/yt/client/driver/config.h>

#include <yt/yt/client/api/sticky_transaction_pool.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/core/misc/crash_handler.h>
#include <yt/yt/core/misc/signal_registry.h>
#include <yt/yt/core/misc/shutdown.h>

#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/rpc/dispatcher.h>
#include <yt/yt/core/service_discovery/yp/config.h>
#include <yt/yt/core/service_discovery/yp/service_discovery.h>

#include <yt/yt/core/net/address.h>

#include <yt/yt/core/net/config.h>

#include <yt/yt/core/tracing/trace_context.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

using namespace NApi;
using namespace NYTree;
using namespace NDriver;
using namespace NConcurrency;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DriverLogger;

////////////////////////////////////////////////////////////////////////////////

static THashMap<TGuid, TWeakPtr<IDriver>> ActiveDrivers;

INodePtr ConvertToNodeWithUtf8Decoding(const Py::Object& obj)
{
    auto factory = GetEphemeralNodeFactory();
    auto builder = CreateBuilderFromFactory(factory);
    builder->BeginTree();
    Serialize(obj, builder.get(), std::make_optional<TString>("utf-8"));
    return builder->EndTree();
}

////////////////////////////////////////////////////////////////////////////////

TDriverBase::TDriverBase()
    : Id_(TGuid::Create())
    , Logger(NYT::NPython::Logger.WithTag("DriverId: %v", Id_))
{ }

void TDriverBase::Initialize(const IDriverPtr& driver, const INodePtr& configNode)
{
    UnderlyingDriver_ = driver;
    ConfigNode_ = configNode;

    YT_VERIFY(ActiveDrivers.emplace(Id_, UnderlyingDriver_).second);

    Initialized_ = true;

    auto config = ConvertTo<NApi::TConnectionConfigPtr>(ConfigNode_);

    YT_LOG_DEBUG("Driver created (ConnectionType: %v)", config->ConnectionType);
}

void TDriverBase::DoTerminate()
{
    if (Initialized_ && !Terminated_) {
        Terminated_ = true;
        UnderlyingDriver_->Terminate();
        ActiveDrivers.erase(Id_);
    }
}

TDriverBase::~TDriverBase()
{
    DoTerminate();
}

Py::Object TDriverBase::Execute(Py::Tuple& args, Py::Dict& kwargs)
{
    auto pyRequest = ExtractArgument(args, kwargs, "request");

    TTraceContextPtr traceContext;
    // Note that trace_id attribute may be missing in case of an old Python caller,
    // trying to keep compatibility here.
    if (auto traceIdObject = FindAttr(pyRequest, "trace_id"); traceIdObject && !traceIdObject->isNone()) {
        auto traceIdString = ConvertStringObjectToString(*traceIdObject);
        TTraceId traceId;
        if (!TTraceId::FromString(traceIdString, &traceId)) {
            throw Py::RuntimeError("Malformed trace id");
        }
        traceContext = TTraceContext::NewRoot("PythonDriver", traceId);
    }

    TCurrentTraceContextGuard guard(traceContext);

    YT_LOG_DEBUG("Preparing driver request");

    ValidateArgumentsEmpty(args, kwargs);

    Py::Callable classType(TDriverResponse::type());
    Py::PythonClassObject<TDriverResponse> pythonResponse(classType.apply(Py::Tuple(), Py::Dict()));
    auto* response = pythonResponse.getCxxObject();

    auto holder = response->GetHolder();
    if (!holder->IsInitialized()) {
        throw Py::RuntimeError("Python response could not be initialized, it usually means that python interpreter is in finalization state");
    }

    TDriverRequest request(holder);
    request.CommandName = ConvertStringObjectToString(GetAttr(pyRequest, "command_name"));
    request.Parameters = ConvertToNodeWithUtf8Decoding(GetAttr(pyRequest, "parameters"))->AsMap();
    request.ResponseParametersConsumer = holder->GetResponseParametersConsumer();
    request.ResponseParametersFinishedCallback = [holder] () {
        holder->OnResponseParametersFinished();
    };

    auto user = GetAttr(pyRequest, "user");
    if (!user.isNone()) {
        request.AuthenticatedUser = ConvertStringObjectToString(user);
    }

    auto userTag = GetAttr(pyRequest, "user_tag");
    if (!userTag.isNone()) {
        request.UserTag = ConvertStringObjectToString(userTag);
    }

    // COMPAT: find may be replaced with get in the future.
    if (auto token = FindAttr(pyRequest, "token"); token && !token->isNone()) {
        request.UserToken = ConvertStringObjectToString(*token);
    }

    if (auto ticket = FindAttr(pyRequest, "service_ticket"); ticket && !ticket->isNone()) {
        request.ServiceTicket = ConvertStringObjectToString(*ticket);
    }

    if (auto id = FindAttr(pyRequest, "id"); id && !id->isNone()) {
        request.Id = static_cast<ui64>(Py::ConvertToLongLong(*id));
    }

    auto inputStreamObj = GetAttr(pyRequest, "input_stream");
    if (!inputStreamObj.isNone()) {
        auto inputStreamHolder = CreateInputStreamWrapper(inputStreamObj, /* wrapPythonExceptions */ true);
        request.InputStream = CreateAsyncAdapter(inputStreamHolder.get());
        holder->HoldInputStream(std::move(inputStreamHolder));
    }

    auto outputStreamObj = GetAttr(pyRequest, "output_stream");
    TBufferedStreamWrap* bufferedOutputStream = nullptr;
    if (!outputStreamObj.isNone()) {
        bool isBufferedStream = PyObject_IsInstance(outputStreamObj.ptr(), TBufferedStreamWrap::type().ptr());
        if (isBufferedStream) {
            bufferedOutputStream = dynamic_cast<TBufferedStreamWrap*>(Py::getPythonExtensionBase(outputStreamObj.ptr()));
            request.OutputStream = bufferedOutputStream->GetStream();
        } else {
            std::unique_ptr<IOutputStream> outputStreamHolder(CreateOutputStreamWrapper(outputStreamObj, /* addBuffering */ false));
            request.OutputStream = CreateAsyncAdapter(outputStreamHolder.get());
            holder->HoldOutputStream(outputStreamHolder);
        }
    }

    try {
        auto driverResponse = UnderlyingDriver_->Execute(request);
        response->SetResponse(driverResponse);
        response->SetTraceContextFinishGuard(TTraceContextFinishGuard(std::move(traceContext)));
        if (bufferedOutputStream) {
            auto outputStream = bufferedOutputStream->GetStream();
            driverResponse.Subscribe(BIND([=] (const TError& /*error*/) {
                outputStream->Finish();
            }));
        }
    } CATCH_AND_CREATE_YT_ERROR("Driver command execution failed");

    YT_LOG_DEBUG("Request execution started (RequestId: %v, CommandName: %v, User: %v)",
        request.Id,
        request.CommandName,
        request.AuthenticatedUser);

    return pythonResponse;
}

Py::Object TDriverBase::RegisterAlienTransaction(Py::Tuple& args, Py::Dict& kwargs)
{
    try {
        auto pyTransactionId = ExtractArgument(args, kwargs, "transaction_id");
        auto transactionId = NTransactionClient::TTransactionId::FromString(ConvertStringObjectToString(pyTransactionId));

        auto pyAlienDriver = ExtractArgument(args, kwargs, "alien_driver");
        auto* alienDriver = dynamic_cast<TDriverBase*>(Py::getPythonExtensionBase(pyAlienDriver.ptr()));
        if (!alienDriver) {
            THROW_ERROR_EXCEPTION("'alien_driver' does not represent a valid driver instance");
        }

        const auto& localTransactionPool = UnderlyingDriver_->GetStickyTransactionPool();
        auto localTransaction = localTransactionPool->FindTransactionAndRenewLease(transactionId);
        if (!localTransaction) {
            THROW_ERROR_EXCEPTION("Local transaction %v is not registered",
                transactionId);
        }

        const auto& alienTransactionPool = alienDriver->UnderlyingDriver_->GetStickyTransactionPool();
        auto alienTransaction = alienTransactionPool->FindTransactionAndRenewLease(transactionId);
        if (!alienTransaction) {
            THROW_ERROR_EXCEPTION("Alien transaction %v is not registered",
                transactionId);
        }

        localTransaction->RegisterAlienTransaction(alienTransaction);

        return Py::None();
    } CATCH_AND_CREATE_YT_ERROR("Error registering alien transaction");
}

Py::Object TDriverBase::GetCommandDescriptor(Py::Tuple& args, Py::Dict& kwargs)
{
    auto commandName = ConvertStringObjectToString(ExtractArgument(args, kwargs, "command_name"));
    ValidateArgumentsEmpty(args, kwargs);

    Py::Callable class_type(TCommandDescriptor::type());
    Py::PythonClassObject<TCommandDescriptor> descriptor(class_type.apply(Py::Tuple(), Py::Dict()));
    try {
        descriptor.getCxxObject()->SetDescriptor(UnderlyingDriver_->GetCommandDescriptor(commandName));
    } CATCH_AND_CREATE_YT_ERROR("Failed to get command descriptor");

    return descriptor;
}

Py::Object TDriverBase::GetCommandDescriptors(Py::Tuple& args, Py::Dict& kwargs)
{
    ValidateArgumentsEmpty(args, kwargs);

    try {
        auto descriptors = Py::Dict();
        for (const auto& nativeDescriptor : UnderlyingDriver_->GetCommandDescriptors()) {
            Py::Callable class_type(TCommandDescriptor::type());
            Py::PythonClassObject<TCommandDescriptor> descriptor(class_type.apply(Py::Tuple(), Py::Dict()));
            descriptor.getCxxObject()->SetDescriptor(nativeDescriptor);
            descriptors.setItem(nativeDescriptor.CommandName.data(), descriptor);
        }
        return descriptors;
    } CATCH_AND_CREATE_YT_ERROR("Failed to get command descriptors");
}

Py::Object TDriverBase::Terminate(const Py::Tuple& args, const Py::Dict& kwargs)
{
    ValidateArgumentsEmpty(args, kwargs);
    DoTerminate();
    return Py::None();
}

Py::Object TDriverBase::GetConfig(const Py::Tuple& args, const Py::Dict& kwargs)
{
    ValidateArgumentsEmpty(args, kwargs);

    Py::Object object;
#if PY_MAJOR_VERSION < 3
    Deserialize(object, ConfigNode_, std::nullopt);
#else
    Deserialize(object, ConfigNode_, std::make_optional<TString>("utf-8"));
#endif
    return object;
}

Py::Object TDriverBase::DeepCopy(const Py::Tuple& /*args*/)
{
    Py::Callable classType(GetDriverType());
    auto configDict = GetConfig(Py::Tuple(), Py::Dict());
    return classType.apply(Py::TupleN(configDict), Py::Dict());
}

////////////////////////////////////////////////////////////////////////////////

void TDriverModuleBase::Initialize(
    const TString& moduleName,
    std::function<void()> initType,
    std::function<void()> initModule,
    std::function<Py::Dict()> getModuleDictionary,
    std::function<void(const char*, PycxxMethod, const char*)> addPycxxMethod)
{
#if PY_VERSION_HEX < 0x03090000
    PyEval_InitThreads();
#endif

    TSignalRegistry::Get()->PushCallback(AllCrashSignals, CrashSignalHandler);
    TSignalRegistry::Get()->PushDefaultSignalHandler(AllCrashSignals);

    TBufferedStreamWrap::InitType(moduleName);
    TDriverResponse::InitType(moduleName);
    TCommandDescriptor::InitType(moduleName);
    initType();

    addPycxxMethod("configure_logging", &TDriverModuleBase::ConfigureLogging, "Configures YT driver logging");
    addPycxxMethod("configure_address_resolver", &TDriverModuleBase::ConfigureAddressResolver, "Configures YT address resolver");
    addPycxxMethod("configure_yp_service_discovery", &TDriverModuleBase::ConfigureYPServiceDiscovery, "Configures YP service discovery.");
    addPycxxMethod("reopen_logs", &TDriverModuleBase::ReopenLogs, "Reopen driver logs");
    addPycxxMethod("shutdown", &TDriverModuleBase::Shutdown, "Shutdown YT subsystem");
    addPycxxMethod("_internal_shutdown", &TDriverModuleBase::InternalShutdown, "Internal shutdown");

    initModule();

    auto moduleDict = getModuleDictionary();
    moduleDict.setItem("BufferedStream", TBufferedStreamWrap::type());
    moduleDict.setItem("Response", TDriverResponse::type());

    RegisterShutdown();
    RegisterBeforeFinalizeShutdownCallback(
        BIND(&TDriverResponseHolder::OnBeforePythonFinalize),
        /*index*/ 0);
    RegisterBeforeFinalizeShutdownCallback(
        BIND(&FinalizeFutures),
        /*index*/ 1);
    RegisterAfterFinalizeShutdownCallback(
        BIND(&TDriverResponseHolder::OnAfterPythonFinalize),
        /*index*/ 0);
    RegisterAfterFinalizeShutdownCallback(
        BIND([] () {
            YT_LOG_INFO("Module shutdown started");
            for (const auto& [driverId, weakDriver] : ActiveDrivers) {
                auto driver = weakDriver.Lock();
                if (!driver) {
                    continue;
                }
                YT_LOG_INFO("Terminating leaked driver (DriverId: %v)", driverId);
                driver->Terminate();
            }
            ActiveDrivers.clear();
            YT_LOG_INFO("Module shutdown finished");
        }),
        /*index*/ 1);
}

Py::Object TDriverModuleBase::ConfigureLogging(const Py::Tuple& args_, const Py::Dict& kwargs_)
{
    auto args = args_;
    auto kwargs = kwargs_;

    auto configNode = ConvertToNode(ExtractArgument(args, kwargs, "config"));
    ValidateArgumentsEmpty(args, kwargs);
    auto config = ConvertTo<NLogging::TLogManagerConfigPtr>(configNode);

    NLogging::TLogManager::Get()->EnableReopenOnSighup();
    NLogging::TLogManager::Get()->Configure(config);

    return Py::None();
}

Py::Object TDriverModuleBase::ConfigureAddressResolver(const Py::Tuple& args_, const Py::Dict& kwargs_)
{
    auto args = args_;
    auto kwargs = kwargs_;

    auto configNode = ConvertToNode(ExtractArgument(args, kwargs, "config"));
    ValidateArgumentsEmpty(args, kwargs);
    auto config = ConvertTo<NNet::TAddressResolverConfigPtr>(configNode);

    NNet::TAddressResolver::Get()->Configure(config);

    return Py::None();
}

Py::Object TDriverModuleBase::ConfigureYPServiceDiscovery(const Py::Tuple& args_, const Py::Dict& kwargs_)
{
    auto args = args_;
    auto kwargs = kwargs_;

    auto configNode = ConvertToNode(ExtractArgument(args, kwargs, "config"));
    ValidateArgumentsEmpty(args, kwargs);

    auto config = ConvertTo<NServiceDiscovery::NYP::TServiceDiscoveryConfigPtr>(configNode);
    NRpc::TDispatcher::Get()->SetServiceDiscovery(
        NServiceDiscovery::NYP::CreateServiceDiscovery(config));

    return Py::None();
}

Py::Object TDriverModuleBase::ReopenLogs(const Py::Tuple& /*args_*/, const Py::Dict& /*kwargs_*/)
{
    NLogging::TLogManager::Get()->Reopen();

    return Py::None();
}

Py::Object TDriverModuleBase::Shutdown(const Py::Tuple& args_, const Py::Dict& kwargs_)
{
    auto args = args_;
    auto kwargs = kwargs_;
    ValidateArgumentsEmpty(args, kwargs);

    NYT::Shutdown();

    return Py::None();
}

Py::Object TDriverModuleBase::InternalShutdown(const Py::Tuple& args_, const Py::Dict& kwargs_)
{
    auto args = args_;
    auto kwargs = kwargs_;
    ValidateArgumentsEmpty(args, kwargs);

    TDriverResponseHolder::OnBeforePythonFinalize();

    return Py::None();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
