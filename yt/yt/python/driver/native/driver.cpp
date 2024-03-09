#include <yt/yt/python/driver/lib/descriptor.h>
#include <yt/yt/python/driver/lib/response.h>
#include <yt/yt/python/driver/lib/error.h>
#include <yt/yt/python/driver/lib/driver.h>

#include <yt/yt/python/common/helpers.h>
#include <yt/yt/python/common/buffered_stream.h>

#include <yt/yt/ytlib/api/connection.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/driver/config.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/queue_client/registration_manager.h>

#include <yt/yt/library/tvm/service/tvm_service.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/driver/config.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

using namespace NFormats;
using namespace NDriver;
using namespace NYson;
using namespace NYTree;
using namespace NConcurrency;
using namespace NTabletClient;
using namespace NJobTrackerClient;
using namespace NApi;

using NApi::NNative::EClusterConnectionDynamicConfigPolicy;

////////////////////////////////////////////////////////////////////////////////

class TDriver
    : public Py::PythonClass<TDriver>
    , public TDriverBase
{
public:
    TDriver(Py::PythonClassInstance *self, Py::Tuple& args, Py::Dict& kwargs)
        : Py::PythonClass<TDriver>::PythonClass(self, args, kwargs)
    {
        auto configDict = ExtractArgument(args, kwargs, "config");
        ValidateArgumentsEmpty(args, kwargs);

        INodePtr configNode;
        IDriverPtr driver;

        try {
            configNode = ConvertToNode(configDict);

            auto driverConfig = ConvertTo<TNativeDriverConfigPtr>(configNode);

            NAuth::IDynamicTvmServicePtr tvmService;
            if (driverConfig->TvmService) {
                tvmService = NAuth::CreateDynamicTvmService(driverConfig->TvmService);
            }
            auto connection = CreateConnection(
                configNode,
                /*options*/ {},
                std::move(tvmService));
            Connection_ = connection;

            if (auto nativeConnection = DynamicPointerCast<NNative::IConnection>(connection)) {
                auto configNodeMap = configNode->AsMap();
                auto configPolicy = EClusterConnectionDynamicConfigPolicy::FromStaticConfig;
                if (auto configPolicyNode = configNodeMap->FindChild("cluster_connection_dynamic_config_policy")) {
                    configPolicy = ConvertTo<EClusterConnectionDynamicConfigPolicy>(configPolicyNode);
                }

                NApi::NNative::SetupClusterConnectionDynamicConfigUpdate(
                    nativeConnection,
                    configPolicy,
                    configNode,
                    Logger);

                nativeConnection->GetClusterDirectorySynchronizer()->Start();
                nativeConnection->GetQueueConsumerRegistrationManager()->StartSync();
            }

            driver = CreateDriver(std::move(connection), std::move(driverConfig));
        } catch (const std::exception& ex) {
            throw Py::RuntimeError(TString("Error creating driver\n") + ex.what());
        }

        TDriverBase::Initialize(driver, configNode);
    }

    static void InitType()
    {
        behaviors().name("driver_lib.Driver");
        behaviors().doc("Represents a YT driver");
        behaviors().supportGetattro();
        behaviors().supportSetattro();

        PYCXX_ADD_DRIVER_METHODS

        PYCXX_ADD_KEYWORDS_METHOD(kill_process, KillProcess, "Forces a remote YT process (node, scheduler or master) to exit immediately");
        PYCXX_ADD_KEYWORDS_METHOD(write_core_dump, WriteCoreDump, "Writes a core dump of a remote YT process (node, scheduler or master)");
        PYCXX_ADD_KEYWORDS_METHOD(write_log_barrier, WriteLogBarrier, "Writes a special line called barrier with a unique ID into structured logs.");
        PYCXX_ADD_KEYWORDS_METHOD(write_operation_controller_core_dump, WriteOperationControllerCoreDump, "Write a core dump of a controller agent holding the operation controller for a given operation id");
        PYCXX_ADD_KEYWORDS_METHOD(build_snapshot, BuildSnapshot, "Forces to build a snapshot");
        PYCXX_ADD_KEYWORDS_METHOD(build_master_snapshots, BuildMasterSnapshots, "Forces to build snapshots for all master cells");
        PYCXX_ADD_KEYWORDS_METHOD(get_master_consistent_state, GetMasterConsistentState, "Records a consistent global state");
        PYCXX_ADD_KEYWORDS_METHOD(exit_read_only, ExitReadOnly, "Exits read-only mode at given cell");
        PYCXX_ADD_KEYWORDS_METHOD(master_exit_read_only, MasterExitReadOnly, "Exits read-only mode at all master cells");
        PYCXX_ADD_KEYWORDS_METHOD(discombobulate_nonvoting_peers, DiscombobulateNonvotingPeers, "Do not restart nonvoting peers in leader`s absence");
        PYCXX_ADD_KEYWORDS_METHOD(gc_collect, GCCollect, "Runs garbage collection");
        PYCXX_ADD_KEYWORDS_METHOD(clear_metadata_caches, ClearMetadataCaches, "Clears metadata caches");

        behaviors().readyType();
    }

    Py::Object GCCollect(Py::Tuple& /*args*/, Py::Dict& /*kwargs*/)
    {
        try {
            auto client = CreateClient();
            WaitFor(client->GCCollect())
                .ThrowOnError();
            return Py::None();
        } CATCH_AND_CREATE_YT_ERROR("Failed to perform garbage collect");
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, GCCollect)

    Py::Object KillProcess(Py::Tuple& args, Py::Dict& kwargs)
    {
        auto options = TKillProcessOptions();

        if (!HasArgument(args, kwargs, "address")) {
            throw CreateYtError("Missing argument 'address'");
        }
        auto address = ConvertStringObjectToString(ExtractArgument(args, kwargs, "address"));

        if (HasArgument(args, kwargs, "exit_code")) {
            options.ExitCode = static_cast<int>(Py::Int(ExtractArgument(args, kwargs, "exit_code")));
        }

        ValidateArgumentsEmpty(args, kwargs);

        try {
            auto client = CreateClient();
            WaitFor(client->KillProcess(address, options))
                .ThrowOnError();
            return Py::None();
        } CATCH_AND_CREATE_YT_ERROR("Failed to kill process");
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, KillProcess)

    Py::Object WriteCoreDump(Py::Tuple& args, Py::Dict& kwargs)
    {
        auto options = TWriteCoreDumpOptions();

        if (!HasArgument(args, kwargs, "address")) {
            throw CreateYtError("Missing argument 'address'");
        }
        auto address = ConvertStringObjectToString(ExtractArgument(args, kwargs, "address"));

        ValidateArgumentsEmpty(args, kwargs);

        try {
            auto client = CreateClient();
            auto path = WaitFor(client->WriteCoreDump(address, options))
                .ValueOrThrow();
            return Py::String(path);
        } CATCH_AND_CREATE_YT_ERROR("Failed to write core dump");
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, WriteCoreDump)

    Py::Object WriteLogBarrier(Py::Tuple& args, Py::Dict& kwargs)
    {
        auto options = TWriteLogBarrierOptions();

        if (!HasArgument(args, kwargs, "address")) {
            throw CreateYtError("Missing argument 'address'");
        }
        auto address = ConvertStringObjectToString(ExtractArgument(args, kwargs, "address"));

        if (!HasArgument(args, kwargs, "category")) {
            throw CreateYtError("Missing argument 'category'");
        }
        options.Category = Py::ConvertStringObjectToString(ExtractArgument(args, kwargs, "category"));

        ValidateArgumentsEmpty(args, kwargs);

        try {
            auto client = CreateClient();
            auto barrierId = WaitFor(client->WriteLogBarrier(address, options))
                .ValueOrThrow();
            return Py::String(ToString(barrierId));
        } CATCH_AND_CREATE_YT_ERROR("Failed to write log barrier");
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, WriteLogBarrier)

    Py::Object WriteOperationControllerCoreDump(Py::Tuple& args, Py::Dict& kwargs)
    {
        if (!HasArgument(args, kwargs, "operation_id")) {
            throw CreateYtError("Missing argument 'operation_id'");
        }
        auto operationId = TOperationId(TGuid::FromString(ConvertStringObjectToString(ExtractArgument(args, kwargs, "operation_id"))));

        ValidateArgumentsEmpty(args, kwargs);

        try {
            auto client = CreateClient();
            auto path = WaitFor(client->WriteOperationControllerCoreDump(operationId))
                .ValueOrThrow();
            return Py::String(path);
        } CATCH_AND_CREATE_YT_ERROR("Failed to write operation controller core dump");
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, WriteOperationControllerCoreDump)

    Py::Object BuildSnapshot(Py::Tuple& args, Py::Dict& kwargs)
    {
        auto options = TBuildSnapshotOptions();

        if (HasArgument(args, kwargs, "set_read_only")) {
            options.SetReadOnly = static_cast<bool>(Py::Boolean(ExtractArgument(args, kwargs, "set_read_only")));
        }
        if (HasArgument(args, kwargs, "wait_for_snapshot_completion")) {
            options.WaitForSnapshotCompletion = static_cast<bool>(Py::Boolean(ExtractArgument(args, kwargs, "wait_for_snapshot_completion")));
        }
        if (!HasArgument(args, kwargs, "cell_id")) {
            throw CreateYtError("Missing argument 'cell_id'");
        }

        auto cellId = ExtractArgument(args, kwargs, "cell_id");

        ValidateArgumentsEmpty(args, kwargs);

        try {
            if (!cellId.isNone()) {
                options.CellId = NHydra::TCellId::FromString(ConvertStringObjectToString(cellId));
            }

            auto client = CreateClient();
            int snapshotId = WaitFor(client->BuildSnapshot(options))
                .ValueOrThrow();
            return Py::Long(snapshotId);
        } CATCH_AND_CREATE_YT_ERROR("Failed to build snapshot");
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, BuildSnapshot)

    Py::Object BuildMasterSnapshots(Py::Tuple& args, Py::Dict& kwargs)
    {
        auto options = TBuildMasterSnapshotsOptions();

        if (HasArgument(args, kwargs, "set_read_only")) {
            options.SetReadOnly = static_cast<bool>(Py::Boolean(ExtractArgument(args, kwargs, "set_read_only")));
        }
        if (HasArgument(args, kwargs, "retry")) {
            options.Retry = static_cast<bool>(Py::Boolean(ExtractArgument(args, kwargs, "retry")));
        }
        if (HasArgument(args, kwargs, "wait_for_snapshot_completion")) {
            options.WaitForSnapshotCompletion = static_cast<bool>(Py::Boolean(ExtractArgument(args, kwargs, "wait_for_snapshot_completion")));
        }

        ValidateArgumentsEmpty(args, kwargs);

        try {
            auto client = CreateClient();
            auto cellIdToSnapshotId = WaitFor(client->BuildMasterSnapshots(options))
                .ValueOrThrow();

            Py::Dict dict;
            for (auto [cellId, snapshotId] : cellIdToSnapshotId) {
                dict.setItem(ToString(cellId), Py::Long(snapshotId));
            }
            return dict;
        } CATCH_AND_CREATE_YT_ERROR("Failed to build master snapshots");
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, BuildMasterSnapshots)

    Py::Object GetMasterConsistentState(Py::Tuple& args, Py::Dict& kwargs)
    {
        ValidateArgumentsEmpty(args, kwargs);

        try {
            auto client = CreateClient();
            WaitFor(client->GetMasterConsistentState())
                .ThrowOnError();
            return Py::None();
        } CATCH_AND_CREATE_YT_ERROR("Failed to get master consistent state");
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, GetMasterConsistentState)

    Py::Object ExitReadOnly(Py::Tuple& args, Py::Dict& kwargs)
    {
        auto options = TExitReadOnlyOptions();

        if (!HasArgument(args, kwargs, "cell_id")) {
            throw CreateYtError("Missing argument 'cell_id'");
        }

        auto cellId = NHydra::TCellId::FromString(ConvertStringObjectToString(ExtractArgument(args, kwargs, "cell_id")));

        ValidateArgumentsEmpty(args, kwargs);

        try {
            auto client = CreateClient();
            WaitFor(client->ExitReadOnly(cellId, options))
                .ThrowOnError();
            return Py::None();
        } CATCH_AND_CREATE_YT_ERROR("Failed to exit read-only mode at given cell");
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, ExitReadOnly)

    Py::Object MasterExitReadOnly(Py::Tuple& args, Py::Dict& kwargs)
    {
        auto options = TMasterExitReadOnlyOptions();

        if (HasArgument(args, kwargs, "retry")) {
            options.Retry = static_cast<bool>(Py::Boolean(ExtractArgument(args, kwargs, "retry")));
        }

        ValidateArgumentsEmpty(args, kwargs);

        try {
            auto client = CreateClient();
            WaitFor(client->MasterExitReadOnly(options))
                .ThrowOnError();
            return Py::None();
        } CATCH_AND_CREATE_YT_ERROR("Failed to exit read-only mode at all master cells");
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, MasterExitReadOnly)

    Py::Object DiscombobulateNonvotingPeers(Py::Tuple& args, Py::Dict& kwargs)
    {
        auto options = TDiscombobulateNonvotingPeersOptions();

        if (!HasArgument(args, kwargs, "cell_id")) {
            throw CreateYtError("Missing argument 'cell_id'");
        }

        auto cellId = NHydra::TCellId::FromString(ConvertStringObjectToString(ExtractArgument(args, kwargs, "cell_id")));

        ValidateArgumentsEmpty(args, kwargs);

        try {
            auto client = CreateClient();
            WaitFor(client->DiscombobulateNonvotingPeers(cellId, options))
                .ThrowOnError();
            return Py::None();
        } CATCH_AND_CREATE_YT_ERROR("Failed to discombobulate nonvoting peers at given cell");
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, DiscombobulateNonvotingPeers)

    Py::Object ClearMetadataCaches(Py::Tuple& args, Py::Dict& kwargs)
    {
        ValidateArgumentsEmpty(args, kwargs);

        try {
            UnderlyingDriver_->ClearMetadataCaches();
            return Py::None();
        } CATCH_AND_CREATE_YT_ERROR("Failed to clear metadata caches");
    }
    PYCXX_KEYWORDS_METHOD_DECL(TDriver, ClearMetadataCaches)

    PYCXX_DECLARE_DRIVER_METHODS(TDriver)

    Py::Type GetDriverType() const override
    {
        return TDriver::type();
    }

private:
    TWeakPtr<IConnection> Connection_;

    IClientPtr CreateClient()
    {
        auto options = TClientOptions::FromUser(NSecurityClient::RootUserName);
        return UnderlyingDriver_->GetConnection()->CreateClient(options);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDriverModule
    : public Py::ExtensionModule<TDriverModule>
    , public TDriverModuleBase
{
public:
    TDriverModule()
        // This should be the same as .so file name.
        : Py::ExtensionModule<TDriverModule>("driver_lib")
    {
        TDriverModuleBase::Initialize(
            "driver_lib",
            [](){TDriver::InitType();},
            [&](){initialize("Python bindings for YT driver");},
            std::bind(&TDriverModule::moduleDictionary, this),
            &TDriverModule::add_keyword_method);

        moduleDictionary().setItem("Driver", TDriver::type());
    }

    virtual ~TDriverModule() = default;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython

////////////////////////////////////////////////////////////////////////////////

#if defined( _WIN32 )
#define EXPORT_SYMBOL __declspec( dllexport )
#else
#define EXPORT_SYMBOL
#endif

static PyObject* init_module()
{
    static auto* driverModule = new NYT::NPython::TDriverModule;
    return driverModule->module().ptr();
}

#if PY_MAJOR_VERSION < 3
extern "C" EXPORT_SYMBOL void initdriver_lib() { Y_UNUSED(init_module()); }
extern "C" EXPORT_SYMBOL void initdriver_lib_d() { initdriver_lib(); }
#else
extern "C" EXPORT_SYMBOL PyObject* PyInit_driver_lib() { return init_module(); }
extern "C" EXPORT_SYMBOL PyObject* PyInit_driver_lib_d() { return PyInit_driver_lib(); }
#endif
