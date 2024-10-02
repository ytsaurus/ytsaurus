#include "bootstrap.h"

#include <yt/yt/orm/example/server/library/autogen/config.h>
#include <yt/yt/orm/example/server/library/autogen/db_schema.h>
#include <yt/yt/orm/example/server/library/autogen/dynamic_config_manager.h>
#include <yt/yt/orm/example/server/library/autogen/object_manager.h>
#include <yt/yt/orm/example/server/proto/autogen/continuation_token.pb.h>

#include <yt/yt/orm/example/client/native/autogen/object_service_proxy.h>
#include <yt/yt/orm/example/client/proto/data_model/autogen/schema.pb.h>
#include <yt/yt/orm/example/client/proto/api/discovery_service.pb.h>
#include <yt/yt/orm/example/client/proto/api/object_service.pb.h>

#include <yt/yt/orm/server/access_control/access_control_manager.h>
#include <yt/yt/orm/server/access_control/data_model_interop.h>
#include <yt/yt/orm/server/api/discovery_service.h>
#include <yt/yt/orm/server/api/object_service.h>
#include <yt/yt/orm/server/master/bootstrap_detail.h>
#include <yt/yt/orm/server/master/helpers.h>
#include <yt/yt/orm/server/master/yt_connector.h>
#include <yt/yt/orm/server/objects/db_config.h>
#include <yt/yt/orm/server/objects/history_manager.h>
#include <yt/yt/orm/server/objects/transaction_manager.h>
#include <yt/yt/orm/server/objects/watch_log_consumer_interop.h>
#include <yt/yt/orm/server/objects/watch_manager.h>

#include <yt/yt/library/auth_server/authentication_manager.h>
#include <yt/yt/library/program/build_attributes.h>
#include <yt/yt/library/program/helpers.h>

#include <yt/yt/core/rpc/grpc/server.h>
#include <yt/yt/core/ytree/helpers.h>
#include <yt/yt/core/ytree/virtual.h>

#include <util/system/compiler.h>

namespace NYT::NOrm::NExample::NServer::NLibrary {

using namespace NYT::NAuth;
using namespace NYT::NConcurrency;
using namespace NYT::NYTree;
using namespace NYT::NYson;

using namespace NYT::NOrm::NServer::NAccessControl;
using namespace NYT::NOrm::NServer::NMaster;
using namespace NYT::NOrm::NServer::NObjects;

////////////////////////////////////////////////////////////////////////////////

namespace {

DEFINE_OBJECT_SERVICE_PROTO_MODULE_FROM_NAMESPACES_SEPARATELY(
    NClient::NProto::NDataModel,
    NClient::NProto,
    NServer::NProto::NAutogen)
DEFINE_DISCOVERY_SERVICE_PROTO_MODULE_FROM_NAMESPACE(NClient::NProto)

static const NRpc::TServiceDescriptor DiscoveryServiceDescriptor("NYT.NOrm.NExample.NClient.NProto.DiscoveryService");

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public TBootstrapBase
{
public:
    TBootstrap(TMasterConfigPtr config, INodePtr configNode)
        : TBootstrapBase(config, configNode)
        , DBConfig_(CreateDBConfig())
        , InitialConfig_(std::move(config))
        , OrmMasterInitialConfig_(InitialConfig_)
    {
        ValidateHistoryTables(&HistoryEventsTable, &HistoryIndexTable);
        ValidateHistoryTables(&HistoryEventsV2Table, &HistoryIndexV2Table);
    }

    const TString& GetServiceName() const override
    {
        static const TString ServiceName = "example";
        return ServiceName;
    }

    const TDBConfig& GetDBConfig() const override
    {
        return DBConfig_;
    }

protected:
    NYT::NOrm::NServer::NMaster::TYTConnectorPtr CreateYTConnector() override
    {
        return New<NYT::NOrm::NServer::NMaster::TYTConnector>(
            this,
            InitialConfig_->YTConnector,
            DBVersion);
    }

    NYT::NOrm::NServer::NObjects::TObjectManagerPtr CreateObjectManager() override
    {
        return NLibrary::CreateObjectManager(
            this,
            InitialConfig_->ObjectManager);
    }

    NYT::NOrm::NServer::NAccessControl::TAccessControlManagerPtr CreateAccessControlManager() override
    {
        return New<NYT::NOrm::NServer::NAccessControl::TAccessControlManager>(
            this,
            NYT::NYson::ReflectProtobufEnumType(NClient::NProto::NDataModel::EAccessControlPermission_descriptor()),
            InitialConfig_->AccessControlManager);
    }

    NYT::NOrm::NServer::NAccessControl::IDataModelInteropPtr CreateAccessControlDataModelInterop() override
    {
        return NYT::NOrm::NServer::NAccessControl::CreateDataModelInterop<
            TUsersTable,
            TGroupsTable,
            NClient::NProto::NDataModel::TUserSpec,
            NClient::NProto::NDataModel::TGroupSpec,
            NClient::NProto::NDataModel::TAccessControlEntry>(
            &UsersTable,
            &GroupsTable);
    }

    NYT::NOrm::NServer::NObjects::IWatchLogConsumerInteropPtr CreateWatchLogConsumerInterop() override
    {
        return NOrm::NServer::NObjects::CreateWatchLogConsumerInterop<
            TWatchLogConsumersTable,
            NClient::NProto::NDataModel::TWatchLogConsumerStatus,
            NClient::NProto::NDataModel::TWatchLogConsumerSpec>(&WatchLogConsumersTable);
    }

    NYT::NOrm::NServer::NObjects::TTransactionManagerPtr CreateTransactionManager() override
    {
        return New<NYT::NOrm::NServer::NObjects::TTransactionManager>(
            this,
            InitialConfig_->TransactionManager);
    }

////////////////////////////////////////////////////////////////////////////////

    NRpc::IServicePtr CreateObjectService() override
    {
        return NYT::NOrm::NServer::NApi::CreateObjectService<
            TObjectServiceProtoModule>(
            this,
            InitialConfig_->ObjectService,
            NClient::NNative::TObjectServiceProxy::GetDescriptor());
    }

    NRpc::IServicePtr CreateClientDiscoveryService() override
    {
        return NYT::NOrm::NServer::NApi::CreateClientDiscoveryService<
            TDiscoveryServiceProtoModule>(
            this,
            DiscoveryServiceDescriptor);
    }

    NRpc::IServicePtr CreateSecureClientDiscoveryService() override
    {
        return NYT::NOrm::NServer::NApi::CreateSecureClientDiscoveryService<
            TDiscoveryServiceProtoModule>(
            this,
            DiscoveryServiceDescriptor);
    }

    void InitializeDynamicConfigManager() override
    {
        DynamicConfigManager_ = CreateDynamicConfigManager(this, InitialConfig_);
        DynamicConfigManager_->SubscribeConfigChanged(BIND(&TBootstrap::OnDynamicConfigChanged, this));
    }

    void StartDynamicConfigManager() override
    {
        DynamicConfigManager_->Start();
        WaitFor(DynamicConfigManager_->GetConfigLoadedFuture())
            .ThrowOnError();
    }

////////////////////////////////////////////////////////////////////////////////

    void SetupOrchid(NYTree::IMapNodePtr root) override
    {
        auto controlInvoker = GetControlInvoker();
        // COMPAT(ignat)
        SetNodeByYPath(
            root,
            "/config",
            CreateVirtualNode(IYPathService::FromProducer(
                BIND([this] (IYsonConsumer* consumer) {
                    BuildYsonFluently(consumer)
                        .Value(DynamicConfigManager_->GetConfig());
                }))));
        SetNodeByYPath(
            root,
            "/initial_config",
            CreateVirtualNode(IYPathService::FromProducer(
                BIND([this] (IYsonConsumer* consumer) {
                    BuildYsonFluently(consumer)
                        .Value(DynamicConfigManager_->GetInitialConfig());
                }))));

        SetNodeByYPath(
            root,
            "/dynamic_config_manager",
            CreateVirtualNode(DynamicConfigManager_->GetOrchidService()));
        SetNodeByYPath(
            root,
            "/access_control",
            CreateVirtualNode(GetAccessControlManager()->CreateOrchidService()->Via(controlInvoker)));
        SetNodeByYPath(
            root,
            "/object_manager",
            CreateVirtualNode(GetObjectManager()->CreateOrchidService()->Via(controlInvoker)));
        SetBuildAttributes(
            root,
            "example_master");
    }

////////////////////////////////////////////////////////////////////////////////

    const NYT::NOrm::NServer::NObjects::TParentsTable* GetParentsTable() const override
    {
        return DoGetParentsTable();
    }

    IHistoryManagerPtr MakeHistoryManager(TTransaction* transaction) const override
    {
        return MakeMigrationHistoryManager(
            transaction,
            /*currentTable*/ &HistoryIndexTable,
            /*targetTable*/ &HistoryIndexV2Table);
    }

private:
    const TDBConfig DBConfig_;

    TMasterConfigPtr InitialConfig_;
    NOrm::NServer::NMaster::TMasterConfigPtr OrmMasterInitialConfig_;

    IDynamicConfigManagerPtr DynamicConfigManager_;

    void OnDynamicConfigChanged(
        const TMasterDynamicConfigPtr& /*oldConfig*/,
        const TMasterDynamicConfigPtr& newConfig)
    {
        ReconfigureSingletons(InitialConfig_, newConfig);

        ConfigUpdate_.Fire(newConfig);
    }
};

////////////////////////////////////////////////////////////////////////////////

IBootstrap* StartBootstrap(TMasterConfigPtr config, INodePtr configNode)
{
    // TODO(babenko): This memory leak is intentional.
    // We should avoid destroying bootstrap since some of the subsystems
    // may be holding a reference to it and continue running some actions in background threads.
    auto* bootstrap = new TBootstrap(std::move(config), std::move(configNode));
    DoNotOptimizeAway(bootstrap);
    bootstrap->Start();
    return bootstrap;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NExample::NServer::NLibrary
