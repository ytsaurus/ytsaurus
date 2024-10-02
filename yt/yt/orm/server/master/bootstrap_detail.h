#pragma once

#include "bootstrap.h"
#include "config.h"

#include <yt/yt/orm/server/access_control/public.h>

#include <yt/yt/orm/server/objects/public.h>

#include <yt/yt/ytlib/event_log/public.h>

#include <yt/yt/library/auth_server/public.h>

#include <yt/yt/library/monitoring/public.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/bus/tcp/public.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/https/public.h>

#include <yt/yt/core/net/public.h>

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/core/rpc/bus/public.h>

#include <yt/yt/core/rpc/grpc/public.h>

#include <yt/yt/core/rpc/http/public.h>

namespace NYT::NOrm::NServer::NMaster {

////////////////////////////////////////////////////////////////////////////////

class TBootstrapBase
    : public IBootstrap
{
public:
    TBootstrapBase(
        TMasterConfigPtr config,
        NYTree::INodePtr configNode);

    const NYT::NApi::IStickyTransactionPoolPtr& GetUnderlyingTransactionPool() const override;
    const TYTConnectorPtr& GetYTConnector() const override;
    const NObjects::TObjectManagerPtr& GetObjectManager() const override;
    const NAccessControl::TAccessControlManagerPtr& GetAccessControlManager() const override;
    const NAccessControl::IDataModelInteropPtr& GetAccessControlDataModelInterop() const override;
    const NObjects::TTransactionManagerPtr& GetTransactionManager() const override;
    const NObjects::IPoolWeightManagerPtr& GetPoolWeightManager() const override;
    const NObjects::TWatchManagerPtr& GetWatchManager() const override;
    const NObjects::IWatchLogConsumerInteropPtr& GetWatchLogConsumerInterop() const override;
    const NAuth::ITvmServicePtr& GetTvmService() const override;
    const NAuth::IAuthenticationManagerPtr& GetAuthenticationManager() const override;

    const IInvokerPtr& GetControlInvoker() override;
    IInvokerPtr GetWorkerPoolInvoker() override;
    IInvokerPtr GetWorkerPoolInvoker(const std::string& poolName, const std::string& tag) override;

    const TString& GetDBName() const override;
    const TString& GetFqdn() const override;
    const TString& GetIP6Address() const override;
    const NYT::NNodeTrackerClient::TAddressMap& GetInternalRpcAddresses() const override;
    const TString& GetClientGrpcAddress() const override;
    const TString& GetClientGrpcIP6Address() const override;
    const TString& GetSecureClientGrpcAddress() const override;
    const TString& GetSecureClientGrpcIP6Address() const override;
    const TString& GetClientHttpAddress() const override;
    const TString& GetClientHttpIP6Address() const override;
    const TString& GetSecureClientHttpAddress() const override;
    const TString& GetSecureClientHttpIP6Address() const override;
    const TString& GetMonitoringAddress() const override;
    const TString& GetRpcProxyAddress() const override;
    const TString& GetRpcProxyIP6Address() const override;

    const NYT::NTracing::TSamplerPtr& GetTracingSampler() const override;

    NYTree::IAttributeDictionaryPtr GetInstanceDynamicAttributes() const override;

    const NYTree::INodePtr& GetInitialConfigNode() const override;
    const TMasterConfigPtr& GetInitialConfig() const override;

    TEventLoggerPtr CreateEventLogger(NLogging::TLogger logger) override;

    void Start();
    void Stop();

    DEFINE_SIGNAL_OVERRIDE(void(const TMasterDynamicConfigPtr&), ConfigUpdate);

protected:
    // ORM -> data model interface.
    TString BuildGrpcAddress(const NRpc::NGrpc::TServerConfigPtr& config);
    const NConcurrency::IThreadPoolPollerPtr& GetHttpPoller();

    // ORM <- data model interface.
    virtual void ProcessConfig();

    virtual TYTConnectorPtr CreateYTConnector() = 0;
    virtual NObjects::TObjectManagerPtr CreateObjectManager() = 0;
    virtual NObjects::TTransactionManagerPtr CreateTransactionManager() = 0;
    virtual NAccessControl::TAccessControlManagerPtr CreateAccessControlManager() = 0;
    virtual NAccessControl::IDataModelInteropPtr CreateAccessControlDataModelInterop() = 0;
    virtual NObjects::IWatchLogConsumerInteropPtr CreateWatchLogConsumerInterop() = 0;
    virtual void CreateComponents();
    virtual void InitializeDynamicConfigManager();
    virtual void InitializeComponents();

    virtual NRpc::IServicePtr CreateObjectService() = 0;
    virtual NRpc::IServicePtr CreateClientDiscoveryService() = 0;
    virtual NRpc::IServicePtr CreateSecureClientDiscoveryService() = 0;
    virtual void CreateServices();

    virtual void SetupOrchid(NYTree::IMapNodePtr root) = 0;
    virtual void SetupSensors();

    virtual void RegisterClientGrpcServerServices(NRpc::IServerPtr server);
    virtual void CreateServers();

    virtual void StartDynamicConfigManager();
    virtual void StopDynamicConfigManager();

    virtual void StartComponents();
    virtual void StopComponents();

    // If you override StartServers/StopServers, do preserve the ordering inversion.
    virtual void StartServers();
    // Shut everything down in reverse order w.r.t. startup.
    virtual void StopServers();

    //! Allows adding client-specific handlers for internal use.
    const NHttp::IServerPtr& GetHttpMonitoringServer();

private:
    const NYTree::INodePtr InitialConfigNode_;
    const TMasterConfigPtr InitialConfig_;

    const NConcurrency::TActionQueuePtr ControlQueue_;
    const NObjects::IPoolWeightManagerPtr PoolWeightManager_;
    const NConcurrency::ITwoLevelFairShareThreadPoolPtr WorkerPool_;

    TMasterDynamicConfigPtr DynamicConfig_;

    //! Used by transaction manager; managed by the collocated RPC proxy.
    NYT::NApi::IStickyTransactionPoolPtr UnderlyingTransactionPool_;

    TYTConnectorPtr YTConnector_;
    NObjects::TObjectManagerPtr ObjectManager_;
    NAccessControl::TAccessControlManagerPtr AccessControlManager_;
    NAccessControl::IDataModelInteropPtr AccessControlDataModelInterop_;
    NObjects::TTransactionManagerPtr TransactionManager_;
    NObjects::TWatchManagerPtr WatchManager_;
    NObjects::IWatchLogConsumerInteropPtr WatchLogConsumerInterop_;
    NAuth::ITvmServicePtr TvmService_;
    NAuth::IAuthenticationManagerPtr AuthenticationManager_;
    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    NEventLog::IEventLogWriterPtr EventLogWriter_;

    NRpc::IServicePtr ObjectService_;
    NRpc::IServicePtr ClientDiscoveryService_;
    NRpc::IServicePtr SecureClientDiscoveryService_;

    NConcurrency::IThreadPoolPollerPtr HttpPoller_;

    NHttp::IServerPtr HttpMonitoringServer_;
    NRpc::IServerPtr ClientHttpRpcServer_;
    NRpc::IServerPtr SecureClientHttpRpcServer_;
    NRpc::IServerPtr ClientGrpcServer_;
    NRpc::IServerPtr SecureClientGrpcServer_;
    NBus::IBusServerPtr InternalBusServer_;
    NRpc::IServerPtr InternalRpcServer_;

    NTracing::TSamplerPtr TracingSampler_;

    TString Fqdn_;
    TString IP6Address_;
    NNodeTrackerClient::TAddressMap InternalRpcAddresses_;
    TString ClientGrpcAddress_;
    TString ClientGrpcIP6Address_;
    TString SecureClientGrpcAddress_;
    TString SecureClientGrpcIP6Address_;
    TString ClientHttpAddress_;
    TString ClientHttpIP6Address_;
    TString SecureClientHttpAddress_;
    TString SecureClientHttpIP6Address_;
    TString MonitoringAddress_;
    TString RpcProxyAddress_;
    TString RpcProxyIP6Address_;

    NConcurrency::TPeriodicExecutorPtr IP6ValidationExecutor_;
    NProfiling::TCounter IP6ResolutionFailures_;

    void OnConfigUpdate(const TMasterDynamicConfigPtr& masterConfig);

    int GetGrpcPort(const NRpc::NGrpc::TServerConfigPtr& config);

    TString BuildGrpcIP6Address(const NRpc::NGrpc::TServerConfigPtr& config);
    TString BuildHttpAddress(const NHttp::TServerConfigPtr& config);
    TString BuildHttpIP6Address(const NHttp::TServerConfigPtr& config);
    TString BuildInternalRpcAddress(const NBus::TBusServerConfigPtr& config);
    TString BuildInternalRpcIP6Address(const NBus::TBusServerConfigPtr& config);

    TString ResolveIP6Address() const;
    void ValidateIP6Address() const;

    void DoStart();
    void DoStop();

    bool IsAlive() const;

    void HealthCheckHandler(
        const NHttp::IRequestPtr& /*req*/,
        const NHttp::IResponseWriterPtr& rsp) const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NMaster
