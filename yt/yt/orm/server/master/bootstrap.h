#pragma once

#include "public.h"

#include <yt/yt/orm/server/objects/db_schema.h>
#include <yt/yt/orm/server/objects/public.h>

#include <yt/yt/orm/server/access_control/public.h>

#include <yt/yt/library/auth_server/public.h>

#include <yt/yt/library/tracing/jaeger/public.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/client/api/public.h>

#include <yt/yt/core/actions/public.h>
#include <yt/yt/core/concurrency/public.h>
#include <yt/yt/core/ytree/public.h>

namespace NYT::NOrm::NServer::NMaster {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap
{
    virtual ~IBootstrap() = default;

    virtual const NYT::NApi::IStickyTransactionPoolPtr& GetUnderlyingTransactionPool() const = 0;

    virtual const TYTConnectorPtr& GetYTConnector() const = 0;

    virtual const NAccessControl::TAccessControlManagerPtr& GetAccessControlManager() const = 0;
    virtual const NAccessControl::IDataModelInteropPtr& GetAccessControlDataModelInterop() const = 0;

    virtual const NObjects::TObjectManagerPtr& GetObjectManager() const = 0;
    virtual const NObjects::TTransactionManagerPtr& GetTransactionManager() const = 0;

    virtual const NObjects::IPoolWeightManagerPtr& GetPoolWeightManager() const = 0;

    virtual const NObjects::TWatchManagerPtr& GetWatchManager() const = 0;
    virtual const NObjects::IWatchLogConsumerInteropPtr& GetWatchLogConsumerInterop() const = 0;

    virtual const NAuth::ITvmServicePtr& GetTvmService() const = 0;
    virtual const NAuth::IAuthenticationManagerPtr& GetAuthenticationManager() const = 0;

    virtual const IInvokerPtr& GetControlInvoker() = 0;
    virtual IInvokerPtr GetWorkerPoolInvoker() = 0;
    virtual IInvokerPtr GetWorkerPoolInvoker(const std::string& poolName, const std::string& tag) = 0;

    virtual const TString& GetServiceName() const = 0;
    virtual const TString& GetDBName() const = 0;
    virtual const TString& GetFqdn() const = 0;
    virtual const TString& GetIP6Address() const = 0;
    virtual const NYT::NNodeTrackerClient::TAddressMap& GetInternalRpcAddresses() const = 0;
    virtual const TString& GetClientGrpcAddress() const = 0;
    virtual const TString& GetClientGrpcIP6Address() const = 0;
    virtual const TString& GetSecureClientGrpcAddress() const = 0;
    virtual const TString& GetSecureClientGrpcIP6Address() const = 0;
    virtual const TString& GetClientHttpAddress() const = 0;
    virtual const TString& GetClientHttpIP6Address() const = 0;
    virtual const TString& GetSecureClientHttpAddress() const = 0;
    virtual const TString& GetSecureClientHttpIP6Address() const = 0;
    virtual const TString& GetMonitoringAddress() const = 0;
    virtual const TString& GetRpcProxyAddress() const = 0;
    virtual const TString& GetRpcProxyIP6Address() const = 0;

    virtual const NYT::NTracing::TSamplerPtr& GetTracingSampler() const = 0;

    virtual NYTree::IAttributeDictionaryPtr GetInstanceDynamicAttributes() const = 0;

    virtual const NYTree::INodePtr& GetInitialConfigNode() const = 0;
    virtual const TMasterConfigPtr& GetInitialConfig() const = 0;

    virtual const NObjects::TDBConfig& GetDBConfig() const = 0;

    virtual const NObjects::TParentsTable* GetParentsTable() const = 0;

    virtual NObjects::IHistoryManagerPtr MakeHistoryManager(NObjects::TTransaction* transaction) const = 0;

    virtual TEventLoggerPtr CreateEventLogger(NLogging::TLogger logger) = 0;

    DECLARE_INTERFACE_SIGNAL(void(const TMasterDynamicConfigPtr&), ConfigUpdate);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NMaster
