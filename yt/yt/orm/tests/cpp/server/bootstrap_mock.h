#pragma once

#include <yt/yt/orm/server/master/bootstrap.h>

#include <yt/yt/orm/server/objects/history_manager.h>

#include <yt/yt/library/tvm/service/tvm_service.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NOrm::NServer::NMaster {

////////////////////////////////////////////////////////////////////////////////

class TBootstrapMock
    : public IBootstrap
{
public:
    MOCK_METHOD(const NYT::NApi::IStickyTransactionPoolPtr&, GetUnderlyingTransactionPool, (), (const, override));
    MOCK_METHOD(const TYTConnectorPtr&, GetYTConnector, (), (const, override));
    MOCK_METHOD(const NAccessControl::TAccessControlManagerPtr&, GetAccessControlManager, (), (const, override));
    MOCK_METHOD(const NAccessControl::IDataModelInteropPtr&, GetAccessControlDataModelInterop, (), (const, override));
    MOCK_METHOD(const NObjects::TObjectManagerPtr&, GetObjectManager, (), (const, override));
    MOCK_METHOD(const NObjects::TTransactionManagerPtr&, GetTransactionManager, (), (const, override));
    MOCK_METHOD(const NObjects::IPoolWeightManagerPtr&, GetPoolWeightManager, (), (const, override));
    MOCK_METHOD(const NObjects::TWatchManagerPtr&, GetWatchManager, (), (const, override));
    MOCK_METHOD(const NObjects::IWatchLogConsumerInteropPtr&, GetWatchLogConsumerInterop, (), (const, override));
    MOCK_METHOD(const NAuth::ITvmServicePtr&, GetTvmService, (), (const, override));
    MOCK_METHOD(const NAuth::IAuthenticationManagerPtr&, GetAuthenticationManager, (), (const, override));
    MOCK_METHOD(const IInvokerPtr&, GetControlInvoker, (), (override));
    MOCK_METHOD(IInvokerPtr, GetWorkerPoolInvoker, (), (override));
    MOCK_METHOD(IInvokerPtr, GetWorkerPoolInvoker, (const std::string&, const std::string&), (override));
    MOCK_METHOD(const TString&, GetServiceName, (), (const, override));
    MOCK_METHOD(const TString&, GetDBName, (), (const, override));
    MOCK_METHOD(const TString&, GetFqdn, (), (const, override));
    MOCK_METHOD(const TString&, GetIP6Address, (), (const, override));
    MOCK_METHOD(const NYT::NNodeTrackerClient::TAddressMap&, GetInternalRpcAddresses, (), (const, override));
    MOCK_METHOD(const TString&, GetClientGrpcAddress, (), (const, override));
    MOCK_METHOD(const TString&, GetClientGrpcIP6Address, (), (const, override));
    MOCK_METHOD(const TString&, GetSecureClientGrpcAddress, (), (const, override));
    MOCK_METHOD(const TString&, GetSecureClientGrpcIP6Address, (), (const, override));
    MOCK_METHOD(const TString&, GetClientHttpAddress, (), (const, override));
    MOCK_METHOD(const TString&, GetClientHttpIP6Address, (), (const, override));
    MOCK_METHOD(const TString&, GetSecureClientHttpAddress, (), (const, override));
    MOCK_METHOD(const TString&, GetSecureClientHttpIP6Address, (), (const, override));
    MOCK_METHOD(const TString&, GetMonitoringAddress, (), (const, override));
    MOCK_METHOD(const TString&, GetRpcProxyAddress, (), (const, override));
    MOCK_METHOD(const TString&, GetRpcProxyIP6Address, (), (const, override));
    MOCK_METHOD(NYTree::IAttributeDictionaryPtr, GetInstanceDynamicAttributes, (), (const, override));
    MOCK_METHOD(const NYTree::INodePtr&, GetInitialConfigNode, (), (const, override));
    MOCK_METHOD(const TMasterConfigPtr&, GetInitialConfig, (), (const, override));
    MOCK_METHOD(void, SubscribeConfigUpdate, (const TCallback<void(const NOrm::NServer::NMaster::TMasterDynamicConfigPtr&)>&), (override));
    MOCK_METHOD(void, UnsubscribeConfigUpdate, (const TCallback<void(const NOrm::NServer::NMaster::TMasterDynamicConfigPtr&)>&), (override));
    MOCK_METHOD(const NObjects::TDBConfig&, GetDBConfig, (), (const, override));
    MOCK_METHOD(const NObjects::TParentsTable*, GetParentsTable, (), (const, override));
    MOCK_METHOD(NObjects::IHistoryManagerPtr, MakeHistoryManager, (NObjects::TTransaction* transaction), (const, override));
    MOCK_METHOD(NOrm::NServer::NMaster::TEventLoggerPtr, CreateEventLogger, (NLogging::TLogger), (override));
    MOCK_METHOD(const NYT::NTracing::TSamplerPtr&, GetTracingSampler, (), (const, override));
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NMaster
