#pragma once

#include "public.h"

#include <yt/yt/orm/server/access_control/config.h>
#include <yt/yt/orm/server/api/config.h>
#include <yt/yt/orm/server/objects/config.h>

#include <yt/yt/server/lib/rpc_proxy/config.h>

#include <yt/yt/client/api/rpc_proxy/config.h>
#include <yt/yt/client/federated/config.h>

#include <yt/yt/ytlib/api/native/public.h>
#include <yt/yt/ytlib/event_log/public.h>

#include <yt/yt/library/auth_server/config.h>
#include <yt/yt/library/program/config.h>
#include <yt/yt/library/program/helpers.h>

#include <yt/yt/library/tracing/jaeger/public.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/http/config.h>
#include <yt/yt/core/https/config.h>
#include <yt/yt/core/misc/config.h>
#include <yt/yt/core/rpc/grpc/config.h>
#include <yt/yt/core/ypath/public.h>

namespace NYT::NOrm::NServer::NMaster {

////////////////////////////////////////////////////////////////////////////////

class TYTConnectorConfig
    : public NYT::NYTree::TYsonStruct
{
public:
    NYT::NApi::NNative::TConnectionCompoundConfigPtr Connection;
    NYT::NApi::NRpcProxy::TConnectionConfigPtr RpcProxyConnection;
    // Used for setup over chaos.
    NYT::NClient::NFederated::TConnectionConfigPtr FederatedConnection;

    //! Used for native connection discovery and authentication.
    std::string ConnectionClusterUrl;
    std::string User;
    //! Token as plain text.
    //! Do not use this option as the plaintext value of secret may be exposed via service orchid or somehow else.
    std::optional<TString> Token;
    //! Name of env variable to get token, it is "YT_TOKEN" by default.
    std::string TokenEnv;
    bool UseServiceTicketAuth;
    TSlruCacheConfigPtr ClientCache;
    NYPath::TYPath RootPath;
    std::string ConsumerDir;
    TClusterTag ClusterTag;
    std::string ClusterName;
    TMasterInstanceTag InstanceTag;
    TDuration InstanceTransactionTimeout;
    TDuration LeaderTransactionTimeout;
    TDuration NodeExpirationTimeout;
    TDuration ReconnectPeriod;
    TDuration MasterDiscoveryPeriod;
    TDuration BanCheckPeriod;
    bool SetupOrchid;

    int ValidationsConcurrencyLimit;

    const TString& GetToken() const;

protected:
    using TRegistrar = NYT::NYTree::TYsonStructRegistrar<TYTConnectorConfig>;
    using TThis = TYTConnectorConfig;

    // Must be called by a derived class.
    static void DoRegister(TRegistrar registrar, bool authenticationRequired);

private:
    TString TokenEnvCachedValue_;
};

DEFINE_REFCOUNTED_TYPE(TYTConnectorConfig)

////////////////////////////////////////////////////////////////////////////////

// Must be used only for ORM instantiation for the given data model.
template <bool AuthenticationRequired = true>
class TDataModelYTConnectorConfig
    : public TYTConnectorConfig
{
protected:
    REGISTER_YSON_STRUCT(TDataModelYTConnectorConfig);

    static void Register(TRegistrar registrar);
};

////////////////////////////////////////////////////////////////////////////////

class TRpcProxyCollocationConfig
    : public NYT::NYTree::TYsonStruct
{
public:
    NRpcProxy::TApiServiceConfigPtr ApiService;
    NRpcProxy::TApiServiceDynamicConfigPtr ApiServiceDynamic;

    REGISTER_YSON_STRUCT(TRpcProxyCollocationConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRpcProxyCollocationConfig)

////////////////////////////////////////////////////////////////////////////////

class TEventLogManagerConfig
    : public NYTree::TYsonStruct
{
public:
    bool Enable;
    TDuration PendingRowsFlushPeriod;

    NEventLog::TEventLogManagerConfigPtr AsYTEventLogManagerConfig();

    REGISTER_YSON_STRUCT(TEventLogManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TEventLogManagerConfig);

////////////////////////////////////////////////////////////////////////////////

class TMasterConfigBase
    : virtual public NYT::NYTree::TYsonStruct
{
public:
    NHttp::TServerConfigPtr MonitoringServer;
    NBus::TBusServerConfigPtr InternalBusServer;
    NRpc::TServerConfigPtr InternalRpcServer;
    NRpc::NGrpc::TServerConfigPtr ClientGrpcServer;
    NRpc::NGrpc::TServerConfigPtr SecureClientGrpcServer;
    NHttps::TServerConfigPtr SecureClientHttpServer;
    NHttp::TServerConfigPtr ClientHttpServer;
    std::optional<TString> FqdnOverride;
    std::optional<TString> IP6AddressOverride;
    NObjects::TTransactionManagerConfigPtr TransactionManager;
    NObjects::TWatchManagerConfigPtr WatchManager;
    NObjects::TPoolWeightManagerConfigPtr PoolWeightManager;
    NAuth::TTvmServiceConfigPtr TvmService;
    NAuth::TAuthenticationManagerConfigPtr AuthenticationManager;
    NApi::TObjectServiceConfigPtr ObjectService;
    TString DBName;
    int WorkerThreadPoolSize;
    int HttpPollerThreadPoolSize;
    bool EnableIP6AddressResolving;
    bool EnableIP6AddressValidation;
    TDuration IP6AddressValidationPeriod;
    //! Config of RPC proxy collocated with the master.
    TRpcProxyCollocationConfigPtr RpcProxyCollocation;
    NYT::NTracing::TSamplerConfigPtr TracingSampler;
    //! Max count of objects to be logged in batch requests.
    i64 LogCountLimitForPluralRequests;
    TEventLogManagerConfigPtr EventLogManager;

    virtual NObjects::TObjectManagerConfigPtr GetObjectManagerConfig() const = 0;
    virtual NAccessControl::TAccessControlManagerConfigPtr GetAccessControlManagerConfig() const = 0;

protected:
    using TRegistrar = NYT::NYTree::TYsonStructRegistrar<TMasterConfigBase>;
    using TThis = TMasterConfigBase;

    // Must be called by a derived class.
    static void DoRegister(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterConfigBase)

////////////////////////////////////////////////////////////////////////////////

class TMasterConfig
    : public NYT::TSingletonsConfig
    , public NYT::TDiagnosticDumpConfig
    , public TMasterConfigBase
{
public:
    NYPath::TYPath EventLogPath;

    bool EnablePortoResourceTracker;
    NContainers::TPodSpecConfigPtr PodSpec;

protected:
    using TRegistrar = NYT::NYTree::TYsonStructRegistrar<TMasterConfig>;
    using TThis = TMasterConfig;

    // Must be called by a derived class.
    static void DoRegister(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TMasterConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterDynamicConfig
    : public NYT::TSingletonsDynamicConfig
    , public TMasterConfigBase
{ };

DEFINE_REFCOUNTED_TYPE(TMasterDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NServer::NMaster

#define CONFIG_INL_H_
#include "config-inl.h"
#undef CONFIG_INL_H_
