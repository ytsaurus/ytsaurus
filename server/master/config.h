#pragma once

#include "public.h"

#include <yp/server/objects/config.h>

#include <yp/server/net/config.h>

#include <yp/server/nodes/config.h>

#include <yp/server/scheduler/config.h>

#include <yp/server/access_control/config.h>

#include <yt/ytlib/program/config.h>

#include <yt/ytlib/api/config.h>

#include <yt/ytlib/auth/config.h>

#include <yt/core/http/config.h>

#include <yt/core/https/config.h>

#include <yt/core/rpc/grpc/config.h>

#include <yt/core/ypath/public.h>

namespace NYP {
namespace NServer {
namespace NMaster {

////////////////////////////////////////////////////////////////////////////////

class TYTConnectorConfig
    : public NYT::NYTree::TYsonSerializable
{
public:
    NYT::NApi::TNativeConnectionConfigPtr Connection;
    TString User;
    NYT::NYPath::TYPath RootPath;
    TClusterTag ClusterTag;
    TMasterInstanceTag InstanceTag;
    TDuration InstanceTransactionTimeout;
    TDuration LeaderTransactionTimeout;
    TDuration ReconnectPeriod;
    TDuration MasterDiscoveryPeriod;

    TYTConnectorConfig()
    {
        RegisterParameter("connection", Connection);
        RegisterParameter("user", User)
            .Default("yp");
        RegisterParameter("root_path", RootPath)
            .Default("//yp");
        RegisterParameter("cluster_tag", ClusterTag);
        RegisterParameter("instance_tag", InstanceTag);
        RegisterParameter("instance_transaction_timeout", InstanceTransactionTimeout)
            .Default(TDuration::Seconds(10));
        RegisterParameter("leader_transaction_timeout", LeaderTransactionTimeout)
            .Default(TDuration::Seconds(10));
        RegisterParameter("reconnect_period", ReconnectPeriod)
            .Default(TDuration::Seconds(5));
        RegisterParameter("master_discovery_period", MasterDiscoveryPeriod)
            .Default(TDuration::Seconds(5));

        RegisterPostprocessor([&] {
            // Don't use custom thread pool in YT connection.
            Connection->ThreadPoolSize = Null;
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TYTConnectorConfig)

////////////////////////////////////////////////////////////////////////////////

class TMasterConfig
    : public NYT::TSingletonsConfig
{
public:
    NHttp::TServerConfigPtr MonitoringServer;
    NYT::NRpc::NGrpc::TServerConfigPtr ClientGrpcServer;
    NYT::NRpc::NGrpc::TServerConfigPtr SecureClientGrpcServer;
    NYT::NRpc::NGrpc::TServerConfigPtr AgentGrpcServer;
    NYT::NHttps::TServerConfigPtr SecureClientHttpServer;
    NYT::NHttp::TServerConfigPtr ClientHttpServer;
    TYTConnectorConfigPtr YTConnector;
    NObjects::TObjectManagerConfigPtr ObjectManager;
    NNet::TNetManagerConfigPtr NetManager;
    NObjects::TTransactionManagerConfigPtr TransactionManager;
    NNodes::TNodeTrackerConfigPtr NodeTracker;
    NScheduler::TSchedulerConfigPtr Scheduler;
    NAccessControl::TAccessControlManagerConfigPtr AccessControlManager;
    int WorkerThreadPoolSize;

    TMasterConfig()
    {
        RegisterParameter("monitoring_server", MonitoringServer);
        RegisterParameter("client_grpc_server", ClientGrpcServer)
            .Optional();
        RegisterParameter("secure_client_grpc_server", SecureClientGrpcServer)
            .Optional();
        RegisterParameter("agent_grpc_server", AgentGrpcServer)
            .Optional();
        RegisterParameter("secure_client_http_server", SecureClientHttpServer)
            .Optional();
        RegisterParameter("client_http_server", ClientHttpServer)
            .Optional();
        RegisterParameter("yt_connector", YTConnector);
        RegisterParameter("object_manager", ObjectManager)
            .DefaultNew();
        RegisterParameter("net_manager", NetManager)
            .DefaultNew();
        RegisterParameter("transaction_manager", TransactionManager)
            .DefaultNew();
        RegisterParameter("node_tracker", NodeTracker)
            .DefaultNew();
        RegisterParameter("scheduler", Scheduler)
            .DefaultNew();
        RegisterParameter("access_control_manager", AccessControlManager)
            .DefaultNew();
        RegisterParameter("worker_thread_pool_size", WorkerThreadPoolSize)
            .GreaterThan(0)
            .Default(8);

        RegisterPostprocessor([&] {
            if (ClientGrpcServer && ClientGrpcServer->Addresses.size() != 1) {
                THROW_ERROR_EXCEPTION("Exactly one GRPC API server address must be given in \"client_grpc_server\"");
            }
            if (SecureClientGrpcServer && SecureClientGrpcServer->Addresses.size() != 1) {
                THROW_ERROR_EXCEPTION("Exactly one GRPC API server address must be given \"secure_client_grpc_server\"");
            }
            if (AgentGrpcServer->Addresses.size() != 1) {
                THROW_ERROR_EXCEPTION("Exactly one GRPC agent server address must be given in \"agent_grpc_server\"");
            }
        });
    }
};

DEFINE_REFCOUNTED_TYPE(TMasterConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NMaster
} // namespace NNodes
} // namespace NYP
