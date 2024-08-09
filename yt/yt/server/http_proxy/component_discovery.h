#pragma once

#include <yt/yt/core/misc/error.h>

#include <yt/yt/ytlib/api/public.h>
#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

//! Singular (!) names of cluster components.
DEFINE_ENUM(EClusterComponentType,
    ((Master)             (0))

    ((Scheduler)          (1))
    ((ControllerAgent)    (2))

    ((ClusterNode)        (3))
    ((DataNode)           (4))
    ((TabletNode)         (5))
    ((ExecNode)           (6))
    ((JobProxy)           (7))

    ((HttpProxy)          (8))
    ((RpcProxy)           (9))
);

// TODO(achulkov2): MasterCache, QueueAgent, QueryTracker, TabletBalancer.

////////////////////////////////////////////////////////////////////////////////

//! This struct represents common attributes of instances of all YT components.
//! It is used in multiple endpoints exposed by the http proxy.
struct TClusterComponentInstance
{
    EClusterComponentType Type;
    //! Instance fqdn with port.
    TString Address;
    //! Instance build version string.
    TString Version;
    TString StartTime;

    bool Banned = false;
    bool Online = true;
    TString State;

    TError Error;

    //! For exec nodes only.
    std::optional<TString> JobProxyVersion;
};

void Serialize(const TClusterComponentInstance& instance, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TComponentDiscoveryOptions
{
    //! Duration after which a HTTP proxy is considered offline by the discovery mechanism.
    TDuration ProxyDeathAge = TDuration::Minutes(2);
    //! Timeout used for batch requests to master.
    TDuration BatchRequestTimeout = TDuration::Seconds(5);
};

//! This class encapsulates everything related to the discovery of YT component instances from Cypress virtual maps and orchids.
class TComponentDiscoverer
{
public:
    //! The provided master read options will be used for all Get/List requests performed for discovery.
    //! The provided death age duration will be used as a threshold for HTTP proxy liveness considerations.
    TComponentDiscoverer(
        NApi::NNative::IClientPtr client,
        NApi::TMasterReadOptions masterReadOptions,
        TComponentDiscoveryOptions componentDiscoveryOptions);

    std::vector<TClusterComponentInstance> GetInstances(EClusterComponentType component) const;
    //! Lists instances of all components present in the `EClusterComponentType` enum.
    std::vector<TClusterComponentInstance> GetAllInstances() const;

private:
    const NApi::NNative::IClientPtr Client_;
    const NApi::TMasterReadOptions MasterReadOptions_;
    const TComponentDiscoveryOptions ComponentDiscoveryOptions_;

    static TString GetCypressDirectory(EClusterComponentType component);
    std::vector<TString> GetCypressSubpaths(EClusterComponentType component) const;

    std::vector<TClusterComponentInstance> GetAttributes(
        EClusterComponentType component,
        const std::vector<TString>& subpaths,
        EClusterComponentType instanceType,
        const NYPath::TYPath& suffix = "/orchid/service") const;

    std::vector<TClusterComponentInstance> ListClusterNodes(EClusterComponentType component) const;
    std::vector<TClusterComponentInstance> ListProxies(EClusterComponentType component) const;
    std::vector<TClusterComponentInstance> ListJobProxies() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
