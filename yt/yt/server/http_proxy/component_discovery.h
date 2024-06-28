#pragma once

#include <yt/yt/core/misc/error.h>

#include <yt/yt/ytlib/api/public.h>
#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

//! Singular (!) names of cluster components.
DEFINE_ENUM(EClusterComponentType,
    ((PrimaryMaster)      (0))
    ((SecondaryMaster)    (1))

    ((Scheduler)          (2))
    ((ControllerAgent)    (3))

    ((ClusterNode)        (4))
    ((DataNode)           (5))
    ((TabletNode)         (6))
    ((ExecNode)           (7))
    ((JobProxy)           (8))

    ((HttpProxy)          (9))
    ((RpcProxy)           (10))
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

//! This class encapsulates everything related to the discovery of YT component instances from Cypress virtual maps and orchids.
class TComponentDiscoverer
{
public:
    //! The provided master read options will be used for all Get/List requests performed for discovery.
    //! The provided death age duration will be used as a threshold for HTTP proxy liveness considerations.
    TComponentDiscoverer(
        NApi::IClientPtr client,
        NApi::TMasterReadOptions masterReadOptions,
        TDuration proxyDeathAge);

    std::vector<TClusterComponentInstance> GetInstances(EClusterComponentType component) const;
    //! Lists instances of all components present in the `EClusterComponentType` enum.
    std::vector<TClusterComponentInstance> GetAllInstances() const;

private:
    const NApi::IClientPtr Client_;
    const NApi::TMasterReadOptions MasterReadOptions_;
    const TDuration ProxyDeathAge_;

    static TString GetCypressDirectory(EClusterComponentType component);
    std::vector<TString> GetCypressSubpaths(EClusterComponentType component) const;

    std::vector<TClusterComponentInstance> GetAttributes(
        EClusterComponentType component,
        const std::vector<TString>& subpaths,
        EClusterComponentType instanceType,
        const NYPath::TYPath& suffix = "/orchid/service") const;

    std::vector<TClusterComponentInstance> ListNodes(EClusterComponentType component) const;
    std::vector<TClusterComponentInstance> ListProxies(EClusterComponentType component) const;
    std::vector<TClusterComponentInstance> ListJobProxies() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
