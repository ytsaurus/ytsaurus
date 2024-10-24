#pragma once

#include <yt/yt/core/misc/error.h>

#include <yt/yt/ytlib/api/public.h>
#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

//! Singular (!) names of cluster components.
DEFINE_ENUM(EClusterComponentType,
    //! All masters (primary and secondary).
    ((ClusterMaster)      (0))
    ((PrimaryMaster)      (1))

    ((Scheduler)          (2))
    ((ControllerAgent)    (3))

    //! All nodes (data, tablet and exec).
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

struct TComponentDiscoveryOptions
{
    //! This callback *must* be provided and should return a duration after
    //! which a HTTP proxy is considered offline by the discovery mechanism.
    TCallback<TDuration()> ProxyDeathAgeCallback;
    //! Timeout used for batch requests to master.
    TDuration BatchRequestTimeout = TDuration::Seconds(5);
};

//! This class encapsulates everything related to the discovery of YT component instances from Cypress virtual maps and orchids.
class TComponentDiscoverer
{
public:
    //! The provided master read options will be used for all Get/List requests performed for discovery.
    TComponentDiscoverer(
        NApi::NNative::IClientPtr client,
        NApi::TMasterReadOptions masterReadOptions,
        TComponentDiscoveryOptions componentDiscoveryOptions);

    std::vector<TClusterComponentInstance> GetInstances(EClusterComponentType component) const;
    //! Lists instances of all components present in the `EClusterComponentType` enum.
    std::vector<TClusterComponentInstance> GetAllInstances() const;

    //! Returns object node Cypress paths for instances of the specified component.
    static std::vector<TString> GetCypressPaths(NApi::IClientPtr client, const NApi::TMasterReadOptions& masterReadOptions, EClusterComponentType component);

private:
    const NApi::NNative::IClientPtr Client_;
    const NApi::TMasterReadOptions MasterReadOptions_;
    const TComponentDiscoveryOptions ComponentDiscoveryOptions_;

    //! Returns Cypress directory containing object nodes for the requested component.
    static TString GetCypressDirectory(EClusterComponentType component);
    //! Returns Cypress subpaths that can be joined with the directory returned by the function above
    //! to produce full paths to object nodes for instances of the requested component.
    static std::vector<TString> GetCypressSubpaths(NApi::IClientPtr client, const NApi::TMasterReadOptions& masterReadOptions, EClusterComponentType component);

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
