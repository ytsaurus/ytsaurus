#pragma once

#include <yt/yt/core/misc/error.h>

#include <yt/yt/ytlib/api/public.h>
#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

//! Singular (!) names of cluster components.
DEFINE_ENUM(EClusterComponentType,
    ((PrimaryMaster)            (0))
    ((SecondaryMaster)          (1))

    ((Scheduler)                (2))
    ((ControllerAgent)          (3))

    ((ClusterNode)              (4))
    ((DataNode)                 (5))
    ((TabletNode)               (6))
    ((ExecNode)                 (7))
    ((JobProxy)                 (8))

    ((HttpProxy)                (9))
    ((RpcProxy)                (10))

    ((TimestampProvider)       (11))
    ((Discovery)               (12))
    ((MasterCache)             (13))

    ((TabletBalancer)          (14))
    ((BundleController)        (15))
    ((ReplicatedTableTracker)  (16))
    ((QueueAgent)              (17))
    ((QueryTracker)            (18))
);

////////////////////////////////////////////////////////////////////////////////

//! This struct represents common attributes of instances of all YT components.
//! It is used in multiple endpoints exposed by the http proxy.
struct TClusterComponentInstance
{
    EClusterComponentType Type;
    //! Instance FQDN with port.
    std::string Address;
    //! Instance build version string.
    std::string Version;
    std::string StartTime;

    bool Banned = false;
    bool Online = true;
    std::string State;

    TError Error;

    //! For exec nodes only.
    std::optional<std::string> JobProxyVersion;
};

void Serialize(const TClusterComponentInstance& instance, NYson::IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

struct TComponentDiscoveryOptions
{
    //! This callback *must* be provided and should return a duration after
    //! which a HTTP proxy is considered offline by the discovery mechanism.
    TCallback<TDuration()> ProxyDeathAgeCallback;
};

//! This class encapsulates everything related to the discovery of YT component instances from Cypress virtual maps and orchids.
class TComponentDiscoverer
{
public:
    //! The provided master read options will be used for all Get/List requests performed for discovery.
    TComponentDiscoverer(
        NApi::IClientPtr client,
        NApi::TMasterReadOptions masterReadOptions,
        TComponentDiscoveryOptions componentDiscoveryOptions);

    std::vector<TClusterComponentInstance> GetInstances(EClusterComponentType component) const;
    //! Lists instances of all components present in the `EClusterComponentType` enum.
    std::vector<TClusterComponentInstance> GetAllInstances() const;

    //! Returns object node Cypress paths for instances of the specified component.
    static std::vector<NYPath::TYPath> GetCypressPaths(const NApi::IClientPtr& client, const NApi::TMasterReadOptions& masterReadOptions, EClusterComponentType component);

private:
    const NApi::IClientPtr Client_;
    const NApi::TMasterReadOptions MasterReadOptions_;
    const TComponentDiscoveryOptions ComponentDiscoveryOptions_;

    //! Returns Cypress directory containing object nodes for the requested component.
    static NYPath::TYPath GetCypressDirectory(EClusterComponentType component);
    //! Returns Cypress subpaths that can be joined with the directory returned by the function above
    //! to produce full paths to object nodes for instances of the requested component.
    static std::vector<NYPath::TYPath> GetCypressSubpaths(const NApi::IClientPtr& client, const NApi::TMasterReadOptions& masterReadOptions, EClusterComponentType component);

    // COMPAT(koloshmet)
    TErrorOr<std::string> GetCompatBinaryVersion(const NYPath::TYPath& path) const;

    std::vector<TClusterComponentInstance> GetAttributes(
        EClusterComponentType component,
        const std::vector<NYPath::TYPath>& subpaths,
        EClusterComponentType instanceType,
        const NYPath::TYPath& suffix = "/orchid/service") const;

    std::vector<TClusterComponentInstance> ListClusterNodes(EClusterComponentType component) const;
    std::vector<TClusterComponentInstance> ListProxies(EClusterComponentType component) const;
    std::vector<TClusterComponentInstance> ListJobProxies() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
