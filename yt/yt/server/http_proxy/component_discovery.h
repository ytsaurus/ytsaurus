#pragma once

#include <yt/yt/core/misc/error.h>

#include <yt/yt/ytlib/api/public.h>
#include <yt/yt/ytlib/api/native/public.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

//! Plural (!) names of cluster components.
DEFINE_ENUM(EYTComponentType,
    ((PrimaryMasters)      (0))
    ((SecondaryMasters)    (1))

    ((Schedulers)          (2))
    ((ControllerAgents)    (3))

    ((ClusterNodes)        (4))
    ((DataNodes)           (5))
    ((TabletNodes)         (6))
    ((ExecNodes)           (7))
    ((JobProxies)          (8))

    ((HttpProxies)         (9))
    ((RpcProxies)          (10))
);

// TODO(achulkov2): MasterCaches, QueueAgents, QueryTrackers, TabletBalancers.

//! Returns singular instance type name.
TString GetInstanceType(EYTComponentType componentType);

////////////////////////////////////////////////////////////////////////////////

//! This struct represents common attributes of instances of all YT components.
//! It is used in multiple endpoints exposed by the http proxy.
struct TYTInstance
{
    //! Singular instance type, e.g. primary_master, http_proxy, etc.
    TString Type;
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

void Serialize(const TYTInstance& instance, NYson::IYsonConsumer* consumer);

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

    std::vector<TYTInstance> GetInstances(EYTComponentType component) const;
    //! Lists instances of all components present in the `EYTComponentType` enum.
    std::vector<TYTInstance> GetAllInstances() const;

private:
    const NApi::IClientPtr Client_;
    const NApi::TMasterReadOptions MasterReadOptions_;
    const TDuration ProxyDeathAge_;

    static TString GetCypressDirectory(EYTComponentType component);
    std::vector<TString> GetCypressSubpaths(EYTComponentType component) const;

    std::vector<TYTInstance> GetAttributes(
        EYTComponentType component,
        const std::vector<TString>& subpaths,
        const TString& instanceType,
        const NYPath::TYPath& suffix = "/orchid/service") const;

    std::vector<TYTInstance> ListNodes(EYTComponentType component) const;
    std::vector<TYTInstance> ListProxies(EYTComponentType component) const;
    std::vector<TYTInstance> ListJobProxies() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
