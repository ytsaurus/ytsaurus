#pragma once

#include "public.h"

#include <yt/yt/ytlib/discovery_client/discovery_client.h>

#include <yt/yt/server/lib/discovery_server/helpers.h>

namespace NYT::NDiscoveryServer {

////////////////////////////////////////////////////////////////////////////////

struct TListGroupsResult
{
    std::vector<TGroupPtr> Groups;
    bool Incomplete = false;
};

class TGroupTree
    : public TRefCounted
{
public:
    explicit TGroupTree(NLogging::TLogger logger);
    ~TGroupTree();

    NYson::TYsonString List(const NYPath::TYPath& path, const NYTree::TAttributeFilter& attributeFilter);
    NYson::TYsonString Get(const NYPath::TYPath& path, const NYTree::TAttributeFilter& attributeFilter);
    bool Exists(const NYPath::TYPath& path);

    TGroupPtr FindGroup(const NYPath::TYPath& path);

    TListGroupsResult ListGroups(const NYPath::TYPath& path, const NDiscoveryClient::TListGroupsOptions& options);
    THashMap<TGroupId, TGroupPtr> GetOrCreateGroups(
        const std::vector<TGroupId>& groupIds,
        TGroupManagerInfo& groupManagerInfo,
        bool respectLimits);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TGroupTree)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
