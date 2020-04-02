#pragma once

#include "public.h"

namespace NYT::NDiscoveryServer {

//////////////////////////////////////////////////////////////////////////////////

class TGroupTree
    : public TRefCounted
{
public:
    explicit TGroupTree(NLogging::TLogger logger);
    ~TGroupTree();

    NYson::TYsonString List(const NYPath::TYPath& path);
    NYson::TYsonString Get(const NYPath::TYPath& path);
    bool Exists(const NYPath::TYPath& path);

    TGroupPtr FindGroup(const NYPath::TYPath& path);
    THashMap<TGroupId, TGroupPtr> GetOrCreateGroups(const std::vector<TGroupId>& groupIds);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TGroupTree)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDiscoveryClient
