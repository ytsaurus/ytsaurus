#pragma once

#include "yql_ytflow_config_clusters.h"

#include <library/cpp/yt/memory/intrusive_ptr.h>

#include <yt/yt/client/api/public.h>

#include <util/generic/string.h>


namespace NYql::NYtflow {

DECLARE_REFCOUNTED_CLASS(IYtClientsCache);

class IYtClientsCache
    : public NYT::TRefCounted
{
public:
    virtual NYT::NApi::IClientPtr GetClient(
        const TString& cluster, const TString& token) = 0;
};

IYtClientsCachePtr CreateYtClientsCache(TConfigClusters::TPtr configClusters);

} // namespace NYql::NYtflow
