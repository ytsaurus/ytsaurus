#pragma once

#include "fwd.h"

#include <library/cpp/threading/cron/cron.h>

#include <util/generic/string.h>
#include <util/generic/hash.h>
#include <util/system/spinlock.h>


namespace NYT::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class THostManager
{
public:
    THostManager();

    static THostManager& Get();

    void Restart();
    TString GetProxyForHeavyRequest(TStringBuf cluster);

private:
    class TClusterHostList;

private:
    NCron::IHandlePtr UpdateHandle_;
    TAdaptiveLock Lock_;
    THashMap<TString, TClusterHostList> ClusterHosts_;

private:
    static TClusterHostList GetHosts(TStringBuf cluster);
    void UpdateHosts();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPrivate
