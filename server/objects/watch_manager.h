#pragma once

#include "public.h"
#include "config.h"

#include <yt/client/api/client.h>

namespace NYP::NServer::NObjects {

////////////////////////////////////////////////////////////////////////////////

class TWatchManager
    : public TRefCounted
{
public:
    TWatchManager(
        NMaster::TBootstrap* bootstrap,
        TWatchManagerConfigPtr config);
    ~TWatchManager();

    void Initialize();

    bool Enabled() const;

    std::vector<NYT::NApi::TTabletInfo> GetTabletInfos(
        EObjectType objectType);

    std::vector<NYT::NApi::TTabletInfo> WaitForTabletInfos(
        EObjectType objectType,
        TTimestamp barrierTimestamp,
        TDuration timeLimit);

    void RegisterWatchableObjectType(
        EObjectType objectType,
        const TString& objectTable);

    i64 GetTabletCount(
        EObjectType objectType) const;

    const TDBTable* GetWatchLogTable(
        EObjectType objectType) const;

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TWatchManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NObjects
