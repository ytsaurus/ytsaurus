#pragma once

#include "public.h"

#include <yt/core/ytree/public.h>

namespace NYP::NServer::NHeavyScheduler {

////////////////////////////////////////////////////////////////////////////////

class TYTConnector
    : public TRefCounted
{
public:
    TYTConnector(TBootstrap* bootstrap, TYTConnectorConfigPtr config);

    void Initialize();

    bool IsLeading() const;

    NYTree::IYPathServicePtr CreateOrchidService();

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;
};

DEFINE_REFCOUNTED_TYPE(TYTConnector);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYP::NServer::NHeavyScheduler
