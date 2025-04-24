#pragma once

#include "chaos_object_base.h"
#include "public.h"

#include <yt/yt/server/node/tablet_node/object_detail.h>

#include <yt/yt/client/chaos_client/public.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NChaosNode {

////////////////////////////////////////////////////////////////////////////////

class TChaosLease
    : public TChaosObjectBase
    , public TRefTracked<TChaosLease>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(NTransactionClient::TTimestamp, LastPingTimestamp);

public:
    using TChaosObjectBase::TChaosObjectBase;

    void Save(TSaveContext& context) const;
    void Load(TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
