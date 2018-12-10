#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/object_server/object_detail.h>

#include <yt/core/misc/property.h>
#include <yt/core/misc/ref_tracked.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

class TRack
    : public NObjectServer::TObjectBase
    , public TRefTracked<TRack>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TString, Name);
    DEFINE_BYVAL_RW_PROPERTY(int, Index);
    DEFINE_BYVAL_RW_PROPERTY(TDataCenter*, DataCenter);

public:
    explicit TRack(const TRackId& id);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
