#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/core/misc/property.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

class TRack
    : public NObjectServer::TObject
    , public TRefTracked<TRack>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(std::string, Name);
    DEFINE_BYVAL_RW_PROPERTY(int, Index, -1);
    DEFINE_BYVAL_RW_PROPERTY(TDataCenterRawPtr, DataCenter);

public:
    using TObject::TObject;

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;
    NYPath::TYPath GetObjectPath() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

DEFINE_MASTER_OBJECT_TYPE(TRack)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
