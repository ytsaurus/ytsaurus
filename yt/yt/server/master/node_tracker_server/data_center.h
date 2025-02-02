#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/core/misc/property.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

class TDataCenter
    : public NObjectServer::TObject
    , public TRefTracked<TDataCenter>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(std::string, Name);

public:
    using TObject::TObject;

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;
    NYPath::TYPath GetObjectPath() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

DEFINE_MASTER_OBJECT_TYPE(TDataCenter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
