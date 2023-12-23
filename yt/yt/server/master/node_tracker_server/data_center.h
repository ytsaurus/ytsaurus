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
    DEFINE_BYVAL_RW_PROPERTY(TString, Name);

public:
    using TObject::TObject;

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;
    TString GetObjectPath() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
