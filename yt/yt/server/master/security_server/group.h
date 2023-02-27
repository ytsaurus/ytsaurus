#pragma once

#include "public.h"
#include "subject.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/core/misc/property.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TGroup
    : public TSubject
{
public:
    DEFINE_BYREF_RW_PROPERTY(THashSet<TSubject*>, Members);

public:
    using TSubject::TSubject;

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;
    TString GetObjectPath() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
