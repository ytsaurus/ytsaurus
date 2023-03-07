#pragma once

#include "public.h"
#include "subject.h"

#include <yt/server/master/cell_master/public.h>

#include <yt/server/master/object_server/object.h>

#include <yt/core/misc/property.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TGroup
    : public TSubject
{
public:
    DEFINE_BYREF_RW_PROPERTY(THashSet<TSubject*>, Members);

public:
    explicit TGroup(TGroupId id);

    virtual TString GetLowercaseObjectName() const override;
    virtual TString GetCapitalizedObjectName() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
