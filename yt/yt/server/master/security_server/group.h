#pragma once

#include "public.h"
#include "subject.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/object.h>

#include <library/cpp/yt/misc/property.h>

namespace NYT::NSecurityServer {

////////////////////////////////////////////////////////////////////////////////

class TGroup
    : public TSubject
{
public:
    DEFINE_BYREF_RW_PROPERTY(THashSet<TSubjectRawPtr>, Members);

public:
    using TSubject::TSubject;

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;
    NYPath::TYPath GetObjectPath() const override;

private:
    PHOENIX_DECLARE_TYPE(TGroup, 0x48c2e5b9);
};

DEFINE_MASTER_OBJECT_TYPE(TGroup)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSecurityServer
