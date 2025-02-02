#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/object_server/object_detail.h>

#include <yt/yt/server/lib/chaos_server/public.h>

#include <yt/yt/core/misc/arithmetic_formula.h>
#include <yt/yt/core/misc/property.h>

#include <library/cpp/yt/memory/ref_tracked.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

class TArea
    : public NObjectServer::TObject
    , public TRefTracked<TArea>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(std::string, Name);
    DEFINE_BYVAL_RW_PROPERTY(TCellBundleRawPtr, CellBundle);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TCellBaseRawPtr>, Cells);
    DEFINE_BYREF_RW_PROPERTY(TBooleanFormula, NodeTagFilter);
    DEFINE_BYREF_RW_PROPERTY(NChaosServer::TChaosHydraConfigPtr, ChaosOptions);

public:
    using TObject::TObject;

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;
    NYPath::TYPath GetObjectPath() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

DEFINE_MASTER_OBJECT_TYPE(TArea)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
