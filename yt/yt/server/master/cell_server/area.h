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
    DEFINE_BYVAL_RW_PROPERTY(TString, Name);
    DEFINE_BYVAL_RW_PROPERTY(TCellBundle*, CellBundle);
    DEFINE_BYREF_RW_PROPERTY(THashSet<TCellBase*>, Cells);
    DEFINE_BYREF_RW_PROPERTY(TBooleanFormula, NodeTagFilter);
    DEFINE_BYREF_RW_PROPERTY(NChaosServer::TChaosHydraConfigPtr, ChaosOptions);

public:
    using TObject::TObject;

    TString GetLowercaseObjectName() const override;
    TString GetCapitalizedObjectName() const override;
    TString GetObjectPath() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
