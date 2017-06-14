#pragma once

#include "public.h"

#include <yt/core/misc/ref_tracked.h>
#include <yt/core/misc/boolean_formula.h>

#include <yt/server/object_server/object.h>

#include <yt/server/security_server/acl.h>

#include <yt/server/cell_master/public.h>

#include <yt/ytlib/tablet_client/public.h>

#include <yt/server/cell_master/serialize.h>

namespace NYT {
namespace NTabletServer {

////////////////////////////////////////////////////////////////////////////////

class TTabletCellBundle
    : public NObjectServer::TNonversionedObjectBase
    , public TRefTracked<TTabletCellBundle>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(TString, Name);

    DEFINE_BYREF_RW_PROPERTY(NSecurityServer::TAccessControlDescriptor, Acd);

    DEFINE_BYVAL_RW_PROPERTY(TTabletCellOptionsPtr, Options);
    DEFINE_BYREF_RW_PROPERTY(TBooleanFormula, NodeTagFilter);

    DEFINE_BYREF_RW_PROPERTY(yhash_set<TTabletCell*>, TabletCells);

public:
    explicit TTabletCellBundle(const TTabletCellBundleId& id);

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletServer
} // namespace NYT
