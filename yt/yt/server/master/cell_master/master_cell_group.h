#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/object.h>

#include <yt/yt/client/object_client/public.h>

#include <library/cpp/yt/memory/ref_tracked.h>

#include <library/cpp/yt/misc/property.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

//! Represents a named group of master cells.
/*!
 *  Master cell groups are used to impose per-group (rather than clusterwide)
 *  restrictions on account resources: an account may limit its resource usage
 *  at the cells comprising a certain group.
 */
class TMasterCellGroup
    : public NObjectServer::TObject
    , public TRefTracked<TMasterCellGroup>
{
public:
    DEFINE_BYVAL_RW_PROPERTY(std::string, Name);
    DEFINE_BYREF_RW_PROPERTY(NObjectClient::TCellTagSet, CellTags);

public:
    using TObject::TObject;

    std::string GetLowercaseObjectName() const override;
    std::string GetCapitalizedObjectName() const override;
    NYPath::TYPath GetObjectPath() const override;

    void Save(NCellMaster::TSaveContext& context) const;
    void Load(NCellMaster::TLoadContext& context);
};

DEFINE_MASTER_OBJECT_TYPE(TMasterCellGroup)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
