#pragma once

#include "public.h"

#include <yt/yt/server/master/object_server/public.h>

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/client/object_client/public.h>

namespace NYT::NCellMaster {

////////////////////////////////////////////////////////////////////////////////

struct IMasterCellGroupManager
    : public virtual TRefCounted
{
    virtual void Initialize() = 0;

    virtual TMasterCellGroup* CreateMasterCellGroup(
        const std::string& name,
        const NObjectClient::TCellTagSet& cellTags,
        NObjectClient::TObjectId hintId) = 0;

    virtual void ZombifyMasterCellGroup(TMasterCellGroup* group) = 0;

    virtual void RenameMasterCellGroup(
        TMasterCellGroup* group,
        const std::string& newName) = 0;

    virtual void SetMasterCellGroupCellTags(
        TMasterCellGroup* group,
        const NObjectClient::TCellTagSet& cellTags) = 0;

    virtual TMasterCellGroup* FindMasterCellGroupByName(const std::string& name) = 0;
    virtual TMasterCellGroup* GetMasterCellGroupByNameOrThrow(const std::string& name) = 0;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(MasterCellGroup, TMasterCellGroup);
};

DEFINE_REFCOUNTED_TYPE(IMasterCellGroupManager)

////////////////////////////////////////////////////////////////////////////////

IMasterCellGroupManagerPtr CreateMasterCellGroupManager(TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
