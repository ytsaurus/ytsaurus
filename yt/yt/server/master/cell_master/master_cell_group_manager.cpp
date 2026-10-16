#include "master_cell_group_manager.h"

#include "automaton.h"
#include "bootstrap.h"
#include "config.h"
#include "config_manager.h"
#include "master_cell_group.h"
#include "master_cell_group_type_handler.h"
#include "multicell_manager.h"
#include "private.h"
#include "serialize.h"

#include <yt/yt/server/master/object_server/helpers.h>
#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/core/misc/pool_allocator.h>

namespace NYT::NCellMaster {

using namespace NHydra;
using namespace NObjectClient;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = CellMasterLogger;

constexpr int MaxMasterCellGroupNameLength = 100;

////////////////////////////////////////////////////////////////////////////////

class TMasterCellGroupManager
    : public IMasterCellGroupManager
    , public TMasterAutomatonPart
{
public:
    explicit TMasterCellGroupManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::MasterCellGroupManager)
    {
        RegisterLoader(
            "MasterCellGroupManager.Keys",
            BIND_NO_PROPAGATE(&TMasterCellGroupManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "MasterCellGroupManager.Values",
            BIND_NO_PROPAGATE(&TMasterCellGroupManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "MasterCellGroupManager.Keys",
            BIND_NO_PROPAGATE(&TMasterCellGroupManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "MasterCellGroupManager.Values",
            BIND_NO_PROPAGATE(&TMasterCellGroupManager::SaveValues, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(CreateMasterCellGroupTypeHandler(Bootstrap_, &MasterCellGroupMap_));

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (multicellManager->IsPrimaryMaster()) {
            multicellManager->SubscribeReplicateKeysToSecondaryMaster(
                BIND_NO_PROPAGATE(&TMasterCellGroupManager::OnReplicateKeysToSecondaryMaster, MakeWeak(this)));
            multicellManager->SubscribeReplicateValuesToSecondaryMaster(
                BIND_NO_PROPAGATE(&TMasterCellGroupManager::OnReplicateValuesToSecondaryMaster, MakeWeak(this)));
        }
    }

    TMasterCellGroup* CreateMasterCellGroup(
        const std::string& name,
        const TCellTagSet& cellTags,
        TObjectId hintId) override
    {
        YT_VERIFY(HasHydraContext());

        ValidateMasterCellGroupName(name);

        if (FindMasterCellGroupByName(name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Master cell group %Qv already exists",
                name);
        }

        auto maxMasterCellGroupCount = GetDynamicConfig()->MaxMasterCellGroupCount;
        // NameToMasterCellGroupMap_ doesn't contain zombies, so we check its size here.
        if (std::ssize(NameToMasterCellGroupMap_) >= maxMasterCellGroupCount) {
            THROW_ERROR_EXCEPTION("Master cell group count limit %v is reached",
                maxMasterCellGroupCount);
        }

        ValidateCellTags(cellTags);
        ValidateCellTagsUnique(cellTags);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::MasterCellGroup, hintId);

        auto groupHolder = TPoolAllocator::New<TMasterCellGroup>(id);
        groupHolder->SetName(name);
        groupHolder->CellTags() = cellTags;

        auto* group = MasterCellGroupMap_.Insert(id, std::move(groupHolder));
        EmplaceOrCrash(NameToMasterCellGroupMap_, name, group);

        // Make the artificial reference.
        YT_VERIFY(group->RefObject() == 1);

        YT_TLOG_DEBUG("Master cell group created")
            .With("Name", name)
            .With("Id", id)
            .With("CellTags", cellTags);

        return group;
    }

    void ZombifyMasterCellGroup(TMasterCellGroup* group) override
    {
        if (NameToMasterCellGroupMap_.erase(group->GetName()) != 1) {
            YT_TLOG_ALERT("Master cell group is missing from name map during zombification")
                .With("Name", group->GetName())
                .With("Id", group->GetId());
        }

        YT_TLOG_DEBUG("Master cell group zombified")
            .With("Name", group->GetName())
            .With("Id", group->GetId());
    }

    void RenameMasterCellGroup(TMasterCellGroup* group, const std::string& newName) override
    {
        if (group->GetName() == newName) {
            return;
        }

        ValidateMasterCellGroupName(newName);

        if (FindMasterCellGroupByName(newName)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Master cell group %Qv already exists",
                newName);
        }

        if (NameToMasterCellGroupMap_.erase(group->GetName()) != 1) {
            YT_TLOG_ALERT("Master cell group is missing from name map during rename")
                .With("Name", group->GetName())
                .With("Id", group->GetId());
        }
        EmplaceOrCrash(NameToMasterCellGroupMap_, newName, group);

        YT_TLOG_DEBUG("Master cell group renamed")
            .With("OldName", group->GetName())
            .With("NewName", newName)
            .With("Id", group->GetId());

        group->SetName(newName);
    }

    void SetMasterCellGroupCellTags(TMasterCellGroup* group, const TCellTagSet& cellTags) override
    {
        ValidateCellTags(cellTags);
        ValidateCellTagsUnique(cellTags, group);

        YT_TLOG_DEBUG("Master cell group cell tags updated")
            .With("Name", group->GetName())
            .With("Id", group->GetId())
            .With("OldCellTags", group->CellTags())
            .With("NewCellTags", cellTags);

        group->CellTags() = cellTags;
    }

    TMasterCellGroup* FindMasterCellGroupByName(const std::string& name) override
    {
        auto it = NameToMasterCellGroupMap_.find(name);
        return it == NameToMasterCellGroupMap_.end() ? nullptr : it->second;
    }

    TMasterCellGroup* GetMasterCellGroupByNameOrThrow(const std::string& name) override
    {
        auto* group = FindMasterCellGroupByName(name);
        if (!group) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::ResolveError,
                "No such master cell group %Qv",
                name);
        }
        return group;
    }

    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(MasterCellGroup, TMasterCellGroup);

private:
    NHydra::TEntityMap<TMasterCellGroup> MasterCellGroupMap_;

    THashMap<std::string, TMasterCellGroup*> NameToMasterCellGroupMap_;

    const TDynamicMulticellManagerConfigPtr& GetDynamicConfig()
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->MulticellManager;
    }

    void ValidateCellTags(const TCellTagSet& cellTags)
    {
        if (cellTags.empty()) {
            if (Bootstrap_->IsSecondaryMaster()) {
                YT_TLOG_ALERT("Master cell group contains no master cells");
            } else {
                THROW_ERROR_EXCEPTION("Master cell group must contain at least one master cell");
            }
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        for (auto cellTag : cellTags) {
            if (cellTag == multicellManager->GetCellTag()) {
                continue;
            }

            if (!multicellManager->IsRegisteredMasterCell(cellTag)) {
                if (Bootstrap_->IsSecondaryMaster()) {
                    YT_TLOG_ALERT("Master cell group contains unknown master cell tag")
                        .With("CellTag", cellTag);
                } else {
                    THROW_ERROR_EXCEPTION("Unknown master cell tag %v",
                        cellTag);
                }
            }
        }
    }

    void ValidateCellTagsUnique(
        const TCellTagSet& cellTags,
        const TMasterCellGroup* groupToIgnore = nullptr)
    {
        for (auto [groupId, group] : MasterCellGroupMap_) {
            if (group == groupToIgnore || !IsObjectAlive(group)) {
                continue;
            }

            if (group->CellTags() == cellTags) {
                THROW_ERROR_EXCEPTION(
                    NYTree::EErrorCode::AlreadyExists,
                    "Master cell group with the same cell tags already exists")
                    .With("master_cell_group_id", groupId)
                    .With("master_cell_group_name", group->GetName())
                    .With("cell_tags", cellTags);
            }
        }
    }

    static void ValidateMasterCellGroupName(const std::string& name)
    {
        ValidateObjectName(name, EObjectType::MasterCellGroup, MaxMasterCellGroupNameLength);

        CheckObjectName(name)
            .ThrowOnError();
    }

    void OnReplicateKeysToSecondaryMaster(TCellTag cellTag)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto* group : GetValuesSortedByKey(MasterCellGroupMap_)) {
            objectManager->ReplicateObjectCreationToSecondaryMaster(group, cellTag);
        }
    }

    void OnReplicateValuesToSecondaryMaster(TCellTag cellTag)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto* group : GetValuesSortedByKey(MasterCellGroupMap_)) {
            objectManager->ReplicateObjectAttributesToSecondaryMaster(group, cellTag);
        }
    }

    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        MasterCellGroupMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        MasterCellGroupMap_.SaveValues(context);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        MasterCellGroupMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        MasterCellGroupMap_.LoadValues(context);
    }

    void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        NameToMasterCellGroupMap_.clear();
        for (auto [groupId, group] : MasterCellGroupMap_) {
            if (!IsObjectAlive(group)) {
                continue;
            }
            EmplaceOrCrash(NameToMasterCellGroupMap_, group->GetName(), group);
        }
    }

    void Clear() override
    {
        MasterCellGroupMap_.Clear();
        NameToMasterCellGroupMap_.clear();

        TMasterAutomatonPart::Clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENTITY_MAP_ACCESSORS(TMasterCellGroupManager, MasterCellGroup, TMasterCellGroup, MasterCellGroupMap_);

////////////////////////////////////////////////////////////////////////////////

IMasterCellGroupManagerPtr CreateMasterCellGroupManager(TBootstrap* bootstrap)
{
    return New<TMasterCellGroupManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
