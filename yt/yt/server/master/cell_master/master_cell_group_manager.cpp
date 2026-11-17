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

#include <yt/yt/server/master/security_server/security_manager.h>

#include <yt/yt/server/lib/hive/hive_manager.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/misc/collection_helpers.h>
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
        multicellManager->SubscribeMasterCellRegistered(
            BIND_NO_PROPAGATE(&TMasterCellGroupManager::OnMasterCellRegistered, MakeWeak(this)));
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
        auto groupCountWithoutUniversum = std::ssize(NameToMasterCellGroupMap_) - 1;
        YT_VERIFY(groupCountWithoutUniversum >= 0);
        if (groupCountWithoutUniversum + 1 > maxMasterCellGroupCount) {
            THROW_ERROR_EXCEPTION("Master cell group count limit %v is reached",
                maxMasterCellGroupCount);
        }

        ValidateCellTags(cellTags);
        ValidateGroupUnique(cellTags);

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::MasterCellGroup, hintId);

        auto groupHolder = TPoolAllocator::New<TMasterCellGroup>(id);
        groupHolder->SetName(name);
        groupHolder->CellTags() = cellTags;

        auto* group = MasterCellGroupMap_.Insert(id, std::move(groupHolder));

        // Make the artificial reference.
        YT_VERIFY(group->RefObject() == 1);
        EmplaceOrCrash(NameToMasterCellGroupMap_, name, TWeakObjectPtr<TMasterCellGroup>(group));

        YT_TLOG_DEBUG("Master cell group created")
            .With("Name", name)
            .With("Id", id)
            .With("CellTags", cellTags);

        OnMasterCellGroupChanged(group);
        return group;
    }

    void ZombifyMasterCellGroup(TMasterCellGroup* group) override
    {
        if (NameToMasterCellGroupMap_.erase(group->GetName()) != 1) {
            YT_TLOG_ALERT("Master cell group is missing from name map during zombification")
                .With("Name", group->GetName())
                .With("Id", group->GetId());
        }
        OnMasterCellGroupChanged(group);

        YT_TLOG_DEBUG("Master cell group zombified")
            .With("Name", group->GetName())
            .With("Id", group->GetId());
    }

    void RenameMasterCellGroup(TMasterCellGroup* group, const std::string& newName) override
    {
        if (group->IsBuiltin()) {
            THROW_ERROR_EXCEPTION("Cannot rename the built-in master cell group %Qv",
                group->GetName());
        }

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
        EmplaceOrCrash(NameToMasterCellGroupMap_, newName, TWeakObjectPtr<TMasterCellGroup>(group));

        YT_TLOG_DEBUG("Master cell group renamed")
            .With("OldName", group->GetName())
            .With("NewName", newName)
            .With("Id", group->GetId());

        group->SetName(newName);
    }

    void SetMasterCellGroupCellTags(TMasterCellGroup* group, const TCellTagSet& cellTags) override
    {
        if (group->IsBuiltin()) {
            THROW_ERROR_EXCEPTION("Cannot change cell tags of the built-in master cell group %Qv",
                group->GetName());
        }

        if (group->CellTags() == cellTags) {
            return;
        }

        ValidateCellTags(cellTags);
        ValidateGroupUnique(cellTags, group);

        YT_TLOG_DEBUG("Master cell group cell tags updated")
            .With("Name", group->GetName())
            .With("Id", group->GetId())
            .With("OldCellTags", group->CellTags())
            .With("NewCellTags", cellTags);

        group->CellTags() = cellTags;
        OnMasterCellGroupChanged(group);
    }

    TMasterCellGroup* FindMasterCellGroupByName(const std::string& name) override
    {
        auto it = NameToMasterCellGroupMap_.find(name);
        if (it == NameToMasterCellGroupMap_.end()) {
            return nullptr;
        }

        auto* group = it->second.Get();
        if (!IsObjectAlive(group)) {
            YT_TLOG_ALERT("Master cell group name index refers to a non-live group")
                .With("Name", name)
                .With("GroupId", GetObjectId(group));
            return nullptr;
        }
        return group;
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

    std::vector<TMasterCellGroup*> GetMasterCellGroupsForCell(TCellTag cellTag) const override
    {
        const auto& indexedGroups = GetOrCrash(CellTagToMasterCellGroupsIndex_, cellTag);
        std::vector<TMasterCellGroup*> groups;
        groups.reserve(indexedGroups.size());
        for (const auto& indexedGroup : indexedGroups) {
            auto* group = indexedGroup.Get();
            YT_VERIFY(IsObjectAlive(group));
            groups.push_back(group);
        }
        return groups;
    }

    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(MasterCellGroup, TMasterCellGroup);

private:
    NHydra::TEntityMap<TMasterCellGroup> MasterCellGroupMap_;

    THashMap<std::string, TWeakObjectPtr<TMasterCellGroup>> NameToMasterCellGroupMap_;

    THashMap<TCellTag, std::vector<TWeakObjectPtr<TMasterCellGroup>>> CellTagToMasterCellGroupsIndex_;

    const TDynamicMulticellManagerConfigPtr& GetDynamicConfig()
    {
        return Bootstrap_->GetConfigManager()->GetConfig()->MulticellManager;
    }

    void RebuildCellTagToMasterCellGroupsIndex()
    {
        CellTagToMasterCellGroupsIndex_.clear();
        for (auto* group : GetValuesSortedByKey(MasterCellGroupMap_)) {
            for (auto cellTag : group->CellTags()) {
                CellTagToMasterCellGroupsIndex_[cellTag].emplace_back(group);
            }
        }
    }

    void OnMasterCellGroupChanged(TMasterCellGroup* group)
    {
        RebuildCellTagToMasterCellGroupsIndex();
        Bootstrap_->GetSecurityManager()->OnMasterCellGroupChanged(group);
    }

    void OnMasterCellRegistered(TCellTag cellTag)
    {
        auto groupId = GetUniversumGroupId(Bootstrap_->GetMulticellManager()->GetPrimaryCellTag());
        auto* group = GetMasterCellGroup(groupId);
        InsertOrCrash(group->CellTags(), cellTag);
        RebuildCellTagToMasterCellGroupsIndex();
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

    void ValidateGroupUnique(
        const TCellTagSet& cellTags,
        const TMasterCellGroup* groupToIgnore = nullptr)
    {
        for (auto [groupId, group] : MasterCellGroupMap_) {
            if (group == groupToIgnore) {
                continue;
            }
            if (!IsObjectAlive(group)) {
                continue;
            }
            if (IsUniversumGroupId(groupId)) {
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

        // These checks currently overlap, but general object name rules may evolve independently.
        CheckObjectName(name)
            .ThrowOnError();
    }

    void OnReplicateKeysToSecondaryMaster(TCellTag cellTag)
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        for (auto* group : GetValuesSortedByKey(MasterCellGroupMap_)) {
            if (group->IsBuiltin()) {
                // Built-in groups are initialized locally.
                continue;
            }
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
        Save(context, NameToMasterCellGroupMap_);
        Save(context, CellTagToMasterCellGroupsIndex_);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        MasterCellGroupMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        MasterCellGroupMap_.LoadValues(context);
        // COMPAT(evanevannnn)
        if (context.GetVersion() >= EMasterReign::AccountMasterCellGroupLimits) {
            Load(context, NameToMasterCellGroupMap_);
            Load(context, CellTagToMasterCellGroupsIndex_);
        }
    }

    void OnAfterSnapshotLoaded() override
    {
        TMasterAutomatonPart::OnAfterSnapshotLoaded();

        CellTagToMasterCellGroupsIndex_.clear();
        NameToMasterCellGroupMap_.clear();
        for (auto [groupId, group] : MasterCellGroupMap_) {
            if (!IsObjectAlive(group)) {
                continue;
            }
            EmplaceOrCrash(NameToMasterCellGroupMap_, group->GetName(), TWeakObjectPtr<TMasterCellGroup>(group));
        }

        InitBuiltins();
    }

    void InitBuiltins()
    {
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto id = GetUniversumGroupId(multicellManager->GetPrimaryCellTag());
        auto* group = MasterCellGroupMap_.Find(id);
        if (!group) {
            auto holder = TPoolAllocator::New<TMasterCellGroup>(id);
            holder->SetName("universum");
            group = MasterCellGroupMap_.Insert(id, std::move(holder));
            YT_VERIFY(group->RefObject() == 1);
            EmplaceOrCrash(NameToMasterCellGroupMap_, group->GetName(), TWeakObjectPtr<TMasterCellGroup>(group));
        }
        group->CellTags() = multicellManager->GetRegisteredMasterCellTags();
        group->CellTags().insert(multicellManager->GetCellTag());
        RebuildCellTagToMasterCellGroupsIndex();
    }

    void SetZeroState() override
    {
        TMasterAutomatonPart::SetZeroState();
        InitBuiltins();
    }

    void Clear() override
    {
        CellTagToMasterCellGroupsIndex_.clear();
        NameToMasterCellGroupMap_.clear();
        MasterCellGroupMap_.Clear();

        TMasterAutomatonPart::Clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENTITY_MAP_ACCESSORS(TMasterCellGroupManager, MasterCellGroup, TMasterCellGroup, MasterCellGroupMap_);

////////////////////////////////////////////////////////////////////////////////

TMasterCellGroupId GetUniversumGroupId(TCellTag primaryCellTag)
{
    return MakeWellKnownId(EObjectType::MasterCellGroup, primaryCellTag, 0xffffffffffffffff);
}

bool IsUniversumGroupId(TMasterCellGroupId groupId)
{
    return groupId == GetUniversumGroupId(CellTagFromId(groupId));
}

IMasterCellGroupManagerPtr CreateMasterCellGroupManager(TBootstrap* bootstrap)
{
    return New<TMasterCellGroupManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
