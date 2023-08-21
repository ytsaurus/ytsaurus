#include "zookeeper_manager.h"

#include "zookeeper_shard.h"
#include "zookeeper_shard_type_handler.h"

#include <yt/yt/server/master/cell_master/automaton.h>
#include <yt/yt/server/master/cell_master/serialize.h>

#include <yt/yt/server/lib/zookeeper_master/bootstrap.h>
#include <yt/yt/server/lib/zookeeper_master/zookeeper_manager.h>

namespace NYT::NZookeeperServer {

using namespace NCellMaster;
using namespace NHydra;
using namespace NObjectClient;

////////////////////////////////////////////////////////////////////////////////

class TZookeeperManager
    : public IZookeeperManager
    , public TMasterAutomatonPart
{
public:
    explicit TZookeeperManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::Zookeeper)
    {
        RegisterLoader(
            "MasterZookeeperManager.Keys",
            BIND(&TZookeeperManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "MasterZookeeperManager.Values",
            BIND(&TZookeeperManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "MasterZookeeperManager.Keys",
            BIND(&TZookeeperManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "MasterZookeeperManager.Values",
            BIND(&TZookeeperManager::SaveValues, Unretained(this)));
    }

    void Initialize() override
    {
        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->RegisterHandler(CreateZookeeperShardTypeHandler(Bootstrap_, &ZookeeperShardMap_));

        ZookeeperManager_ = Bootstrap_->GetZookeeperBootstrap()->GetZookeeperManager();
    }

    DECLARE_ENTITY_MAP_ACCESSORS_OVERRIDE(ZookeeperShard, TZookeeperShard);

    TZookeeperShard* CreateZookeeperShard(const TCreateZookeeperShardOptions& options) override
    {
        YT_VERIFY(HasHydraContext());

        if (FindZookeeperShardByName(options.Name)) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Zookeeper shard %Qv already exists",
                options.Name);
        }

        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        if (options.CellTag != Bootstrap_->GetCellTag() &&
            !multicellManager->IsRegisteredMasterCell(options.CellTag))
        {
            THROW_ERROR_EXCEPTION("Unknown cell tag %v", options.CellTag);
        }

        if (options.RootPath != "/") {
            THROW_ERROR_EXCEPTION("Zookeeper over YT is not sharded yet");
        }

        if (ZookeeperManager_->HasRootShard()) {
            THROW_ERROR_EXCEPTION(
                NYTree::EErrorCode::AlreadyExists,
                "Zookeeper shard \"/\" is already created");
        }

        const auto& objectManager = Bootstrap_->GetObjectManager();
        auto id = objectManager->GenerateId(EObjectType::ZookeeperShard, options.HintId);

        auto zookeeperShardHolder = TPoolAllocator::New<TZookeeperShard>(id);
        zookeeperShardHolder->SetName(options.Name);
        zookeeperShardHolder->SetRootPath(options.RootPath);
        zookeeperShardHolder->SetCellTag(options.CellTag);

        auto* zookeeperShard = ZookeeperShardMap_.Insert(id, std::move(zookeeperShardHolder));
        RegisterShard(zookeeperShard);

        // Make the fake reference.
        YT_VERIFY(zookeeperShard->RefObject() == 1);

        return zookeeperShard;
    }

    void ZombifyZookeeperShard(TZookeeperShard* shard) override
    {
        YT_VERIFY(HasHydraContext());

        UnregisterShard(shard);
    }

    TZookeeperShard* FindZookeeperShardByName(const TString& name) const override
    {
        Bootstrap_->VerifyPersistentStateRead();

        auto it = ShardNameToShard_.find(name);
        return it == ShardNameToShard_.end() ? nullptr : it->second;
    }

private:
    NZookeeperMaster::IZookeeperManagerPtr ZookeeperManager_;

    TEntityMap<TZookeeperShard> ZookeeperShardMap_;

    THashMap<TString, TZookeeperShard*> ShardNameToShard_;

    void Clear() override
    {
        TMasterAutomatonPart::Clear();

        ZookeeperShardMap_.Clear();
        ShardNameToShard_.clear();
    }

    void SaveKeys(NCellMaster::TSaveContext& context) const
    {
        ZookeeperShardMap_.SaveKeys(context);
    }

    void SaveValues(NCellMaster::TSaveContext& context) const
    {
        ZookeeperShardMap_.SaveValues(context);
    }

    void LoadKeys(NCellMaster::TLoadContext& context)
    {
        ZookeeperShardMap_.LoadKeys(context);
    }

    void LoadValues(NCellMaster::TLoadContext& context)
    {
        ZookeeperShardMap_.LoadValues(context);
    }

    void OnAfterSnapshotLoaded() override
    {
        for (const auto& [shardId, shard] : ZookeeperShardMap_) {
            RegisterShard(shard);
        }
    }

    void RegisterShard(TZookeeperShard* shard)
    {
        YT_VERIFY(HasHydraContext());

        ZookeeperManager_->RegisterShard(shard);

        EmplaceOrCrash(ShardNameToShard_, shard->GetName(), shard);
    }

    void UnregisterShard(TZookeeperShard* shard)
    {
        YT_VERIFY(HasHydraContext());

        ZookeeperManager_->UnregisterShard(shard);

        EraseOrCrash(ShardNameToShard_, shard->GetName());
    }
};

DEFINE_ENTITY_MAP_ACCESSORS(TZookeeperManager, ZookeeperShard, TZookeeperShard, ZookeeperShardMap_);

////////////////////////////////////////////////////////////////////////////////

IZookeeperManagerPtr CreateZookeeperManager(TBootstrap* bootstrap)
{
    return New<TZookeeperManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperServer
