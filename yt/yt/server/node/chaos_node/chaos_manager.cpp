#include "chaos_manager.h"

#include "automaton.h"
#include "bootstrap.h"
#include "chaos_slot.h"
#include "private.h"
#include "slot_manager.h"

#include <yt/yt/server/node/cluster_node/bootstrap.h>

#include <yt/yt/server/lib/hydra/distributed_hydra_manager.h>
#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/server/lib/hive/hive_manager.h>
#include <yt/yt/server/lib/hive/mailbox.h>

#include <yt/yt/server/lib/chaos_node/config.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>

namespace NYT::NChaosNode {

using namespace NYson;
using namespace NYTree;
using namespace NHydra;
using namespace NClusterNode;
using namespace NHiveServer;
using namespace NConcurrency;

using NYT::FromProto;
using NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TChaosManager
    : public IChaosManager
    , public TChaosAutomatonPart
{
public:
    TChaosManager(
        TChaosManagerConfigPtr config,
        IChaosSlotPtr slot,
        IBootstrap* bootstrap)
        : TChaosAutomatonPart(
            slot,
            bootstrap)
        , Config_(config)
        , OrchidService_(TOrchidService::Create(MakeWeak(this), Slot_->GetGuardedAutomatonInvoker()))
    {
        VERIFY_INVOKER_THREAD_AFFINITY(Slot_->GetAutomatonInvoker(), AutomatonThread);

        RegisterLoader(
            "ChaosManager.Keys",
            BIND(&TChaosManager::LoadKeys, Unretained(this)));
        RegisterLoader(
            "ChaosManager.Values",
            BIND(&TChaosManager::LoadValues, Unretained(this)));

        RegisterSaver(
            ESyncSerializationPriority::Keys,
            "ChaosManager.Keys",
            BIND(&TChaosManager::SaveKeys, Unretained(this)));
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "ChaosManager.Values",
            BIND(&TChaosManager::SaveValues, Unretained(this)));
    }

    void Initialize() override
    { }

    IYPathServicePtr GetOrchidService() const override
    {
        return OrchidService_;
    }

private:
    class TOrchidService
        : public TVirtualMapBase
    {
    public:
        static IYPathServicePtr Create(TWeakPtr<TChaosManager> impl, IInvokerPtr invoker)
        {
            return New<TOrchidService>(std::move(impl))
                ->Via(invoker);
        }

        std::vector<TString> GetKeys(i64 /*limit*/) const override
        {
            std::vector<TString> keys;
            return keys;
        }

        i64 GetSize() const override
        {
            return 0;
        }

        IYPathServicePtr FindItemService(TStringBuf /*key*/) const override
        {
            return nullptr;
        }

    private:
        const TWeakPtr<TChaosManager> Owner_;

        explicit TOrchidService(TWeakPtr<TChaosManager> impl)
            : Owner_(std::move(impl))
        { }

        DECLARE_NEW_FRIEND();
    };

    const TChaosManagerConfigPtr Config_;

    const IYPathServicePtr OrchidService_;

    DECLARE_THREAD_AFFINITY_SLOT(AutomatonThread);


    void SaveKeys(TSaveContext& /*context*/) const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
    }

    void SaveValues(TSaveContext& /*context*/) const
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
    }

    void LoadKeys(TLoadContext& /*context*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
    }

    void LoadValues(TLoadContext& /*context*/)
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);
    }

    void Clear() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::Clear();
    }


    void OnLeaderRecoveryComplete() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnLeaderRecoveryComplete();

        StartEpoch();
    }

    void OnLeaderActive() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnLeaderActive();
    }


    void OnStopLeading() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnStopLeading();

        StopEpoch();
    }


    void OnFollowerRecoveryComplete() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnFollowerRecoveryComplete();

        StartEpoch();
    }

    void OnStopFollowing() override
    {
        VERIFY_THREAD_AFFINITY(AutomatonThread);

        TChaosAutomatonPart::OnStopFollowing();

        StopEpoch();
    }


    void StartEpoch()
    {
        // TODO(savrus): Start chaos cell synchronization
    }

    void StopEpoch()
    {
        // TODO(savrus): Stop chaos cell synchronization
    }
};

////////////////////////////////////////////////////////////////////////////////

IChaosManagerPtr CreateChaosManager(
    TChaosManagerConfigPtr config,
    IChaosSlotPtr slot,
    IBootstrap* bootstrap)
{
    return New<TChaosManager>(
        config,
        slot,
        bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosNode
