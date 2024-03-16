#include "epoch_history_manager.h"

#include "automaton.h"
#include "config.h"
#include "config_manager.h"
#include "hydra_facade.h"
#include "private.h"
#include "serialize.h"

#include <yt/yt/server/master/cell_master/proto/epoch_history_manager.pb.h>

#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/misc/serialize.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

namespace NYT::NCellMaster {

using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;
using namespace NProto;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellMasterLogger;

////////////////////////////////////////////////////////////////////////////////

class TEpochHistoryManager
    : public IEpochHistoryManager
    , public TMasterAutomatonPart
{
public:
    TEpochHistoryManager(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::Default)
        , StoreMutationTimeExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Periodic),
            BIND(&TEpochHistoryManager::Run, MakeWeak(this))))
        , Versions_(1)
        , Instants_(1)
    {
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "EpochHistoryManager",
            BIND(&TEpochHistoryManager::Save, Unretained(this)));

        RegisterLoader(
            "EpochHistoryManager",
            BIND(&TEpochHistoryManager::Load, Unretained(this)));

        RegisterMethod(BIND_NO_PROPAGATE(&TEpochHistoryManager::HydraStoreMutationTime, Unretained(this)));

        Bootstrap_->GetConfigManager()->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TEpochHistoryManager::OnDynamicConfigChanged, MakeWeak(this)));
    }

    std::pair<TInstant, TInstant> GetEstimatedMutationTime(TVersion version, TInstant now) const override
    {
        int index = std::upper_bound(Versions_.begin(), Versions_.end(), version) - Versions_.begin();
        if (index == 0) {
            return {};
        }

        if (index == std::ssize(Instants_)) {
            return {Instants_[index - 1], now};
        }

        return {Instants_[index - 1], Instants_[index]};
    }

    std::pair<TInstant, TInstant> GetEstimatedCreationTime(TObjectId id, TInstant now) const override
    {
        if (IsSequoiaId(id)) {
            return TimestampToInstant(TimestampFromId(id));
        } else {
            return GetEstimatedMutationTime(VersionFromId(id), now);
        }
    }

private:
    const TPeriodicExecutorPtr StoreMutationTimeExecutor_;

    std::vector<TVersion> Versions_;
    std::vector<TInstant> Instants_;

    void HydraStoreMutationTime(TReqStoreMutationTime* /*request*/)
    {
        const auto* mutationContext = GetCurrentMutationContext();
        Versions_.push_back(mutationContext->GetVersion());
        Instants_.push_back(mutationContext->GetTimestamp());
    }

    void OnLeaderActive() override
    {
        TMasterAutomatonPart::OnLeaderActive();

        StoreMutationTimeExecutor_->Start();
    }

    void OnStopLeading() override
    {
        TMasterAutomatonPart::OnStopLeading();

        YT_UNUSED_FUTURE(StoreMutationTimeExecutor_->Stop());
    }

    void Run()
    {
        TReqStoreMutationTime request;
        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        YT_UNUSED_FUTURE(CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger));
    }

    void OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
    {
        StoreMutationTimeExecutor_->SetPeriod(
            Bootstrap_->GetConfigManager()->GetConfig()->CellMaster->MutationTimeCommitPeriod);
    }

    void Clear() override
    {
        TMasterAutomatonPart::Clear();

        Versions_.clear();
        Instants_.clear();
    }

    void SetZeroState() override
    {
        TMasterAutomatonPart::SetZeroState();

        Versions_ = std::vector<TVersion>(1);
        Instants_ = std::vector<TInstant>(1);
    }

    void Save(TSaveContext& context)
    {
        using NYT::Save;

        Save(context, Versions_);
        Save(context, Instants_);
    }

    void Load(TLoadContext& context)
    {
        using NYT::Load;

        Load(context, Versions_);
        Load(context, Instants_);
    }
};

////////////////////////////////////////////////////////////////////////////////

IEpochHistoryManagerPtr CreateEpochHistoryManager(TBootstrap* bootstrap)
{
    return New<TEpochHistoryManager>(bootstrap);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
