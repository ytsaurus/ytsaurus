#include "epoch_history_manager.h"

#include "automaton.h"
#include "config.h"
#include "config_manager.h"
#include "hydra_facade.h"
#include "private.h"
#include "serialize.h"

#include <yt/server/master/cell_master/epoch_history_manager.pb.h>

#include <yt/server/lib/hydra/mutation.h>

#include <yt/core/misc/serialize.h>

#include <yt/core/concurrency/periodic_executor.h>

namespace NYT::NCellMaster {

using namespace NConcurrency;
using namespace NHydra;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellMasterLogger;

////////////////////////////////////////////////////////////////////////////////

class TEpochHistoryManager::TImpl
    : public TMasterAutomatonPart
{
public:
    TImpl(TBootstrap* bootstrap)
        : TMasterAutomatonPart(bootstrap, EAutomatonThreadQueue::Default)
        , StoreMutationTimeExecutor_(New<TPeriodicExecutor>(
            Bootstrap_->GetHydraFacade()->GetAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Periodic),
            BIND(&TImpl::Run, MakeWeak(this))))
        , Versions_(1)
        , Instants_(1)
    {
        RegisterSaver(
            ESyncSerializationPriority::Values,
            "EpochHistoryManager",
            BIND(&TImpl::Save, Unretained(this)));

        RegisterLoader(
            "EpochHistoryManager",
            BIND(&TImpl::Load, Unretained(this)));

        RegisterMethod(BIND(&TImpl::HydraStoreMutationTime, Unretained(this)));

        Bootstrap_->GetConfigManager()->SubscribeConfigChanged(BIND(&TImpl::OnDynamicConfigChanged, MakeWeak(this)));
    }

    std::pair<TInstant, TInstant> GetEstimatedMutationTime(TVersion version)
    {
        int index = std::upper_bound(Versions_.begin(), Versions_.end(), version) - Versions_.begin();
        if (index == 0) {
            return {};
        }
        return {
            Instants_[index - 1],
            index < Instants_.size() ? Instants_[index] : TInstant::Max()
        };
    }

private:
    const TPeriodicExecutorPtr StoreMutationTimeExecutor_;

    std::vector<TVersion> Versions_;
    std::vector<TInstant> Instants_;

    void HydraStoreMutationTime(TStoreMutationTimeReq* /*request*/)
    {
        const auto* mutationContext = GetCurrentMutationContext();
        Versions_.push_back(mutationContext->GetVersion());
        Instants_.push_back(mutationContext->GetTimestamp());
    }

    virtual void OnLeaderActive() override
    {
        TMasterAutomatonPart::OnLeaderActive();

        OnDynamicConfigChanged();

        StoreMutationTimeExecutor_->Start();
    }

    virtual void OnStopLeading() override
    {
        TMasterAutomatonPart::OnStopLeading();

        StoreMutationTimeExecutor_->Stop();
    }

    void Run()
    {
        TStoreMutationTimeReq request;
        const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
        CreateMutation(hydraManager, request)
            ->CommitAndLog(Logger);
    }

    void OnDynamicConfigChanged()
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

TEpochHistoryManager::TEpochHistoryManager(TBootstrap* bootstrap)
    : Impl_(New<TImpl>(bootstrap))
{ }

TEpochHistoryManager::~TEpochHistoryManager() = default;

std::pair<TInstant, TInstant> TEpochHistoryManager::GetEstimatedMutationTime(TVersion version) const
{
    return Impl_->GetEstimatedMutationTime(version);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellMaster
