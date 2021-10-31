#pragma once

#include <yt/yt/server/lib/hydra/hydra_manager.h>
#include <yt/yt/server/lib/hydra/mutation.h>

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

class TSimpleHydraManagerMock
    : public ISimpleHydraManager
{
public:
    TSimpleHydraManagerMock(
        TCompositeAutomatonPtr automaton,
        IInvokerPtr automatonInvoker,
        TReign reign);

    void ApplyUpTo(int sequenceNumber);
    void ApplyAll();
    int GetPendingMutationCount() const;
    int GetAppliedSequenceNumber() const;
    int GetCommittedSequenceNumber() const;

    struct TSnapshot
    {
        int SequenceNumber;
        TSharedRef Data;
    };

    TSnapshot SaveSnapshot();
    void LoadSnapshot(const TSnapshot& snapshot);
    void SaveLoad();

private:
    const TCompositeAutomatonPtr Automaton_;
    const IInvokerPtr AutomatonInvoker_;
    TReign Reign_;

    const TCancelableContextPtr CancelableContext = New<TCancelableContext>();
    std::deque<TMutationRequest> MutationRequests_;
    std::deque<TPromise<TMutationResponse>> MutationResponsePromises_;
    int AppliedSequenceNumber_ = 0;

    void DoApplyUpTo(int sequenceNumber);
    TSnapshot DoSaveSnapshot();
    void DoLoadSnapshot(const TSnapshot& snapshot);

    // ISimpleHydraManager overrides.

    TFuture<TMutationResponse> CommitMutation(TMutationRequest&& request) override;
    bool IsMutationLoggingEnabled() const override;
    TReign GetCurrentReign() override;
    EPeerState GetAutomatonState() const override;
    bool IsActiveLeader() const override;
    bool IsActiveFollower() const override;
    TCancelableContextPtr GetAutomatonCancelableContext() const override;

    DEFINE_SIGNAL_OVERRIDE(void(), StartLeading);
    DEFINE_SIGNAL_OVERRIDE(void(), AutomatonLeaderRecoveryComplete);
    DEFINE_SIGNAL_OVERRIDE(void(), ControlLeaderRecoveryComplete);
    DEFINE_SIGNAL_OVERRIDE(void(), LeaderActive);
    DEFINE_SIGNAL_OVERRIDE(void(), StopLeading);

    DEFINE_SIGNAL_OVERRIDE(void(), StartFollowing);
    DEFINE_SIGNAL_OVERRIDE(void(), AutomatonFollowerRecoveryComplete);
    DEFINE_SIGNAL_OVERRIDE(void(), ControlFollowerRecoveryComplete);
    DEFINE_SIGNAL_OVERRIDE(void(), StopFollowing);
};

DEFINE_REFCOUNTED_TYPE(TSimpleHydraManagerMock)
DECLARE_REFCOUNTED_CLASS(TSimpleHydraManagerMock)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
