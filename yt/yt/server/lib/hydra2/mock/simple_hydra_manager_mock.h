#pragma once

#include <yt/yt/server/lib/hydra2/hydra_manager.h>
#include <yt/yt/server/lib/hydra2/mutation.h>

namespace NYT::NHydra2 {

////////////////////////////////////////////////////////////////////////////////

class TSimpleHydraManagerMock
    : public ISimpleHydraManager
{
public:
    TSimpleHydraManagerMock(
        TCompositeAutomatonPtr automaton,
        IInvokerPtr automatonInvoker,
        TReign reign);

    void ApplyUpTo(int sequenceNumber, bool recovery = false);
    void ApplyAll(bool recovery = false);
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

    // ISimpleHydraManager overrides.

    TFuture<TMutationResponse> CommitMutation(TMutationRequest&& request) override;
    bool IsMutationLoggingEnabled() const override;
    TReign GetCurrentReign() override;
    EPeerState GetAutomatonState() const override;
    bool IsActiveLeader() const override;
    bool IsActiveFollower() const override;
    TCancelableContextPtr GetAutomatonCancelableContext() const override;

    // NB: semantics for these signals is not properly reproduced. Only the
    // parts necessary for tablet write manager are introduced.

    DEFINE_SIGNAL_OVERRIDE(void(), StartLeading);
    DEFINE_SIGNAL_OVERRIDE(void(), AutomatonLeaderRecoveryComplete);
    DEFINE_SIGNAL_OVERRIDE(void(), ControlLeaderRecoveryComplete);
    DEFINE_SIGNAL_OVERRIDE(void(), LeaderActive);
    DEFINE_SIGNAL_OVERRIDE(void(), StopLeading);

    DEFINE_SIGNAL_OVERRIDE(void(), StartFollowing);
    DEFINE_SIGNAL_OVERRIDE(void(), AutomatonFollowerRecoveryComplete);
    DEFINE_SIGNAL_OVERRIDE(void(), ControlFollowerRecoveryComplete);
    DEFINE_SIGNAL_OVERRIDE(void(), StopFollowing);

private:
    const TCompositeAutomatonPtr Automaton_;
    const IInvokerPtr AutomatonInvoker_;
    TReign Reign_;

    const TCancelableContextPtr CancelableContext = New<TCancelableContext>();
    std::deque<TMutationRequest> MutationRequests_;
    std::deque<TPromise<TMutationResponse>> MutationResponsePromises_;
    int AppliedSequenceNumber_ = 0;

    std::optional<int> InRecoveryUntilSequenceNumber_;

    void DoApplyUpTo(int sequenceNumber);
    TSnapshot DoSaveSnapshot();
    void DoLoadSnapshot(const TSnapshot& snapshot);
};

DEFINE_REFCOUNTED_TYPE(TSimpleHydraManagerMock)
DECLARE_REFCOUNTED_CLASS(TSimpleHydraManagerMock)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
