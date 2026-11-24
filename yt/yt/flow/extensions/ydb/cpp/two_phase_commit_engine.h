#pragma once

#include "public.h"

#include "client_factory.h"
#include "deferred_publication.h"
#include "options.h"
#include "state_access.h"

#include <yt/yt/flow/library/cpp/common/sink.h>

#include <util/generic/function_ref.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

using TYdbTargetCooldownMap = THashMap<TYdbWriteTarget, TInstant, TYdbWriteTargetHash>;

std::vector<TYdbWriteTarget> ExtractExpiredYdbTargets(
    TYdbTargetCooldownMap* retryAfterByTarget,
    TInstant now);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

using TYdbEventLagObserver = TFunctionRef<void(const TStreamId&, TSystemTimestamp)>;

//! Writes every epoch's output into a deferred publication and publishes it after the epoch commits.
class TYdbTwoPhaseCommitEngine
    : public TRefCounted
{
public:
    TYdbTwoPhaseCommitEngine(
        TSinkContextPtr context,
        std::shared_ptr<const TYdbTwoPhaseCommitOptions> options,
        std::shared_ptr<const TYdbTwoPhaseCommitDynamicOptions> dynamicOptions,
        TYdbSinkEnginePtr writer,
        TYdbDeferredPublicationPtr publication,
        TYdbStateAccess<TYdbSinkCheckpoint> checkpointState);
    ~TYdbTwoPhaseCommitEngine() override;

    //! Throws unless the sink has exactly one input stream with the payload column.
    void ValidateInput(const std::string& sinkTypeName) const;

    void Init(IInitContextPtr initContext);
    void Distribute(const TOutputMessageConstPtr& message, TOnDistributedCallback onDistributed);
    void PreSyncFlush(TYdbEventLagObserver observeEventLag);
    void Sync();
    void Commit();
    THashMap<TStreamId, TInflightStreamTraverseDataPtr> BuildInflight() const;
    void Reconfigure(std::shared_ptr<const TYdbTwoPhaseCommitDynamicOptions> dynamicOptions);

private:
    const TSinkContextPtr Context_;
    const std::shared_ptr<const TYdbTwoPhaseCommitOptions> Options_;
    std::shared_ptr<const TYdbTwoPhaseCommitDynamicOptions> DynamicOptions_;
    const TYdbSinkEnginePtr Writer_;
    const TYdbDeferredPublicationPtr Publication_;
    const TYdbStateAccess<TYdbSinkCheckpoint> CheckpointState_;
    const NLogging::TLogger Logger;

    struct TRequest
    {
        TOutputMessageConstPtr Message;
        TOnDistributedCallback Callback;
    };

    TMessageId LastMessageId_;
    i64 LastSeqNo_ = 0;
    std::vector<TRequest> Requests_;
    TFuture<TYdbWriteTargetResolution> TargetFuture_;
    TInstant TargetResolutionStartedAt_;
    std::optional<TInstant> LastTargetRefreshAt_;
    std::optional<TYdbWriteTarget> Target_;
    std::optional<bool> Federated_;
    IStatusErrorStatePtr TargetErrorState_;
    ui64 PreparationAttemptSerial_ = 0;
    NDetail::TYdbTargetCooldownMap RetryAfterByTarget_;

    struct TMetrics;
    const std::unique_ptr<TMetrics> Metrics_;
    THashMap<TYdbWriteTarget, IStatusErrorStatePtr, TYdbWriteTargetHash> PreparationErrorStates_;

    void ExpireTargetCooldowns();
    void StartTargetResolution();
    THashSet<TYdbWriteTarget, TYdbWriteTargetHash> GetExcludedTargets() const;
    void EnsureWriteTarget(bool initializeSession = true);
    void WritePublicationMessages(std::optional<TInstant> deadline, const TYdbBatchingOptions& batching);
    void RetirePreparationAttempt(TDuration targetBackoff, TInstant startedAt, TStringBuf phase);
};

DEFINE_REFCOUNTED_TYPE(TYdbTwoPhaseCommitEngine);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
