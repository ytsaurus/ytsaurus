#pragma once

#include "public.h"

#include "client_factory.h"
#include "options.h"
#include "state_access.h"

#include <yt/yt/flow/library/cpp/common/inflight_tracker.h>
#include <yt/yt/flow/library/cpp/common/sink.h>
#include <yt/yt/flow/library/cpp/common/state_client.h>

#include <ydb-cpp-sdk/client/topic/deferred_publications.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

struct IDeferredPublicationClient
    : public TRefCounted
{
    virtual TFuture<NYdb::NTopic::TBeginPublicationResult> BeginPublication(
        const std::string& externalPublicationId,
        const std::string& writerIdentity) = 0;
    virtual TFuture<NYdb::NTopic::TListPublicationsResult> ListPublications() = 0;
    virtual TFuture<NYdb::NTopic::TCancelPublicationResult> CancelPublication(
        const NYdb::NTopic::TDeferredPublication& publication) = 0;
    virtual TFuture<NYdb::NTopic::TPublishResult> Publish(
        const NYdb::NTopic::TDeferredPublication& publication) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDeferredPublicationClient);

////////////////////////////////////////////////////////////////////////////////

// Owns a sink's prepared and pending publications.
class TYdbDeferredPublication
    : public TRefCounted
{
public:
    TYdbDeferredPublication(
        TSinkContextPtr context,
        IYdbClientFactoryPtr clientFactory,
        std::string writerIdentityType,
        TYdbStateAccess<TYdbPublicationSnapshot> stateAccess);
    ~TYdbDeferredPublication() override;

    void Init(IInitContextPtr initContext);
    void HarvestFinishedPublishes();
    void WaitForPendingPublications();
    void Prepare(
        const TYdbWriteTarget& target,
        ui64 preparationAttempt,
        std::optional<TInstant> deadline);
    void AbandonPreparedPublication();
    void Sync();
    void Commit(bool waitForPublish);

    const NYdb::NTopic::TDeferredPublication& GetPublication() const;
    void RegisterInflight(const TOutputMessageConstPtr& message);
    void CompactInflight();
    THashMap<TStreamId, TInflightStreamTraverseDataPtr> BuildInflight() const;
    THashSet<TYdbWriteTarget, TYdbWriteTargetHash> GetBusyTargets() const;
    bool HasPendingPublications() const;

private:
    const TSinkContextPtr Context_;
    const IYdbClientFactoryPtr ClientFactory_;
    const std::string WriterIdentityType_;
    const TYdbStateAccess<TYdbPublicationSnapshot> StateAccess_;
    const TWatermarkPercentileSpecPtr WatermarkPercentile_;
    const std::string ExternalId_;
    const NLogging::TLogger Logger;
    const IStatusErrorStatePtr ControlErrorState_;

    struct TMetrics;
    const std::unique_ptr<TMetrics> Metrics_;
    struct TPendingPublication;

    struct TPreparedPublication
    {
        TYdbWriteTarget Target;
        IDeferredPublicationClientPtr Client;
        NYdb::NTopic::TDeferredPublication Publication;
    };

    std::vector<std::unique_ptr<TPendingPublication>> PendingPublications_;
    std::optional<TPreparedPublication> PreparedPublication_;
    std::vector<TOutputMessageConstPtr> InflightMessages_;
    THashMap<TStreamId, TInflightStreamTraverseDataPtr> InflightSnapshot_;

    NYdb::NTopic::TDeferredPublication OpenPublication(
        IDeferredPublicationClientPtr client,
        const TYdbWriteTarget& target,
        ui64 preparationAttempt,
        std::optional<TInstant> deadline);
    void StartPublish(TPendingPublication* pending, bool recovery);
    void FinishPublish(int index);
    void SaveState(std::optional<TYdbPendingPublicationSnapshot> prepared);
};

DEFINE_REFCOUNTED_TYPE(TYdbDeferredPublication);

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

struct TDeferredPublicationWriterIdentity
{
    TUniqueSeqNo Generation;
    ui64 PreparationAttempt = 0;
};

std::string MakeDeferredPublicationWriterIdentity(
    const std::string& writerIdentityType,
    TUniqueSeqNo generation,
    ui64 preparationAttempt,
    const TSinkId& sinkId,
    const TPartitionId& partitionId);
TDeferredPublicationWriterIdentity ParseDeferredPublicationWriterIdentity(
    const std::string& writerIdentityType,
    const std::string& writerIdentity,
    const TSinkId& sinkId,
    const TPartitionId& partitionId);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
