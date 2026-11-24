#pragma once

#include <yt/yt/flow/extensions/ydb/cpp/client_factory.h>
#include <yt/yt/flow/extensions/ydb/cpp/deferred_publication.h>
#include <yt/yt/flow/extensions/ydb/cpp/writer.h>

#include <yt/yt/flow/library/cpp/resources/resource_base.h>

#include <yt/yt/library/profiling/solomon/registry.h>

#include <yt/yt/core/actions/invoker_detail.h>

#include <library/cpp/yt/system/spin_lock.h>

#include <atomic>
#include <deque>
#include <functional>
#include <map>
#include <optional>
#include <thread>

namespace NYT::NFlow::NYdbTesting {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TFakeYdbWriter);
DECLARE_REFCOUNTED_CLASS(TControllableRetryableYdbWriter);
DECLARE_REFCOUNTED_CLASS(TFakeDeferredPublicationClient);
DECLARE_REFCOUNTED_CLASS(TFakeYdbResource);
DECLARE_REFCOUNTED_CLASS(THoldingInvoker);

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger TestLogger;

////////////////////////////////////////////////////////////////////////////////

//! Serialized-invoker stand-in that can keep the callbacks enqueued by the holding thread
//! and run them one at a time, so a test can observe the state between two queued callbacks.
class THoldingInvoker
    : public TInvokerWrapper<false>
{
public:
    explicit THoldingInvoker(IInvokerPtr underlyingInvoker);

    using TInvokerWrapper<false>::Invoke;
    void Invoke(TClosure callback) override;

    void StartHolding();
    void StopHolding();
    int GetHeldCount() const;
    void ReleaseNext();

private:
    YT_DECLARE_SPIN_LOCK(TSpinLock, Lock_);
    std::optional<std::thread::id> HoldingThread_;
    std::deque<TClosure> Held_;

    void RunBound(TClosure callback);
};

DEFINE_REFCOUNTED_TYPE(THoldingInvoker);

i64 ReadTaggedCounter(
    const NProfiling::TSolomonRegistryPtr& registry,
    TStringBuf sensorName,
    TStringBuf tagKey,
    TStringBuf tagValue);

i64 ReadCounter(
    const NProfiling::TSolomonRegistryPtr& registry,
    TStringBuf sensorName);

void CollectProfiler(const NProfiling::TSolomonRegistryPtr& registry);

////////////////////////////////////////////////////////////////////////////////

struct TFakeYdbWriteState
{
    std::vector<TYdbMessageToWrite> Writes;
    bool AutoAcknowledge = true;
    std::vector<TPromise<void>> PendingWritePromises;
    size_t NotifyAfterWriteCount = 0;
    TPromise<void> WriteCountReached = NewPromise<void>();
    size_t SecondNotifyAfterWriteCount = 0;
    TPromise<void> SecondWriteCountReached = NewPromise<void>();

    void AcknowledgeNext();
    void AcknowledgeAll();
};

////////////////////////////////////////////////////////////////////////////////

class TFakeYdbWriter
    : public IYdbWriter
{
public:
    explicit TFakeYdbWriter(std::shared_ptr<TFakeYdbWriteState> state);

    TFuture<void> Write(TYdbMessageToWrite message) override;

private:
    const std::shared_ptr<TFakeYdbWriteState> State_;
};

DEFINE_REFCOUNTED_TYPE(TFakeYdbWriter);

////////////////////////////////////////////////////////////////////////////////

class TControllableRetryableYdbWriter
    : public TRetryableYdbWriter
{
public:
    TControllableRetryableYdbWriter();

    TFuture<void> Write(TYdbMessageToWrite message) override;

    void ApplyAck(
        i64 seqNo,
        NYdb::NTopic::TWriteSessionEvent::TWriteAck::EEventState state);
    void ApplySessionClosed(NYdb::EStatus status);

    std::vector<i64> GetInflightSeqNos() const;

private:
    std::map<i64, TMessageToWrite> Inflight_;
    std::optional<NYdb::NTopic::TContinuationToken> Token_;
};

DEFINE_REFCOUNTED_TYPE(TControllableRetryableYdbWriter);

////////////////////////////////////////////////////////////////////////////////

class TFakeDeferredPublicationClient
    : public IDeferredPublicationClient
{
public:
    struct TBeginCall
    {
        std::string ExternalPublicationId;
        std::string WriterIdentity;
    };

    ui64 NextPublicationId = 100;
    int ListCallCount = 0;
    std::vector<TBeginCall> BeginCalls;
    std::vector<ui64> CancelCalls;
    std::vector<ui64> PublishCalls;
    std::vector<std::string> Actions;
    std::atomic<NYdb::EStatus> BeginStatus = NYdb::EStatus::SUCCESS;
    NYdb::EStatus ListStatus = NYdb::EStatus::SUCCESS;
    std::optional<NYdb::EStatus> CancelStatus;
    std::deque<NYdb::EStatus> BeginStatuses;
    std::deque<NYdb::EStatus> ListStatuses;
    std::deque<NYdb::EStatus> CancelStatuses;
    std::deque<NYdb::EStatus> PublishStatuses;
    std::optional<TPromise<NYdb::NTopic::TBeginPublicationResult>> PendingBegin;
    std::optional<TPromise<NYdb::NTopic::TListPublicationsResult>> PendingList;
    bool AutoPublish = true;
    bool ApplyBeginBeforeTransientStatus = false;
    bool ApplyCancelBeforeTransientStatus = false;
    bool ApplyPublishBeforeTransientStatus = false;
    TPromise<void> PublishStarted = NewPromise<void>();
    TPromise<void> PendingPublishStarted = NewPromise<void>();
    TFuture<NYdb::NTopic::TPublishResult> LastPendingPublishFuture;
    std::function<void(TFakeDeferredPublicationClient&, const NYdb::NTopic::TDeferredPublication&)> BeforeCancel;

    TFuture<NYdb::NTopic::TBeginPublicationResult> BeginPublication(
        const std::string& externalPublicationId,
        const std::string& writerIdentity) override;
    TFuture<NYdb::NTopic::TListPublicationsResult> ListPublications() override;
    TFuture<NYdb::NTopic::TCancelPublicationResult> CancelPublication(
        const NYdb::NTopic::TDeferredPublication& publication) override;
    TFuture<NYdb::NTopic::TPublishResult> Publish(const NYdb::NTopic::TDeferredPublication& publication) override;

    void CompletePublish(NYdb::EStatus status = NYdb::EStatus::SUCCESS);

    void SetActivePublication(
        ui64 intPublicationId,
        const std::string& externalPublicationId,
        std::optional<std::string> writerIdentity);

private:
    THashMap<std::string, NYdb::NTopic::TPublicationSummary> ActivePublications_;
    std::deque<std::pair<NYdb::NTopic::TDeferredPublication, TPromise<NYdb::NTopic::TPublishResult>>> PendingPublishes_;

    NYdb::EStatus Finalize(const NYdb::NTopic::TDeferredPublication& publication);
};

DEFINE_REFCOUNTED_TYPE(TFakeDeferredPublicationClient);

////////////////////////////////////////////////////////////////////////////////

class TFakeYdbResource
    : public TResourceBase
    , public IYdbClientFactory
{
public:
    int WriterCreationCount = 0;
    int WriterCreationFailures = 0;
    int PublicationClientCreationCount = 0;
    std::vector<std::pair<std::string, std::string>> WriterTargets;
    std::vector<std::pair<std::string, std::string>> PublicationTargets;
    std::vector<NYdb::NTopic::TWriteSessionSettings> WriterSettings;
    std::deque<TYdbWriteTargetResolution> Resolutions;
    int ResolutionCallCount = 0;
    std::vector<std::optional<TYdbWriteTarget>> ResolutionPreferredTargets;
    std::vector<THashSet<TYdbWriteTarget, TYdbWriteTargetHash>> ResolutionExcludedTargets;
    TPromise<void> PublicationClientCreated = NewPromise<void>();
    TPromise<void> ResolutionCalled = NewPromise<void>();
    std::optional<TPromise<TYdbWriteTargetResolution>> PendingResolution;
    std::map<std::pair<std::string, std::string>, IDeferredPublicationClientPtr> PublicationClients;

    TFakeYdbResource(
        TResourceContextPtr context,
        TDynamicResourceContextPtr dynamicContext,
        std::shared_ptr<TFakeYdbWriteState> writeState,
        IDeferredPublicationClientPtr publicationClient);

    IYdbWriterPtr CreateWriter(
        const std::string& endpoint,
        const std::string& database,
        NYdb::NTopic::TWriteSessionSettings settings,
        IInvokerPtr invoker,
        NLogging::TLogger logger,
        IStatusProfilerPtr statusProfiler) override;

    IDeferredPublicationClientPtr CreateDeferredPublicationClient(
        const std::string& endpoint,
        const std::string& database) override;

    std::shared_ptr<NYdb::NTopic::TTopicClient> CreateTopicClient(
        const std::string& endpoint,
        const std::string& database) override;

    std::shared_ptr<NYdb::NFederatedTopic::TFederatedTopicClient> CreateFederatedTopicClient(
        const std::string& endpoint,
        const std::string& database) override;

    TFuture<TYdbWriteTargetResolution> ResolveWriteTarget(
        const TYdbTopologyOptions& options,
        TDuration timeout,
        std::optional<TYdbWriteTarget> preferredTarget,
        THashSet<TYdbWriteTarget, TYdbWriteTargetHash> excludedTargets) override;

private:
    const std::shared_ptr<TFakeYdbWriteState> WriteState_;
    const IDeferredPublicationClientPtr PublicationClient_;
};

DEFINE_REFCOUNTED_TYPE(TFakeYdbResource);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NYdbTesting
