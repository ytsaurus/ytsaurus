#pragma once

#include "private_v2.h"
#include "persistent_mailbox_state_cookie.h"

#include <yt/yt/server/lib/hive/proto/hive_manager.pb.h>

#include <yt/yt/server/lib/hydra/entity_map.h>

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/misc/property.h>

#include <yt/yt/core/concurrency/async_batcher.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/tracing/public.h>

#include <library/cpp/yt/memory/ref_tracked.h>

#include <library/cpp/yt/threading/atomic_object.h>

#include <deque>

namespace NYT::NHiveServer::NV2 {

////////////////////////////////////////////////////////////////////////////////

struct TMessageIdRange
{
    TMessageId Begin; // inclusive
    TMessageId End;   // exclusive

    bool IsEmpty() const;
    i64 GetCount() const;
};

////////////////////////////////////////////////////////////////////////////////

class TPersistentMailboxState final
{
public:
    void Save(NHydra::TSaveContext& context) const;
    void Load(NHydra::TLoadContext& context);

    TPersistentMailboxStateCookie SaveToCookie() const;
    void LoadFromCookie(TPersistentMailboxStateCookie&& cookie);

    TMessageId GetNextPersistentIncomingMessageId() const;
    void SetNextPersistentIncomingMessageId(TMessageId id);

    TMessageId AddOutcomingMessage(TOutcomingMessage message);
    TMessageId TrimLastestOutcomingMessages(int count);
    TMessageIdRange GetOutcomingMessageIdRange() const;
    void IterateOutcomingMessages(
        TMessageId firstMessageId,
        const std::function<bool(const TOutcomingMessage&)>& visitor) const;

private:
    std::atomic<TMessageId> NextPersistentIncomingMessageId_ = 0;

    struct TOutcomingMessages
    {
        TMessageId FirstId = 0;
        std::deque<TOutcomingMessage> Messages;
    };

    NThreading::TAtomicObject<TOutcomingMessages> OutcomingMessages_;
};

DEFINE_REFCOUNTED_TYPE(TPersistentMailboxState)

////////////////////////////////////////////////////////////////////////////////

using TMailboxConnectionEpoch = ui64;

struct TMailboxRuntimeData
    : public TRefCounted
{
    TMailboxRuntimeData(
        bool isLeader,
        TEndpointId endpointId,
        TPersistentMailboxStatePtr persistentState);

    const bool IsLeader;
    const TEndpointId EndpointId;
    const TPersistentMailboxStatePtr PersistentState;

    bool AcknowledgeInProgress = false;

    bool PostInProgress = false;
    TMailboxConnectionEpoch ConnectionEpoch = {};

    // NB: Initialized in ctor.
    TMessageId NextTransientIncomingMessageId;

    //! The id of the first message for which |PostMessages| request to the destination
    //! cell is still in progress. If no request is in progress then this is
    //! just the id of the first message to be sent.
    //! If the mailbox is disconnected, then the value is null.
    std::optional<TMessageId> FirstInFlightOutcomingMessageId;

    //! The number of messages in the above request.
    //! If this value is zero then there is no in-flight request.
    int InFlightOutcomingMessageCount = 0;
};

DEFINE_REFCOUNTED_TYPE(TMailboxRuntimeData)

////////////////////////////////////////////////////////////////////////////////

class TMailbox
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TEndpointId, EndpointId);

public:
    explicit TMailbox(TEndpointId endpointId);

    void Save(NHydra::TSaveContext& context) const;
    void Load(NHydra::TLoadContext& context);

    bool IsCell() const;
    bool IsAvenue() const;

    TCellMailbox* AsCell();
    const TCellMailbox* AsCell() const;

    TAvenueMailbox* AsAvenue();
    const TAvenueMailbox* AsAvenue() const;

    const TPersistentMailboxStatePtr& GetPersistentState() const;
    void RecreatePersistentState();

    const TMailboxRuntimeDataPtr& GetRuntimeData() const;

protected:
    TPersistentMailboxStatePtr PersistentState_ = New<TPersistentMailboxState>();
    TMailboxRuntimeDataPtr RuntimeData_;
};

////////////////////////////////////////////////////////////////////////////////

struct TCellMailboxRuntimeData
    : public TMailboxRuntimeData
{
    TCellMailboxRuntimeData(
        const NProfiling::TProfiler& profiler,
        bool isLeader,
        TEndpointId endpointId,
        TPersistentMailboxStatePtr persistentState,
        TIntrusivePtr<NConcurrency::TAsyncBatcher<void>> synchronizationBatcher);

    // NB: Synchronization batcher is asynchronous.
    const TIntrusivePtr<NConcurrency::TAsyncBatcher<void>> SyncBatcher;
    std::map<TMessageId, TPromise<void>> SyncRequests;

    // NB: Accessed both from automaton and background invokers.
    std::atomic<bool> Connected = false;

    NConcurrency::TDelayedExecutorCookie IdlePostCookie;

    NRpc::IChannelPtr CachedChannel;
    TCpuInstant CachedChannelDeadline = {};

    NConcurrency::TDelayedExecutorCookie PostBatchingCookie;

    const NProfiling::TTimeGauge SyncTimeGauge;

    //! Avenues that are known to be bound to this cell at the moment.
    THashSet<TAvenueMailboxRuntimeDataPtr> RegisteredAvenues;
    //! A subset of #RegisteredAvenues with nonempty outcoming message queue.
    THashSet<TAvenueMailboxRuntimeDataPtr> ActiveAvenues;
};

DEFINE_REFCOUNTED_TYPE(TCellMailboxRuntimeData)

////////////////////////////////////////////////////////////////////////////////

class TCellMailbox
    : public TMailbox
    , public NHydra::TEntityBase
    , public TRefTracked<TCellMailbox>
{
public:
    using TMailbox::TMailbox;

    TCellId GetCellId() const;

    const TCellMailboxRuntimeDataPtr& GetRuntimeData() const;
    void SetRuntimeData(TCellMailboxRuntimeDataPtr runtimeData);

private:
    TCellMailboxRuntimeDataPtr RuntimeData_;
};

////////////////////////////////////////////////////////////////////////////////

struct TAvenueMailboxRuntimeData
    : public TMailboxRuntimeData
{
    using TMailboxRuntimeData::TMailboxRuntimeData;

    //! A (weak) pointer to TCellMailboxRuntimeData whose ActiveAvenues contains this one.
    TWeakPtr<TCellMailboxRuntimeData> Cell;
};

DEFINE_REFCOUNTED_TYPE(TAvenueMailboxRuntimeData)

////////////////////////////////////////////////////////////////////////////////

class TAvenueMailbox
    : public TMailbox
    , public NHydra::TEntityBase
    , public TRefTracked<TCellMailbox>
{
public:
    using TMailbox::TMailbox;

    //! Denotes a mailbox which was unregistered within a mutation which came
    //! as a message to itself. Such mailboxes shound not be unregistered
    //! immediately. Instead they are flagged and removed after the message
    //! is fully applied.
    DEFINE_BYVAL_RW_BOOLEAN_PROPERTY(RemovalScheduled);

public:
    const TAvenueMailboxRuntimeDataPtr& GetRuntimeData() const;
    void SetRuntimeData(TAvenueMailboxRuntimeDataPtr runtimeData);

private:
    TAvenueMailboxRuntimeDataPtr RuntimeData_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHiveServer::NV2
