#include "subscriptions.h"

#include "private.h"

#include "attributes_helpers.h"
#include "backoff.h"

#include <yt/core/concurrency/delayed_executor.h>
#include <yt/core/misc/error.h>
#include <yt/core/misc/id_generator.h>
#include <yt/core/misc/public.h>
#include <yt/core/ytree/convert.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <library/threading/blocking_queue/blocking_queue.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>
#include <util/system/guard.h>
#include <util/system/mutex.h>

#include <thread>

namespace NYT {
namespace NClickHouse {

using namespace NYT::NApi;
using namespace NYT::NConcurrency;
using namespace NYT::NYTree;
using namespace NYT::NProfiling;

static const NLogging::TLogger& Logger = ServerLogger;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsNodeNotFound(const TError& error)
{
   return error.GetCode() == NYTree::EErrorCode::ResolveError;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

using TSubscriptionId = ui64;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TNotification);

class TNotification
    : public virtual TRefCounted
{
private:
    TSubscriptionId SubscriptionId;

public:
    virtual ~TNotification() = default;

    TNotification(TSubscriptionId id)
        : SubscriptionId(id)
    {}

    TSubscriptionId GetSubscriptionId() const {
        return SubscriptionId;
    }

    virtual void Execute() = 0;
};

DEFINE_REFCOUNTED_TYPE(TNotification);

using TNotificationList = TVector<TNotificationPtr>;

////////////////////////////////////////////////////////////////////////////////

class TOnUpdateNotification
    : public TNotification
{
private:
    TString Path;
    NInterop::INodeEventHandlerWeakPtr EventHandler;
    NInterop::TNodeRevision NewRevision;

public:
    TOnUpdateNotification(
        TSubscriptionId id,
        TString path,
        NInterop::INodeEventHandlerWeakPtr eventHandler,
        NInterop::TNodeRevision newRevision)
        : TNotification(id)
        , Path(std::move(path))
        , EventHandler(std::move(eventHandler))
        , NewRevision(newRevision)
    {}

    void Execute() override
    {
        if (auto handler = EventHandler.lock()) {
            handler->OnUpdate(Path, NewRevision);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TOnRemoveNotification
    : public TNotification
{
private:
    TString Path;
    NInterop::INodeEventHandlerWeakPtr EventHandler;

public:
    TOnRemoveNotification(
        TSubscriptionId id,
        TString path,
        NInterop::INodeEventHandlerWeakPtr eventHandler)
        : TNotification(id)
        , Path(std::move(path))
        , EventHandler(std::move(eventHandler))
    {}

    void Execute() override
    {
        if (auto handler = EventHandler.lock()) {
            handler->OnRemove(Path);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TOnErrorNotification
    : public TNotification
{
private:
    TString Path;
    NInterop::INodeEventHandlerWeakPtr EventHandler;
    TString ErrorMessage;

public:
    TOnErrorNotification(
        TSubscriptionId id,
        TString path,
        NInterop::INodeEventHandlerWeakPtr eventHandler,
        TString errorMessage)
        : TNotification(id)
        , Path(std::move(path))
        , EventHandler(std::move(eventHandler))
        , ErrorMessage(std::move(errorMessage))
    {}

    void Execute() override
    {
        if (auto handler = EventHandler.lock()) {
            handler->OnError(Path, ErrorMessage);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TNotificationQueue);

class TNotificationQueue
    : public TRefCounted
{
private:
    NThreading::TBlockingQueue<TNotificationPtr> Queue{0}; // Unlimited capacity

public:
    // Invoked in fiber context
    void Enqueue(TNotificationPtr notification)
    {
        // Unlimited capacity + low contention
        bool pushed = Queue.Push(std::move(notification));
        Y_UNUSED(pushed); // Queue closed by notification executor
    }

    // Invoked in thread context
    // Returns false if queue is closed
    bool Dequeue(TNotificationList& notifications)
    {
        notifications.clear();
        auto popped = Queue.Pop();
        if (popped) {
            notifications.push_back(*popped);
        }
        return popped.Defined();
    }

    void Close()
    {
        Queue.Stop();
    }
};

DEFINE_REFCOUNTED_TYPE(TNotificationQueue);

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TNodePoller);

class TNodePoller
    : public TRefCounted
{
private:
    NNative::IClientPtr Client;
    TSubscriptionId SubscriptionId;
    TString Path;
    NInterop::TNodeRevision ExpectedRevision;
    NInterop::INodeEventHandlerWeakPtr EventHandler;
    TNotificationQueuePtr NotificationQueue;
    TDuration PollFrequency;

    TBackoff Backoff;

public:
    TNodePoller(
        NNative::IClientPtr client,
        TSubscriptionId id,
        TString path,
        NInterop::TNodeRevision expectedRevision,
        NInterop::INodeEventHandlerWeakPtr eventHandler,
        TNotificationQueuePtr notificationQueue,
        TDuration pollFrequency);

    void Start(TDuration delay);

private:
    void ResetBackoff();

    TErrorOr<NInterop::TNodeRevision> GetRevision();
    bool IsTransientError(const TError& error) const;

    void CheckNode();
    void CheckNodeLater(TDuration delay);
    TDuration GetRegularCheckDelay() const;

    // Notifications

    void NotifyOnUpdate(NInterop::TNodeRevision newRevision);
    void NotifyOnRemove();
    void NotifyOnError(TString errorMessage);
};

DEFINE_REFCOUNTED_TYPE(TNodePoller);

////////////////////////////////////////////////////////////////////////////////

TNodePoller::TNodePoller(
    NNative::IClientPtr client,
    TSubscriptionId id,
    TString path,
    NInterop::TNodeRevision expectedRevision,
    NInterop::INodeEventHandlerWeakPtr eventHandler,
    TNotificationQueuePtr notificationQueue,
    TDuration pollFrequency)
    : Client(std::move(client))
    , SubscriptionId(std::move(id))
    , Path(std::move(path))
    , ExpectedRevision(expectedRevision)
    , EventHandler(std::move(eventHandler))
    , NotificationQueue(std::move(notificationQueue))
    , PollFrequency(pollFrequency)
{
}

void TNodePoller::Start(TDuration delay)
{
    ResetBackoff();
    CheckNodeLater(delay);
}

void TNodePoller::ResetBackoff()
{
    Backoff.ResetTo(PollFrequency);
}

bool TNodePoller::IsTransientError(const TError& error) const
{
    return error.FindMatching(NYT::EErrorCode::Timeout) ||
           error.FindMatching(NYT::NApi::EErrorCode::TooManyConcurrentRequests);
}

void TNodePoller::CheckNode()
{
    auto result = GetRevision();

    if (result.IsOK()) {
        ResetBackoff();

        auto revision = result.ValueOrThrow();
        LOG_DEBUG("Current revision of target node %Qv: %v", Path, revision);

        if (revision != ExpectedRevision) {
            if (revision == -1) {
                NotifyOnRemove();
            } else {
                NotifyOnUpdate(revision);
            }
        } else {
            CheckNodeLater(GetRegularCheckDelay());
        }
    } else if (IsTransientError(result)) {
        // Backoff
        LOG_WARNING("Failed to get revision of target node %Qv: %v", Path, result);
        CheckNodeLater(Backoff.GetNextPause());
    } else {
        // Stop polling and report error to subscriber
        LOG_ERROR(result);
        NotifyOnError(result.GetMessage());
    }
}

TErrorOr<NInterop::TNodeRevision> TNodePoller::GetRevision()
{
    TGetNodeOptions options;
    options.Attributes = {
        "revision",
    };
    options.SuppressAccessTracking = true;
    options.ReadFrom = EMasterChannelKind::Follower;

    auto result = WaitFor(Client->GetNode(GetAttributePath(Path, "revision"), options));

    if (result.IsOK()) {
        const auto node = ConvertToNode(result.Value());
        return node->AsInt64()->GetValue();
    } else if (IsNodeNotFound(result)) {
        return -1; // Node not found
    } else {
        return TError(result);
    }
}

TDuration TNodePoller::GetRegularCheckDelay() const
{
    // TDDO: move poll frequency and jitter coefficient to config
    return AddJitter(PollFrequency, 0.1);
}

void TNodePoller::CheckNodeLater(TDuration delay)
{
    TDelayedExecutor::Submit(
        BIND(&TNodePoller::CheckNode, MakeWeak(this)),
        delay);
}

// Create notification in fiber context and pass it to executor through
// notification queue

void TNodePoller::NotifyOnUpdate(NInterop::TNodeRevision newRevision)
{
    Y_VERIFY(newRevision != ExpectedRevision);

    auto notification = New<TOnUpdateNotification>(
        SubscriptionId,
        std::move(Path),
        std::move(EventHandler),
        newRevision);

    NotificationQueue->Enqueue(std::move(notification));
}

void TNodePoller::NotifyOnRemove()
{
    auto notification = New<TOnRemoveNotification>(
        SubscriptionId,
        std::move(Path),
        std::move(EventHandler));

    NotificationQueue->Enqueue(std::move(notification));
}

void TNodePoller::NotifyOnError(TString errorMessage)
{
    auto notification = New<TOnErrorNotification>(
        SubscriptionId,
        std::move(Path),
        std::move(EventHandler),
        std::move(errorMessage));

    NotificationQueue->Enqueue(std::move(notification));
}

////////////////////////////////////////////////////////////////////////////////

class TSubscriptionManager
    : public ISubscriptionManager
{
    using TSubscriptionMap = THashMap<TSubscriptionId, TNodePollerPtr>;

private:
    TSubscriptionMap Subscriptions;
    TMutex SubscriptionsMutex;

    TNotificationQueuePtr PendingNotificationQueue;
    std::thread NotificationProcessorThread;

public:
    TSubscriptionManager();
    ~TSubscriptionManager();

    void Subscribe(
        NNative::IClientPtr client,
        TString path,
        NInterop::TNodeRevision expectedRevision,
        NInterop::INodeEventHandlerWeakPtr eventHandler) override;

private:
    static TSubscriptionId GenerateSubscriptionId();

    void Register(TSubscriptionId id,
                  TNodePollerPtr poller);

    void UnRegister(const TNotificationList& notifications);
    void Execute(const TNotificationList& notifications);
    void Execute(const TNotificationPtr& notification);

    void ProcessNotificationsLoop();

    void Stop();
};

TSubscriptionManager::TSubscriptionManager()
    : PendingNotificationQueue(New<TNotificationQueue>())
    , NotificationProcessorThread(
        &TSubscriptionManager::ProcessNotificationsLoop, this)
{
}

TSubscriptionManager::~TSubscriptionManager()
{
    Stop();
}

// TODO: move to config
static const TDuration DEFAULT_POLL_FREQUENCY = TDuration::Seconds(1);

void TSubscriptionManager::Subscribe(
    NNative::IClientPtr client,
    TString path,
    NInterop::TNodeRevision expectedRevision,
    NInterop::INodeEventHandlerWeakPtr eventHandler)
{
    auto id = GenerateSubscriptionId();

    LOG_DEBUG("Subscribe to node %Qv, expected revision %v", path, expectedRevision);

    auto pollFrequency = DEFAULT_POLL_FREQUENCY;

    auto poller = New<TNodePoller>(
        client,
        id,
        path,
        expectedRevision,
        std::move(eventHandler),
        PendingNotificationQueue,
        pollFrequency);

    Register(id, poller);

    poller->Start(/*delay=*/ pollFrequency);
}

TSubscriptionId TSubscriptionManager::GenerateSubscriptionId()
{
    static TIdGenerator idGenerator;
    return idGenerator.Next();
}

void TSubscriptionManager::Register(
    TSubscriptionId subscriptionId,
    TNodePollerPtr poller)
{
    auto guard = Guard(SubscriptionsMutex);

    LOG_DEBUG("Register subscription %Qv", subscriptionId);

    // Hold strong reference to node poller
    Subscriptions.emplace(subscriptionId, poller);
}

void TSubscriptionManager::UnRegister(const TNotificationList& notifications)
{
    auto guard = Guard(SubscriptionsMutex);

    for (const auto& notification : notifications) {
        auto id = notification->GetSubscriptionId();
        LOG_DEBUG("Unregister subscription %Qv", id);
        Subscriptions.erase(id); // Release node poller
    }
}

void TSubscriptionManager::Execute(const TNotificationPtr& notification)
{
    try {
        notification->Execute();
    } catch (...) {
        LOG_ERROR("Event handler of subscription %Qv raised error: %v",
            notification->GetSubscriptionId(), CurrentExceptionMessage());
        throw; // TODO
    }
}

void TSubscriptionManager::Execute(const TNotificationList& notifications)
{
    for (const auto& notification : notifications) {
        Execute(notification);
    }
}

void TSubscriptionManager::ProcessNotificationsLoop()
{
    TNotificationList notifications;
    while (PendingNotificationQueue->Dequeue(notifications)) {
        UnRegister(notifications);
        Execute(notifications);
    }
}

void TSubscriptionManager::Stop()
{
    PendingNotificationQueue->Close();
    NotificationProcessorThread.join();
    Subscriptions.clear();
}

////////////////////////////////////////////////////////////////////////////////

ISubscriptionManagerPtr CreateSubscriptionManager()
{
    return New<TSubscriptionManager>();
}

}   // namespace NClickHouse
}   // namespace NYT
