#include "log_manager.h"
#include "private.h"
#include "config.h"
#include "log.h"
#include "writer.h"

#include <yt/core/concurrency/fork_aware_spinlock.h>
#include <yt/core/concurrency/periodic_executor.h>
#include <yt/core/concurrency/scheduler_thread.h>
#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/invoker_queue.h>

#include <yt/core/misc/fs.h>
#include <yt/core/misc/hash.h>
#include <yt/core/misc/lock_free.h>
#include <yt/core/misc/pattern_formatter.h>
#include <yt/core/misc/proc.h>
#include <yt/core/misc/property.h>
#include <yt/core/misc/raw_formatter.h>
#include <yt/core/misc/shutdown.h>
#include <yt/core/misc/variant.h>
#include <yt/core/misc/ref_counted_tracker.h>
#include <yt/core/misc/heap.h>

#include <yt/core/profiling/profile_manager.h>
#include <yt/core/profiling/profiler.h>
#include <yt/core/profiling/timing.h>

#include <yt/core/ytree/ypath_client.h>
#include <yt/core/ytree/ypath_service.h>
#include <yt/core/ytree/yson_serializable.h>
#include <yt/core/ytree/convert.h>

#include <util/system/defaults.h>
#include <util/system/sigset.h>
#include <util/system/yield.h>

#include <util/string/vector.h>

#include <atomic>
#include <mutex>

#ifdef _win_
    #include <io.h>
#else
    #include <unistd.h>
#endif

#ifdef _linux_
    #include <sys/inotify.h>
#endif

#include <errno.h>

namespace NYT::NLogging {

using namespace NYTree;
using namespace NConcurrency;
using namespace NFS;
using namespace NProfiling;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

static const TLogger Logger(SystemLoggingCategoryName);
static const auto& Profiler = LoggingProfiler;

static constexpr auto ProfilingPeriod = TDuration::Seconds(10);
static constexpr auto DiskProfilingPeriod = TDuration::Minutes(5);
static constexpr auto DequeuePeriod = TDuration::MilliSeconds(30);

////////////////////////////////////////////////////////////////////////////////

bool operator == (const TLogWritersCacheKey& lhs, const TLogWritersCacheKey& rhs)
{
    return lhs.Category == rhs.Category && lhs.LogLevel == rhs.LogLevel && lhs.MessageFormat == rhs.MessageFormat;
}

////////////////////////////////////////////////////////////////////////////////

class TNotificationHandle
    : private TNonCopyable
{
public:
    TNotificationHandle()
        : FD_(-1)
    {
#ifdef _linux_
        FD_ = inotify_init1(IN_NONBLOCK | IN_CLOEXEC);
        YT_VERIFY(FD_ >= 0);
#endif
    }

    ~TNotificationHandle()
    {
#ifdef _linux_
        YT_VERIFY(FD_ >= 0);
        ::close(FD_);
#endif
    }

    int Poll()
    {
#ifdef _linux_
        YT_VERIFY(FD_ >= 0);

        char buffer[sizeof(struct inotify_event) + NAME_MAX + 1];
        ssize_t rv = HandleEintr(::read, FD_, buffer, sizeof(buffer));

        if (rv < 0) {
            if (errno != EAGAIN) {
                YT_LOG_ERROR(
                    TError::FromSystem(errno),
                    "Unable to poll inotify() descriptor %v",
                    FD_);
            }
        } else if (rv > 0) {
            YT_ASSERT(rv >= sizeof(struct inotify_event));
            struct inotify_event* event = (struct inotify_event*)buffer;

            if (event->mask & IN_ATTRIB) {
                YT_LOG_TRACE(
                    "Watch %v has triggered metadata change (IN_ATTRIB)",
                    event->wd);
            }
            if (event->mask & IN_DELETE_SELF) {
                YT_LOG_TRACE(
                    "Watch %v has triggered a deletion (IN_DELETE_SELF)",
                    event->wd);
            }
            if (event->mask & IN_MOVE_SELF) {
                YT_LOG_TRACE(
                    "Watch %v has triggered a movement (IN_MOVE_SELF)",
                    event->wd);
            }

            return event->wd;
        } else {
            // Do nothing.
        }
#endif
        return 0;
    }

    DEFINE_BYVAL_RO_PROPERTY(int, FD);
};

////////////////////////////////////////////////////////////////////////////////

class TNotificationWatch
    : private TNonCopyable
{
public:
    TNotificationWatch(
        TNotificationHandle* handle,
        const TString& path,
        TClosure callback)
        : FD_(handle->GetFD())
        , WD_(-1)
        , Path_(path)
        , Callback_(std::move(callback))

    {
        FD_ = handle->GetFD();
        YT_VERIFY(FD_ >= 0);

        CreateWatch();
    }

    ~TNotificationWatch()
    {
        DropWatch();
    }

    DEFINE_BYVAL_RO_PROPERTY(int, FD);
    DEFINE_BYVAL_RO_PROPERTY(int, WD);

    void Run()
    {
        Callback_.Run();
        // Reinitialize watch to hook to the newly created file.
        DropWatch();
        CreateWatch();
    }

private:
    void CreateWatch()
    {
        YT_VERIFY(WD_ <= 0);
#ifdef _linux_
        WD_ = inotify_add_watch(
            FD_,
            Path_.c_str(),
            IN_ATTRIB | IN_DELETE_SELF | IN_MOVE_SELF);

        if (WD_ < 0) {
            YT_LOG_ERROR(TError::FromSystem(errno), "Error registering watch for %v",
                Path_);
            WD_ = -1;
        } else if (WD_ > 0) {
            YT_LOG_TRACE("Registered watch %v for %v",
                WD_,
                Path_);
        } else {
            YT_ABORT();
        }
#else
        WD_ = -1;
#endif
    }

    void DropWatch()
    {
#ifdef _linux_
        if (WD_ > 0) {
            YT_LOG_TRACE("Unregistering watch %v for %v",
                WD_,
                Path_);
            inotify_rm_watch(FD_, WD_);
        }
#endif
        WD_ = -1;
    }

private:
    TString Path_;
    TClosure Callback_;

};

////////////////////////////////////////////////////////////////////////////////

namespace {

void ReloadSignalHandler(int signal)
{
    NLogging::TLogManager::Get()->Reopen();
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <class TElement>
class TExpiringSet
{
public:
    TExpiringSet()
    {
        Reconfigure(TDuration::Zero());
    }

    explicit TExpiringSet(TDuration livetime)
    {
        Reconfigure(livetime);
    }

    void Update(std::vector<TElement> elements)
    {
        RemoveExpired();
        Insert(std::move(elements));
    }

    bool Contains(const TElement& element)
    {
        return Set_.contains(element);
    }

    void Reconfigure(TDuration livetime)
    {
        Livetime_ = DurationToCpuDuration(livetime);
    }

    void Clear()
    {
        Set_.clear();
        ExpirationQueue_ = std::priority_queue<TPack>();
    }

private:
    struct TPack
    {
        std::vector<TElement> Elements;
        TCpuInstant ExpirationTime;

        bool operator<(const TPack& other) const
        {
            // Reversed ordering for the priority queue.
            return ExpirationTime > other.ExpirationTime;
        }
    };

    TCpuDuration Livetime_;
    THashSet<TElement> Set_;
    std::priority_queue<TPack> ExpirationQueue_;


    void Insert(std::vector<TElement> elements)
    {
        for (const auto& element : elements) {
            Set_.insert(element);
        }

        ExpirationQueue_.push(TPack{std::move(elements), GetCpuInstant() + Livetime_});
    }

    void RemoveExpired()
    {
        auto now = GetCpuInstant();
        while (!ExpirationQueue_.empty() && ExpirationQueue_.top().ExpirationTime < now) {
            for (const auto& element : ExpirationQueue_.top().Elements) {
                Set_.erase(element);
            }

            ExpirationQueue_.pop();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TConfigEvent
{
    NProfiling::TCpuInstant Instant = 0;
    TLogManagerConfigPtr Config;
    TPromise<void> Promise = NewPromise<void>();
};

using TLoggerQueueItem = std::variant<
    TLogEvent,
    TConfigEvent
>;

NProfiling::TCpuInstant GetEventInstant(const TLoggerQueueItem& item)
{
    return Visit(item,
        [&] (const TConfigEvent& event) {
            return event.Instant;
        },
        [&] (const TLogEvent& event) {
            return event.Instant;
        });
}

using TThreadLocalQueue = TSingleProducerSingleConsumerQueue<TLoggerQueueItem>;

static constexpr uintptr_t ThreadQueueDestroyedSentinel = -1;
static thread_local TThreadLocalQueue* PerThreadQueue;

/////////////////////////////////////////////////////////////////////////////

class TLogManager::TImpl
    : public TRefCounted
{
public:
    friend struct TLocalQueueReclaimer;

    TImpl()
        : EventQueue_(New<TInvokerQueue>(
            EventCount_,
            NProfiling::EmptyTagIds,
            false,
            false))
        , LoggingThread_(New<TThread>(this))
        , SystemWriters_({New<TStderrLogWriter>()})
    {
        DoUpdateConfig(TLogManagerConfig::CreateDefault());
        SystemCategory_ = GetCategory(SystemLoggingCategoryName);
    }

    ~TImpl()
    {
        RegisteredLocalQueues_.DequeueAll(true, [&] (TThreadLocalQueue* item) {
            LocalQueues_.insert(item);
        });

        for (auto localQueue : LocalQueues_) {
            delete localQueue;
        }
    }

    void Configure(INodePtr node)
    {
        Configure(TLogManagerConfig::CreateFromNode(node));
    }

    void Configure(TLogManagerConfigPtr config)
    {
        if (LoggingThread_->IsShutdown()) {
            return;
        }

        EnsureStarted();

        TConfigEvent event;
        event.Instant = NProfiling::GetCpuInstant();
        event.Config = std::move(config);
        auto future = event.Promise.ToFuture();

        PushEvent(std::move(event));

        DequeueExecutor_->ScheduleOutOfBand();

        future.Get();
    }

    void ConfigureSimple(
        const char* logLevelStr,
        const char* logExcludeCategoriesStr,
        const char* logIncludeCategoriesStr)
    {
        if (!logLevelStr && !logExcludeCategoriesStr && !logIncludeCategoriesStr) {
            return;
        }

        const char* const stderrWriterName = "stderr";

        auto rule = New<TRuleConfig>();
        rule->Writers.push_back(stderrWriterName);
        rule->MinLevel = ELogLevel::Fatal;

        if (logLevelStr) {
            TString logLevel = logLevelStr;
            if (!logLevel.empty()) {
                // This handles most typical casings like "DEBUG", "debug", "Debug".
                logLevel.to_title();
                rule->MinLevel = TEnumTraits<ELogLevel>::FromString(logLevel);
            }
        }

        std::vector<TString> logExcludeCategories;
        if (logExcludeCategoriesStr) {
            logExcludeCategories = SplitString(logExcludeCategoriesStr, ",");
        }

        for (const auto& excludeCategory : logExcludeCategories) {
            rule->ExcludeCategories.insert(excludeCategory);
        }

        std::vector<TString> logIncludeCategories;
        if (logIncludeCategoriesStr) {
            logIncludeCategories = SplitString(logIncludeCategoriesStr, ",");
        }

        if (!logIncludeCategories.empty()) {
            rule->IncludeCategories.emplace();
            for (const auto& includeCategory : logIncludeCategories) {
                rule->IncludeCategories->insert(includeCategory);
            }
        }

        auto config = New<TLogManagerConfig>();
        config->Rules.push_back(std::move(rule));

        config->MinDiskSpace = 0;
        config->HighBacklogWatermark = std::numeric_limits<int>::max();
        config->LowBacklogWatermark = 0;

        auto stderrWriter = New<TWriterConfig>();
        stderrWriter->Type = EWriterType::Stderr;

        config->WriterConfigs.insert(std::make_pair(stderrWriterName, std::move(stderrWriter)));

        Configure(std::move(config));
    }

    void ConfigureFromEnv()
    {
        ConfigureSimple(
            getenv("YT_LOG_LEVEL"),
            getenv("YT_LOG_EXCLUDE_CATEGORIES"),
            getenv("YT_LOG_INCLUDE_CATEGORIES"));
    }

    void Shutdown()
    {
        ShutdownRequested_ = true;

        if (LoggingThread_->GetId() == ::TThread::CurrentThreadId()) {
            FlushWriters();
        } else {
            // Wait for all previously enqueued messages to be flushed
            // but no more than ShutdownGraceTimeout to prevent hanging.
            Synchronize(TInstant::Now() + Config_->ShutdownGraceTimeout);
        }

        EventQueue_->Shutdown();
        LoggingThread_->Shutdown();
    }

    /*!
     * In some cases (when configuration is being updated at the same time),
     * the actual version is greater than the version returned by this method.
     */
    int GetVersion() const
    {
        return Version_.load();
    }

    bool GetAbortOnAlert() const
    {
        return AbortOnAlert_.load();
    }

    const TLoggingCategory* GetCategory(const char* categoryName)
    {
        if (!categoryName) {
            return nullptr;
        }

        TGuard<TForkAwareSpinLock> guard(SpinLock_);
        auto it = NameToCategory_.find(categoryName);
        if (it == NameToCategory_.end()) {
            auto category = std::make_unique<TLoggingCategory>();
            category->Name = categoryName;
            category->ActualVersion = &Version_;
            it = NameToCategory_.emplace(categoryName, std::move(category)).first;
            DoUpdateCategory(it->second.get());
        }
        return it->second.get();
    }

    void UpdateCategory(TLoggingCategory* category)
    {
        TGuard<TForkAwareSpinLock> guard(SpinLock_);
        DoUpdateCategory(category);
    }

    void UpdatePosition(TLoggingPosition* position, TStringBuf message)
    {
        TGuard<TForkAwareSpinLock> guard(SpinLock_);
        DoUpdatePosition(position, message);
    }

    void Enqueue(TLogEvent&& event)
    {
        if (event.Level == ELogLevel::Fatal) {
            bool shutdown = false;
            if (!ShutdownRequested_.compare_exchange_strong(shutdown, true)) {
                // Fatal events should not get out of this call.
                Sleep(TDuration::Max());
            }

            // Collect last-minute information.
            TRawFormatter<1024> formatter;
            formatter.AppendString("\n*** Fatal error ***\n");
            formatter.AppendString(TStringBuf(event.Message.Begin(), event.Message.End()));
            formatter.AppendString("\n*** Aborting ***\n");

            HandleEintr(::write, 2, formatter.GetData(), formatter.GetBytesWritten());

            // Add fatal message to log and notify event log queue.
            PushEvent(std::move(event));

            // Flush everything and die.
            Shutdown();

            std::terminate();
        }

        if (ShutdownRequested_) {
            ++DroppedEvents_;
            return;
        }

        if (LoggingThread_->IsShutdown()) {
            ++DroppedEvents_;
            return;
        }

        EnsureStarted();

        // Order matters here; inherent race may lead to negative backlog and integer overflow.
        ui64 writtenEvents = WrittenEvents_.load();
        ui64 enqueuedEvents = EnqueuedEvents_.load();
        ui64 backlogEvents = enqueuedEvents - writtenEvents;

        // NB: This is somewhat racy but should work fine as long as more messages keep coming.
        if (Suspended_) {
            if (backlogEvents < LowBacklogWatermark_) {
                Suspended_ = false;
                YT_LOG_INFO("Backlog size has dropped below low watermark %v, logging resumed",
                    LowBacklogWatermark_);
            }
        } else {
            if (backlogEvents >= LowBacklogWatermark_ && !ScheduledOutOfBand_.test_and_set()) {
                DequeueExecutor_->ScheduleOutOfBand();
            }

            if (backlogEvents >= HighBacklogWatermark_) {
                Suspended_ = true;
                YT_LOG_WARNING("Backlog size has exceeded high watermark %v, logging suspended",
                    HighBacklogWatermark_);
            }
        }

        // NB: Always allow system messages to pass through.
        if (Suspended_ && event.Category != SystemCategory_) {
            ++DroppedEvents_;
            return;
        }

        PushEvent(std::move(event));
    }

    void Reopen()
    {
        ReopenRequested_ = true;
    }

    void SuppressTrace(TTraceId traceId)
    {
        if (traceId == InvalidTraceId) {
            return;
        }

        if (!TraceSuppressionEnabled_) {
            return;
        }

        SuppressedTraceIdQueue_.Enqueue(traceId);
    }

    void Synchronize(TInstant deadline = TInstant::Max())
    {
        auto enqueuedEvents = EnqueuedEvents_.load();
        while (enqueuedEvents > FlushedEvents_.load() && TInstant::Now() < deadline) {
            SchedYield();
        }
    }

private:
    class TThread
        : public TSchedulerThread
    {
    public:
        explicit TThread(TImpl* owner)
            : TSchedulerThread(
                owner->EventCount_,
                SystemLoggingCategoryName,
                NProfiling::EmptyTagIds,
                false,
                false)
            , Owner_(owner)
        { }

    private:
        TImpl* const Owner_;

        virtual void OnThreadStart() override
        {
#ifdef _unix_
            // Set mask.
            sigset_t ss;
            sigemptyset(&ss);
            sigaddset(&ss, SIGHUP);
            sigprocmask(SIG_UNBLOCK, &ss, nullptr);

            // Set handler.
            struct sigaction sa;
            memset(&sa, 0, sizeof(sa));
            sigemptyset(&sa.sa_mask);
            sa.sa_handler = &ReloadSignalHandler;

            YT_VERIFY(sigaction(SIGHUP, &sa, nullptr) == 0);
#endif
        }

        virtual TClosure BeginExecute() override
        {
            return Owner_->BeginExecute();
        }

        virtual void EndExecute() override
        {
            Owner_->EndExecute();
        }
    };

    TClosure BeginExecute()
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        return EventQueue_->BeginExecute(&CurrentAction_);
    }

    void EndExecute()
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        EventQueue_->EndExecute(&CurrentAction_);
    }


    void EnsureStarted()
    {
        std::call_once(Started_, [&] {
            if (LoggingThread_->IsShutdown()) {
                return;
            }

            LoggingThread_->Start();
            EventQueue_->SetThreadId(LoggingThread_->GetId());

            ProfilingExecutor_ = New<TPeriodicExecutor>(
                EventQueue_,
                BIND(&TImpl::OnProfiling, MakeStrong(this)),
                ProfilingPeriod);
            ProfilingExecutor_->Start();

            DiskProfilingExecutor_ = New<TPeriodicExecutor>(
                EventQueue_,
                BIND(&TImpl::OnDiskProfiling, MakeStrong(this)),
                DiskProfilingPeriod);
            DiskProfilingExecutor_->Start();

            DequeueExecutor_ = New<TPeriodicExecutor>(
                EventQueue_,
                BIND(&TImpl::OnDequeue, MakeStrong(this)),
                DequeuePeriod);
            DequeueExecutor_->Start();
        });
    }

    const std::vector<ILogWriterPtr>& GetWriters(const TLogEvent& event)
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        if (event.Category == SystemCategory_) {
            return SystemWriters_;
        }

        TLogWritersCacheKey cacheKey{event.Category->Name, event.Level, event.MessageFormat};
        auto it = CachedWriters_.find(cacheKey);
        if (it != CachedWriters_.end()) {
            return it->second;
        }

        THashSet<TString> writerIds;
        for (const auto& rule : Config_->Rules) {
            if (rule->IsApplicable(event.Category->Name, event.Level, event.MessageFormat)) {
                writerIds.insert(rule->Writers.begin(), rule->Writers.end());
            }
        }

        std::vector<ILogWriterPtr> writers;
        for (const auto& writerId : writerIds) {
            writers.push_back(GetOrCrash(Writers_, writerId));
        }

        auto pair = CachedWriters_.insert(std::make_pair(cacheKey, writers));
        YT_VERIFY(pair.second);

        return pair.first->second;
    }

    std::unique_ptr<TNotificationWatch> CreateNotificationWatch(ILogWriterPtr writer, const TString& fileName)
    {
#ifdef _linux_
        if (Config_->WatchPeriod) {
            if (!NotificationHandle_) {
                NotificationHandle_.reset(new TNotificationHandle());
            }
            return std::unique_ptr<TNotificationWatch>(
                new TNotificationWatch(
                    NotificationHandle_.get(),
                    fileName.c_str(),
                    BIND(&ILogWriter::Reload, writer)));
        }
#endif
        return nullptr;
    }

    void UpdateConfig(TConfigEvent& event)
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        if (ShutdownRequested_) {
            return;
        }

        if (LoggingThread_->IsShutdown()) {
            return;
        }

        AbortOnAlert_.store(event.Config->AbortOnAlert);

        EnsureStarted();

        FlushWriters();

        DoUpdateConfig(event.Config);

        if (FlushExecutor_) {
            FlushExecutor_->Stop();
            FlushExecutor_.Reset();
        }

        if (WatchExecutor_) {
            WatchExecutor_->Stop();
            WatchExecutor_.Reset();
        }

        auto flushPeriod = Config_->FlushPeriod;
        if (flushPeriod) {
            FlushExecutor_ = New<TPeriodicExecutor>(
                EventQueue_,
                BIND(&TImpl::FlushWriters, MakeStrong(this)),
                *flushPeriod);
            FlushExecutor_->Start();
        }

        auto watchPeriod = Config_->WatchPeriod;
        if (watchPeriod) {
            WatchExecutor_ = New<TPeriodicExecutor>(
                EventQueue_,
                BIND(&TImpl::WatchWriters, MakeStrong(this)),
                *watchPeriod);
            WatchExecutor_->Start();
        }

        auto checkSpacePeriod = Config_->CheckSpacePeriod;
        if (checkSpacePeriod) {
            CheckSpaceExecutor_ = New<TPeriodicExecutor>(
                EventQueue_,
                BIND(&TImpl::CheckSpace, MakeStrong(this)),
                *checkSpacePeriod);
            CheckSpaceExecutor_->Start();
        }

        event.Promise.Set();
    }

    void DoUpdateConfig(const TLogManagerConfigPtr& logConfig)
    {
        {
            decltype(Writers_) writers;
            decltype(CachedWriters_) cachedWriters;

            TGuard<TForkAwareSpinLock> guard(SpinLock_);
            Writers_.swap(writers);
            CachedWriters_.swap(cachedWriters);
            Config_ = logConfig;
            HighBacklogWatermark_ = Config_->HighBacklogWatermark;
            LowBacklogWatermark_ = Config_->LowBacklogWatermark;
            TraceSuppressionEnabled_ = Config_->TraceSuppressionTimeout != TDuration::Zero();

            guard.Release();

            // writers and cachedWriters will die here where we don't
            // hold the spinlock anymore.
        }

        if (TraceSuppressionEnabled_) {
            SuppressedTraceIdSet_.Reconfigure((Config_->TraceSuppressionTimeout + DequeuePeriod) * 2);
        } else {
            SuppressedTraceIdSet_.Clear();
            SuppressedTraceIdQueue_.DequeueAll();
        }

        for (const auto& pair : Config_->WriterConfigs) {
            const auto& name = pair.first;
            const auto& config = pair.second;

            ILogWriterPtr writer;
            std::unique_ptr<ILogFormatter> formatter;
            std::unique_ptr<TNotificationWatch> watch;

            switch (config->AcceptedMessageFormat) {
                case ELogMessageFormat::PlainText:
                    formatter = std::make_unique<TPlainTextLogFormatter>();
                    break;
                case ELogMessageFormat::Structured:
                    formatter = std::make_unique<TJsonLogFormatter>(config->CommonFields);
                    break;
                default:
                    YT_ABORT();
            }

            switch (config->Type) {
                case EWriterType::Stdout:
                    writer = New<TStdoutLogWriter>(std::move(formatter), name);
                    break;
                case EWriterType::Stderr:
                    writer = New<TStderrLogWriter>(std::move(formatter), name);
                    break;
                case EWriterType::File:
                    writer = New<TFileLogWriter>(std::move(formatter), name, config->FileName, config->EnableCompression);
                    watch = CreateNotificationWatch(writer, config->FileName);
                    break;
                default:
                    YT_ABORT();
            }

            writer->SetRateLimit(config->RateLimit);
            writer->SetCategoryRateLimits(Config_->CategoryRateLimits);

            YT_VERIFY(Writers_.insert(std::make_pair(name, std::move(writer))).second);

            if (watch) {
                if (watch->GetWD() >= 0) {
                    // Watch can fail to initialize if the writer is disabled
                    // e.g. due to the lack of space.
                    YT_VERIFY(NotificationWatchesIndex_.insert(
                        std::make_pair(watch->GetWD(), watch.get())).second);
                }
                NotificationWatches_.emplace_back(std::move(watch));
            }
        }

        Version_++;
    }

    void WriteEvent(const TLogEvent& event)
    {
        if (ReopenRequested_) {
            ReopenRequested_ = false;
            ReloadWriters();
        }
        LoggingProfiler.Increment(*GetCategoryEventsCounter(event.Category->Name), 1);
        for (const auto& writer : GetWriters(event)) {
            writer->Write(event);
        }
    }

    void FlushWriters()
    {
        for (auto& pair : Writers_) {
            pair.second->Flush();
        }
    }

    void ReloadWriters()
    {
        Version_++;
        for (auto& pair : Writers_) {
            pair.second->Reload();
        }
    }

    void CheckSpace()
    {
        for (auto& pair : Writers_) {
            pair.second->CheckSpace(Config_->MinDiskSpace);
        }
    }

    void WatchWriters()
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        if (!NotificationHandle_)
            return;

        int previousWD = -1, currentWD = -1;
        while ((currentWD = NotificationHandle_->Poll()) > 0) {
            if (currentWD == previousWD) {
                continue;
            }
            auto&& it = NotificationWatchesIndex_.find(currentWD);
            auto&& jt = NotificationWatchesIndex_.end();
            if (it == jt) {
                continue;
            }

            auto* watch = it->second;
            watch->Run();

            if (watch->GetWD() != currentWD) {
                NotificationWatchesIndex_.erase(it);
                if (watch->GetWD() >= 0) {
                    // Watch can fail to initialize if the writer is disabled
                    // e.g. due to the lack of space.
                    YT_VERIFY(NotificationWatchesIndex_.insert(
                        std::make_pair(watch->GetWD(), watch)).second);
                }
            }

            previousWD = currentWD;
        }
    }

    void PushEvent(TLoggerQueueItem&& event)
    {
        if (!PerThreadQueue) {
            PerThreadQueue = new TThreadLocalQueue();
            RegisteredLocalQueues_.Enqueue(PerThreadQueue);
        }

        ++EnqueuedEvents_;
        if (PerThreadQueue == reinterpret_cast<TThreadLocalQueue*>(ThreadQueueDestroyedSentinel)) {
            GlobalQueue_.Enqueue(std::move(event));
        } else {
            PerThreadQueue->Push(std::move(event));
        }
    }

    void OnProfiling()
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        auto writtenEvents = WrittenEvents_.load();
        auto enqueuedEvents = EnqueuedEvents_.load();
        auto suppressedEvents = SuppressedEvents_.load();
        auto droppedEvents = DroppedEvents_.load();
        auto messageBuffersSize = TRefCountedTracker::Get()->GetBytesAlive(GetRefCountedTypeKey<NDetail::TMessageBufferTag>());

        Profiler.Enqueue("/enqueued_events", enqueuedEvents, EMetricType::Counter);
        Profiler.Enqueue("/written_events", writtenEvents, EMetricType::Counter);
        Profiler.Enqueue("/backlog_events", enqueuedEvents - writtenEvents, EMetricType::Gauge);
        Profiler.Enqueue("/dropped_events", droppedEvents, EMetricType::Counter);
        Profiler.Enqueue("/suppressed_events", suppressedEvents, EMetricType::Counter);
        Profiler.Enqueue("/message_buffers_size", messageBuffersSize, EMetricType::Gauge);
    }

    void OnDiskProfiling()
    {
        try {
            auto minLogStorageAvailableSpace = std::numeric_limits<i64>::max();
            auto minLogStorageFreeSpace = std::numeric_limits<i64>::max();

            for (const auto& [name, writerConfig] : Config_->WriterConfigs) {
                if (writerConfig->Type == EWriterType::File) {
                    auto logStorageDiskSpaceStatistics = GetDiskSpaceStatistics(GetDirectoryName(writerConfig->FileName));
                    minLogStorageAvailableSpace = std::min<i64>(minLogStorageAvailableSpace, logStorageDiskSpaceStatistics.AvailableSpace);
                    minLogStorageFreeSpace = std::min<i64>(minLogStorageFreeSpace, logStorageDiskSpaceStatistics.FreeSpace);
                }
            }

            if (minLogStorageAvailableSpace != std::numeric_limits<i64>::max()) {
                Profiler.Enqueue("/min_log_storage_available_space", minLogStorageAvailableSpace, EMetricType::Gauge);
            }
            if (minLogStorageFreeSpace != std::numeric_limits<i64>::max()) {
                Profiler.Enqueue("/min_log_storage_free_space", minLogStorageFreeSpace, EMetricType::Gauge);
            }
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to get log storage disk statistics");
        }
    }

    void OnDequeue()
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        ScheduledOutOfBand_.clear();

        auto instant = NProfiling::GetCpuInstant();

        RegisteredLocalQueues_.DequeueAll(true, [&] (TThreadLocalQueue* item) {
            YT_VERIFY(LocalQueues_.insert(item).second);
        });

        struct THeapItem
        {
            TThreadLocalQueue* Queue;

            THeapItem(TThreadLocalQueue* queue)
                : Queue(queue)
            { }

            TLoggerQueueItem* Front() const
            {
                return Queue->Front();
            }

            void Pop()
            {
                Queue->Pop();
            }

            NProfiling::TCpuInstant GetInstant() const
            {
                auto front = Front();
                if (Y_LIKELY(front)) {
                    return GetEventInstant(*front);
                } else {
                    return std::numeric_limits<TCpuInstant>::max();
                }
            }

            bool operator < (const THeapItem& other) const
            {
                return GetInstant() < other.GetInstant();
            }
        };

        // TODO(lukyan): Reuse heap.
        std::vector<THeapItem> heap;
        for (auto localQueue : LocalQueues_) {
            if (localQueue->Front()) {
                heap.push_back(localQueue);
            }
        }

        NYT::MakeHeap(heap.begin(), heap.end());

        // TODO(lukyan): Get next minimum instant and pop from top queue in loop.
        // NB: Messages are not totally ordered beacause of race around high/low watermark check
        while (!heap.empty()) {
            auto& queue = heap.front();
            TLoggerQueueItem* event = queue.Front();

            if (!event || GetEventInstant(*event) >= instant) {
                break;
            }

            TimeOrderedBuffer_.emplace_back(std::move(*event));
            queue.Pop();
            NYT::AdjustHeapFront(heap.begin(), heap.end());
        }

        UnregisteredLocalQueues_.DequeueAll(true, [&] (TThreadLocalQueue* item) {
            if (item->IsEmpty()) {
                YT_VERIFY(LocalQueues_.erase(item));
                delete item;
            } else {
                UnregisteredLocalQueues_.Enqueue(item);
            }
        });

        // TODO(lukyan): To achive total order of messages copy them from GlobalQueue to
        // separate TThreadLocalQueue sort it and merge it with LocalQueues
        // TODO(lukyan): Reuse nextEvents
        // NB: Messages from global queue are not sorted
        std::vector<TLoggerQueueItem> nextEvents;
        while (GlobalQueue_.DequeueAll(true, [&] (TLoggerQueueItem& event) {
            if (GetEventInstant(event) < instant) {
                TimeOrderedBuffer_.emplace_back(std::move(event));
            } else {
                nextEvents.push_back(std::move(event));
            }
        }))
        { }

        for (auto& event : nextEvents) {
            GlobalQueue_.Enqueue(std::move(event));
        }

        auto eventsWritten = ProcessTimeOrderedBuffer();

        if (eventsWritten == 0) {
            return;
        }

        WrittenEvents_ += eventsWritten;

        if (!Config_->FlushPeriod || ShutdownRequested_) {
            FlushWriters();
            FlushedEvents_ = WrittenEvents_.load();
        }
    }

    int ProcessTimeOrderedBuffer()
    {
        int eventsWritten = 0;
        if (!TraceSuppressionEnabled_) {
            // Fast path.
            while (!TimeOrderedBuffer_.empty()) {
                auto& event = TimeOrderedBuffer_.front();

                ++eventsWritten;

                Visit(event,
                    [&] (TConfigEvent& event) {
                        return UpdateConfig(event);
                    },
                    [&] (const TLogEvent& event) {
                        WriteEvent(event);
                    });

                TimeOrderedBuffer_.pop_front();
            }

            return eventsWritten;
        }

        SuppressedTraceIdSet_.Update(SuppressedTraceIdQueue_.DequeueAll());

        auto deadline = GetCpuInstant() - DurationToCpuDuration(Config_->TraceSuppressionTimeout);

        int suppressed = 0;
        while (!TimeOrderedBuffer_.empty()) {
            auto& event = TimeOrderedBuffer_.front();

            if (GetEventInstant(event) > deadline) {
                break;
            }

            ++eventsWritten;

            Visit(event,
                [&] (TConfigEvent& event) {
                    return UpdateConfig(event);
                },
                [&] (const TLogEvent& event) {
                    if (SuppressedTraceIdSet_.Contains(event.TraceId)) {
                        ++suppressed;
                    } else {
                        WriteEvent(event);
                    }
                });

            TimeOrderedBuffer_.pop_front();
        }

        SuppressedEvents_ += suppressed;

        return eventsWritten;
    }

    void DoUpdateCategory(TLoggingCategory* category)
    {
        auto level = ELogLevel::Maximum;
        for (const auto& rule : Config_->Rules) {
            if (rule->IsApplicable(category->Name, ELogMessageFormat::PlainText)) {
                level = std::min(level, rule->MinLevel);
            }
        }

        category->MinLevel.store(level, std::memory_order_relaxed);
        category->CurrentVersion.store(GetVersion(), std::memory_order_relaxed);
    }

    void DoUpdatePosition(TLoggingPosition* position, TStringBuf message)
    {
        bool positionEnabled = true;
        for (const auto& prefix : Config_->SuppressedMessages) {
            if (message.StartsWith(prefix)) {
                positionEnabled = false;
                break;
            }
        }

        position->Enabled.store(positionEnabled, std::memory_order_relaxed);
        position->CurrentVersion.store(GetVersion(), std::memory_order_relaxed);
    }

    TMonotonicCounter* GetCategoryEventsCounter(const TString& category)
    {
        auto it = CategoryToEvents_.find(category);
        if (it == CategoryToEvents_.end()) {
            auto tagId = TProfileManager::Get()->RegisterTag("category", category);
            TMonotonicCounter counter("/log_events_enqueued", {tagId});
            it = CategoryToEvents_.insert({category, counter}).first;
        }
        return &it->second;
    }

private:
    const std::shared_ptr<TEventCount> EventCount_ = std::make_shared<TEventCount>();
    const TInvokerQueuePtr EventQueue_;

    const TIntrusivePtr<TThread> LoggingThread_;
    DECLARE_THREAD_AFFINITY_SLOT(LoggingThread);

    TEnqueuedAction CurrentAction_;

    // Configuration.
    TForkAwareSpinLock SpinLock_;
    // Version forces this very module's Logger object to update to our own
    // default configuration (default level etc.).
    std::atomic<int> Version_ = {0};
    std::atomic<bool> AbortOnAlert_ = {0};
    TLogManagerConfigPtr Config_;
    THashMap<const char*, std::unique_ptr<TLoggingCategory>> NameToCategory_;
    const TLoggingCategory* SystemCategory_;

    // These are just copies from _Config.
    // The values are being read from arbitrary threads but stale values are fine.
    int HighBacklogWatermark_ = -1;
    int LowBacklogWatermark_ = -1;

    bool Suspended_ = false;
    std::once_flag Started_;
    std::atomic_flag ScheduledOutOfBand_ = {false};

    THashSet<TThreadLocalQueue*> LocalQueues_;
    TMultipleProducerSingleConsumerLockFreeStack<TThreadLocalQueue*> RegisteredLocalQueues_;
    TMultipleProducerSingleConsumerLockFreeStack<TThreadLocalQueue*> UnregisteredLocalQueues_;

    TMultipleProducerSingleConsumerLockFreeStack<TLoggerQueueItem> GlobalQueue_;
    TMultipleProducerSingleConsumerLockFreeStack<TTraceId> SuppressedTraceIdQueue_;

    std::deque<TLoggerQueueItem> TimeOrderedBuffer_;
    TExpiringSet<TTraceId> SuppressedTraceIdSet_;

    THashMap<TString, TMonotonicCounter> CategoryToEvents_;

    std::atomic<ui64> EnqueuedEvents_ = {0};
    std::atomic<ui64> WrittenEvents_ = {0};
    std::atomic<ui64> FlushedEvents_ = {0};
    std::atomic<ui64> SuppressedEvents_ = {0};
    std::atomic<ui64> DroppedEvents_ = {0};

    THashMap<TString, ILogWriterPtr> Writers_;
    THashMap<TLogWritersCacheKey, std::vector<ILogWriterPtr>> CachedWriters_;
    std::vector<ILogWriterPtr> SystemWriters_;

    std::atomic<bool> ReopenRequested_ = {false};
    std::atomic<bool> ShutdownRequested_ = {false};
    std::atomic<bool> TraceSuppressionEnabled_ = {false};

    TPeriodicExecutorPtr FlushExecutor_;
    TPeriodicExecutorPtr WatchExecutor_;
    TPeriodicExecutorPtr CheckSpaceExecutor_;
    TPeriodicExecutorPtr ProfilingExecutor_;
    TPeriodicExecutorPtr DiskProfilingExecutor_;
    TPeriodicExecutorPtr DequeueExecutor_;

    std::unique_ptr<TNotificationHandle> NotificationHandle_;
    std::vector<std::unique_ptr<TNotificationWatch>> NotificationWatches_;
    THashMap<int, TNotificationWatch*> NotificationWatchesIndex_;
};

/////////////////////////////////////////////////////////////////////////////

struct TLocalQueueReclaimer
{
    ~TLocalQueueReclaimer()
    {
        if (PerThreadQueue) {
            auto logManager = TLogManager::Get()->Impl_;
            logManager->UnregisteredLocalQueues_.Enqueue(PerThreadQueue);
            PerThreadQueue = reinterpret_cast<TThreadLocalQueue*>(ThreadQueueDestroyedSentinel);
        }
    }
};

static thread_local TLocalQueueReclaimer LocalQueueReclaimer;

////////////////////////////////////////////////////////////////////////////////

TLogManager::TLogManager()
    : Impl_(New<TImpl>())
{ }

TLogManager::~TLogManager() = default;

TLogManager* TLogManager::Get()
{
    return LeakySingleton<TLogManager>();
}

void TLogManager::StaticShutdown()
{
    Get()->Shutdown();
}

void TLogManager::Configure(TLogManagerConfigPtr config)
{
    Impl_->Configure(std::move(config));
}

void TLogManager::ConfigureFromEnv()
{
    Impl_->ConfigureFromEnv();
}

void TLogManager::Shutdown()
{
    Impl_->Shutdown();
}

int TLogManager::GetVersion() const
{
    return Impl_->GetVersion();
}

bool TLogManager::GetAbortOnAlert() const
{
    return Impl_->GetAbortOnAlert();
}

const TLoggingCategory* TLogManager::GetCategory(const char* categoryName)
{
    return Impl_->GetCategory(categoryName);
}

void TLogManager::UpdateCategory(TLoggingCategory* category)
{
    Impl_->UpdateCategory(category);
}

void TLogManager::UpdatePosition(TLoggingPosition* position, TStringBuf message)
{
    Impl_->UpdatePosition(position, message);
}

void TLogManager::Enqueue(TLogEvent&& event)
{
    Impl_->Enqueue(std::move(event));
}

void TLogManager::Reopen()
{
    Impl_->Reopen();
}

void TLogManager::SuppressTrace(TTraceId traceId)
{
    Impl_->SuppressTrace(traceId);
}

void TLogManager::Synchronize(TInstant deadline)
{
    Impl_->Synchronize(deadline);
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_SHUTDOWN_CALLBACK(5, TLogManager::StaticShutdown);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
