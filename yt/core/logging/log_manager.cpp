#include "stdafx.h"
#include "log_manager.h"
#include "log.h"
#include "writer.h"
#include "config.h"
#include "private.h"

#include <core/misc/property.h>
#include <core/misc/pattern_formatter.h>
#include <core/misc/raw_formatter.h>
#include <core/misc/hash.h>
#include <core/misc/lock_free.h>
#include <core/misc/variant.h>
#include <core/misc/singleton.h>

#include <core/concurrency/periodic_executor.h>
#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/scheduler_thread.h>
#include <core/concurrency/fork_aware_spinlock.h>

#include <core/ytree/ypath_client.h>
#include <core/ytree/ypath_service.h>
#include <core/ytree/yson_serializable.h>

#include <core/profiling/profiler.h>
#include <core/profiling/timing.h>

#include <util/system/defaults.h>
#include <util/system/sigset.h>
#include <util/system/yield.h>

#include <atomic>

#ifdef _win_
    #include <io.h>
#else
    #include <unistd.h>
#endif

#ifdef _linux_
    #include <sys/inotify.h>
#endif

namespace NYT {
namespace NLogging {

using namespace NYTree;
using namespace NConcurrency;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static TLogger Logger(SystemLoggingCategory);
static const auto& Profiler = LoggingProfiler;

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
        YCHECK(FD_ >= 0);
#endif
    }

    ~TNotificationHandle()
    {
#ifdef _linux_
        YCHECK(FD_ >= 0);
        ::close(FD_);
#endif
    }

    int Poll()
    {
#ifdef _linux_
        YCHECK(FD_ >= 0);

        char buffer[sizeof(struct inotify_event) + NAME_MAX + 1];
        auto rv = ::read(FD_, buffer, sizeof(buffer));

        if (rv < 0) {
            if (errno != EAGAIN) {
                LOG_ERROR(
                    TError::FromSystem(errno),
                    "Unable to poll inotify() descriptor %v",
                    FD_);
            }
        } else if (rv > 0) {
            YASSERT(rv >= sizeof(struct inotify_event));
            struct inotify_event* event = (struct inotify_event*)buffer;

            if (event->mask & IN_ATTRIB) {
                LOG_TRACE(
                    "Watch %v has triggered metadata change (IN_ATTRIB)",
                    event->wd);
            }
            if (event->mask & IN_DELETE_SELF) {
                LOG_TRACE(
                    "Watch %v has triggered a deletion (IN_DELETE_SELF)",
                    event->wd);
            }
            if (event->mask & IN_MOVE_SELF) {
                LOG_TRACE(
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
        const Stroka& path,
        TClosure callback)
        : FD_(handle->GetFD())
        , WD_(-1)
        , Path_(path)
        , Callback_(std::move(callback))

    {
        FD_ = handle->GetFD();
        YCHECK(FD_ >= 0);

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
        YCHECK(WD_ <= 0);
#ifdef _linux_
        WD_ = inotify_add_watch(
            FD_,
            Path_.c_str(),
            IN_ATTRIB | IN_DELETE_SELF | IN_MOVE_SELF);

        if (WD_ < 0) {
            LOG_ERROR(TError::FromSystem(errno), "Error registering watch for %v",
                Path_);
            WD_ = -1;
        } else if (WD_ > 0) {
            LOG_TRACE("Registered watch %v for %v",
                WD_,
                Path_);
        } else {
            YUNREACHABLE();
        }
#else
        WD_ = -1;
#endif
    }

    void DropWatch()
    {
#ifdef _linux_
        if (WD_ > 0) {
            LOG_TRACE("Unregistering watch %v for %v",
                WD_,
                Path_);
            inotify_rm_watch(FD_, WD_);
        }
#endif
        WD_ = -1;
    }

private:
    Stroka Path_;
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

class TLogManager::TImpl
    : public TRefCounted
{
public:
    TImpl()
        : EventQueue_(New<TInvokerQueue>(
            &EventCount_,
            NProfiling::EmptyTagIds,
            false,
            false))
        , LoggingThread_(New<TThread>(this))
        , EnqueuedEventCounter_("/enqueued_events")
        , WrittenEventCounter_("/written_events")
        , BacklogEventCounter_("/backlog_events")
    {
        SystemWriters_.push_back(New<TStderrLogWriter>());
        UpdateConfig(TLogConfig::CreateDefault(), false);

        LoggingThread_->Start();
        EventQueue_->SetThreadId(LoggingThread_->GetId());
    }

    void Configure(INodePtr node, const TYPath& path = "")
    {
        if (LoggingThread_->IsRunning()) {
            auto config = TLogConfig::CreateFromNode(node, path);
            LoggerQueue_.Enqueue(std::move(config));
            EventCount_.NotifyOne();
        }
    }

    void Configure(const Stroka& fileName, const TYPath& path)
    {
        try {
            TIFStream configStream(fileName);
            auto root = ConvertToNode(&configStream);
            auto configNode = GetNodeByYPath(root, path);
            Configure(configNode, path);
        } catch (const std::exception& ex) {
            LOG_ERROR(ex, "Error while configuring logging");
        }
    }

    void Configure(TLogConfigPtr&& config)
    {
        if (LoggingThread_->IsRunning()) {
            LoggerQueue_.Enqueue(std::move(config));
            EventCount_.NotifyOne();
        }
    }

    void Shutdown()
    {
        EventQueue_->Shutdown();
        LoggingThread_->Shutdown();
        FlushWriters();
    }

    /*!
     * In some cases (when configuration is being updated at the same time),
     * the actual version is greater than the version returned by this method.
     */
    int GetVersion() const
    {
        return Version_;
    }

    ELogLevel GetMinLevel(const Stroka& category) const
    {
        TGuard<TForkAwareSpinLock> guard(SpinLock_);

        ELogLevel level = ELogLevel::Maximum;
        for (const auto& rule : Config_->Rules) {
            if (rule->IsApplicable(category)) {
                level = Min(level, rule->MinLevel);
            }
        }
        return level;
    }

    void Enqueue(TLogEvent&& event)
    {
        if (ShutdownRequested_) {
            if (event.Level == ELogLevel::Fatal) {
                // Fatal events should not get out of this call.
                Sleep(TDuration::Max());
            }
            return;
        }

        if (event.Level == ELogLevel::Fatal) {
            ShutdownRequested_ = true;

            // Add fatal message to log and notify event log queue.
            Profiler.Increment(EnqueuedEventCounter_);
            PushLogEvent(std::move(event));

            if (LoggingThread_->GetId() != GetCurrentThreadId()) {
                // Waiting for output of all previous messages.
                // Waiting no more than 1 second to prevent hanging.
                auto now = TInstant::Now();
                auto enqueuedEvents = EnqueuedLogEvents_.load();
                while (enqueuedEvents > DequeuedLogEvents_.load() ||
                    TInstant::Now() - now < Config_->ShutdownGraceTimeout)
                {
                    EventCount_.NotifyOne();
                    SchedYield();
                }
            }

            // Flush everything and die.
            Shutdown();

            // Last-minute information.
            TRawFormatter<1024> formatter;
            formatter.AppendString("*** Fatal error encountered in ");
            formatter.AppendString(event.Function);
            formatter.AppendString(" (");
            formatter.AppendString(event.FileName);
            formatter.AppendString(":");
            formatter.AppendNumber(event.Line);
            formatter.AppendString(") ***\n");
            formatter.AppendString(event.Message.c_str());
            formatter.AppendString("\n*** Aborting ***\n");

            auto unused = ::write(2, formatter.GetData(), formatter.GetBytesWritten());
            (void)unused;

            std::terminate();
        }

        if (!LoggingThread_->IsRunning() || Suspended_) {
            return;
        }

        int backlogSize = Profiler.Increment(BacklogEventCounter_);
        Profiler.Increment(EnqueuedEventCounter_);
        PushLogEvent(std::move(event));
        EventCount_.NotifyOne();

        if (!Suspended_ && backlogSize == Config_->HighBacklogWatermark) {
            LOG_WARNING("Backlog size has exceeded high watermark %v, logging suspended",
                Config_->HighBacklogWatermark);
            Suspended_ = true;
        }
    }

    void Reopen()
    {
        ReopenRequested_ = true;
    }

private:
    using TLoggerQueueItem = TVariant<TLogEvent, TLogConfigPtr>;

    class TThread
        : public TSchedulerThread
    {
    public:
        explicit TThread(TImpl* owner)
            : TSchedulerThread(
                &owner->EventCount_,
                "Logging",
                NProfiling::EmptyTagIds,
                false,
                false)
            , Owner_(owner)
        { }

    private:
        TImpl* Owner_;

        virtual void OnThreadStart() override
        {
#ifdef _unix_
            // Set mask.
            sigset_t ss;
            sigemptyset(&ss);
            sigaddset(&ss, SIGHUP);
            sigprocmask(SIG_UNBLOCK, &ss, NULL);

            // Set handler.
            struct sigaction sa;
            memset(&sa, 0, sizeof(sa));
            sigemptyset(&sa.sa_mask);
            sa.sa_handler = &ReloadSignalHandler;

            YCHECK(sigaction(SIGHUP, &sa, nullptr) == 0);
#endif
        }

        virtual EBeginExecuteResult BeginExecute() override
        {
            return Owner_->BeginExecute();
        }

        virtual void EndExecute() override
        {
            Owner_->EndExecute();
        }

    };


    EBeginExecuteResult BeginExecute()
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        auto result = EventQueue_->BeginExecute(&CurrentAction_);
        if (result != EBeginExecuteResult::QueueEmpty) {
            return result;
        }

        int eventsWritten = 0;
        bool empty = true;

        while (LoggerQueue_.DequeueAll(true, [&] (TLoggerQueueItem& eventOrConfig) {
                if (empty) {
                    EventCount_.CancelWait();
                    empty = false;
                }

                if (const auto* configPtr = eventOrConfig.TryAs<TLogConfigPtr>()) {
                    UpdateConfig(*configPtr);
                } else if (const auto* eventPtr = eventOrConfig.TryAs<TLogEvent>()) {
                    if (ReopenRequested_) {
                        ReopenRequested_ = false;
                        ReloadWriters();
                    }

                    Write(*eventPtr);
                    ++eventsWritten;
                } else {
                    YUNREACHABLE();
                }
            }))
        { }

        int backlogSize = Profiler.Increment(BacklogEventCounter_, -eventsWritten);
        if (Suspended_ && backlogSize < Config_->LowBacklogWatermark) {
            Suspended_ = false;
            LOG_INFO("Backlog size has dropped below low watermark %v, logging resumed",
                Config_->LowBacklogWatermark);
        }

        if (eventsWritten > 0 && !Config_->FlushPeriod) {
            FlushWriters();
        }

        DequeuedLogEvents_ += eventsWritten;

        return empty ? EBeginExecuteResult::QueueEmpty : EBeginExecuteResult::Success;
    }

    void EndExecute()
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        EventQueue_->EndExecute(&CurrentAction_);
    }

    std::vector<ILogWriterPtr> GetWriters(const TLogEvent& event)
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        if (event.Category == SystemLoggingCategory) {
            return SystemWriters_;
        } else {
            std::vector<ILogWriterPtr> writers;

            std::pair<Stroka, ELogLevel> cacheKey(event.Category, event.Level);
            auto it = CachedWriters_.find(cacheKey);
            if (it != CachedWriters_.end())
                return it->second;

            yhash_set<Stroka> writerIds;
            for (const auto& rule : Config_->Rules) {
                if (rule->IsApplicable(event.Category, event.Level)) {
                    writerIds.insert(rule->Writers.begin(), rule->Writers.end());
                }
            }

            for (const Stroka& writerId : writerIds) {
                auto writerIt = Writers_.find(writerId);
                YCHECK(writerIt != Writers_.end());
                writers.push_back(writerIt->second);
            }

            YCHECK(CachedWriters_.insert(std::make_pair(cacheKey, writers)).second);

            return writers;
        }
    }

    void Write(const TLogEvent& event)
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        for (auto& writer : GetWriters(event)) {
            Profiler.Increment(WrittenEventCounter_);
            writer->Write(event);
        }
    }

    std::unique_ptr<TNotificationWatch> CreateNoficiationWatch(ILogWriterPtr writer, const Stroka& fileName)
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

    void UpdateConfig(const TLogConfigPtr& config, bool verifyThreadAffinity = true)
    {
        if (verifyThreadAffinity) {
            VERIFY_THREAD_AFFINITY(LoggingThread);
        }

        FlushWriters();

        {
            TGuard<TForkAwareSpinLock> guard(SpinLock_);

            Writers_.clear();
            CachedWriters_.clear();

            Config_ = config;
        }

        for (const auto& pair : Config_->WriterConfigs) {
            const auto& name = pair.first;
            const auto& config = pair.second;

            ILogWriterPtr writer;
            std::unique_ptr<TNotificationWatch> watch;

            switch (config->Type) {
                case EWriterType::Stdout:
                    writer = New<TStdoutLogWriter>();
                    break;
                case EWriterType::Stderr:
                    writer = New<TStderrLogWriter>();
                    break;
                case EWriterType::File:
                    writer = New<TFileLogWriter>(config->FileName);
                    watch = CreateNoficiationWatch(writer, config->FileName);
                    break;
                default:
                    YUNREACHABLE();
            }

            YCHECK(Writers_.insert(std::make_pair(name, std::move(writer))).second);

            if (watch) {
                if (watch->GetWD() >= 0) {
                    // Watch can fail to initialize if the writer is disabled
                    // e.g. due to the lack of space.
                    YCHECK(NotificationWatchesIndex_.insert(
                        std::make_pair(watch->GetWD(), watch.get())).second);
                }
                NotificationWatches_.emplace_back(std::move(watch));
            }
        }

        Version_++;

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
                TDuration::MilliSeconds(1000));
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
            YCHECK(it != jt);

            auto* watch = it->second;
            watch->Run();

            if (watch->GetWD() != currentWD) {
                NotificationWatchesIndex_.erase(it);
                if (watch->GetWD() >= 0) {
                    // Watch can fail to initialize if the writer is disabled
                    // e.g. due to the lack of space.
                    YCHECK(NotificationWatchesIndex_.insert(
                        std::make_pair(watch->GetWD(), watch)).second);
                }
            }

            previousWD = currentWD;
        }
    }

    void PushLogEvent(TLogEvent&& event)
    {
        EnqueuedLogEvents_++;
        LoggerQueue_.Enqueue(std::move(event));
    }

    TEventCount EventCount_;
    TInvokerQueuePtr EventQueue_;

    TIntrusivePtr<TThread> LoggingThread_;
    DECLARE_THREAD_AFFINITY_SLOT(LoggingThread);

    TEnqueuedAction CurrentAction_;

    // Configuration.
    TForkAwareSpinLock SpinLock_;
    // Version forces this very module's Logger object to update to our own
    // default configuration (default level etc.).
    std::atomic<int> Version_ = {-1};
    TLogConfigPtr Config_;

    NProfiling::TSimpleCounter EnqueuedEventCounter_;
    NProfiling::TSimpleCounter WrittenEventCounter_;
    NProfiling::TSimpleCounter BacklogEventCounter_;

    bool Suspended_ = false;

    TMultipleProducerSingleConsumerLockFreeStack<TLoggerQueueItem> LoggerQueue_;

    std::atomic<ui64> EnqueuedLogEvents_ = {0};
    std::atomic<ui64> DequeuedLogEvents_ = {0};

    yhash_map<Stroka, ILogWriterPtr> Writers_;
    yhash_map<std::pair<Stroka, ELogLevel>, std::vector<ILogWriterPtr>> CachedWriters_;
    std::vector<ILogWriterPtr> SystemWriters_;

    volatile bool ReopenRequested_ = false;
    std::atomic<bool> ShutdownRequested_ = {false};

    TPeriodicExecutorPtr FlushExecutor_;
    TPeriodicExecutorPtr WatchExecutor_;
    TPeriodicExecutorPtr CheckSpaceExecutor_;

    std::unique_ptr<TNotificationHandle> NotificationHandle_;
    std::vector<std::unique_ptr<TNotificationWatch>> NotificationWatches_;
    yhash_map<int, TNotificationWatch*> NotificationWatchesIndex_;
};

////////////////////////////////////////////////////////////////////////////////

TLogManager::TLogManager()
    : Impl_(new TImpl())
{ }

TLogManager::~TLogManager()
{ }

TLogManager* TLogManager::Get()
{
    return TSingletonWithFlag<TLogManager>::Get();
}

void TLogManager::StaticShutdown()
{
    if (TSingletonWithFlag<TLogManager>::WasCreated()) {
        TSingletonWithFlag<TLogManager>::Get()->Shutdown();
    }
}

void TLogManager::Configure(INodePtr node)
{
    Impl_->Configure(node);
}

void TLogManager::Configure(const Stroka& fileName, const TYPath& path)
{
    Impl_->Configure(fileName, path);
}

void TLogManager::Configure(TLogConfigPtr&& config)
{
    Impl_->Configure(std::move(config));
}

void TLogManager::Shutdown()
{
    Impl_->Shutdown();
}

int TLogManager::GetVersion() const
{
    return Impl_->GetVersion();
}

ELogLevel TLogManager::GetMinLevel(const Stroka& category) const
{
    return Impl_->GetMinLevel(category);
}

void TLogManager::Enqueue(TLogEvent&& event)
{
    Impl_->Enqueue(std::move(event));
}

void TLogManager::Reopen()
{
    Impl_->Reopen();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging
} // namespace NYT
