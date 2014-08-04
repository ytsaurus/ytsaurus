#include "stdafx.h"
#include "log_manager.h"
#include "writer.h"
#include "config.h"
#include "private.h"

#include <core/misc/property.h>
#include <core/misc/pattern_formatter.h>
#include <core/misc/raw_formatter.h>
#include <core/misc/hash.h>
#include <core/misc/singleton.h>

#include <core/concurrency/periodic_executor.h>
#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/action_queue_detail.h>
#include <core/concurrency/fork_aware_spinlock.h>

#include <core/ytree/ypath_client.h>
#include <core/ytree/ypath_service.h>
#include <core/ytree/yson_serializable.h>

#include <core/profiling/profiler.h>

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
namespace NLog {

using namespace NYTree;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static TLogger Logger(SystemLoggingCategory);
static NProfiling::TProfiler LoggingProfiler("/logging");

////////////////////////////////////////////////////////////////////////////////

class TNotificationHandle
    : private TNonCopyable
{
public:
    TNotificationHandle()
        : Fd_(-1)
    {
#ifdef _linux_
        Fd_ = inotify_init1(IN_NONBLOCK | IN_CLOEXEC);
        YCHECK(Fd_ >= 0);
#endif
    }

    ~TNotificationHandle()
    {
#ifdef _linux_
        YCHECK(Fd_ >= 0);
        ::close(Fd_);
#endif
    }

    int Poll()
    {
#ifdef _linux_
        YCHECK(Fd_ >= 0);

        char buffer[sizeof(struct inotify_event) + NAME_MAX + 1];
        auto rv = ::read(Fd_, buffer, sizeof(buffer));

        if (rv < 0) {
            if (errno != EAGAIN) {
                LOG_ERROR(
                    TError::FromSystem(errno),
                    "Unable to poll inotify() descriptor %v",
                    Fd_);
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

    DEFINE_BYVAL_RO_PROPERTY(int, Fd);
};

class TNotificationWatch
    : private TNonCopyable
{
public:
    TNotificationWatch(
        TNotificationHandle* handle,
        const Stroka& path,
        TClosure callback)
        : Fd_(handle->GetFd())
        , Wd_(-1)
        , Path_(path)
        , Callback_(std::move(callback))

    {
        Fd_ = handle->GetFd();
        YCHECK(Fd_ >= 0);

        CreateWatch();
    }

    ~TNotificationWatch()
    {
        DropWatch();
    }

    DEFINE_BYVAL_RO_PROPERTY(int, Fd);
    DEFINE_BYVAL_RO_PROPERTY(int, Wd);

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
        YCHECK(Wd_ <= 0);
#ifdef _linux_
        Wd_ = inotify_add_watch(
            Fd_,
            Path_.c_str(),
            IN_ATTRIB | IN_DELETE_SELF | IN_MOVE_SELF);

        if (Wd_ < 0) {
            LOG_ERROR(TError::FromSystem(errno), "Error registering watch for %v",
                Path_);
            Wd_ = -1;
        } else if (Wd_ > 0) {
            LOG_TRACE("Registered watch %v for %v",
                Wd_,
                Path_);
        } else {
            YUNREACHABLE();
        }
#else
        Wd_ = -1;
#endif
    }

    void DropWatch()
    {
#ifdef _linux_
        if (Wd_ > 0) {
            LOG_TRACE("Unregistering watch %v for %v",
                Wd_,
                Path_);
            inotify_rm_watch(Fd_, Wd_);
        }
#endif
        Wd_ = -1;
    }

private:
    Stroka Path_;
    TClosure Callback_;

};

////////////////////////////////////////////////////////////////////////////////

namespace {

void ReloadSignalHandler(int signal)
{
    NLog::TLogManager::Get()->Reopen();
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
        // Version forces this very module's Logger object to update to our own
        // default configuration (default level etc.).
        , Version_(-1)
        , EnqueueCounter_("/enqueue_rate")
        , WriteCounter_("/write_rate")
        , BacklogCounter_("/backlog")
        , Suspended_(false)
        , ReopenRequested_(false)
        , ShutdownRequested_(false)
    {
        SystemWriters_.push_back(New<TStderrLogWriter>());
        DoUpdateConfig(TLogConfig::CreateDefault(), false);
        Writers_.insert(std::make_pair(DefaultStderrWriterName, New<TStderrLogWriter>()));

        LoggingThread_->Start();
        EventQueue_->SetThreadId(LoggingThread_->GetId());
    }

    void Configure(INodePtr node, const TYPath& path = "")
    {
        if (LoggingThread_->IsRunning()) {
            auto config = TLogConfig::CreateFromNode(node, path);
            ConfigsToUpdate_.Enqueue(config);
            EventCount_.Notify();
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
            return;
        }

        if (event.Level == ELogLevel::Fatal) {
            ShutdownRequested_ = true;

            // Add fatal message to log and notify event log queue.
            LoggingProfiler.Increment(EnqueueCounter_);
            LogEventQueue_.Enqueue(event);

            // Waiting for release log queue
            while (!LogEventQueue_.IsEmpty() && EventQueue_->IsRunning()) {
                EventCount_.Notify();
                SchedYield();
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

        int backlogSize = LoggingProfiler.Increment(BacklogCounter_);
        LoggingProfiler.Increment(EnqueueCounter_);
        LogEventQueue_.Enqueue(std::move(event));
        EventCount_.Notify();

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

        bool configsUpdated = false;
        TLogConfigPtr config;
        while (ConfigsToUpdate_.Dequeue(&config)) {
            DoUpdateConfig(config);
            configsUpdated = true;
        }

        int eventsWritten = 0;
        TLogEvent event;
        while (LogEventQueue_.Dequeue(&event)) {
            // To avoid starvation of config update
            while (ConfigsToUpdate_.Dequeue(&config)) {
                DoUpdateConfig(config);
            }

            if (ReopenRequested_) {
                ReopenRequested_ = false;
                ReloadWriters();
            }

            Write(event);
            ++eventsWritten;
        }

        int backlogSize = LoggingProfiler.Increment(BacklogCounter_, -eventsWritten);
        if (Suspended_ && backlogSize < Config_->LowBacklogWatermark) {
            Suspended_ = false;
            LOG_INFO("Backlog size has dropped below low watermark %v, logging resumed",
                Config_->LowBacklogWatermark);
        }

        if (eventsWritten > 0 && !Config_->FlushPeriod) {
            FlushWriters();
        }

        if (configsUpdated || eventsWritten > 0) {
            EventCount_.CancelWait();
            return EBeginExecuteResult::Success;
        } else {
            return EBeginExecuteResult::QueueEmpty;
        }
    }

    void EndExecute()
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        EventQueue_->EndExecute(&CurrentAction_);
    }

    ILogWriters GetWriters(const TLogEvent& event)
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        if (event.Category == SystemLoggingCategory) {
            return SystemWriters_;
        } else {
            ILogWriters writers;

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
                YASSERT(writerIt != Writers_.end());
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
            LoggingProfiler.Increment(WriteCounter_);
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

    void DoUpdateConfig(TLogConfigPtr config, bool verifyThreadAffinity = true)
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
                if (watch->GetWd() >= 0) {
                    // Watch can fail to initialize if the writer is disabled
                    // e.g. due to the lack of space.
                    YCHECK(NotificationWatchesIndex_.insert(
                        std::make_pair(watch->GetWd(), watch.get())).second);
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

        int previousWd = -1, currentWd = -1;
        while ((currentWd = NotificationHandle_->Poll()) > 0) {
            if (currentWd == previousWd) {
                continue;
            }
            auto&& it = NotificationWatchesIndex_.find(currentWd);
            auto&& jt = NotificationWatchesIndex_.end();
            YCHECK(it != jt);

            auto* watch = it->second;
            watch->Run();

            if (watch->GetWd() != currentWd) {
                NotificationWatchesIndex_.erase(it);
                if (watch->GetWd() >= 0) {
                    // Watch can fail to initialize if the writer is disabled
                    // e.g. due to the lack of space.
                    YCHECK(NotificationWatchesIndex_.insert(
                        std::make_pair(watch->GetWd(), watch)).second);
                }
            }

            previousWd = currentWd;
        }
    }


    TEventCount EventCount_;
    TInvokerQueuePtr EventQueue_;

    TIntrusivePtr<TThread> LoggingThread_;
    DECLARE_THREAD_AFFINITY_SLOT(LoggingThread);

    TEnqueuedAction CurrentAction_;

    // Configuration.
    TForkAwareSpinLock SpinLock_;
    std::atomic<int> Version_;
    TLogConfigPtr Config_;
    NProfiling::TRateCounter EnqueueCounter_;
    NProfiling::TRateCounter WriteCounter_;
    NProfiling::TAggregateCounter BacklogCounter_;
    bool Suspended_;

    TLockFreeQueue<TLogConfigPtr> ConfigsToUpdate_;
    TLockFreeQueue<TLogEvent> LogEventQueue_;

    yhash_map<Stroka, ILogWriterPtr> Writers_;
    yhash_map<std::pair<Stroka, ELogLevel>, ILogWriters> CachedWriters_;
    ILogWriters SystemWriters_;

    volatile bool ReopenRequested_;
    std::atomic<bool> ShutdownRequested_;

    TPeriodicExecutorPtr FlushExecutor_;
    TPeriodicExecutorPtr WatchExecutor_;
    TPeriodicExecutorPtr CheckSpaceExecutor_;

    std::unique_ptr<TNotificationHandle> NotificationHandle_;
    std::vector<std::unique_ptr<TNotificationWatch>> NotificationWatches_;
    yhash_map<int, TNotificationWatch*> NotificationWatchesIndex_;

};

////////////////////////////////////////////////////////////////////////////////

TLogManager::TLogManager()
    : Impl(new TImpl())
{ }

TLogManager::~TLogManager()
{
    Impl->Shutdown();
}

TLogManager* TLogManager::Get()
{
    return TSingleton::Get();
}

void TLogManager::Shutdown()
{
    auto instance = TSingleton::TryGet();
    if (instance) {
        instance->Impl->Shutdown();
    }
}

void TLogManager::Configure(INodePtr node)
{
    Impl->Configure(node);
}

void TLogManager::Configure(const Stroka& fileName, const TYPath& path)
{
    Impl->Configure(fileName, path);
}

int TLogManager::GetVersion() const
{
    return Impl->GetVersion();
}

ELogLevel TLogManager::GetMinLevel(const Stroka& category) const
{
    return Impl->GetMinLevel(category);
}

void TLogManager::Enqueue(TLogEvent&& event)
{
    Impl->Enqueue(std::move(event));
}

void TLogManager::Reopen()
{
    Impl->Reopen();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
