#include "stdafx.h"
#include "log_manager.h"
#include "writer.h"
#include "config.h"
#include "private.h"

#include <core/misc/property.h>
#include <core/misc/pattern_formatter.h>
#include <core/misc/raw_formatter.h>

#include <core/concurrency/periodic_executor.h>
#include <core/concurrency/thread_affinity.h>
#include <core/concurrency/action_queue_detail.h>

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
static NProfiling::TProfiler LoggingProfiler("");

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
                    "Unable to poll inotify() descriptor %d",
                    Fd_);
            }
        } else if (rv > 0) {
            YASSERT(rv >= sizeof(struct inotify_event));
            struct inotify_event* event = (struct inotify_event*)buffer;

            if (event->mask & IN_ATTRIB) {
                LOG_TRACE(
                    "Watch %d has triggered metadata change (IN_ATTRIB)",
                    event->wd);
            }
            if (event->mask & IN_DELETE_SELF) {
                LOG_TRACE(
                    "Watch %d has triggered a deletion (IN_DELETE_SELF)",
                    event->wd);
            }
            if (event->mask & IN_MOVE_SELF) {
                LOG_TRACE(
                    "Watch %d has triggered a movement (IN_MOVE_SELF)",
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
        , Path(path)
        , Callback(std::move(callback))

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
        Callback.Run();
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
            Path.c_str(),
            IN_ATTRIB | IN_DELETE_SELF | IN_MOVE_SELF);

        if (Wd_ < 0) {
            LOG_ERROR(
                TError::FromSystem(errno),
                "Unable to register watch for path %s",
                Path.Quote().c_str());
            Wd_ = -1;
        } else if (Wd_ > 0) {
            LOG_TRACE(
                "Registered watch %d for path %s",
                Wd_,
                Path.Quote().c_str());
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
            LOG_TRACE(
                "Unregistering watch %d for path %s",
                Wd_,
                Path.Quote().c_str());
            inotify_rm_watch(Fd_, Wd_);
        }
#endif
        Wd_ = -1;
    }

private:
    Stroka Path;
    TClosure Callback;
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
        : EventQueue(New<TInvokerQueue>(
            &EventCount,
            NProfiling::EmptyTagIds,
            false,
            false))
        , LoggingThread(New<TThread>(this))
        // Version forces this very module's Logger object to update to our own
        // default configuration (default level etc.).
        , Version(-1)
        , EnqueueCounter("/enqueue_rate")
        , WriteCounter("/write_rate")
        , BacklogCounter("/backlog")
        , Suspended(false)
        , FatalShutdown(false)
        , ReopenEnqueued(false)
    {
        SystemWriters.push_back(New<TStderrLogWriter>());
        DoUpdateConfig(TLogConfig::CreateDefault(), false);
        Writers.insert(std::make_pair(DefaultStderrWriterName, New<TStderrLogWriter>()));

        LoggingThread->Start();
        EventQueue->SetThreadId(LoggingThread->GetId());
    }

    void Configure(INodePtr node, const TYPath& path = "")
    {
        if (LoggingThread->IsRunning()) {
            auto config = TLogConfig::CreateFromNode(node, path);
            ConfigsToUpdate.Enqueue(config);
            EventCount.Notify();
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
        EventQueue->Shutdown();
        LoggingThread->Shutdown();
        FlushWriters();
    }

    /*!
     * In some cases (when configuration is being updated at the same time),
     * the actual version is greater than the version returned by this method.
     */
    int GetVersion()
    {
        return Version;
    }

    ELogLevel GetMinLevel(const Stroka& category) const
    {
        TGuard<TSpinLock> guard(&SpinLock);

        ELogLevel level = ELogLevel::Maximum;
        for (const auto& rule : Config->Rules) {
            if (rule->IsApplicable(category)) {
                level = Min(level, rule->MinLevel);
            }
        }
        return level;
    }

    void Enqueue(TLogEvent&& event)
    {
        if (FatalShutdown) {
            return;
        }

        if (event.Level == ELogLevel::Fatal) {
            FatalShutdown = true;

            // Add fatal message to log and notify event log queue.
            LoggingProfiler.Increment(EnqueueCounter);
            LogEventQueue.Enqueue(event);

            if (LoggingThread->GetId() != GetCurrentThreadId()) {
                // Waiting for release of log queue.
                // Waiting no more than 1 second to prevent hanging.
                auto now = TInstant::Now();
                while (
                    !LogEventQueue.IsEmpty() &&
                    EventQueue->IsRunning() &&
                    TInstant::Now() - now < Config->ShutdownGraceTimeout)
                {
                    EventCount.Notify();
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

        if (!LoggingThread->IsRunning() || Suspended) {
            return;
        }

        int backlogSize = LoggingProfiler.Increment(BacklogCounter);
        LoggingProfiler.Increment(EnqueueCounter);
        LogEventQueue.Enqueue(std::move(event));
        EventCount.Notify();

        if (!Suspended && backlogSize == Config->HighBacklogWatermark) {
            LOG_WARNING("Backlog size has exceeded high watermark %d, logging suspended",
                Config->HighBacklogWatermark);
            Suspended = true;
        }
    }

    void Reopen()
    {
        ReopenEnqueued = true;
    }

private:
    class TThread
        : public TExecutorThread
    {
    public:
        explicit TThread(TImpl* owner)
            : TExecutorThread(
                &owner->EventCount,
                "Logging",
                NProfiling::EmptyTagIds,
                false,
                false)
            , Owner(owner)
        { }

    private:
        TImpl* Owner;

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

            YCHECK(sigaction(SIGHUP, &sa, NULL) == 0);
#endif
        }

        virtual EBeginExecuteResult BeginExecute() override
        {
            return Owner->BeginExecute();
        }

        virtual void EndExecute() override
        {
            Owner->EndExecute();
        }

    };


    EBeginExecuteResult BeginExecute()
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        auto result = EventQueue->BeginExecute(&CurrentAction);
        if (result != EBeginExecuteResult::QueueEmpty) {
            return result;
        }

        bool configsUpdated = false;
        TLogConfigPtr config;
        while (ConfigsToUpdate.Dequeue(&config)) {
            DoUpdateConfig(config);
            configsUpdated = true;
        }

        int eventsWritten = 0;
        TLogEvent event;
        while (LogEventQueue.Dequeue(&event)) {
            // To avoid starvation of config update
            while (ConfigsToUpdate.Dequeue(&config)) {
                DoUpdateConfig(config);
            }

            if (ReopenEnqueued) {
                ReopenEnqueued = false;
                ReloadWriters();
            }

            Write(event);
            ++eventsWritten;
        }

        int backlogSize = LoggingProfiler.Increment(BacklogCounter, -eventsWritten);
        if (Suspended && backlogSize < Config->LowBacklogWatermark) {
            Suspended = false;
            LOG_INFO("Backlog size has dropped below low watermark %d, logging resumed",
                Config->LowBacklogWatermark);
        }

        if (eventsWritten > 0 && !Config->FlushPeriod) {
            FlushWriters();
        }

        if (configsUpdated || eventsWritten > 0) {
            EventCount.CancelWait();
            return EBeginExecuteResult::Success;
        } else {
            return EBeginExecuteResult::QueueEmpty;
        }
    }

    void EndExecute()
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        EventQueue->EndExecute(&CurrentAction);
    }

    ILogWriters GetWriters(const TLogEvent& event)
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        if (event.Category == SystemLoggingCategory) {
            return SystemWriters;
        } else {
            ILogWriters writers;

            std::pair<Stroka, ELogLevel> cacheKey(event.Category, event.Level);
            auto it = CachedWriters.find(cacheKey);
            if (it != CachedWriters.end())
                return it->second;

            yhash_set<Stroka> writerIds;
            for (const auto& rule : Config->Rules) {
                if (rule->IsApplicable(event.Category, event.Level)) {
                    writerIds.insert(rule->Writers.begin(), rule->Writers.end());
                }
            }

            for (const Stroka& writerId : writerIds) {
                auto writerIt = Writers.find(writerId);
                YASSERT(writerIt != Writers.end());
                writers.push_back(writerIt->second);
            }

            YCHECK(CachedWriters.insert(std::make_pair(cacheKey, writers)).second);

            return writers;
        }
    }

    void Write(const TLogEvent& event)
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        for (auto& writer : GetWriters(event)) {
            LoggingProfiler.Increment(WriteCounter);
            writer->Write(event);
        }
    }

    std::unique_ptr<TNotificationWatch> CreateNoficiationWatch(ILogWriterPtr writer, const Stroka& fileName)
    {
#ifdef _linux_
        if (Config->WatchPeriod) {
            if (!NotificationHandle) {
                NotificationHandle.reset(new TNotificationHandle());
            }
            return std::unique_ptr<TNotificationWatch>(
                new TNotificationWatch(
                    ~NotificationHandle,
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
            TGuard<TSpinLock> guard(&SpinLock);

            Writers.clear();
            CachedWriters.clear();

            Config = config;
        }

        for (const auto& pair : Config->WriterConfigs) {
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

            YCHECK(Writers.insert(std::make_pair(name, std::move(writer))).second);

            if (watch) {
                if (watch->GetWd() >= 0) {
                    // Watch can fail to initialize if the writer is disabled
                    // e.g. due to the lack of space.
                    YCHECK(NotificationWatchesIndex.insert(
                        std::make_pair(watch->GetWd(), ~watch)).second);
                }
                NotificationWatches.emplace_back(std::move(watch));
            }
        }

        Version++;

        if (FlushExecutor) {
            FlushExecutor->Stop();
            FlushExecutor.Reset();
        }

        if (WatchExecutor) {
            WatchExecutor->Stop();
            WatchExecutor.Reset();
        }

        auto flushPeriod = Config->FlushPeriod;
        if (flushPeriod) {
            FlushExecutor = New<TPeriodicExecutor>(
                EventQueue,
                BIND(&TImpl::FlushWriters, MakeStrong(this)),
                *flushPeriod);
            FlushExecutor->Start();
        }

        auto watchPeriod = Config->WatchPeriod;
        if (watchPeriod) {
            WatchExecutor = New<TPeriodicExecutor>(
                EventQueue,
                BIND(&TImpl::WatchWriters, MakeStrong(this)),
                TDuration::MilliSeconds(1000));
            WatchExecutor->Start();
        }

        auto checkSpacePeriod = Config->CheckSpacePeriod;
        if (checkSpacePeriod) {
            CheckSpaceExecutor = New<TPeriodicExecutor>(
                EventQueue,
                BIND(&TImpl::CheckSpace, MakeStrong(this)),
                *checkSpacePeriod);
            CheckSpaceExecutor->Start();
        }
    }

    void FlushWriters()
    {
        for (auto& pair : Writers) {
            pair.second->Flush();
        }
    }

    void ReloadWriters()
    {
        Version++;
        for (auto& pair : Writers) {
            pair.second->Reload();
        }
    }

    void CheckSpace()
    {
        for (auto& pair : Writers) {
            pair.second->CheckSpace(Config->MinDiskSpace);
        }
    }

    void WatchWriters()
    {
        VERIFY_THREAD_AFFINITY(LoggingThread);

        if (!NotificationHandle)
            return;

        int previousWd = -1, currentWd = -1;
        while ((currentWd = NotificationHandle->Poll()) > 0) {
            if (currentWd == previousWd) {
                continue;
            }
            auto&& it = NotificationWatchesIndex.find(currentWd);
            auto&& jt = NotificationWatchesIndex.end();
            YCHECK(it != jt);

            auto* watch = it->second;
            watch->Run();

            if (watch->GetWd() != currentWd) {
                NotificationWatchesIndex.erase(it);
                if (watch->GetWd() >= 0) {
                    // Watch can fail to initialize if the writer is disabled
                    // e.g. due to the lack of space.
                    YCHECK(NotificationWatchesIndex.insert(
                        std::make_pair(watch->GetWd(), watch)).second);
                }
            }

            previousWd = currentWd;
        }
    }


    TEventCount EventCount;
    TInvokerQueuePtr EventQueue;

    TIntrusivePtr<TThread> LoggingThread;
    DECLARE_THREAD_AFFINITY_SLOT(LoggingThread);

    TEnqueuedAction CurrentAction;

    // Configuration.
    std::atomic<int> Version;

    TLogConfigPtr Config;
    NProfiling::TRateCounter EnqueueCounter;
    NProfiling::TRateCounter WriteCounter;
    NProfiling::TAggregateCounter BacklogCounter;
    bool Suspended;
    TSpinLock SpinLock;

    std::atomic<bool> FatalShutdown;

    TLockFreeQueue<TLogConfigPtr> ConfigsToUpdate;
    TLockFreeQueue<TLogEvent> LogEventQueue;

    yhash_map<Stroka, ILogWriterPtr> Writers;
    ymap<std::pair<Stroka, ELogLevel>, ILogWriters> CachedWriters;
    ILogWriters SystemWriters;

    volatile bool ReopenEnqueued;

    TPeriodicExecutorPtr FlushExecutor;
    TPeriodicExecutorPtr WatchExecutor;
    TPeriodicExecutorPtr CheckSpaceExecutor;

    std::unique_ptr<TNotificationHandle> NotificationHandle;
    std::vector<std::unique_ptr<TNotificationWatch>> NotificationWatches;
    yhash_map<int, TNotificationWatch*> NotificationWatchesIndex;
};

////////////////////////////////////////////////////////////////////////////////

TLogManager::TLogManager()
    : Impl(new TImpl())
{ }

TLogManager* TLogManager::Get()
{
    return Singleton<TLogManager>();
}

void TLogManager::Configure(INodePtr node)
{
    Impl->Configure(node);
}

void TLogManager::Configure(const Stroka& fileName, const TYPath& path)
{
    Impl->Configure(fileName, path);
}

void TLogManager::Shutdown()
{
    Impl->Shutdown();
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
