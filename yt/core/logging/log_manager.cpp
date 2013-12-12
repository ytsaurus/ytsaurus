#include "stdafx.h"
#include "log_manager.h"
#include "writer.h"

#include <core/misc/property.h>
#include <core/misc/pattern_formatter.h>
#include <core/misc/raw_formatter.h>
#include <core/concurrency/periodic_executor.h>

#include <core/concurrency/action_queue_detail.h>

#include <core/ytree/ypath_client.h>
#include <core/ytree/ypath_service.h>
#include <core/ytree/yson_serializable.h>

#include <core/profiling/profiler.h>

#include <util/system/defaults.h>
#include <util/system/sigset.h>

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

// TODO: review this and that
static const char* const SystemPattern = "$(datetime) $(level) $(category) $(message)";

static const char* const DefaultStdErrWriterName = "StdErr";
static const ELogLevel DefaultStdErrMinLevel= ELogLevel::Info;
static const char* const DefaultStdErrPattern = "$(datetime) $(level) $(category) $(message)";

static const char* const AllCategoriesName = "*";

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

struct TRule
    : public TYsonSerializable
{
    bool IncludeAllCategories;
    yhash_set<Stroka> IncludeCategories;
    yhash_set<Stroka> ExcludeCategories;

    ELogLevel MinLevel;
    ELogLevel MaxLevel;

    std::vector<Stroka> Writers;

    TRule()
        : IncludeAllCategories(false)
    {
        // TODO(babenko): rename to include_categories
        RegisterParameter("categories", IncludeCategories)
            .NonEmpty();
        RegisterParameter("exclude_categories", ExcludeCategories)
            .Default(yhash_set<Stroka>());
        RegisterParameter("min_level", MinLevel)
            .Default(ELogLevel::Minimum);
        RegisterParameter("max_level", MaxLevel)
            .Default(ELogLevel::Maximum);
        RegisterParameter("writers", Writers)
            .NonEmpty();
    }

    virtual void OnLoaded() override
    {
        if (IncludeCategories.size() == 1 && *IncludeCategories.begin() == AllCategoriesName) {
            IncludeAllCategories = true;
        }
    }

    bool IsApplicable(const Stroka& category) const
    {
        if (!IncludeAllCategories && IncludeCategories.find(category) == IncludeCategories.end()) {
            // No match in include_categories.
            return false;
        }

        if (ExcludeCategories.find(category) != ExcludeCategories.end()) {
            // Match in exclude_categories.
            return false;
        }

        return true;
    }

    bool IsApplicable(const Stroka& category, ELogLevel level) const
    {
        return
            MinLevel <= level && level <= MaxLevel &&
            IsApplicable(category);
    }

};

typedef TIntrusivePtr<TRule> TRulePtr;

////////////////////////////////////////////////////////////////////////////////

typedef std::vector<ILogWriterPtr> TLogWriters;

class TLogConfig;
typedef TIntrusivePtr<TLogConfig> TLogConfigPtr;

////////////////////////////////////////////////////////////////////////////////

class TLogConfig
    : public TYsonSerializable
{
public:
    /*!
     * Needs to be public for TYsonSerializable.
     * Not for public use.
     * Use #CreateDefault instead.
     */
    TLogConfig()
        : Version(0)
    {
        RegisterParameter("flush_period", FlushPeriod)
            .Default(Null);
        RegisterParameter("watch_period", WatchPeriod)
            .Default(Null);
        RegisterParameter("check_space_period", CheckSpacePeriod)
            .Default(Null);
        RegisterParameter("min_disk_space", MinDiskSpace)
            .GreaterThanOrEqual((i64) 1024 * 1024 * 1024)
            .Default((i64) 5 * 1024 * 1024 * 1024);
        RegisterParameter("high_backlog_watermark", HighBacklogWatermark)
            .GreaterThan(0)
            .Default(1000000);
        RegisterParameter("low_backlog_watermark", LowBacklogWatermark)
            .GreaterThan(0)
            .Default(100000);

        RegisterParameter("writers", WriterConfigs);
        RegisterParameter("rules", Rules);

        RegisterValidator([&] () {
            for (const auto& rule : Rules) {
                for (const Stroka& writer : rule->Writers) {
                    if (WriterConfigs.find(writer) == WriterConfigs.end()) {
                        THROW_ERROR_EXCEPTION("Unknown writer: %s", ~writer.Quote());
                    }
                }
            }
        });
    }

    TLogWriters GetWriters(const TLogEvent& event)
    {
        // Place a return value on top to promote RVO.
        TLogWriters writers;
        std::pair<Stroka, ELogLevel> cacheKey(event.Category, event.Level);
        auto it = CachedWriters.find(cacheKey);
        if (it != CachedWriters.end())
            return it->second;

        yhash_set<Stroka> writerIds;
        for (auto& rule : Rules) {
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

    ELogLevel GetMinLevel(const Stroka& category) const
    {
        ELogLevel level = ELogLevel::Maximum;
        for (const auto& rule : Rules) {
            if (rule->IsApplicable(category)) {
                level = Min(level, rule->MinLevel);
            }
        }
        return level;
    }

    void CheckSpace()
    {
        for (auto& pair : Writers) {
            pair.second->CheckSpace(MinDiskSpace);
        }
    }

    void FlushWriters()
    {
        for (auto& pair : Writers) {
            pair.second->Flush();
        }
    }

    void WatchWriters()
    {
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

    void ReloadWriters()
    {
        AtomicIncrement(Version);
        for (auto& pair : Writers) {
            pair.second->Reload();
        }
    }

    static TLogConfigPtr CreateDefault()
    {
        auto config = New<TLogConfig>();

        config->Writers.insert(std::make_pair(
            DefaultStdErrWriterName,
            New<TStdErrLogWriter>(DefaultStdErrPattern)));

        auto rule = New<TRule>();

        rule->IncludeAllCategories = true;
        rule->MinLevel = DefaultStdErrMinLevel;
        rule->Writers.push_back(DefaultStdErrWriterName);

        config->Rules.push_back(rule);

        return config;
    }

    static TLogConfigPtr CreateFromNode(INodePtr node, const TYPath& path = "")
    {
        auto config = New<TLogConfig>();
        config->Load(node, true, true, path);
        config->CreateWriters();
        return config;
    }

    int GetVersion() const
    {
        return Version;
    }

    TNullable<TDuration> GetFlushPeriod() const
    {
        return FlushPeriod;
    }

    TNullable<TDuration> GetWatchPeriod() const
    {
        return WatchPeriod;
    }

    TNullable<TDuration> GetCheckSpacePeriod() const
    {
        return CheckSpacePeriod;
    }

    int GetHighBacklogWatermark() const
    {
        return HighBacklogWatermark;
    }

    int GetLowBacklogWatermark() const
    {
        return LowBacklogWatermark;
    }

private:
    std::unique_ptr<TNotificationWatch> CreateNoficiationWatch(ILogWriterPtr writer, const Stroka& fileName)
    {
#ifdef _linux_
        if (WatchPeriod) {
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

    void CreateWriters()
    {
        for (const auto& pair : WriterConfigs) {
            const auto& name = pair.first;
            const auto& config = pair.second;
            const auto& pattern = config->Pattern;

            ILogWriterPtr writer;
            std::unique_ptr<TNotificationWatch> watch;

            switch (config->Type) {
                case ILogWriter::EType::StdOut:
                    writer = New<TStdOutLogWriter>(pattern);
                    break;

                case ILogWriter::EType::StdErr:
                    writer = New<TStdErrLogWriter>(pattern);
                    break;

                case ILogWriter::EType::File:
                    writer = New<TFileLogWriter>(config->FileName, pattern);
                    watch = CreateNoficiationWatch(writer, config->FileName);
                    break;

                case ILogWriter::EType::Raw:
                    writer = New<TRawFileLogWriter>(config->FileName);
                    watch = CreateNoficiationWatch (writer, config->FileName);
                    break;
                default:
                    YUNREACHABLE();
            }

            if (writer) {
                YCHECK(Writers.insert(
                    std::make_pair(name, std::move(writer))).second);
            }

            if (watch) {
                if (watch->GetWd() >= 0) {
                    // Watch can fail to initialize if the writer is disabled
                    // e.g. due to the lack of space.
                    YCHECK(NotificationWatchesIndex.insert(
                        std::make_pair(watch->GetWd(), ~watch)).second);
                }
                NotificationWatches.emplace_back(std::move(watch));
            }

            AtomicIncrement(Version);
        }
    }

    TAtomic Version;

    TNullable<TDuration> FlushPeriod;
    TNullable<TDuration> WatchPeriod;
    TNullable<TDuration> CheckSpacePeriod;

    i64 MinDiskSpace;

    int HighBacklogWatermark;
    int LowBacklogWatermark;

    std::vector<TRulePtr> Rules;
    yhash_map<Stroka, ILogWriter::TConfigPtr> WriterConfigs;

    yhash_map<Stroka, ILogWriterPtr> Writers;
    ymap<std::pair<Stroka, ELogLevel>, TLogWriters> CachedWriters;

    std::unique_ptr<TNotificationHandle> NotificationHandle;
    std::vector<std::unique_ptr<TNotificationWatch>> NotificationWatches;
    std::map<int, TNotificationWatch*> NotificationWatchesIndex;
};

////////////////////////////////////////////////////////////////////////////////

namespace {

void ReloadSignalHandler(int signal)
{
    NLog::TLogManager::Get()->Reopen();
}

} // namespace

class TLogManager::TImpl
    : public TRefCounted
{
public:
    TImpl()
        : WasShutdown(false)
        , Queue(New<TInvokerQueue>(
            &EventCount,
            NProfiling::EmptyTagIds,
            false,
            false))
        , Thread(New<TThread>(this))
        // Version forces this very module's Logger object to update to our own
        // default configuration (default level etc.).
        , Version(-1)
        , EnqueueCounter("/enqueue_rate")
        , WriteCounter("/write_rate")
        , BacklogCounter("/backlog")
        , Suspended(false)
        , ReopenEnqueued(false)
    {
        SystemWriters.push_back(New<TStdErrLogWriter>(SystemPattern));
        DoUpdateConfig(TLogConfig::CreateDefault());

        Thread->Start();
        Queue->SetThreadId(Thread->GetId());
    }

    void Configure(INodePtr node, const TYPath& path = "")
    {
        if (Thread->IsRunning()) {
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
        WasShutdown = true;
        Queue->Shutdown();
        Thread->Shutdown();
        Config->FlushWriters();
    }

    /*!
     * In some cases (when configuration is being updated at the same time),
     * the actual version is greater than the version returned by this method.
     */
    int GetConfigVersion()
    {
        return Version;
    }

    int GetConfigRevision()
    {
        return Config->GetVersion();
    }

    void GetLoggerConfig(
        const Stroka& category,
        ELogLevel* minLevel,
        int* configVersion)
    {
        TGuard<TSpinLock> guard(&SpinLock);
        *minLevel = Config->GetMinLevel(category);
        *configVersion = Version;
    }

    void Enqueue(const TLogEvent& event)
    {
        if (WasShutdown || Suspended) {
            return;
        }

        int backlogSize = LoggingProfiler.Increment(BacklogCounter);
        LoggingProfiler.Increment(EnqueueCounter);
        LogEventQueue.Enqueue(event);
        EventCount.Notify();

        if (event.Level == ELogLevel::Fatal) {
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

        if (!Suspended && backlogSize == Config->GetHighBacklogWatermark()) {
            LOG_WARNING("Backlog size has exceeded high watermark %d, logging suspended",
                Config->GetHighBacklogWatermark());
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
        auto result = Queue->BeginExecute(&CurrentAction);
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
                Config->ReloadWriters();
            }

            Write(event);
            ++eventsWritten;
        }

        int backlogSize = LoggingProfiler.Increment(BacklogCounter, -eventsWritten);
        if (Suspended && backlogSize < Config->GetLowBacklogWatermark()) {
            Suspended = false;
            LOG_INFO("Backlog size has dropped below low watermark %d, logging resumed",
                Config->GetLowBacklogWatermark());
        }

        if (eventsWritten > 0 && Config->GetFlushPeriod() == TDuration::Zero()) {
            Config->FlushWriters();
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
        Queue->EndExecute(&CurrentAction);
    }


    typedef std::vector<ILogWriterPtr> TWriters;

    TWriters GetWriters(const TLogEvent& event)
    {
        if (event.Category == SystemLoggingCategory) {
            return SystemWriters;
        } else {
            return Config->GetWriters(event);
        }
    }

    void Write(const TLogEvent& event)
    {
        for (auto& writer : GetWriters(event)) {
            LoggingProfiler.Increment(WriteCounter);
            writer->Write(event);
        }
    }

    void DoUpdateConfig(TLogConfigPtr config)
    {
        if (Config) {
            Config->FlushWriters();
        }

        {
            TGuard<TSpinLock> guard(&SpinLock);

            Config = config;
            AtomicIncrement(Version);

            if (FlushExecutor) {
                FlushExecutor->Stop();
                FlushExecutor.Reset();
            }

            if (WatchExecutor) {
                WatchExecutor->Stop();
                WatchExecutor.Reset();
            }

            auto flushPeriod = Config->GetFlushPeriod();
            if (flushPeriod) {
                FlushExecutor = New<TPeriodicExecutor>(
                    Queue,
                    BIND(&TImpl::DoFlushWritersPeriodically, MakeStrong(this)),
                    *flushPeriod);
                FlushExecutor->Start();
            }

            auto watchPeriod = Config->GetWatchPeriod();
            if (watchPeriod) {
                WatchExecutor = New<TPeriodicExecutor>(
                    Queue,
                    BIND(&TImpl::DoWatchWritersPeriodically, MakeStrong(this)),
                    *watchPeriod);
                WatchExecutor->Start();
            }

            auto checkSpacePeriod = Config->GetCheckSpacePeriod();
            if (checkSpacePeriod) {
                CheckSpaceExecutor = New<TPeriodicExecutor>(
                    Queue,
                    BIND(&TImpl::DoCheckSpacePeriodically, MakeStrong(this)),
                    *checkSpacePeriod);
                CheckSpaceExecutor->Start();
            }
        }
    }

    void DoFlushWritersPeriodically()
    {
        Config->FlushWriters();
    }

    void DoWatchWritersPeriodically()
    {
        Config->WatchWriters();
    }

    void DoCheckSpacePeriodically()
    {
        Config->CheckSpace();
    }


    TEventCount EventCount;
    volatile bool WasShutdown;
    TInvokerQueuePtr Queue;
    TIntrusivePtr<TThread> Thread;
    TEnqueuedAction CurrentAction;

    // Configuration.
    TAtomic Version;

    TLogConfigPtr Config;
    NProfiling::TRateCounter EnqueueCounter;
    NProfiling::TRateCounter WriteCounter;
    NProfiling::TAggregateCounter BacklogCounter;
    bool Suspended;
    TSpinLock SpinLock;

    TLockFreeQueue<TLogConfigPtr> ConfigsToUpdate;
    TLockFreeQueue<TLogEvent> LogEventQueue;

    TWriters SystemWriters;

    volatile bool ReopenEnqueued;

    TPeriodicExecutorPtr FlushExecutor;
    TPeriodicExecutorPtr WatchExecutor;
    TPeriodicExecutorPtr CheckSpaceExecutor;

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

int TLogManager::GetConfigVersion()
{
    return Impl->GetConfigVersion();
}

int TLogManager::GetConfigRevision()
{
    return Impl->GetConfigRevision();
}

void TLogManager::GetLoggerConfig(
    const Stroka& category,
    ELogLevel* minLevel,
    int* configVersion)
{
    Impl->GetLoggerConfig(category, minLevel, configVersion);
}

void TLogManager::Enqueue(const TLogEvent& event)
{
    Impl->Enqueue(event);
}

void TLogManager::Reopen()
{
    Impl->Reopen();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
