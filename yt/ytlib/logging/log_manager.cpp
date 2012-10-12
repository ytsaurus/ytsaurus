#include "stdafx.h"
#include "log_manager.h"
#include "writer.h"

#include <ytlib/misc/pattern_formatter.h>
#include <yt/ytlib/misc/raw_formatter.h>

#include <ytlib/actions/action_queue_detail.h>

#include <ytlib/ytree/ypath_client.h>
#include <ytlib/ytree/ypath_service.h>
#include <ytlib/ytree/yson_serializable.h>

#include <ytlib/profiling/profiler.h>

#include <util/system/defaults.h>
#include <util/system/sigset.h>

#include <io.h>

namespace NYT {
namespace NLog {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

// TODO: review this and that
static const char* const SystemPattern = "$(datetime) $(level) $(category) $(message)";

static const char* const DefaultStdErrWriterName = "StdErr";
static const ELogLevel DefaultStdErrMinLevel= ELogLevel::Info;
static const char* const DefaultStdErrPattern = "$(datetime) $(level) $(category) $(message)";

static const char* const AllCategoriesName = "*";

static TLogger Logger(SystemLoggingCategory);
static NProfiling::TProfiler Profiler("/logging");

////////////////////////////////////////////////////////////////////////////////

struct TRule
    : public TYsonSerializable
{
    typedef TIntrusivePtr<TRule> TPtr;

    bool AllCategories;
    yhash_set<Stroka> Categories;

    ELogLevel MinLevel;
    ELogLevel MaxLevel;

    std::vector<Stroka> Writers;
    
    TRule()
        : AllCategories(false)
    {
        Register("categories", Categories).NonEmpty();
        Register("min_level", MinLevel).Default(ELogLevel::Minimum);
        Register("max_level", MaxLevel).Default(ELogLevel::Maximum);
        Register("writers", Writers).NonEmpty();
    }

    virtual void OnLoaded() override
    {
        if (Categories.size() == 1 && *Categories.begin() == AllCategoriesName) {
            AllCategories = true;
        }
    }

    bool IsApplicable(const Stroka& category) const
    {
        return AllCategories || Categories.find(category) != Categories.end();
    }

    bool IsApplicable(const Stroka& category, ELogLevel level) const
    {
        return
            MinLevel <= level && level <= MaxLevel &&
            IsApplicable(category);
    }
};

////////////////////////////////////////////////////////////////////////////////

typedef std::vector<ILogWriterPtr> TLogWriters;

struct TLogConfig;
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
        Register("writers", WriterConfigs);
        Register("rules", Rules);
    }

    TLogWriters GetWriters(const TLogEvent& event)
    {
        // Place a return value on top to promote RVO.
        TLogWriters writers;
        TPair<Stroka, ELogLevel> cacheKey(event.Category, event.Level);
        auto it = CachedWriters.find(cacheKey);
        if (it != CachedWriters.end())
            return it->second;
    
        yhash_set<Stroka> writerIds;
        FOREACH (auto& rule, Rules) {
            if (rule->IsApplicable(event.Category, event.Level)) {
                writerIds.insert(rule->Writers.begin(), rule->Writers.end());
            }
        }

        FOREACH (const Stroka& writerId, writerIds) {
            auto writerIt = Writers.find(writerId);
            YASSERT(writerIt != Writers.end());
            writers.push_back(writerIt->second);
        }

        YCHECK(CachedWriters.insert(MakePair(cacheKey, writers)).second);
        return writers;
    }

    ELogLevel GetMinLevel(const Stroka& category) const
    {
        ELogLevel level = ELogLevel::Maximum;
        FOREACH (const auto& rule, Rules) {
            if (rule->IsApplicable(category)) {
                level = Min(level, rule->MinLevel);
            }
        }
        return level;
    }

    void FlushWriters()
    {
        AtomicIncrement(Version);
        FOREACH (auto& pair, Writers) {
            pair.second->Flush();
        }
    }

    void ReloadWriters()
    {
        AtomicIncrement(Version);
        FOREACH (auto& pair, Writers) {
            pair.second->Reload();
        }
    }

    static TLogConfigPtr CreateDefault()
    {
        auto config = New<TLogConfig>();

        config->Writers.insert(MakePair(
            DefaultStdErrWriterName,
            New<TStdErrLogWriter>(DefaultStdErrPattern)));
        
        auto rule = New<TRule>();

        rule->AllCategories = true;
        rule->MinLevel = DefaultStdErrMinLevel;
        rule->Writers.push_back(DefaultStdErrWriterName);

        config->Rules.push_back(rule);

        return config;
    }

    static TLogConfigPtr CreateFromNode(INodePtr node, const TYPath& path = "")
    {
        auto config = New<TLogConfig>();
        config->Load(node, true, path);
        config->CreateWriters();
        return config;
    }

    int GetVersion()
    {
        return Version;
    }

private:
    virtual void DoValidate() const
    {
        FOREACH (const auto& rule, Rules) {
            FOREACH (const Stroka& writer, rule->Writers) {
                if (WriterConfigs.find(writer) == WriterConfigs.end()) {
                    THROW_ERROR_EXCEPTION("Unknown writer: %s", ~writer.Quote());
                }
            }
        }
    }

    void CreateWriters()
    {
        FOREACH (const auto& pair, WriterConfigs) {
            const auto& name = pair.first;
            const auto& config = pair.second;
            const auto& pattern = config->Pattern;
            switch (config->Type) {
                case ILogWriter::EType::File:
                    YCHECK(
                        Writers.insert(MakePair(
                            name, New<TFileLogWriter>(config->FileName, pattern))).second);
                    break;
                case ILogWriter::EType::StdOut:
                    YCHECK(
                        Writers.insert(MakePair(
                            name, New<TStdOutLogWriter>(pattern))).second);
                    break;
                case ILogWriter::EType::StdErr:
                    YCHECK(
                        Writers.insert(MakePair(
                            name, New<TStdErrLogWriter>(pattern))).second);
                    break;
                case ILogWriter::EType::Raw:
                    YCHECK(
                        Writers.insert(MakePair(
                            name, New<TRawFileLogWriter>(config->FileName))).second);
                    break;
                default:
                    YUNREACHABLE();
            }
            AtomicIncrement(Version);
        }
    }

    TAtomic Version;

    std::vector<TRule::TPtr> Rules;
    yhash_map<Stroka, ILogWriter::TConfig::TPtr> WriterConfigs;
    yhash_map<Stroka, ILogWriterPtr> Writers;
    ymap<TPair<Stroka, ELogLevel>, TLogWriters> CachedWriters;
};

////////////////////////////////////////////////////////////////////////////////

namespace {

void ReloadSignalHandler(int signal)
{
    NLog::TLogManager::Get()->Reopen();
}

} // namespace

class TLogManager::TImpl
    : public TActionQueueBase
{
public:
    TImpl()
        : TActionQueueBase("Logging", false)
        // Version forces this very module's Logger object to update to our own
        // default configuration (default level etc.).
        , Version(-1)
        , Config(TLogConfig::CreateDefault())
        , EnqueueCounter("/enqueue_rate")
        , WriteCounter("/write_rate")
        , ReopenEnqueued(false)
    {
        SystemWriters.push_back(New<TStdErrLogWriter>(SystemPattern));
        Start();
    }

    ~TImpl()
    {
        Shutdown();
    }

    void Configure(INodePtr node, const TYPath& path = "")
    {
        if (IsRunning()) {
            auto config = TLogConfig::CreateFromNode(node, path);
            ConfigsToUpdate.Enqueue(config);
            Signal();
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
        TActionQueueBase::Shutdown();
    }

    virtual void OnThreadStart()
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
        if (!IsRunning()) {
            return;
        }

        Profiler.Increment(EnqueueCounter);
        LogEventQueue.Enqueue(event);
        Signal();

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

            std::terminate();
        }
    }

    virtual bool DequeueAndExecute()
    {
        bool result = false;

        TLogConfigPtr config;
        while (ConfigsToUpdate.Dequeue(&config)) {
            DoUpdateConfig(config);
            result = true;
        }

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
            result = true;
        }

        return result;
    }

    void Reopen()
    {
        ReopenEnqueued = true;
    }

private:
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
        FOREACH (auto& writer, GetWriters(event)) {
            Profiler.Increment(WriteCounter);
            writer->Write(event);
        }
    }

    void DoUpdateConfig(TLogConfigPtr config)
    {
        Config->FlushWriters();

        {
            TGuard<TSpinLock> guard(&SpinLock);
            Config = config;
            AtomicIncrement(Version);
        }
    }

    // Configuration.
    TAtomic Version;

    TLogConfigPtr Config;
    NProfiling::TRateCounter EnqueueCounter;
    NProfiling::TRateCounter WriteCounter;
    TSpinLock SpinLock;

    TLockFreeQueue<TLogConfigPtr> ConfigsToUpdate;
    TLockFreeQueue<TLogEvent> LogEventQueue;

    TWriters SystemWriters;

    volatile bool ReopenEnqueued;
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
