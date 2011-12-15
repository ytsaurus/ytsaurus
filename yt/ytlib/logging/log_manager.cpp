#include "stdafx.h"
#include "log_manager.h"

#include "../misc/pattern_formatter.h"
#include "../misc/config.h"

#include "../ytree/serialize.h"
#include "../ytree/ypath_client.h"
#include "../ytree/ypath_service.h"

#include <util/folder/dirut.h>

namespace NYT {
namespace NLog {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

// TODO: review this and that
static const char* const SystemPattern = "$(datetime) $(level) $(category) $(message)";

static const char* const DefaultStdErrWriterName = "StdErr";
static const ELogLevel DefaultStdErrMinLevel= ELogLevel::Info;
static const char* const DefaultStdErrPattern = "$(datetime) $(level) $(category) $(message)";

static const char* const DefaultFileWriterName = "LogFile";
static const char* const DefaultFileName = "default.log";
static const ELogLevel DefaultFileMinLevel = ELogLevel::Debug;
static const char* const DefaultFilePattern =
    "$(datetime) $(level) $(category) $(message)$(tab)$(file?) $(line?) $(function?) $(thread?)";

static const char* const AllCategoriesName = "*";

static TLogger Logger(SystemLoggingCategory);

////////////////////////////////////////////////////////////////////////////////

struct TRule
{
    struct TConfig
        : public TConfigBase
    {
        typedef TIntrusivePtr<TConfig> TPtr;
        
        yvector<Stroka> Categories;
        ELogLevel MinLevel;
        ELogLevel MaxLevel;

        yvector <Stroka> Writers;

        TConfig()
        {
            Register("categories", Categories);
            Register("min_level", MinLevel).Default(ELogLevel::Minimum);
            Register("max_level", MaxLevel).Default(ELogLevel::Maximum);
            Register("writers", Writers);
        }
    };

    bool AllCategories;
    yhash_set<Stroka> Categories;

    ELogLevel MinLevel;
    ELogLevel MaxLevel;

    yvector<Stroka> Writers;

    TRule()
        : AllCategories(false)
        , MinLevel(ELogLevel::Minimum)
        , MaxLevel(ELogLevel::Maximum)
    { }

    TRule(TConfig* config)
        : MinLevel(config->MinLevel)
        , MaxLevel(config->MaxLevel)
        , Writers(config->Writers)
    {
        if (config->Categories.size() == 1 && config->Categories[0] == AllCategoriesName) {
            AllCategories = true;
        } else {
            AllCategories = false;
            Categories = yhash_set<Stroka>(config->Categories.begin(), config->Categories.end());
        }
    }

    bool IsApplicable(Stroka category) const
    {
        return (AllCategories || Categories.find(category) != Categories.end());
    }

    bool IsApplicable(const TLogEvent& event) const
    {
        ELogLevel level = event.GetLevel();
        return (IsApplicable(event.GetCategory()) &&
                MinLevel <= level && level <= MaxLevel);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TLogManager::TConfig
    : public TConfigBase
{
public:
    typedef TIntrusivePtr<TConfig> TPtr;

    typedef yhash_map<Stroka, ILogWriter::TPtr> TWriterMap;
    TWriterMap Writers;

    typedef yvector<TRule> TRules;
    TRules Rules;

    TConfig();
    void Init();

private:
    void ConfigureWriters();
    void ConfigureRules();

    void ValidateRule(const TRule& rule);

    yhash_map<Stroka, ILogWriter::TConfig::TPtr> WritersConfigs;
    yvector<TRule::TConfig::TPtr> RulesConfigs;

};

void TLogManager::TConfig::Init()
{
    ConfigureWriters();
    ConfigureRules();
}

void TLogManager::TConfig::ConfigureWriters()
{
    FOREACH(const auto& pair, WritersConfigs) {
        const auto& name = pair.first;
        const auto& config = pair.second;
        if (Writers.find(name) != Writers.end()) {
            ythrow yexception() <<
                Sprintf("Writer %s is already defined", ~name);
        }

        const auto& pattern = config->Pattern;
        Stroka errorMessage;
        if (!ValidatePattern(pattern, & errorMessage)) {
            ythrow yexception() <<
                Sprintf("Invalid pattern at writer %s\n%s", ~name, ~errorMessage);
        }

        const auto& type = config->Type;
        if (type == "File") {
            if (config->FileName.empty()) {
                ythrow yexception() <<
                    Sprintf("FileName of writer %s is not initialized", ~name);
            }
            Writers[name] = New<TFileLogWriter>(config->FileName, pattern);
        } else if (type == "StdErr") {
            Writers[name] = New<TStdErrLogWriter>(pattern);
        } else if (type == "StdOut") {
            Writers[name] = New<TStdOutLogWriter>(pattern);
        } else {
            ythrow yexception() <<
                Sprintf("%s is unknown type of writer", ~type);
        }
    }
}

void TLogManager::TConfig::ConfigureRules()
{
    Rules.reserve(RulesConfigs.size());
    FOREACH(const auto& config, RulesConfigs) {
        TRule rule(~config);
        ValidateRule(rule);
        Rules.push_back(rule);
    }
}


void TLogManager::TConfig::ValidateRule(const TRule& rule)
{
    FOREACH(Stroka writer, rule.Writers) {
        if (Writers.find(writer) == Writers.end()) {
            ythrow yexception() <<
                Sprintf("Writer %s wasn't defined", ~writer);
        }
    }
}

TLogManager::TConfig::TConfig()
{
    Register("writers", WritersConfigs);
    Register("rules", RulesConfigs);

    Writers.insert(MakePair(
        DefaultStdErrWriterName,
        New<TStdErrLogWriter>(SystemPattern)));

    TRule stdErrRule;
    stdErrRule.AllCategories = true;
    stdErrRule.MinLevel = DefaultStdErrMinLevel;
    stdErrRule.Writers.push_back(DefaultStdErrWriterName);
    Rules.push_back(stdErrRule);

    Writers.insert(MakePair(
        DefaultFileWriterName,
        New<TFileLogWriter>(DefaultFileName, DefaultFilePattern)));

    TRule fileRule;
    fileRule.AllCategories = true;
    fileRule.MinLevel = DefaultFileMinLevel;
    fileRule.Writers.push_back(DefaultFileWriterName);
    Rules.push_back(fileRule);
}

////////////////////////////////////////////////////////////////////////////////

TLogManager::TLogManager()
    : ConfigVersion(0)
    , Queue(New<TActionQueue>("LogManager", false))
{
    ConfigureSystem();
    ConfigureDefault();
}

TLogManager* TLogManager::Get()
{
    return Singleton<TLogManager>();
}

void TLogManager::Flush()
{
    auto queue = Queue;
    if (~queue != NULL) {
        FromMethod(&TLogManager::DoFlush, this)
            ->AsyncVia(queue->GetInvoker())
            ->Do()
            ->Get();
    }
}

void TLogManager::Shutdown()
{
    Flush();
    
    auto queue = Queue;
    if (~queue != NULL) {
        Queue.Reset();
        queue->Shutdown();
    }
}

TVoid TLogManager::DoFlush()
{
    FOREACH(auto& pair, Configuration->Writers) {
        pair.second->Flush();
    }
    return TVoid();
}

void TLogManager::Write(const TLogEvent& event)
{
    auto queue = Queue;
    if (~queue != NULL) {
        queue->GetInvoker()->Invoke(FromMethod(&TLogManager::DoWrite, this, event));

        // TODO: use system-wide exit function
        if (event.GetLevel() == ELogLevel::Fatal) {
            Shutdown();
            ::std::terminate();
        }
    }
}

void TLogManager::DoWrite(const TLogEvent& event)
{
    FOREACH(auto& writer, GetWriters(event)) {
        writer->Write(event);
    }
}

yvector<ILogWriter::TPtr> TLogManager::GetWriters(const TLogEvent& event)
{
    if (event.GetCategory() == SystemLoggingCategory)
        return SystemWriters;

    TPair<Stroka, ELogLevel> cacheKey(event.GetCategory(), event.GetLevel());
    auto it = CachedWriters.find(cacheKey);
    if (it != CachedWriters.end())
        return it->second;
    
    TLogWriters writers = GetConfiguredWriters(event);
    CachedWriters.insert(MakePair(cacheKey, writers));
    return writers;
}

yvector<ILogWriter::TPtr> TLogManager::GetConfiguredWriters(const TLogEvent& event)
{
    Stroka category = event.GetCategory();
    ELogLevel level = event.GetLevel();

    yhash_set<Stroka> writerIds;
    FOREACH(auto& rule, Configuration->Rules) {
        if (rule.IsApplicable(event)) {
            writerIds.insert(rule.Writers.begin(), rule.Writers.end());
        }
    }

    yvector<ILogWriter::TPtr> writers;
    FOREACH(const Stroka& writerId, writerIds) {
        auto writerIt = Configuration->Writers.find(writerId);
        if (writerIt == Configuration->Writers.end()) {
            ythrow yexception() <<
                Sprintf("Couldn't find writer %s", ~writerId);
        }
        writers.push_back(writerIt->second);
    }

    UNUSED(level); // This is intentional?
    return writers;
}

int TLogManager::GetConfigVersion()
{
    TGuard<TSpinLock> guard(&SpinLock);
    return ConfigVersion;
}

void TLogManager::GetLoggerConfig(
    Stroka category,
    ELogLevel* minLevel,
    int* configVersion)
{
    TGuard<TSpinLock> guard(&SpinLock);
    *minLevel = GetMinLevel(category);
    *configVersion = ConfigVersion;
}

NYT::NLog::ELogLevel TLogManager::GetMinLevel(Stroka category)
{
    ELogLevel level = ELogLevel::Maximum;

    FOREACH(const auto& rule, Configuration->Rules) {
        if (rule.IsApplicable(category)) {
            level = Min(level, rule.MinLevel);
        }
    }
    return level;
}

void TLogManager::Configure(const Stroka& fileName, const TYPath& path)
{
    try {
        LOG_DEBUG("Configuring logging from file %s with YPath %s", ~fileName, ~path);
        TIFStream configStream(fileName);
        auto root = DeserializeFromYson(&configStream);
        auto rootService = IYPathService::FromNode(~root);
        auto configNode = SyncYPathGetNode(~rootService, path);
        Configure(~configNode);
    } catch (const yexception& e) {
        LOG_ERROR("Error configuring logging\n%s", e.what())
        return;
    }
}

void TLogManager::Configure(NYTree::INode* node)
{
    auto configuration = New<TConfig>();
    configuration->Load(node);
    configuration->Init();
    {
        TGuard<TSpinLock> guard(&SpinLock);
        Configuration = configuration;
        ConfigVersion++;
    }
}

void TLogManager::ConfigureSystem()
{
    SystemWriters.push_back(New<TStdErrLogWriter>(SystemPattern));
}

void TLogManager::ConfigureDefault()
{
    auto configuration = New<TConfig>();
    {
        TGuard<TSpinLock> guard(&SpinLock);
        Configuration = configuration;
        ConfigVersion++;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
}  // namespace NYT
