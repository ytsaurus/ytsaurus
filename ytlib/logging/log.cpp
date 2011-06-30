#include "log.h"

#include "../misc/pattern_formatter.h"
#include "../misc/config.h"

#include <quality/util/file_utils.h>

namespace NYT {
namespace NLog {

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

////////////////////////////////////////////////////////////////////////////////

struct TLogManager::TRule
{
    TRule()
        : AllCategories(false)
        , MinLevel(ELogLevel::Minimum)
        , MaxLevel(ELogLevel::Maximum)
    { }

    bool AllCategories;
    yhash_set<Stroka> Categories;

    ELogLevel MinLevel;
    ELogLevel MaxLevel;

    yvector<Stroka> Writers;

    bool IsApplicable(Stroka category) const;
    bool IsApplicable(const TLogEvent& event) const;
};

bool TLogManager::TRule::IsApplicable(Stroka category) const
{
    return (AllCategories || Categories.find(category) != Categories.end());
}

bool TLogManager::TRule::IsApplicable(const TLogEvent& event) const
{
    ELogLevel level = event.GetLevel();
    return (IsApplicable(event.GetCategory()) &&
            MinLevel <= level && level <= MaxLevel);
}

////////////////////////////////////////////////////////////////////////////////

void LogEventImpl(
    TLogger& logger,
    const char* fileName,
    int line,
    const char* function,
    ELogLevel level,
    Stroka message)
{
    TLogEvent event(logger.GetCategory(), level, message);
    event.AddProperty("file", GetFilename(fileName));
    event.AddProperty("line", ToString(line));
    event.AddProperty("thread", ToString(TThread::CurrentThreadId()));
    event.AddProperty("function", function);
    logger.Write(event);
}

////////////////////////////////////////////////////////////////////////////////

TLogManager::TLogManager()
    : ConfigVersion(0)
    , Queue(new TActionQueue(false))
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
    TActionQueue::TPtr queue = Queue;
    if (~queue != NULL) {
        FromMethod(&TLogManager::DoFlush, this)
            ->AsyncVia(~queue)
            ->Do()
            ->Get();
    }
}

void TLogManager::Shutdown()
{
    Flush();
    
    TActionQueue::TPtr queue = Queue;
    if (~queue != NULL) {
        queue->Shutdown();
        Queue = NULL;
    }
}

TVoid TLogManager::DoFlush()
{
    for (TWriterMap::iterator it = Writers.begin();
         it != Writers.end();
         ++it)
    {
        it->second->Flush();
    }
    return TVoid();
}

void TLogManager::Write(const TLogEvent& event)
{
    TActionQueue::TPtr queue = Queue;
    if (~queue != NULL) {
        queue->Invoke(FromMethod(&TLogManager::DoWrite, this, event));

        // TODO: use system-wide exit function
        if (event.GetLevel() == ELogLevel::Fatal) {
            Shutdown();
            exit(1);
        }
    }
}

void TLogManager::DoWrite(const TLogEvent& event)
{
    yvector<ILogWriter::TPtr> writers = GetWriters(event);
    for (yvector<ILogWriter::TPtr>::iterator it = writers.begin();
         it != writers.end();
         ++it)
    {
        (*it)->Write(event);
    }
}

yvector<ILogWriter::TPtr> TLogManager::GetWriters(const TLogEvent& event)
{
    if (event.GetCategory() == SystemLoggingCategory)
        return SystemWriters;
    else
        return GetConfiguredWriters(event);
}

yvector<ILogWriter::TPtr> TLogManager::GetConfiguredWriters(const TLogEvent& event)
{
    Stroka category = event.GetCategory();
    ELogLevel level = event.GetLevel();

    yhash_set<Stroka> writerIds;
    for (TRules::iterator it = Rules.begin(); it != Rules.end(); ++it) {
        if (it->IsApplicable(event)) {
            writerIds.insert(it->Writers.begin(), it->Writers.end());
        }
    }

    yvector<ILogWriter::TPtr> writers;
    for (yhash_set<Stroka>::const_iterator it = writerIds.begin();
         it != writerIds.end();
         ++it)
    {
        writers.push_back(Writers[*it]);
    }

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

    for(int i = 0; i < Rules.ysize(); ++i) {
        if (Rules[i].IsApplicable(category)) {
            level = Min(level, Rules[i].MinLevel);
        }
    }
    return level;
}

void TLogManager::Configure(TJsonObject* root)
{
    TGuard<TSpinLock> guard(&SpinLock);

    ConfigureWriters(root->Value(L"Writers"));
    ConfigureRules(root->Value(L"Rules"));
    
    AtomicIncrement(ConfigVersion);
}

void TLogManager::Configure(Stroka fileName, Stroka rootPath)
{
    TIFStream configStream(fileName);
    TJsonReader reader(CODES_UTF8, &configStream);
    TJsonObject* root = reader.ReadAll();
    root = GetSubTree(root, rootPath);
    Configure(root);
}

void TLogManager::ConfigureSystem()
{
    SystemWriters.push_back(new TStdErrLogWriter(SystemPattern));
}

void TLogManager::ConfigureDefault()
{
    TGuard<TSpinLock> guard(&SpinLock);
    
    Writers.insert(MakePair(
        DefaultStdErrWriterName,
        new TStdErrLogWriter(SystemPattern)));
    
    TRule stdErrRule;
    stdErrRule.AllCategories = true;
    stdErrRule.MinLevel = DefaultStdErrMinLevel;
    stdErrRule.Writers.push_back(DefaultStdErrWriterName);
    Rules.push_back(stdErrRule);

    Writers.insert(MakePair(
        DefaultFileWriterName,
        new TFileLogWriter(DefaultFileName, DefaultFilePattern)));
    
    TRule fileRule;
    fileRule.AllCategories = true;
    fileRule.MinLevel = DefaultFileMinLevel;
    fileRule.Writers.push_back(DefaultFileWriterName);
    Rules.push_back(fileRule);

    AtomicIncrement(ConfigVersion);
}

void TLogManager::ConfigureWriters(const TJsonObject* root)
{
    Writers.clear();
    const TJsonArray* writers = static_cast<const TJsonArray*>(root);
    for (int i = 0; i < writers->Length(); ++i) {
        Stroka name, type, pattern;
        const TJsonObject* item = writers->Item(i);
        TryRead(item, L"Name", &name);
        TryRead(item, L"Type", &type);
        TryRead(item, L"Pattern", &pattern);
        if (type == "File") {
            Stroka fileName;
            NYT::TryRead(item, L"FileName", &fileName);
            Writers[name] = new TFileLogWriter(fileName, pattern);
        } else if (type == "StdErr") {
            Writers[name] = new TStdErrLogWriter(pattern);
        } else if (type == "StdOut") {
            Writers[name] = new TStdOutLogWriter(pattern);
        } else {
            ythrow yexception() <<
                Sprintf("%s is unknown type of writer", ~type);
        }
    }
}

void TLogManager::ConfigureRules(const TJsonObject* root)
{
    Rules.clear();
    const TJsonArray* rules = static_cast<const TJsonArray*>(root);
    Rules.resize(rules->Length());
    for (int i = 0; i < rules->Length(); ++i) {
        // TODO:
        // TRule rule;
        // configure rule
        const TJsonObject* item = rules->Item(i);
        yvector<Stroka> categories;
        TryRead(item, L"Categories", &categories);
        if (categories.size() == 1 && categories[0] == AllCategoriesName) {
            Rules[i].AllCategories = true;
        } else {
            Rules[i].AllCategories = false;
            Rules[i].Categories.insert(categories.begin(), categories.end());
        }

        ReadEnum<ELogLevel>(item, L"MinLevel", &Rules[i].MinLevel, ELogLevel::Minimum);
        ReadEnum<ELogLevel>(item, L"MaxLevel", &Rules[i].MaxLevel, ELogLevel::Maximum);
        TryRead(item, L"Writers", &Rules[i].Writers);
        // push_back rule
    }
}

////////////////////////////////////////////////////////////////////////////////

TLogger::TLogger(Stroka category)
    : Category(category)
    , ConfigVersion(0)
{ }

Stroka TLogger::GetCategory() const
{
    return Category;
}

void TLogger::Write(const TLogEvent& event)
{
    TLogManager::Get()->Write(event);
}

bool TLogger::IsEnabled(ELogLevel level)
{
    TLogManager* manager = TLogManager::Get();
    if (manager->GetConfigVersion() != ConfigVersion) {
        UpdateConfig();
    }
    return level >= MinLevel;
}

void TLogger::UpdateConfig()
{
    TLogManager* manager = TLogManager::Get();
    manager->GetLoggerConfig(
        Category,
        &MinLevel,
        &ConfigVersion);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
