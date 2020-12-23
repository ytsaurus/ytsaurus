#include "config.h"
#include "private.h"

#include <util/string/vector.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

bool TRuleConfig::IsApplicable(TStringBuf category, ELogMessageFormat format) const
{
    return
        MessageFormat == format &&
        ExcludeCategories.find(category) == ExcludeCategories.end() &&
        (!IncludeCategories || IncludeCategories->find(category) != IncludeCategories->end());
}

bool TRuleConfig::IsApplicable(TStringBuf category, ELogLevel level, ELogMessageFormat format) const
{
    return
        IsApplicable(category, format) &&
        MinLevel <= level && level <= MaxLevel;
}

////////////////////////////////////////////////////////////////////////////////

TLogManagerConfigPtr TLogManagerConfig::CreateLogFile(const TString& path)
{
    auto rule = New<TRuleConfig>();
    rule->MinLevel = ELogLevel::Trace;
    rule->Writers.push_back("FileWriter");

    auto fileWriterConfig = New<TWriterConfig>();
    fileWriterConfig->Type = EWriterType::File;
    fileWriterConfig->FileName = path;

    auto config = New<TLogManagerConfig>();
    config->Rules.push_back(rule);
    config->WriterConfigs.emplace("FileWriter", fileWriterConfig);

    config->MinDiskSpace = 0;
    config->HighBacklogWatermark = 100000;
    config->LowBacklogWatermark = 100000;

    return config;
}

TLogManagerConfigPtr TLogManagerConfig::CreateStderrLogger(ELogLevel logLevel)
{
    auto rule = New<TRuleConfig>();
    rule->MinLevel = logLevel;
    rule->Writers.push_back(TString(DefaultStderrWriterName));

    auto stderrWriterConfig = New<TWriterConfig>();
    stderrWriterConfig->Type = EWriterType::Stderr;

    auto config = New<TLogManagerConfig>();
    config->Rules.push_back(rule);
    config->WriterConfigs.emplace(TString(DefaultStderrWriterName), stderrWriterConfig);

    config->MinDiskSpace = 0;
    config->HighBacklogWatermark = 100000;
    config->LowBacklogWatermark = 100000;

    return config;
}

TLogManagerConfigPtr TLogManagerConfig::CreateDefault()
{
    return CreateStderrLogger(DefaultStderrMinLevel);
}

TLogManagerConfigPtr TLogManagerConfig::CreateQuiet()
{
    return CreateStderrLogger(DefaultStderrQuietLevel);
}

TLogManagerConfigPtr TLogManagerConfig::CreateSilent()
{
    auto config = New<TLogManagerConfig>();

    config->MinDiskSpace = 0;
    config->HighBacklogWatermark = 0;
    config->LowBacklogWatermark = 0;

    return config;
}

TLogManagerConfigPtr TLogManagerConfig::CreateYTServer(const TString& componentName)
{
    auto config = New<TLogManagerConfig>();

    for (auto logLevel : {ELogLevel::Debug, ELogLevel::Info, ELogLevel::Error}) {
        auto rule = New<TRuleConfig>();
        rule->MinLevel = logLevel;
        rule->Writers.push_back(ToString(logLevel));

        auto fileWriterConfig = New<TWriterConfig>();
        fileWriterConfig->Type = EWriterType::File;
        fileWriterConfig->FileName = Format(
            "./%v%v.log",
            componentName,
            logLevel == ELogLevel::Info ? "" : "." + FormatEnum(logLevel));

        config->Rules.push_back(rule);
        config->WriterConfigs.emplace(ToString(logLevel), fileWriterConfig);
    }

    return config;
}

TLogManagerConfigPtr TLogManagerConfig::CreateFromFile(const TString& file, const NYPath::TYPath& path)
{
    NYTree::INodePtr node;
    {
        TIFStream stream(file);
        node = NYTree::ConvertToNode(&stream);
    }
    return CreateFromNode(std::move(node), path);
}

TLogManagerConfigPtr TLogManagerConfig::CreateFromNode(NYTree::INodePtr node, const NYPath::TYPath& path)
{
    auto config = New<TLogManagerConfig>();
    config->Load(node, true, true, path);
    return config;
}

TLogManagerConfigPtr TLogManagerConfig::TryCreateFromEnv()
{
    const auto* logLevelStr = getenv("YT_LOG_LEVEL");
    if (!logLevelStr) {
        return nullptr;
    }

    const char* logExcludeCategoriesStr = getenv("YT_LOG_EXCLUDE_CATEGORIES");
    const char* logIncludeCategoriesStr = getenv("YT_LOG_INCLUDE_CATEGORIES");

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

    config->WriterConfigs.emplace(stderrWriterName, std::move(stderrWriter));

    return config;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
