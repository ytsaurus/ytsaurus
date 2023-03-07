#include "config.h"
#include "private.h"

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

bool TRuleConfig::IsApplicable(const TString& category, ELogMessageFormat format) const
{
    return MessageFormat == format
           && ExcludeCategories.find(category) == ExcludeCategories.end()
           && (!IncludeCategories || IncludeCategories->find(category) != IncludeCategories->end());
}

bool TRuleConfig::IsApplicable(const TString& category, ELogLevel level, ELogMessageFormat format) const
{
    if (!IsApplicable(category, format)) {
        return false;
    }

    return MinLevel <= level && level <= MaxLevel;
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
    config->WriterConfigs.insert(std::make_pair("FileWriter", fileWriterConfig));

    config->MinDiskSpace = 0;
    config->HighBacklogWatermark = 100000;
    config->LowBacklogWatermark = 100000;

    return config;
}

TLogManagerConfigPtr TLogManagerConfig::CreateStderrLogger(ELogLevel logLevel)
{
    auto rule = New<TRuleConfig>();
    rule->MinLevel = logLevel;
    rule->Writers.push_back(DefaultStderrWriterName);

    auto stderrWriterConfig = New<TWriterConfig>();
    stderrWriterConfig->Type = EWriterType::Stderr;

    auto config = New<TLogManagerConfig>();
    config->Rules.push_back(rule);
    config->WriterConfigs.insert(std::make_pair(DefaultStderrWriterName, stderrWriterConfig));

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
