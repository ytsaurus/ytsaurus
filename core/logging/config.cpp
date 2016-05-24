#include "config.h"
#include "private.h"

namespace NYT {
namespace NLogging {

////////////////////////////////////////////////////////////////////////////////

bool TRuleConfig::IsApplicable(const Stroka& category) const
{
    if (IncludeCategories && IncludeCategories->find(category) == IncludeCategories->end()) {
        // No match in include_categories.
        return false;
    }

    if (ExcludeCategories.find(category) != ExcludeCategories.end()) {
        // Match in exclude_categories.
        return false;
    }

    return true;
}

bool TRuleConfig::IsApplicable(const Stroka& category, ELogLevel level) const
{
    return MinLevel <= level && level <= MaxLevel && IsApplicable(category);
}

////////////////////////////////////////////////////////////////////////////////

TLogConfigPtr TLogConfig::CreateDefault()
{
    auto rule = New<TRuleConfig>();
    rule->MinLevel = DefaultStderrMinLevel;
    rule->Writers.push_back(DefaultStderrWriterName);

    auto stderrWriterConfig = New<TWriterConfig>();
    stderrWriterConfig->Type = EWriterType::Stderr;

    auto config = New<TLogConfig>();
    config->Rules.push_back(rule);
    config->WriterConfigs.insert(std::make_pair(DefaultStderrWriterName, stderrWriterConfig));

    config->MinDiskSpace = 0;
    config->HighBacklogWatermark = 0;
    config->LowBacklogWatermark = 0;

    return config;
}

TLogConfigPtr TLogConfig::CreateQuiet()
{
    auto rule = New<TRuleConfig>();
    rule->MinLevel = DefaultStderrQuietLevel;
    rule->Writers.push_back(DefaultStderrWriterName);

    auto stderrWriterConfig = New<TWriterConfig>();
    stderrWriterConfig->Type = EWriterType::Stderr;

    auto config = New<TLogConfig>();
    config->Rules.push_back(rule);
    config->WriterConfigs.insert(std::make_pair(DefaultStderrWriterName, stderrWriterConfig));

    config->MinDiskSpace = 0;
    config->HighBacklogWatermark = 0;
    config->LowBacklogWatermark = 0;

    return config;
}

TLogConfigPtr TLogConfig::CreateFromNode(NYTree::INodePtr node, const NYPath::TYPath& path)
{
    auto config = New<TLogConfig>();
    config->Load(node, true, true, path);
    return config;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging
} // namespace NYT
