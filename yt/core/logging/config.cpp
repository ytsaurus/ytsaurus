#include "config.h"
#include "private.h"

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

void TRule::OnLoaded()
{
    if (IncludeCategories.size() == 1 && *IncludeCategories.begin() == AllCategoriesName) {
        IncludeAllCategories = true;
    }
}

bool TRule::IsApplicable(const Stroka& category) const
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

bool TRule::IsApplicable(const Stroka& category, ELogLevel level) const
{
    return MinLevel <= level && level <= MaxLevel && IsApplicable(category);
}

////////////////////////////////////////////////////////////////////////////////

TLogConfigPtr TLogConfig::CreateDefault()
{
    auto rule = New<TRule>();
    rule->IncludeAllCategories = true;
    rule->MinLevel = DefaultStdErrMinLevel;
    rule->Writers.push_back(DefaultStdErrWriterName);
    
    auto config = New<TLogConfig>();
    config->Rules.push_back(rule);
    return config;
}

TLogConfigPtr TLogConfig::CreateFromNode(NYTree::INodePtr node, const NYPath::TYPath& path)
{
    auto config = New<TLogConfig>();
    config->Load(node, true, true, path);
    return config;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
