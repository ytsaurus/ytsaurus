#include "stdafx.h"
#include "log.h"
#include "log_manager.h"

#include "../misc/pattern_formatter.h"
#include "../misc/config.h"
#include "../ytree/ytree.h"

#include <util/folder/dirut.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

TLogger::TLogger(const Stroka& category)
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
    if (TLogManager::Get()->GetConfigVersion() != ConfigVersion) {
        UpdateConfig();
    }
    return level >= MinLevel;
}

void TLogger::UpdateConfig()
{
    TLogManager::Get()->GetLoggerConfig(
        Category,
        &MinLevel,
        &ConfigVersion);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
