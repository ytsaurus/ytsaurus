#include "stdafx.h"
#include "log.h"
#include "log_manager.h"

#include <ytlib/misc/pattern_formatter.h>
#include <ytlib/misc/configurable.h>
#include <ytlib/ytree/ytree.h>

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
    // TODO(sandello): Cache pointer to the TLogManager instance in order
    // to avoid extra locking and synchronization in singleton getter.
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
