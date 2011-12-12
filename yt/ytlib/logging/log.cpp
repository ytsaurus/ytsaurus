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
