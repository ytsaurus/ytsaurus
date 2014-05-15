#include "stdafx.h"
#include "log.h"
#include "log_manager.h"

#include <core/misc/pattern_formatter.h>
#include <core/ytree/node.h>

#include <util/folder/dirut.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

TLogger::TLogger(const Stroka& category)
    : Category(category)
    , Version(-1)
    , LogManager(NULL)
{ }

TLogger::TLogger(const TLogger& other)
    : Category(other.Category)
    , Version(-1)
    , LogManager(NULL)
{ }

const Stroka& TLogger::GetCategory() const
{
    return Category;
}

void TLogger::Write(TLogEvent&& event)
{
    GetLogManager()->Enqueue(std::move(event));
}

bool TLogger::IsEnabled(ELogLevel level) const
{
    if (Category.empty()) {
        return false;
    }

    if (GetLogManager()->GetVersion() != Version) {
        const_cast<TLogger*>(this)->Update();
    }

    return level >= MinLevel;
}

void TLogger::Update()
{
    MinLevel = GetLogManager()->GetMinLevel(Category);
    Version = GetLogManager()->GetVersion();
}

TLogManager* TLogger::GetLogManager() const
{
    if (!LogManager) {
        LogManager = TLogManager::Get();
    }
    return LogManager;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
