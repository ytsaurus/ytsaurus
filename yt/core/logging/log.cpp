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
    : Category_(category)
{ }

TLogger::TLogger(const TLogger& other)
    : Category_(other.Category_)
{ }

const Stroka& TLogger::GetCategory() const
{
    return Category_;
}

void TLogger::Write(TLogEvent&& event) const
{
    GetLogManager()->Enqueue(std::move(event));
}

bool TLogger::IsEnabled(ELogLevel level) const
{
    if (Category_.empty()) {
        return false;
    }

    if (GetLogManager()->GetVersion() != Version_) {
        const_cast<TLogger*>(this)->Update();
    }

    return level >= MinLevel_;
}

void TLogger::Update()
{
    MinLevel_ = GetLogManager()->GetMinLevel(Category_);
    Version_ = GetLogManager()->GetVersion();
}

TLogManager* TLogger::GetLogManager() const
{
    if (!LogManager_) {
        LogManager_ = TLogManager::Get();
    }
    return LogManager_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
