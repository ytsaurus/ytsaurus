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


void TLogger::Write(TLogEvent&& event) const
{
    if (!Context_.empty()) {
        event.Message = GetMessageWithContext(event.Message, Context_);
    }
    GetLogManager()->Enqueue(std::move(event));
}

void TLogger::AddRawTag(const Stroka& tag)
{
    if (!Context_.empty()) {
        Context_ += ", ";
    }
    Context_ += tag;
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

Stroka TLogger::GetMessageWithContext(const Stroka& originalMessage, const Stroka& context)
{
    auto endIndex = originalMessage.find('\n');
    if (endIndex == Stroka::npos) {
        endIndex = originalMessage.length();
    }
    if (endIndex > 0 && originalMessage[endIndex - 1] == ')') {
        return
            originalMessage.substr(0, endIndex - 1) +
            ", " + context +
            originalMessage.substr(endIndex - 1);
    } else {
        return
            originalMessage.substr(0, endIndex) +
            " (" + context + ")" +
            originalMessage.substr(endIndex);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
