#include "log.h"
#include "log_manager.h"

#include <yt/core/misc/pattern_formatter.h>

#include <yt/core/ytree/node.h>

#include <util/folder/dirut.h>

namespace NYT {
namespace NLogging {

////////////////////////////////////////////////////////////////////////////////

TLogger::TLogger()
    : CategoryName_(nullptr)
    , CachedLogManager_(nullptr)
    , CachedCategory_(nullptr)
{ }

TLogger::TLogger(const char* categoryName)
    : CategoryName_(categoryName)
    , CachedLogManager_(nullptr)
    , CachedCategory_(nullptr)
{ }

const TLoggingCategory* TLogger::GetCategory() const
{
    if (!CachedCategory_ && CategoryName_) {
        CachedCategory_ = GetLogManager()->GetCategory(CategoryName_);
    }
    return CachedCategory_;
}

bool TLogger::IsEnabled(ELogLevel level) const
{
    auto* category = GetCategory();
    if (!category) {
        return false;
    }

    if (category->CurrentVersion != category->ActualVersion->load(std::memory_order_relaxed)) {
        GetLogManager()->UpdateCategory(category);
    }

    return level >= category->MinLevel;
}

void TLogger::Write(TLogEvent&& event) const
{
    if (!Context_.empty()) {
        event.Message = GetMessageWithContext(event.Message, Context_);
    }

    GetLogManager()->Enqueue(std::move(event));
}

TLogger& TLogger::AddRawTag(const Stroka& tag)
{
    if (!Context_.empty()) {
        Context_ += ", ";
    }
    Context_ += tag;
    return *this;
}

const Stroka& TLogger::GetContext() const
{
    return Context_;
}

TLogManager* TLogger::GetLogManager() const
{
    if (!CachedLogManager_) {
        CachedLogManager_ = TLogManager::Get();
    }
    return CachedLogManager_;
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

} // namespace NLogging
} // namespace NYT
