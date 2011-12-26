#include "stdafx.h"
#include "tagged_logger.h"

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

TTaggedLogger::TTaggedLogger(TLogger& innerLogger)
    : InnerLogger(innerLogger)
{ }

Stroka TTaggedLogger::GetCategory() const
{
    return InnerLogger.GetCategory();
}

bool TTaggedLogger::IsEnabled(ELogLevel level)
{
    return InnerLogger.IsEnabled(level);
}

void TTaggedLogger::Write(const TLogEvent& event)
{
    TLogEvent modifiedEvent = event;
    modifiedEvent.Message = GetTaggedMessage(event.Message);
    InnerLogger.Write(modifiedEvent);
}

void TTaggedLogger::AddTag(const Stroka& tag)
{
    if (Tags.empty()) {
        Tags = tag;
    } else {
        Tags += ", ";
        Tags += tag;
    }
}

Stroka TTaggedLogger::GetTaggedMessage(const Stroka& originalMessage) const
{
    if (Tags.length() == 0) {
        return originalMessage;
    }

    auto endIndex = originalMessage.find('\n');
    if (endIndex == Stroka::npos) {
        endIndex = originalMessage.length();
    }
    if (endIndex > 0 && originalMessage[endIndex - 1] == ')') {
        return
            originalMessage.substr(0, endIndex - 1) +
            ", " + Tags +
            originalMessage.substr(endIndex - 1);
    } else {
        return
            originalMessage.substr(0, endIndex) +
            " (" + Tags + ")" +
            originalMessage.substr(endIndex);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
