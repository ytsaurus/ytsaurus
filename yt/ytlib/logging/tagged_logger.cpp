#include "stdafx.h"
#include "tagged_logger.h"

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

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
    TLogEvent modifiedEvent(
        event.GetCategory(), 
        event.GetLevel(),
        GetTaggedMessage(event.GetMessage()));
    FOREACH (const auto& property, event.GetProperties()) {
        modifiedEvent.AddProperty(property.First(), property.Second());
    }

    InnerLogger.Write(modifiedEvent);
}

Stroka TTaggedLogger::GetTaggedMessage(const Stroka& originalMessage) const
{
    auto endIndex = originalMessage.find('\n');
    if (endIndex == Stroka::npos) {
        endIndex = originalMessage.length();
    }
    if (endIndex > 0 && originalMessage[endIndex - 1] == ')') {
        return
            originalMessage.substr(0, endIndex - 1) +
            ", " + Tag +
            originalMessage.substr(endIndex - 1);
    } else {
        return
            originalMessage.substr(0, endIndex) +
            " (" + Tag + ")" +
            originalMessage.substr(endIndex);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
