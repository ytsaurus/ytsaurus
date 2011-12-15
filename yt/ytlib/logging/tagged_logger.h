#pragma once

#include "log.h"

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

class TTaggedLogger
    : private TNonCopyable
{
public:
    TTaggedLogger(TLogger& innerLogger, const Stroka& tag);

    Stroka GetCategory() const;
    bool IsEnabled(ELogLevel level);
    void Write(const TLogEvent& event);

private:
    Stroka GetTaggedMessage(const Stroka& originalMessage) const;

    TLogger& InnerLogger;
    Stroka Tag;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
