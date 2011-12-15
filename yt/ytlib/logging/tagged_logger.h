#pragma once

#include "log.h"

#include "../misc/property.h"

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

class TTaggedLogger
    : private TNonCopyable
{
    DEFINE_BYVAL_RW_PROPERTY(Stroka, Tag);

public:
    TTaggedLogger(TLogger& innerLogger);

    Stroka GetCategory() const;
    bool IsEnabled(ELogLevel level);
    void Write(const TLogEvent& event);

private:
    Stroka GetTaggedMessage(const Stroka& originalMessage) const;

    TLogger& InnerLogger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
