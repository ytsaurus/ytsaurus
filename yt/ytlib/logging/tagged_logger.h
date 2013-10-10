#pragma once

#include "log.h"

#include <ytlib/misc/property.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

class TTaggedLogger
{
public:
    explicit TTaggedLogger(TLogger& innerLogger);
    TTaggedLogger(const TTaggedLogger& other);

    Stroka GetCategory() const;
    bool IsEnabled(ELogLevel level) const;
    void Write(const TLogEvent& event);

    void AddTag(const Stroka& tag);

private:
    Stroka GetTaggedMessage(const Stroka& originalMessage) const;

    TLogger* InnerLogger;
    Stroka Tags;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
