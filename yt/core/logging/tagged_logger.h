#pragma once

#include "log.h"

#include <core/misc/property.h>

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

class TTaggedLogger
{
public:
    explicit TTaggedLogger(TLogger& innerLogger);
    TTaggedLogger(const TTaggedLogger& other);

    const Stroka& GetCategory() const;
    bool IsEnabled(ELogLevel level) const;
    void Write(TLogEvent&& event);

    void AddTag(const Stroka& tag);

private:
    Stroka GetTaggedMessage(const Stroka& originalMessage) const;

    TLogger* InnerLogger;
    Stroka Tags;

    Stroka Empty;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
