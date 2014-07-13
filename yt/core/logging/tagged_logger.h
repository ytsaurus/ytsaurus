#pragma once

#include "log.h"

#include <core/misc/property.h>
#include <core/misc/format.h>

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

    void AddRawTag(const Stroka& tag);

    template <class... TArgs>
    void AddTag(const char* format, const TArgs&... args)
    {
        AddRawTag(Format(format, args...));
    }

private:
    Stroka GetTaggedMessage(const Stroka& originalMessage) const;

    TLogger* InnerLogger;
    Stroka Tags;

    Stroka Empty;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
