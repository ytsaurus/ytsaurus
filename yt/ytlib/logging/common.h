#pragma once

#include "../misc/common.h"
#include "../misc/common.h"
#include "../misc/enum.h"

namespace NYT {
namespace NLog {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(ELogLevel,
    (Minimum)
    (Trace)
    (Debug)
    (Info)
    (Warning)
    (Error)
    (Fatal)
    (Maximum)
);

class TLogEvent
{
public:
    typedef TPair<Stroka, Stroka> TProperty;
    typedef yvector<TProperty> TProperties;

    TLogEvent(Stroka category, ELogLevel level, Stroka message);

    void AddProperty(Stroka name, Stroka value);

    Stroka GetCategory() const;
    ELogLevel GetLevel() const;
    Stroka GetMessage() const;
    TInstant GetDateTime() const;
    const TProperties& GetProperties() const;

private:
    Stroka Category;
    ELogLevel Level;
    Stroka Message;
    TInstant DateTime;
    TProperties Properties;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLog
} // namespace NYT
