#pragma once

#include "public.h"

#include <ytlib/misc/property.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonParser
{
public:
    DECLARE_ENUM(EPhase,
        (NotStarted)
        (InProgress)
        (AwaitingAttributes)
        (Parsed)
    );

    TYsonParser(IYsonConsumer* consumer, bool fragmented = false);
    ~TYsonParser();

    void Consume(char ch);
    void Finish();

private:
    class TImpl;
    THolder<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////
            
void ParseYson(
    TInputStream* input,
    IYsonConsumer* consumer,
    bool fragmented = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYtree
} // namespace NYT
