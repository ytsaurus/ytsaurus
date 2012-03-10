#pragma once

#include "public.h"

#include <ytlib/misc/property.h>

#include <stack>

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

    TYsonParser(IYsonConsumer* consumer);
    ~TYsonParser();

    void Consume(char ch);
    void Finish();

    EPhase GetPhase() const;

private:
    class TImpl;
    THolder<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////
            
void ParseYson(TInputStream* input, IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

// TODO(roizner): Rename to Reader
class TYsonFragmentParser
{
public:
    TYsonFragmentParser(IYsonConsumer* consumer, TInputStream* input);

    bool HasNext();
    void ParseNext();

private:


    TYsonParser Parser;
    TInputStream* Input;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYtree
} // namespace NYT
