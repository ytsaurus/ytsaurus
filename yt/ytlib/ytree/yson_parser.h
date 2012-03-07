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
    TYsonParser(IYsonConsumer* consumer);
    ~TYsonParser();

    void Consume(char ch);
    void Finish();

    //! Returns true iff the parser is awaiting for more chars
    bool IsAwaiting(bool ignoreAttributes = true) const;

private:
    class TImpl;
    THolder<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////
            
void ParseYson(TInputStream* input, IYsonConsumer* consumer);

////////////////////////////////////////////////////////////////////////////////

class TYsonFragmentParser
{
public:
    TYsonFragmentParser(IYsonConsumer* consumer, TInputStream* input);

    bool HasNext() const;
    void ParseNext();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYtree
} // namespace NYT
