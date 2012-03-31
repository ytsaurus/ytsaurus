#pragma once

#include "public.h"

#include <ytlib/misc/property.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonParser
{
public:
    DECLARE_ENUM(EMode,
        (Node)
        (ListFragment)
        (MapFragment)
    );

    TYsonParser(IYsonConsumer* consumer, EMode mode = EMode::Node);
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
    TYsonParser::EMode mode = TYsonParser::EMode::Node);

void ParseYson(
    const TYson& yson,
    IYsonConsumer* consumer,
    TYsonParser::EMode mode = TYsonParser::EMode::Node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYtree
} // namespace NYT
