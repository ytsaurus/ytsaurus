#pragma once

#include "public.h"

#include <ytlib/misc/property.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonParser
{
public:
    TYsonParser(IYsonConsumer* consumer, EYsonType type = EYsonType::Node);
    ~TYsonParser();

    void Read(const TStringBuf& data);
    void Finish();

private:
    class TImpl;
    THolder<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////
            
void ParseYson(
    TInputStream* input,
    IYsonConsumer* consumer,
    EYsonType type = EYsonType::Node);

void ParseYson(
    const TStringBuf& yson,
    IYsonConsumer* consumer,
    EYsonType type = EYsonType::Node);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYtree
} // namespace NYT
