#pragma once

#include "public.h"
#include "parser.h"

#include <ytlib/misc/property.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class TYsonParser
    : public IParser
{
public:
    TYsonParser(
        IYsonConsumer* consumer,
        EYsonType type = EYsonType::Node,
        bool enableLinePositionInfo = false);
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
    EYsonType type = EYsonType::Node,
    bool enableLinePositionInfo = false);

void ParseYson(
    const TStringBuf& yson,
    IYsonConsumer* consumer,
    EYsonType type = EYsonType::Node,
    bool enableLinePositionInfo = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYtree
} // namespace NYT
