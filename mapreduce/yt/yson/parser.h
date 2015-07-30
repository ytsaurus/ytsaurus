#pragma once

#include "public.h"

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>

class TInputStream;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TYsonParser
{
public:
    TYsonParser(
        IYsonConsumer* consumer,
        TInputStream* stream,
        EYsonType type = YT_NODE,
        bool enableLinePositionInfo = false,
        TMaybe<ui64> memoryLimit = Nothing());

    ~TYsonParser();

    void Parse();

private:
    class TImpl;
    THolder<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////

class TStatelessYsonParser
{
public:
    TStatelessYsonParser(
        IYsonConsumer* consumer,
        bool enableLinePositionInfo = false,
        TMaybe<ui64> memoryLimit = Nothing());

    ~TStatelessYsonParser();

    void Parse(const TStringBuf& data, EYsonType type = YT_NODE);

private:
    class TImpl;
    THolder<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////

void ParseYsonStringBuffer(
    const TStringBuf& buffer,
    IYsonConsumer* consumer,
    EYsonType type = YT_NODE,
    bool enableLinePositionInfo = false,
    TMaybe<ui64> memoryLimit = Nothing());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
