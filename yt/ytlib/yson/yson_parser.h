#pragma once

#include "public.h"

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

class TYsonParser
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

class TYsonStatelessParser
{
public:
    TYsonStatelessParser(
        IYsonConsumer* consumer,
        bool enableLinePositionInfo = false);

    ~TYsonStatelessParser();

    void Parse(const TStringBuf& data, EYsonType type = EYsonType::Node);

private:
    class TImpl;
    THolder<TImpl> Impl;
};

////////////////////////////////////////////////////////////////////////////////

void ParseStringBuf(const TStringBuf& buffer, IYsonConsumer* consumer, EYsonType type = EYsonType::Node, bool enableLinePositionInfo = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
