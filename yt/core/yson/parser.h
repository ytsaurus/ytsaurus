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
    std::unique_ptr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

class TStatelessYsonParser
{
public:
    TStatelessYsonParser(
        IYsonConsumer* consumer,
        bool enableLinePositionInfo = false);

    ~TStatelessYsonParser();

    void Parse(const TStringBuf& data, EYsonType type = EYsonType::Node);

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

void ParseYsonStringBuffer(
    const TStringBuf& buffer,
    IYsonConsumer* consumer,
    EYsonType type = EYsonType::Node,
    bool enableLinePositionInfo = false);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
