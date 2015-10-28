#pragma once

#include "public.h"

#include <core/misc/nullable.h>

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

class TYsonParser
{
public:
    TYsonParser(
        IYsonConsumer* consumer,
        EYsonType type = EYsonType::Node,
        bool enableLinePositionInfo = false,
        i64 memoryLimit = std::numeric_limits<i64>::max());

    ~TYsonParser();

    void Read(const TStringBuf& data);
    void Finish();

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

class TStatelessYsonParser
{
public:
    TStatelessYsonParser(
        IYsonConsumer* consumer,
        bool enableLinePositionInfo = false,
        i64 memoryLimit = std::numeric_limits<i64>::max());

    ~TStatelessYsonParser();

    void Parse(const TStringBuf& data, EYsonType type = EYsonType::Node);

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

void ParseYsonStringBuffer(
    const TStringBuf& buffer,
    EYsonType type,
    IYsonConsumer* consumer,
    bool enableLinePositionInfo = false,
    i64 memoryLimit = std::numeric_limits<i64>::max());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
