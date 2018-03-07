#pragma once

#include "public.h"

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/ref.h>

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
        i64 memoryLimit = std::numeric_limits<i64>::max(),
        bool enableContext = true);

    ~TYsonParser();

    void Read(const char* begin, const char* end, bool finish = false);
    void Read(const TStringBuf& data);
    void Finish();

    const char* GetCurrentPositionInBlock();

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
        i64 memoryLimit = std::numeric_limits<i64>::max(),
        bool enableContext = true);

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
    i64 memoryLimit = std::numeric_limits<i64>::max(),
    bool enableContext = true);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
