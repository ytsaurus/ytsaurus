#include "parser.h"
#include "consumer.h"
#include "format.h"
#include "parser_detail.h"

#include <util/stream/input.h>
#include <util/generic/buffer.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TYsonParser::TImpl
{
public:
    TImpl(
        IYsonConsumer* consumer,
        TInputStream* stream,
        EYsonType type,
        bool enableLinePositionInfo,
        TMaybe<ui64> memoryLimit = Nothing())
        : Consumer_(consumer)
        , Stream_(stream)
        , Type_(type)
        , EnableLinePositionInfo_(enableLinePositionInfo)
        , MemoryLimit_(memoryLimit)
    { }

    void Parse()
    {
        TBuffer buffer(64 << 10);
        ParseYsonStreamImpl<IYsonConsumer, TStreamReader>(
            TStreamReader(Stream_, buffer.Data(), buffer.Capacity()),
            Consumer_,
            Type_,
            EnableLinePositionInfo_,
            MemoryLimit_);
    }

private:
    IYsonConsumer* Consumer_;
    TInputStream* Stream_;
    EYsonType Type_;
    bool EnableLinePositionInfo_;
    TMaybe<ui64> MemoryLimit_;
};

////////////////////////////////////////////////////////////////////////////////

TYsonParser::TYsonParser(
    IYsonConsumer *consumer,
    TInputStream* stream,
    EYsonType type,
    bool enableLinePositionInfo,
    TMaybe<ui64> memoryLimit)
    : Impl(new TImpl(consumer, stream, type, enableLinePositionInfo, memoryLimit))
{ }

TYsonParser::~TYsonParser()
{ }

void TYsonParser::Parse()
{
    Impl->Parse();
}

////////////////////////////////////////////////////////////////////////////////

class TStatelessYsonParser::TImpl
{
private:
    THolder<TStatelessYsonParserImplBase> Impl;

public:
    TImpl(
        IYsonConsumer *consumer,
        bool enableLinePositionInfo,
        TMaybe<ui64> memoryLimit)
        : Impl(
            enableLinePositionInfo
            ? static_cast<TStatelessYsonParserImplBase*>(new TStatelessYsonParserImpl<IYsonConsumer, true>(consumer, memoryLimit))
            : static_cast<TStatelessYsonParserImplBase*>(new TStatelessYsonParserImpl<IYsonConsumer, false>(consumer, memoryLimit)))
    { }

    void Parse(const TStringBuf& data, EYsonType type = YT_NODE)
    {
        Impl->Parse(data, type);
    }
};

////////////////////////////////////////////////////////////////////////////////

TStatelessYsonParser::TStatelessYsonParser(
    IYsonConsumer *consumer,
    bool enableLinePositionInfo,
    TMaybe<ui64> memoryLimit)
    : Impl(new TImpl(consumer, enableLinePositionInfo, memoryLimit))
{ }

TStatelessYsonParser::~TStatelessYsonParser()
{ }

void TStatelessYsonParser::Parse(const TStringBuf& data, EYsonType type)
{
    Impl->Parse(data, type);
}

////////////////////////////////////////////////////////////////////////////////

void ParseYsonStringBuffer(
    const TStringBuf& buffer,
    IYsonConsumer* consumer,
    EYsonType type,
    bool enableLinePositionInfo,
    TMaybe<ui64> memoryLimit)
{
    ParseYsonStreamImpl<IYsonConsumer, TStringReader>(
        TStringReader(buffer.begin(), buffer.end()),
        consumer,
        type,
        enableLinePositionInfo,
        memoryLimit);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
