#include "parser.h"
#include "consumer.h"
#include "format.h"
#include "parser_detail.h"

namespace NYT {
namespace NYson {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TYsonParser::TImpl
{
private:
    typedef TCoroutine<int(const char* begin, const char* end, bool finish)> TParserCoroutine;

    TParserCoroutine ParserCoroutine_;
    TParserYsonStreamImpl<IYsonConsumer, TBlockReader<TParserCoroutine>> Parser_;

public:
    TImpl(
        IYsonConsumer* consumer,
        EYsonType parsingMode,
        bool enableLinePositionInfo,
        i64 memoryLimit,
        bool enableContext)
        : ParserCoroutine_(BIND(
            [=] (TParserCoroutine& self, const char* begin, const char* end, bool finish) {
                Parser_.DoParse(
                    TBlockReader<TParserCoroutine>(self, begin, end, finish),
                    consumer,
                    parsingMode,
                    enableLinePositionInfo,
                    memoryLimit,
                    enableContext);
            }))
    { }

    void Read(const char* begin, const char* end, bool finish = false)
    {
        if (!ParserCoroutine_.IsCompleted()) {
            ParserCoroutine_.Run(begin, end, finish);
        } else {
            THROW_ERROR_EXCEPTION("Input is already parsed");
        }
    }

    void Read(const TStringBuf& data, bool finish = false)
    {
        Read(data.begin(), data.end(), finish);
    }

    void Finish()
    {
        Read(0, 0, true);
    }

    const char* GetCurrentPositionInBlock()
    {
        return Parser_.GetCurrentPositionInBlock();
    }
};

////////////////////////////////////////////////////////////////////////////////

TYsonParser::TYsonParser(
    IYsonConsumer* consumer,
    EYsonType type,
    bool enableLinePositionInfo,
    i64 memoryLimit,
    bool enableContext)
    : Impl(std::make_unique<TImpl>(
        consumer,
        type,
        enableLinePositionInfo,
        memoryLimit,
        enableContext))
{ }

TYsonParser::~TYsonParser()
{ }

void TYsonParser::Read(const char* begin, const char* end, bool finish)
{
    Impl->Read(begin, end, finish);
}

void TYsonParser::Read(const TStringBuf& data)
{
    Impl->Read(data);
}

void TYsonParser::Finish()
{
    Impl->Finish();
}

const char* TYsonParser::GetCurrentPositionInBlock()
{
    return Impl->GetCurrentPositionInBlock();
}

////////////////////////////////////////////////////////////////////////////////

class TStatelessYsonParser::TImpl
{
private:
    const std::unique_ptr<TStatelessYsonParserImplBase> Impl;

public:
    TImpl(
        IYsonConsumer* consumer,
        bool enableLinePositionInfo,
        i64 memoryLimit,
        bool enableContext)
        : Impl([=] () -> TStatelessYsonParserImplBase* {
            if (enableContext && enableLinePositionInfo) {
                return new TStatelessYsonParserImpl<IYsonConsumer, 64, true>(consumer, memoryLimit);
            } else if (enableContext && !enableLinePositionInfo) {
                return new TStatelessYsonParserImpl<IYsonConsumer, 64, false>(consumer, memoryLimit);
            } else if (!enableContext && enableLinePositionInfo) {
                return new TStatelessYsonParserImpl<IYsonConsumer, 0, true>(consumer, memoryLimit);
            } else {
                return new TStatelessYsonParserImpl<IYsonConsumer, 0, false>(consumer, memoryLimit);
            }
        }())
    { }

    void Parse(const TStringBuf& data, EYsonType type = EYsonType::Node)
    {
        Impl->Parse(data, type);
    }

    void Stop()
    {
        Impl->Stop();
    }
};

////////////////////////////////////////////////////////////////////////////////

TStatelessYsonParser::TStatelessYsonParser(
    IYsonConsumer* consumer,
    bool enableLinePositionInfo,
    i64 memoryLimit,
    bool enableContext)
    : Impl(new TImpl(consumer, enableLinePositionInfo, memoryLimit, enableContext))
{ }

TStatelessYsonParser::~TStatelessYsonParser()
{ }

void TStatelessYsonParser::Parse(const TStringBuf& data, EYsonType type)
{
    Impl->Parse(data, type);
}

void TStatelessYsonParser::Stop()
{
    Impl->Stop();
}

////////////////////////////////////////////////////////////////////////////////

void ParseYsonStringBuffer(
    const TStringBuf& buffer,
    EYsonType type,
    IYsonConsumer* consumer,
    bool enableLinePositionInfo,
    i64 memoryLimit,
    bool enableContext)
{
    TParserYsonStreamImpl<IYsonConsumer, TStringReader> Parser;
    Parser.DoParse(
        TStringReader(buffer.begin(), buffer.end()),
        consumer,
        type,
        enableLinePositionInfo,
        memoryLimit,
        enableContext);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
