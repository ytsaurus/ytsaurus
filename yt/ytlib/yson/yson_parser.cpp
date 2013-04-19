#include "stdafx.h"
#include "yson_parser.h"

#include "yson_consumer.h"
#include "yson_format.h"
#include "yson_parser_detail.h"

#include <ytlib/misc/foreach.h>
#include <ytlib/misc/error.h>

#include <stack>

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

class TYsonParser::TImpl
{
private:
    typedef TYsonParserImpl<IYsonConsumer> TParser;
    TParser Parser;

public:
    TImpl(IYsonConsumer* consumer, EYsonType type, bool enableLinePositionInfo)
        : Parser(consumer, type, enableLinePositionInfo)
    { }
    
    void Read(const TStringBuf& data)
    {
        Parser.Read(data);
    }

    void Finish()
    {
        Parser.Finish();
    }
};

////////////////////////////////////////////////////////////////////////////////

TYsonParser::TYsonParser(
    IYsonConsumer *consumer,
    EYsonType type,
    bool enableLinePositionInfo)
    : Impl(new TImpl(consumer, type, enableLinePositionInfo))
{ }

TYsonParser::~TYsonParser()
{ }

void TYsonParser::Read(const TStringBuf& data)
{
    Impl->Read(data);
}

void TYsonParser::Finish()
{
    Impl->Finish();
}

////////////////////////////////////////////////////////////////////////////////

class TYsonStatelessParser::TImpl
{
private:
    THolder<TYsonStatelessParserImplBase> Impl;

public:
    TImpl(IYsonConsumer *consumer, bool enableLinePositionInfo)
        : Impl(enableLinePositionInfo? 
        static_cast<TYsonStatelessParserImplBase*>(new TYsonStatelessParserImpl<IYsonConsumer, true>(consumer)) 
        : static_cast<TYsonStatelessParserImplBase*>(new TYsonStatelessParserImpl<IYsonConsumer, false>(consumer)))
    { }

    void Parse(const TStringBuf& data, EYsonType type = EYsonType::Node) 
    {
        Impl->Parse(data, type);
    }
};

////////////////////////////////////////////////////////////////////////////////

TYsonStatelessParser::TYsonStatelessParser(
    IYsonConsumer *consumer,
    bool enableLinePositionInfo)
    : Impl(new TImpl(consumer, enableLinePositionInfo))
{ }

TYsonStatelessParser::~TYsonStatelessParser()
{ }

void TYsonStatelessParser::Parse(const TStringBuf& data, EYsonType type)
{
    Impl->Parse(data, type);
}

////////////////////////////////////////////////////////////////////////////////

void ParseStringBuf(const TStringBuf& buffer, IYsonConsumer* consumer, EYsonType type, bool enableLinePositionInfo)
{
    ParseStreamImpl<IYsonConsumer, TStringReader>(TStringReader(buffer.begin(), buffer.end()), consumer, type, enableLinePositionInfo);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
