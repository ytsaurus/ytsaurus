#include "stdafx.h"
#include "yamr_parser.h"

namespace NYT {
namespace NFormats {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYamrParser::TYamrParser(IYsonConsumer* consumer, TYamrFormatConfigPtr config)
    : Consumer(consumer)
    , Config(config)
    , State(EState::InsideKey)
{
    if (!Config) {
        Config = New<TYamrFormatConfig>();
    }
}

void TYamrParser::Read(const TStringBuf& data)
{
    auto current = data.begin();
    while (current != data.end()) {
        current = Consume(current, data.end());
    }
}

void TYamrParser::Finish()
{ }

const char* TYamrParser::Consume(const char* begin, const char* end)
{ }

////////////////////////////////////////////////////////////////////////////////

void ParseYamr(
    TInputStream* input,
    NYTree::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config)
{ }

void ParseYamr(
    const TStringBuf& data,
    NYTree::IYsonConsumer* consumer,
    TYamrFormatConfigPtr config)
{ }

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NFormats
} // namespace NYT
