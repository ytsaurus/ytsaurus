#include "json_parser.h"

namespace NYT {
namespace NFormats {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TJsonParser::TImpl
{
public:
    TImpl(IYsonConsumer*, TJsonFormatConfigPtr, EYsonType)
    {
        YUNREACHABLE();
    }

    void Read(const TStringBuf&)
    {
        YUNREACHABLE();
    }

    void Finish()
    {
        YUNREACHABLE();
    }

    void Parse(TInputStream*)
    {
        YUNREACHABLE();
    }
};

////////////////////////////////////////////////////////////////////////////////

TJsonParser::TJsonParser(
    IYsonConsumer* consumer,
    TJsonFormatConfigPtr config,
    EYsonType type)
    : Impl_(new TImpl(consumer, config, type))
{ }

void TJsonParser::Read(const TStringBuf& data)
{
    Impl_->Read(data);
}

void TJsonParser::Finish()
{
    Impl_->Finish();
}

void TJsonParser::Parse(TInputStream* input)
{
    Impl_->Parse(input);
}

////////////////////////////////////////////////////////////////////////////////

void ParseJson(
    TInputStream* input,
    IYsonConsumer* consumer,
    TJsonFormatConfigPtr config,
    EYsonType type)
{
    TJsonParser jsonParser(consumer, config, type);
    jsonParser.Parse(input);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
