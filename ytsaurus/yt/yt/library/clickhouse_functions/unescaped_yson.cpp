#include "unescaped_yson.h"

namespace NYT::NClickHouseServer {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

EYsonFormat GetBaseYsonFormat(EExtendedYsonFormat format)
{
    switch (format) {
        case EExtendedYsonFormat::Binary:
            return EYsonFormat::Binary;
        case EExtendedYsonFormat::Text:
        case EExtendedYsonFormat::UnescapedText:
            return EYsonFormat::Text;
        case EExtendedYsonFormat::Pretty:
        case EExtendedYsonFormat::UnescapedPretty:
            return EYsonFormat::Pretty;
    }
}

bool IsUnescapedYsonFormat(EExtendedYsonFormat format)
{
    return format == EExtendedYsonFormat::UnescapedText
        || format == EExtendedYsonFormat::UnescapedPretty;
}

////////////////////////////////////////////////////////////////////////////////

TExtendedYsonWriter::TExtendedYsonWriter(
    IOutputStream* stream,
    EExtendedYsonFormat format,
    EYsonType type,
    bool enableRaw,
    int indent)
    : TYsonWriter(
        stream,
        GetBaseYsonFormat(format),
        type,
        enableRaw,
        indent)
    , Unescaped_(IsUnescapedYsonFormat(format))
{ }

void TExtendedYsonWriter::OnStringScalar(TStringBuf value)
{
    if (!Unescaped_) {
        TYsonWriter::OnStringScalar(value);
    } else {
        Stream_->Write('"');

        Buffer_.clear();

        for (char c : value) {
            switch (c) {
                case '\\':
                    Buffer_ += "\\\\";
                    break;
                case '\"':
                    Buffer_ += "\\\"";
                    break;
                case '\n':
                    Buffer_ += "\\n";
                    break;
                case '\r':
                    Buffer_ += "\\r";
                    break;
                default:
                    Buffer_ += c;
            }
        }

        Stream_->Write(Buffer_);

        Stream_->Write('"');
        EndNode();
    }
}

////////////////////////////////////////////////////////////////////////////////

}
