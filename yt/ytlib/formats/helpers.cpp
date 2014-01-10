#include "helpers.h"

#include <ytlib/misc/error.h>

#include <ytlib/yson/format.h>
#include <ytlib/ytree/yson_string.h>
#include <ytlib/yson/token.h>

namespace NYT {

using namespace NYTree;
using namespace NYson;

namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

TFormatsConsumerBase::TFormatsConsumerBase()
    : Parser(this)
{ }
    

void TFormatsConsumerBase::OnRaw(const TStringBuf& yson, EYsonType type)
{
    Parser.Parse(yson, type);
}

////////////////////////////////////////////////////////////////////////////////

bool IsAscii(const TStringBuf& str)
{
    for (unsigned char sym : str) {
        if (sym >= 128) {
            return false;
        }
    }
    return true;
}

Stroka ByteStringToUtf8(const TStringBuf& str)
{
    char buf[3];
    buf[2] = 0;

    TStringStream os;
    for (unsigned char sym : str) {
        if (sym < 128) {
            os.Write(sym);
        } else {
            buf[0] = '\xC0' | (sym >> 6);
            buf[1] = '\x80' | (sym & ~'\xC0');
            os.Write(buf);
        }
    }
    return os.Str();
}

Stroka Utf8ToByteString(const TStringBuf& str)
{
    TStringStream output;
    for (int i = 0; i < str.size(); ++i) {
        if (ui8(str[i]) < 128) {
            output.Write(str[i]);
        } else if ((str[i] & '\xFC') == '\xC0') {
            output.Write(((str[i] & '\x03') << 6) | (str[i + 1] & '\x3F'));
            i += 1;
        } else {
            THROW_ERROR_EXCEPTION("Unicode symbols with codes greater than 255 are not supported");
        }
    }
    return output.Str();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
