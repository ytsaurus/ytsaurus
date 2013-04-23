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

} // namespace NFormats
} // namespace NYT
