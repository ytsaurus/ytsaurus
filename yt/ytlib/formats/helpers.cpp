#include "helpers.h"

#include <ytlib/misc/error.h>

#include <ytlib/yson/yson_format.h>
#include <ytlib/ytree/yson_string.h>
#include <ytlib/yson/token.h>

namespace NYT {

using namespace NYTree;
using namespace NYson;

namespace NFormats {

////////////////////////////////////////////////////////////////////////////////

TFormatsConsumerBase::TFormatsConsumerBase()
    : StatelessParser(this)
{ }
    

void TFormatsConsumerBase::OnRaw(const TStringBuf& yson, EYsonType type)
{
    StatelessParser.Parse(yson, type);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
