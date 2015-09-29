#include "helpers.h"

#include "format.h"

#include <core/misc/error.h>

#include <core/yson/format.h>
#include <core/yson/string.h>
#include <core/yson/token.h>

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

bool IsSpecialJsonKey(const TStringBuf& key)
{
    return key.size() > 0 && key[0] == '$';
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
