#include "helpers.h"
#include "format.h"

namespace NYT {
namespace NFormats {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TFormatsConsumerBase::TFormatsConsumerBase()
    : Parser(this)
{ }

void TFormatsConsumerBase::OnRaw(const TStringBuf& yson, EYsonType type)
{
    Parser.Parse(yson, type);
}

void TFormatsConsumerBase::Flush()
{ }

////////////////////////////////////////////////////////////////////////////////

bool IsSpecialJsonKey(const TStringBuf& key)
{
    return key.size() > 0 && key[0] == '$';
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NFormats
} // namespace NYT
