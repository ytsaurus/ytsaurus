#include "stdafx.h"
#include "yson_consumer.h"

#include "yson_parser.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

void TYsonConsumerBase::OnRaw(const TStringBuf& yson, EYsonType type)
{
    TYsonParser parser(this, type);
    parser.Read(yson);
    parser.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
