#include "stdafx.h"
#include "consumer.h"

#include "parser.h"

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

void TYsonConsumerBase::OnRaw(const TStringBuf& yson, EYsonType type)
{
    TYsonParser parser(this, type);
    parser.Read(yson);
    parser.Finish();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
