#include "stdafx.h"
#include "consumer.h"

#include "parser.h"

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

void TYsonConsumerBase::OnRaw(const TStringBuf& yson, EYsonType type)
{
    ParseYsonStringBuffer(yson, this, type);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
