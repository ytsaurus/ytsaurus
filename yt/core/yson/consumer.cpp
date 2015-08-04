#include "stdafx.h"
#include "consumer.h"
#include "string.h"
#include "parser.h"

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

void IYsonConsumer::OnRaw(const TYsonString& str)
{
    OnRaw(str.Data(), str.GetType());
}

////////////////////////////////////////////////////////////////////////////////

void TYsonConsumerBase::OnRaw(const TStringBuf& yson, EYsonType type)
{
    ParseYsonStringBuffer(yson, this, type);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
