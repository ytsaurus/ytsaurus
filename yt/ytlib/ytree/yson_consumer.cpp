#include "stdafx.h"
#include "yson_consumer.h"

#include "yson_parser.h"

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

void TYsonConsumerBase::OnRaw(const TStringBuf& yson, EYsonType type)
{
    ParseYson(yson, this, type);
}

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NYTree
} // namespace NYT
