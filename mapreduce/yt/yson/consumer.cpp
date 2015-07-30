#include "consumer.h"

#include "parser.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TYsonConsumerBase::OnRaw(const TStringBuf& yson, EYsonType type)
{
    UNUSED(yson);
    UNUSED(type);
    /*
    TYsonParser parser(this, type);
    parser.Read(yson);
    parser.Finish();
    */
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
