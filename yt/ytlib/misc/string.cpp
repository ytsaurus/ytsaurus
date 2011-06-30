#include "string.h"
#include "serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

Stroka StringFromProtoGuid(const Stroka& protoGuid)
{
    return GetGuidAsString(GuidFromProtoGuid(protoGuid));
}

Stroka StringFromGuid(const TGUID& guid)
{
    return GetGuidAsString(guid);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
