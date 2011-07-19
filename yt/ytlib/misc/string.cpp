#include "string.h"
#include "serialize.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

Stroka StringFromProtoGuid(const Stroka& protoGuid)
{
    return GuidFromProtoGuid(protoGuid).ToString();
}

Stroka StringFromGUID(const TGUID& guid)
{
    return GetGuidAsString(guid);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
