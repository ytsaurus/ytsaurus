#include "serialize.h"

namespace NYT
{

////////////////////////////////////////////////////////////////////////////////

// we store TGUID in "bytes" protobuf type which is mapped to Stroka

TGUID GuidFromProtoGuid(const Stroka& protoGuid)
{
    return *(TGUID*) protoGuid.data();
}

Stroka ProtoGuidFromGuid(const TGUID& guid)
{
    Stroka tmp;
    const char* p = (const char*) &guid;
    tmp.assign(p, p + sizeof(TGUID));
    return tmp;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
