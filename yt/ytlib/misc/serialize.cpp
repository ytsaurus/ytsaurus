#include "serialize.h"

namespace NYT
{

////////////////////////////////////////////////////////////////////////////////

// we store TGuid in "bytes" protobuf type which is mapped to Stroka

TGuid GuidFromProtoGuid(const Stroka& protoGuid)
{
    return *(TGuid*) protoGuid.data();
}

Stroka ProtoGuidFromGuid(const TGuid& guid)
{
    Stroka tmp;
    const char* p = (const char*) &guid;
    tmp.assign(p, p + sizeof(TGuid));
    return tmp;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
