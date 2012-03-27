#include "stdafx.h"
#include "composite_meta_state_detail.h"

#include <ytlib/misc/serialize.h>

namespace NYT {
namespace NMetaState {

using namespace NProto;

///////////////////////////////////////////////////////////////////////////////

void DeserializeChangeHeader(
    TRef changeData,
    TMsgChangeHeader* header)
{
    auto* fixedHeader = reinterpret_cast<TFixedChangeHeader*>(changeData.Begin());
    YVERIFY(DeserializeFromProtobuf(
        header,
        TRef(changeData.Begin() + sizeof (fixedHeader), fixedHeader->HeaderSize)));
}

void DeserializeChange(
    TRef changeData,
    TMsgChangeHeader* header,
    TRef* messageData)
{
    auto* fixedHeader = reinterpret_cast<TFixedChangeHeader*>(changeData.Begin());

    YVERIFY(DeserializeFromProtobuf(
        header,
        TRef(changeData.Begin() + sizeof (TFixedChangeHeader), fixedHeader->HeaderSize)));

    *messageData = TRef(
        changeData.Begin() + sizeof (TFixedChangeHeader) + fixedHeader->HeaderSize,
        fixedHeader->MessageSize);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
