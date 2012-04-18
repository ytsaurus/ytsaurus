#include "stdafx.h"
#include "composite_meta_state_detail.h"

#include <ytlib/misc/protobuf_helpers.h>

namespace NYT {
namespace NMetaState {

using namespace NProto;

///////////////////////////////////////////////////////////////////////////////

TBlob SerializeChange(const NProto::TMsgChangeHeader& header, TRef messageData)
{
    TFixedChangeHeader fixedHeader;
    fixedHeader.HeaderSize = header.ByteSize();
    fixedHeader.MessageSize = messageData.Size();

    const size_t allocatedSize = sizeof(TFixedChangeHeader) + fixedHeader.HeaderSize + fixedHeader.MessageSize;    
    TBlob data(allocatedSize);

    YASSERT(data.max_size() >= allocatedSize);

    std::copy(
        reinterpret_cast<ui8*>(&fixedHeader),
        reinterpret_cast<ui8*>(&fixedHeader + 1),
        data.begin());
    YVERIFY(header.SerializeToArray(
        data.begin() + sizeof (TFixedChangeHeader),
        fixedHeader.HeaderSize));
    std::copy(
        messageData.Begin(),
        messageData.End(),
        data.begin() + sizeof (TFixedChangeHeader) + fixedHeader.HeaderSize);
    return data;
}

void DeserializeChangeHeader(
    TRef changeData,
    TMsgChangeHeader* header)
{
    auto* fixedHeader = reinterpret_cast<TFixedChangeHeader*>(changeData.Begin());
    YVERIFY(DeserializeFromProto(
        header,
        TRef(changeData.Begin() + sizeof (fixedHeader), fixedHeader->HeaderSize)));
}

void DeserializeChange(
    TRef changeData,
    TMsgChangeHeader* header,
    TRef* messageData)
{
    auto* fixedHeader = reinterpret_cast<TFixedChangeHeader*>(changeData.Begin());

    YVERIFY(DeserializeFromProto(
        header,
        TRef(changeData.Begin() + sizeof (TFixedChangeHeader), fixedHeader->HeaderSize)));

    *messageData = TRef(
        changeData.Begin() + sizeof (TFixedChangeHeader) + fixedHeader->HeaderSize,
        fixedHeader->MessageSize);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
