#ifndef COMPOSITE_META_STATE_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include composite_meta_state_detail.h"
#endif

#include "../misc/assert.h"

namespace NYT {
namespace NMetaState {

using namespace NProto;

///////////////////////////////////////////////////////////////////////////////

template <class TMessage>
TBlob SerializeChange(
    const NMetaState::NProto::TMsgChangeHeader& header,
    const TMessage& message)
{
    TFixedChangeHeader fixedHeader;
    fixedHeader.HeaderSize = header.ByteSize();
    fixedHeader.MessageSize = message.ByteSize();

    TBlob data(sizeof (TFixedChangeHeader) + fixedHeader.HeaderSize + fixedHeader.MessageSize);

    Copy(
        reinterpret_cast<char*>(&fixedHeader),
        sizeof (TFixedChangeHeader),
        data.begin());
    YVERIFY(header.SerializeToArray(
        data.begin() + sizeof (TFixedChangeHeader),
        fixedHeader.HeaderSize));
    YVERIFY(message.SerializeToArray(
        data.begin() + sizeof (TFixedChangeHeader) + fixedHeader.HeaderSize,
        fixedHeader.MessageSize));
    return data;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
