#ifndef COMPOSITE_META_STATE_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include composite_meta_state_detail.h"
#endif

namespace NYT {
namespace NMetaState {

///////////////////////////////////////////////////////////////////////////////

template <class TMessage>
TBlob SerializeChange(
    const NMetaState::NProto::TChangeHeader& header,
    const TMessage& message)
{
    TFixedChangeHeader fixedHeader;
    fixedHeader.HeaderSize = header.ByteSize();
    fixedHeader.MessageSize = message.ByteSize();

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
    YVERIFY(message.SerializeToArray(
        data.begin() + sizeof (TFixedChangeHeader) + fixedHeader.HeaderSize,
        fixedHeader.MessageSize));
    return data;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
