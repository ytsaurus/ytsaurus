#pragma once

#include "meta_state_manager.pb.h"

#include "../misc/ref.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////
    
#pragma pack(push, 4)

struct TFixedChangeHeader
{
    i32 HeaderSize;
    i32 MessageSize;
};

#pragma pack(pop)

template <class TMessage>
TBlob SerializeChange(
    const NMetaState::NProto::TMsgChangeHeader& header,
    const TMessage& message);

void DeserializeChangeHeader(
    TRef changeData,
    NMetaState::NProto::TMsgChangeHeader* header);

void DeserializeChange(
    TRef changeData,
    NMetaState::NProto::TMsgChangeHeader* header,
    TRef* messageData);

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
