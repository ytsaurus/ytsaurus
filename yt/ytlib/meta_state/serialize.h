#pragma once

#include "public.h"

#include <ytlib/misc/ref.h>
#include <ytlib/meta_state/meta_state_manager.pb.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////
    
#pragma pack(push, 4)

/*!
 *  Each mutation record has the following format:
 *  - TFixedMutationHeader
 *  - serialized NProto::TMutationHeader (of size #TFixedMutation::HeaderSize)
 *  - custom mutation data (of size #TFixedMutation::DataSize)
 */
struct TMutationRecordHeader
{
    i32 HeaderSize;
    i32 DataSize;
};

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

TSharedRef SerializeMutationRecord(
    const NProto::TMutationHeader& mutationHeader,
    const TRef& mutationData);

void DeserializeMutationRecord(
    const TSharedRef& recordData,
    NMetaState::NProto::TMutationHeader* mutationHeader,
    TSharedRef* mutationData);

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT

