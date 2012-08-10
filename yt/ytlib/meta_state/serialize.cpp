#include "stdafx.h"
#include "serialize.h"

#include <ytlib/misc/protobuf_helpers.h>

namespace NYT {
namespace NMetaState {

///////////////////////////////////////////////////////////////////////////////

TSharedRef SerializeMutationRecord(
    const NProto::TMutationHeader& mutationHeader,
    const TRef& data)
{
    TMutationRecordHeader recordHeader;
    recordHeader.HeaderSize = mutationHeader.ByteSize();
    recordHeader.DataSize = data.Size();

    size_t recordSize =
        sizeof(TMutationRecordHeader) +
        recordHeader.HeaderSize +
        recordHeader.DataSize;

    TSharedRef recordData(recordSize);

    YASSERT(recordData.Size() >= recordSize);

    std::copy(
        reinterpret_cast<ui8*>(&recordHeader),
        reinterpret_cast<ui8*>(&recordHeader + 1),
        recordData.Begin());
    YCHECK(mutationHeader.SerializeToArray(
        recordData.Begin() + sizeof (TMutationRecordHeader),
        recordHeader.HeaderSize));
    std::copy(
        data.Begin(),
        data.End(),
        recordData.Begin() + sizeof (TMutationRecordHeader) + recordHeader.HeaderSize);

    return recordData;
}

void DeserializeMutationRecord(
    const TSharedRef& recordData,
    NProto::TMutationHeader* mutationHeader,
    TSharedRef* mutationData)
{
    auto* recordHeader = reinterpret_cast<const TMutationRecordHeader*>(recordData.Begin());

    YCHECK(DeserializeFromProto(
        mutationHeader,
        TRef(const_cast<char*>(recordData.Begin()) + sizeof (TMutationRecordHeader), recordHeader->HeaderSize)));

    *mutationData = TSharedRef(
        recordData, 
        TRef(
            const_cast<char*>(recordData.Begin()) + sizeof (TMutationRecordHeader) + recordHeader->HeaderSize,
            recordHeader->DataSize));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
