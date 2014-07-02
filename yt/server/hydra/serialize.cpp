#include "stdafx.h"
#include "serialize.h"

#include <core/misc/protobuf_helpers.h>

namespace NYT {
namespace NHydra {

///////////////////////////////////////////////////////////////////////////////

#pragma pack(push, 4)

/*!
 *  Each mutation record has the following format:
 *  - TMutationRecordHeader
 *  - serialized NProto::TMutationHeader (of size #TMutationRecordHeader::HeaderSize)
 *  - custom mutation data (of size #TMutationRecordHeader::DataSize)
 */
struct TFixedMutationHeader
{
    i32 HeaderSize;
    i32 DataSize;
};

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

TSharedRef SerializeMutationRecord(
    const NProto::TMutationHeader& mutationHeader,
    const TRef& data)
{
    TFixedMutationHeader recordHeader;
    recordHeader.HeaderSize = mutationHeader.ByteSize();
    recordHeader.DataSize = data.Size();

    size_t recordSize =
        sizeof(TFixedMutationHeader) +
        recordHeader.HeaderSize +
        recordHeader.DataSize;

    struct TMutationRecordTag { };
    auto recordData = TSharedRef::Allocate<TMutationRecordTag>(recordSize, false);
    YASSERT(recordData.Size() >= recordSize);

    std::copy(
        reinterpret_cast<ui8*>(&recordHeader),
        reinterpret_cast<ui8*>(&recordHeader + 1),
        recordData.Begin());
    YCHECK(mutationHeader.SerializeToArray(
        recordData.Begin() + sizeof (TFixedMutationHeader),
        recordHeader.HeaderSize));
    std::copy(
        data.Begin(),
        data.End(),
        recordData.Begin() + sizeof (TFixedMutationHeader) + recordHeader.HeaderSize);

    return recordData;
}

void DeserializeMutationRecord(
    const TSharedRef& recordData,
    NProto::TMutationHeader* mutationHeader,
    TSharedRef* mutationData)
{
    auto* recordHeader = reinterpret_cast<const TFixedMutationHeader*>(recordData.Begin());

    YCHECK(DeserializeFromProto(
        mutationHeader,
        TRef(const_cast<char*>(recordData.Begin()) + sizeof (TFixedMutationHeader), recordHeader->HeaderSize)));

    TRef recordRef(
        const_cast<char*>(recordData.Begin()) + sizeof (TFixedMutationHeader) + recordHeader->HeaderSize,
        recordHeader->DataSize);
    *mutationData = recordData.Slice(recordRef);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
