#include "serialize.h"

#include <yt/core/misc/protobuf_helpers.h>

namespace NYT {
namespace NHydra {

////////////////////////////////////////////////////////////////////////////////

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
    auto recordData = TSharedMutableRef::Allocate<TMutationRecordTag>(recordSize, false);
    Y_ASSERT(recordData.Size() >= recordSize);

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

    size_t headerStartOffset = sizeof (TFixedMutationHeader);
    size_t headerEndOffset = headerStartOffset + recordHeader->HeaderSize;
    DeserializeProto(mutationHeader, recordData.Slice(headerStartOffset, headerEndOffset));

    size_t dataStartOffset = headerEndOffset;
    size_t dataEndOffset = dataStartOffset + recordHeader->DataSize;
    *mutationData = recordData.Slice(dataStartOffset, dataEndOffset);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
