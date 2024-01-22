#ifndef PREPARED_META_INL_H_
#error "Direct inclusion of this file is not allowed, include prepared_meta.h"
// For the sake of sane code completion.
#include "prepared_meta.h"
#endif
#undef PREPARED_META_INL_H_

namespace NYT::NColumnarChunkFormat {

////////////////////////////////////////////////////////////////////////////////

bool TMultiValueIndexMeta::IsDense() const
{
    return ExpectedPerRow != static_cast<ui32>(-1);
}

template <EValueType Type>
void TValueMeta<Type>::InitFromProto(const NProto::TSegmentMeta& meta, const ui64* ptr)
{
    ptr = TMultiValueIndexMeta::InitFromProto(meta, ptr, false);
    TDataMeta<Type>::InitFromProto(meta, ptr);
}

template <EValueType Type>
void TAggregateValueMeta<Type>::InitFromProto(const NProto::TSegmentMeta& meta, const ui64* ptr)
{
    ptr = TMultiValueIndexMeta::InitFromProto(meta, ptr, true);
    TDataMeta<Type>::InitFromProto(meta, ptr);
}

template <EValueType Type>
void TKeyMeta<Type>::InitFromProto(const NProto::TSegmentMeta& meta, const ui64* ptr)
{
    if constexpr (IsStringLikeType(Type)) {
        ptr = TKeyIndexMeta::InitFromProto(meta, Type, ptr);
        TDataMeta<Type>::InitFromProto(meta, ptr);
    } else {
        ptr = TDataMeta<Type>::InitFromProto(meta, ptr);
        TKeyIndexMeta::InitFromProto(meta, Type, ptr);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TRawMeta>
void VerifyRawSegmentMeta(
    const NProto::TSegmentMeta& protoMeta,
    TRange<TSharedRef> segmentData,
    const TRawMeta& rawMeta)
{
    TRawMeta rawMetaFromProto;
    memset(&rawMetaFromProto, 0, sizeof(rawMetaFromProto));

    auto segmentSize = GetByteSize(segmentData);
    std::vector<char> plainSegmentData;
    plainSegmentData.reserve(segmentSize);
    for (const auto& part : segmentData) {
        plainSegmentData.insert(plainSegmentData.end(), part.Begin(), part.End());
    }
    rawMetaFromProto.InitFromProto(protoMeta, reinterpret_cast<const ui64*>(plainSegmentData.data()));

    for (size_t i = 0; i < sizeof(TRawMeta); ++i) {
        auto convertedFromProtoByte = reinterpret_cast<const char*>(&rawMetaFromProto)[i];
        auto rawMetaByte = reinterpret_cast<const char*>(&rawMeta)[i];

        YT_VERIFY(convertedFromProtoByte == rawMetaByte);
    }
}

template <EValueType Type>
void VerifyRawVersionedSegmentMeta(
    const NProto::TSegmentMeta& protoMeta,
    TRange<TSharedRef> segmentData,
    const TValueMeta<Type>& rawMeta,
    bool aggregate)
{
    if (aggregate) {
        const auto& aggregateMeta = static_cast<const TAggregateValueMeta<Type>&>(rawMeta);
        VerifyRawSegmentMeta(protoMeta, segmentData, aggregateMeta);
    } else {
        VerifyRawSegmentMeta(protoMeta, segmentData, rawMeta);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NColumnarChunkFormat
