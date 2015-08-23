#pragma once

#include "private.h"

#include <core/misc/public.h>
#include <core/misc/small_vector.h>
#include <core/misc/chunked_memory_allocator.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EPacketType, i16,
    ((Message)(0))
    ((Ack)    (1))
);

DEFINE_BIT_ENUM_WITH_UNDERLYING_TYPE(EPacketFlags, ui16,
    ((None)      (0x0000))
    ((RequestAck)(0x0001))
);

#pragma pack(push, 4)

const ui32 PacketSignature = 0x78616d6f;
const i32 MaxPacketPartCount = 1 << 28;
const i32 MaxPacketPartSize = DefaultEnvelopePartSize;
const i32 NullPacketPartSize = -1;
const int TypicalPacketPartCount = 64;

struct TPacketHeader
{
    ui32 Signature; // == PacketSignature
    EPacketType Type;
    EPacketFlags Flags;
    TPacketId PacketId;
};

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPacketPhase,
    (Unstarted)
    (Header)
    (PartCount)
    (PartSizes)
    (MessagePart)
    (Finished)
);

template <class TDerived>
class TPacketTranscoderBase
{
public:
    TMutableRef GetFragment();
    bool IsFinished() const;

protected:
    EPacketPhase Phase_ = EPacketPhase::Unstarted;
    char* FragmentPtr_ = nullptr;
    size_t FragmentRemaining_ = 0;
    TPacketHeader Header_;
    SmallVector<i32, TypicalPacketPartCount> PartSizes_;
    i32 PartCount_ = -1;
    int PartIndex_ = -1;
    TSharedRefArray Message_;

    void BeginPhase(EPacketPhase phase, void* fragment, size_t size);
    bool EndPhase();
    void SetFinished();

    TDerived* AsDerived();

};

////////////////////////////////////////////////////////////////////////////////

//! Enables asynchronous zero-copy packet parsing.
class TPacketDecoder
    : public TPacketTranscoderBase<TPacketDecoder>
{
public:
    TPacketDecoder();

    bool Advance(size_t size);
    void Restart();

    EPacketType GetPacketType() const;
    EPacketFlags GetPacketFlags() const;
    const TPacketId& GetPacketId() const;
    TSharedRefArray GetMessage() const;
    size_t GetPacketSize() const;

private:
    friend class TPacketTranscoderBase<TPacketDecoder>;

    TChunkedMemoryAllocator Allocator_;

    std::vector<TSharedRef> Parts_;

    size_t PacketSize_;

    bool EndHeaderPhase();
    bool EndPartCountPhase();
    bool EndPartSizesPhase();
    bool EndMessagePartPhase();
    void NextMessagePartPhase();

};

////////////////////////////////////////////////////////////////////////////////

//! Enables asynchronous zero-copy packet writing.
class TPacketEncoder
    : public TPacketTranscoderBase<TPacketEncoder>
{
public:
    TPacketEncoder();

    static i64 GetPacketSize(
        EPacketType type,
        const TSharedRefArray& message);

    bool Start(
        EPacketType type,
        EPacketFlags flags,
        const TPacketId& packetId,
        TSharedRefArray message);

    bool IsFragmentOwned() const;
    void NextFragment();

private:
    friend class TPacketTranscoderBase<TPacketEncoder>;

    bool EndHeaderPhase();
    bool EndPartCountPhase();
    bool EndPartSizesPhase();
    bool EndMessagePartPhase();

    void NextMessagePartPhase();

};

////////////////////////////////////////////////////////////////////////////////

template <class TDerived>
TMutableRef TPacketTranscoderBase<TDerived>::GetFragment()
{
    return TMutableRef(FragmentPtr_, FragmentRemaining_);
}

template <class TDerived>
bool TPacketTranscoderBase<TDerived>::IsFinished() const
{
    return Phase_ == EPacketPhase::Finished;
}

template <class TDerived>
void TPacketTranscoderBase<TDerived>::BeginPhase(EPacketPhase phase, void* buffer, size_t size)
{
    Phase_ = phase;
    FragmentPtr_ = static_cast<char*>(buffer);
    FragmentRemaining_ = size;
}

template <class TDerived>
void TPacketTranscoderBase<TDerived>::SetFinished()
{
    Phase_ = EPacketPhase::Finished;
    FragmentPtr_ = nullptr;
    FragmentRemaining_ = 0;
}

template <class TDerived>
bool TPacketTranscoderBase<TDerived>::EndPhase()
{
    switch (Phase_) {
        case EPacketPhase::Header:
            return AsDerived()->EndHeaderPhase();

        case EPacketPhase::PartCount:
            return AsDerived()->EndPartCountPhase();

        case EPacketPhase::PartSizes:
            return AsDerived()->EndPartSizesPhase();

        case EPacketPhase::MessagePart:
            return AsDerived()->EndMessagePartPhase();

        default:
            YUNREACHABLE();
    }
}

template <class TDerived>
TDerived* TPacketTranscoderBase<TDerived>::AsDerived()
{
    return static_cast<TDerived*>(this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
