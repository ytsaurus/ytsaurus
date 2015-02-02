#pragma once

#include "private.h"

#include <core/misc/ref.h>
#include <core/misc/small_vector.h>

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
const i32 MaxPacketPartSize = 1 << 28;
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
    TPacketTranscoderBase();

    TRef GetFragment();
    bool IsFinished() const;

protected:
    EPacketPhase Phase;
    char* Fragment;
    size_t FragmentRemaining;
    TPacketHeader Header;
    SmallVector<i32, TypicalPacketPartCount> PartSizes;
    i32 PartCount;
    int PartIndex;
    TSharedRefArray Message;

    void BeginPhase(EPacketPhase phase, void* fragment, size_t size);
    bool EndPhase();
    void SetFinished();

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

    TSharedRef SmallChunk;
    size_t SmallChunkUsed;

    std::vector<TSharedRef> Parts;

    size_t PacketSize;

    bool EndHeaderPhase();
    bool EndPartCountPhase();
    bool EndPartSizesPhase();
    bool EndMessagePartPhase();
    void NextMessagePartPhase();
    TSharedRef AllocatePart(size_t partSize);

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
TPacketTranscoderBase<TDerived>::TPacketTranscoderBase()
    : Phase(EPacketPhase::Unstarted)
    , Fragment(NULL)
    , FragmentRemaining(0)
    , PartCount(-1)
    , PartIndex(-1)
{ }

template <class TDerived>
TRef TPacketTranscoderBase<TDerived>::GetFragment()
{
    return TRef(Fragment, FragmentRemaining);
}

template <class TDerived>
bool TPacketTranscoderBase<TDerived>::IsFinished() const
{
    return Phase == EPacketPhase::Finished;
}

template <class TDerived>
void TPacketTranscoderBase<TDerived>::BeginPhase(EPacketPhase phase, void* buffer, size_t size)
{
    Phase = phase;
    Fragment = static_cast<char*>(buffer);
    FragmentRemaining = size;
}

template <class TDerived>
void TPacketTranscoderBase<TDerived>::SetFinished()
{
    Phase = EPacketPhase::Finished;
    Fragment = NULL;
    FragmentRemaining = 0;
}

template <class TDerived>
bool TPacketTranscoderBase<TDerived>::EndPhase()
{
    switch (Phase) {
        case EPacketPhase::Header:
            return static_cast<TDerived*>(this)->EndHeaderPhase();

        case EPacketPhase::PartCount:
            return static_cast<TDerived*>(this)->EndPartCountPhase();

        case EPacketPhase::PartSizes:
            return static_cast<TDerived*>(this)->EndPartSizesPhase();

        case EPacketPhase::MessagePart:
            return static_cast<TDerived*>(this)->EndMessagePartPhase();

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
