#pragma once

#include "private.h"

#include <yt/core/misc/chunked_memory_allocator.h>
#include <yt/core/misc/small_vector.h>

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

constexpr ui32 PacketSignature = 0x78616d4f;
constexpr ui32 NullPacketPartSize = 0xffffffff;
constexpr int TypicalPacketPartCount = 64;
constexpr int TypicalVariableHeaderSize = TypicalPacketPartCount * (sizeof (ui32) + sizeof (ui64));

struct TPacketHeader
{
    // Should be equal to PacketSignature.
    ui32 Signature;
    EPacketType Type;
    EPacketFlags Flags;
    TPacketId PacketId;
    ui32 PartCount;
    ui64 Checksum;
};

/*
  Variable-sized header:
    ui32 PartSizes[PartCount];
    ui64 PartChecksums[PartCount];
    ui64 Checksum;
*/

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPacketPhase,
    (FixedHeader)
    (VariableHeader)
    (MessagePart)
    (Finished)
);

template <class TDerived>
class TPacketTranscoderBase
{
public:
    explicit TPacketTranscoderBase(const NLogging::TLogger& logger);

    TMutableRef GetFragment();
    bool IsFinished() const;

protected:
    const NLogging::TLogger& Logger;

    EPacketPhase Phase_ = EPacketPhase::Finished;
    char* FragmentPtr_ = nullptr;
    size_t FragmentRemaining_ = 0;

    TPacketHeader FixedHeader_;

    SmallVector<char, TypicalVariableHeaderSize> VariableHeader_;
    size_t VariableHeaderSize_;
    ui32* PartSizes_;
    ui64* PartChecksums_;

    int PartIndex_ = -1;
    TSharedRefArray Message_;

    void AllocateVariableHeader();
    TChecksum GetFixedChecksum();
    TChecksum GetVariableChecksum();

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
    TPacketDecoder(const NLogging::TLogger& logger, bool verifyChecksum);

    bool Advance(size_t size);
    void Restart();

    bool IsInProgress() const;
    EPacketType GetPacketType() const;
    EPacketFlags GetPacketFlags() const;
    const TPacketId& GetPacketId() const;
    TSharedRefArray GrabMessage() const;
    size_t GetPacketSize() const;

private:
    friend class TPacketTranscoderBase<TPacketDecoder>;

    TChunkedMemoryAllocator Allocator_;

    std::vector<TSharedRef> Parts_;

    size_t PacketSize_ = 0;

    const bool VerifyChecksum_;

    bool EndFixedHeaderPhase();
    bool EndVariableHeaderPhase();
    bool EndMessagePartPhase();
    void NextMessagePartPhase();

};

////////////////////////////////////////////////////////////////////////////////

//! Enables asynchronous zero-copy packet writing.
class TPacketEncoder
    : public TPacketTranscoderBase<TPacketEncoder>
{
public:
    explicit TPacketEncoder(const NLogging::TLogger& logger);

    static size_t GetPacketSize(
        EPacketType type,
        const TSharedRefArray& message,
        size_t payloadSize);

    bool Start(
        EPacketType type,
        EPacketFlags flags,
        bool generateChecksums,
        int checksummedPartCount,
        const TPacketId& packetId,
        TSharedRefArray message);

    bool IsFragmentOwned() const;
    void NextFragment();

private:
    friend class TPacketTranscoderBase<TPacketEncoder>;

    bool EndFixedHeaderPhase();
    bool EndVariableHeaderPhase();
    bool EndMessagePartPhase();
    void NextMessagePartPhase();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT

Y_DECLARE_PODTYPE(NYT::NBus::TPacketHeader);

#define PACKET_INL_H_
#include "packet-inl.h"
#undef PACKET_INL_H_

