#pragma once

#include "private.h"

#include <ytlib/misc/ref.h>
#include <ytlib/misc/small_vector.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EPacketType,
    ((Message)(0))
    ((Ack)(1))
);

#pragma pack(push, 4)

const ui32 PacketSignature = 0x78616d6f;
const i32 MaxPacketPartCount = 1 << 28;
const i32 MaxPacketPartSize = 1 << 28;
const int TypicalPacketPartCount = 64;

struct TPacketHeader
{
    ui32 Signature;
    EPacketType Type;
    TPacketId PacketId;
};

#pragma pack(pop)

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EPacketPhase,
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

    TRef GetChunk();
    bool IsFinished() const;  

protected:
    EPacketPhase Phase;
    char* Chunk;
    size_t PendingSize;
    TPacketHeader Header;
    TSmallVector<i32, TypicalPacketPartCount> PartSizes;
    i32 PartCount;
    int PartIndex;
    IMessagePtr Message;

    void BeginPhase(EPacketPhase phase, void* chunk, size_t size);
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
    const TPacketId& GetPacketId() const;
    IMessagePtr GetMessage() const;
    size_t GetPacketSize() const;

private:
    friend class TPacketTranscoderBase<TPacketDecoder>;

    std::vector<TSharedRef> Parts;
    size_t PacketSize;
        
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

    static size_t GetPacketSize(EPacketType type, IMessagePtr message);

    bool Start(EPacketType type, const TPacketId& packetId, IMessagePtr message);
    void NextChunk();

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
    , Chunk(NULL)
    , PendingSize(0)
    , PartCount(-1)
    , PartIndex(-1)
{ }

template <class TDerived>
TRef TPacketTranscoderBase<TDerived>::GetChunk()
{
    return TRef(Chunk, PendingSize);
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
    Chunk = static_cast<char*>(buffer);
    PendingSize = size;
}

template <class TDerived>
void TPacketTranscoderBase<TDerived>::SetFinished()
{
    Phase = EPacketPhase::Finished;
    Chunk = NULL;
    PendingSize = 0;
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
