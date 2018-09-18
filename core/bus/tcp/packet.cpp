#include "packet.h"

#include <yt/core/bus/bus.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

static const i64 PacketDecoderChunkSize = 16_KB;

struct TPacketDecoderTag { };

////////////////////////////////////////////////////////////////////////////////

TPacketDecoder::TPacketDecoder(const NLogging::TLogger& logger, bool verifyChecksum)
    : TPacketTranscoderBase(logger)
    , Allocator_(
        PacketDecoderChunkSize,
        TChunkedMemoryAllocator::DefaultMaxSmallBlockSizeRatio,
        GetRefCountedTypeCookie<TPacketDecoderTag>())
    , VerifyChecksum_(verifyChecksum)
{
    Restart();
}

void TPacketDecoder::Restart()
{
    Phase_ = EPacketPhase::FixedHeader;
    PacketSize_ = 0;
    Parts_.clear();
    PartIndex_ = -1;
    Message_.Reset();

    BeginPhase(EPacketPhase::FixedHeader, &FixedHeader_, sizeof (TPacketHeader));
}

bool TPacketDecoder::Advance(size_t size)
{
    Y_ASSERT(FragmentRemaining_ != 0);
    Y_ASSERT(size <= FragmentRemaining_);

    PacketSize_ += size;
    FragmentRemaining_ -= size;
    FragmentPtr_ += size;
    if (FragmentRemaining_ == 0) {
        return EndPhase();
    } else {
        return true;
    }
}

bool TPacketDecoder::IsInProgress() const
{
    return Phase_ != EPacketPhase::Finished && PacketSize_ > 0;
}

EPacketType TPacketDecoder::GetPacketType() const
{
    return FixedHeader_.Type;
}

EPacketFlags TPacketDecoder::GetPacketFlags() const
{
    return EPacketFlags(FixedHeader_.Flags);
}

const TPacketId& TPacketDecoder::GetPacketId() const
{
    return FixedHeader_.PacketId;
}

TSharedRefArray TPacketDecoder::GetMessage() const
{
    return Message_;
}

size_t TPacketDecoder::GetPacketSize() const
{
    return PacketSize_;
}

bool TPacketDecoder::EndFixedHeaderPhase()
{
    if (FixedHeader_.Signature != PacketSignature) {
        LOG_ERROR("Packet header signature mismatch: expected %X, actual %X",
            PacketSignature,
            FixedHeader_.Signature);
        return false;
    }

    if (FixedHeader_.PartCount > MaxMessagePartCount) {
        LOG_ERROR("Invalid part count %v",
            FixedHeader_.PartCount);
        return false;
    }

    if (VerifyChecksum_) {
        auto expectedChecksum = FixedHeader_.Checksum;
        if (expectedChecksum != NullChecksum) {
            auto actualChecksum = GetFixedChecksum();
            if (expectedChecksum != actualChecksum) {
                LOG_ERROR("Fixed packet header checksum mismatch");
                return false;
            }
        }
    }

    switch (FixedHeader_.Type) {
        case EPacketType::Message:
            AllocateVariableHeader();
            BeginPhase(EPacketPhase::VariableHeader, VariableHeader_.data(), VariableHeaderSize_);
            return true;

        case EPacketType::Ack:
            SetFinished();
            return true;

        default:
            LOG_ERROR("Invalid packet type %v",
                FixedHeader_.Type);
            return false;
    }
}

bool TPacketDecoder::EndVariableHeaderPhase()
{
    if (VerifyChecksum_) {
        auto expectedChecksum = PartChecksums_[FixedHeader_.PartCount];
        if (expectedChecksum != NullChecksum) {
            auto actualChecksum = GetVariableChecksum();
            if (expectedChecksum != actualChecksum) {
                LOG_ERROR("Variable packet header checksum mismatch");
                return false;
            }
        }
    }

    for (int index = 0; index < FixedHeader_.PartCount; ++index) {
        ui32 partSize = PartSizes_[index];
        if (partSize != NullPacketPartSize && partSize > MaxMessagePartSize) {
            LOG_ERROR("Invalid size %v of part %v",
                partSize,
                index);
            return false;
        }
    }

    NextMessagePartPhase();
    return true;
}

bool TPacketDecoder::EndMessagePartPhase()
{
    if (VerifyChecksum_) {
        auto expectedChecksum = PartChecksums_[PartIndex_];
        if (expectedChecksum != NullChecksum) {
            auto actualChecksum = GetChecksum(Parts_[PartIndex_]);
            if (expectedChecksum != actualChecksum) {
                LOG_ERROR("Packet part checksum mismatch");
                return false;
            }
        }
    }

    NextMessagePartPhase();
    return true;
}

void TPacketDecoder::NextMessagePartPhase()
{
    while (true) {
        ++PartIndex_;
        if (PartIndex_ == FixedHeader_.PartCount) {
            Message_ = TSharedRefArray(std::move(Parts_));
            SetFinished();
            break;
        }

        ui32 partSize = PartSizes_[PartIndex_];
        if (partSize == NullPacketPartSize) {
            Parts_.push_back(TSharedRef());
        } else if (partSize == 0) {
            Parts_.push_back(EmptySharedRef);
        } else {
            auto part = Allocator_.AllocateAligned(partSize);
            BeginPhase(EPacketPhase::MessagePart, part.Begin(), part.Size());
            Parts_.push_back(std::move(part));
            break;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TPacketEncoder::TPacketEncoder(const NLogging::TLogger& logger)
    : TPacketTranscoderBase(logger)
{
    FixedHeader_.Signature = PacketSignature;
}

size_t TPacketEncoder::GetPacketSize(
    EPacketType type,
    const TSharedRefArray& message)
{
    size_t size = sizeof (TPacketHeader);
    switch (type) {
        case EPacketType::Ack:
            break;

        case EPacketType::Message:
            size +=
                message.Size() * (sizeof (ui32) + sizeof (ui64)) +
                sizeof (ui64) +
                GetByteSize(message);
            break;

        default:
            Y_UNREACHABLE();
    }
    return size;
}

bool TPacketEncoder::Start(
    EPacketType type,
    EPacketFlags flags,
    bool generateChecksums,
    int checksummedPartCount,
    const TPacketId& packetId,
    TSharedRefArray message)
{
    PartIndex_ = -1;
    Message_ = std::move(message);

    FixedHeader_.Type = type;
    FixedHeader_.Flags = flags;
    FixedHeader_.PacketId = packetId;
    FixedHeader_.PartCount = Message_.Size();
    FixedHeader_.Checksum = generateChecksums ? GetFixedChecksum() : NullChecksum;

    AllocateVariableHeader();

    if (type == EPacketType::Message) {
        if (Message_.Size() > MaxMessagePartCount) {
            LOG_ERROR("Message exceeds part count limit: %v > %v",
                Message_.Size(),
                MaxMessagePartCount);
            return false;
        }

        for (int index = 0; index < Message_.Size(); ++index) {
            const auto& part = Message_[index];
            if (part) {
                if (part.Size() > MaxMessagePartSize) {
                    LOG_ERROR("Part %v exceeds size limit: %v > %v",
                        index,
                        part.Size(),
                        MaxMessagePartSize);
                    return false;
                }
                PartSizes_[index] = part.Size();
                PartChecksums_[index] =
                    generateChecksums && (index < checksummedPartCount || checksummedPartCount == TSendOptions::AllParts)
                    ? GetChecksum(part)
                    : NullChecksum;
            } else {
                PartSizes_[index] = NullPacketPartSize;
                PartChecksums_[index] = NullChecksum;
            }
        }

        PartChecksums_[Message_.Size()] = generateChecksums ? GetVariableChecksum() : NullChecksum;
    }

    BeginPhase(EPacketPhase::FixedHeader, &FixedHeader_, sizeof (TPacketHeader));
    return true;
}

bool TPacketEncoder::IsFragmentOwned() const
{
    return Phase_ == EPacketPhase::MessagePart;
}

void TPacketEncoder::NextFragment()
{
    EndPhase();
}

bool TPacketEncoder::EndFixedHeaderPhase()
{
    switch (FixedHeader_.Type) {
        case EPacketType::Message:
            BeginPhase(EPacketPhase::VariableHeader, VariableHeader_.data(), VariableHeaderSize_);
            return true;

        case EPacketType::Ack:
            SetFinished();
            return true;

        default:
            Y_UNREACHABLE();
    }
}

bool TPacketEncoder::EndVariableHeaderPhase()
{
    NextMessagePartPhase();
    return true;
}

bool TPacketEncoder::EndMessagePartPhase()
{
    NextMessagePartPhase();
    return true;
}

void TPacketEncoder::NextMessagePartPhase()
{
    while (true) {
        ++PartIndex_;
        if (PartIndex_ == FixedHeader_.PartCount) {
            break;
        }

        const auto& part = Message_[PartIndex_];
        if (part.Size() != 0) {
            BeginPhase(EPacketPhase::MessagePart, const_cast<char*>(part.Begin()), part.Size());
            return;
        }
    }

    Message_.Reset();
    SetFinished();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
