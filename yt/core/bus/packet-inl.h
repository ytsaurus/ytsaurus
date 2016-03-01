#ifndef PACKET_INL_H_
#error "Direct inclusion of this file is not allowed, include packet.h"
#endif

namespace NYT {
namespace NBus {

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
void TPacketTranscoderBase<TDerived>::AllocateVariableHeader()
{
    VariableHeaderSize_ = (sizeof (ui32) + sizeof (ui64)) * FixedHeader_.PartCount;
    VariableHeader_.reserve(VariableHeaderSize_);
    PartSizes_ = reinterpret_cast<ui32*>(VariableHeader_.data());
    PartChecksums_ = reinterpret_cast<ui64*>(PartSizes_ + FixedHeader_.PartCount);
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
        case EPacketPhase::FixedHeader:
            return AsDerived()->EndFixedHeaderPhase();

        case EPacketPhase::VariableHeader:
            return AsDerived()->EndVariableHeaderPhase();

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
