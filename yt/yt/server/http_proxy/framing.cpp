#include "framing.h"

#include <util/system/byteorder.h>
#include <util/system/unaligned_mem.h>

namespace NYT::NHttpProxy {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EFrameType, uint8_t,
    ((Data) (0x01))
    ((KeepAlive) (0x02))
);

////////////////////////////////////////////////////////////////////////////////

TFramingAsyncOutputStream::TFramingAsyncOutputStream(NConcurrency::IFlushableAsyncOutputStreamPtr underlying)
    : Underlying_(std::move(underlying))
{ }

TFuture<void> TFramingAsyncOutputStream::WriteDataFrame(const TSharedRef& buffer) {
    if (buffer.Size() > std::numeric_limits<uint32_t>::max()) {
        THROW_ERROR_EXCEPTION("Data frame is too large: got %v bytes, limit is %v",
            buffer.Size(),
            std::numeric_limits<uint32_t>::max());
    }
    auto header = TString::Uninitialized(1 + sizeof(uint32_t));
    header[0] = static_cast<char>(EFrameType::Data);
    auto littleEndianSize = HostToLittle(static_cast<uint32_t>(buffer.Size()));
    WriteUnaligned<uint32_t>(header.Detach() + 1, littleEndianSize);
    return DoWriteFrame(std::move(header), buffer);
}

TFuture<void> TFramingAsyncOutputStream::WriteKeepAliveFrame() {
    auto header = TString::Uninitialized(1);
    header[0] = static_cast<char>(EFrameType::KeepAlive);
    return DoWriteFrame(std::move(header), std::nullopt);
}

TFuture<void> TFramingAsyncOutputStream::Write(const TSharedRef& buffer)
{
    return WriteDataFrame(buffer);
}

TFuture<void> TFramingAsyncOutputStream::Flush() {
    TGuard<TSpinLock> guard(SpinLock_);
    if (Closed_) {
        return MakeFuture(TError("Can not flush closed framing stream"));
    }

    PendingOperationFuture_ = PendingOperationFuture_.Apply(
        BIND(&IFlushableAsyncOutputStream::Flush, Underlying_));
    return PendingOperationFuture_;
}

TFuture<void> TFramingAsyncOutputStream::Close() {
    TGuard<TSpinLock> guard(SpinLock_);
    if (Closed_) {
        return MakeFuture(TError("Framing stream is already closed (or is being closed)"));
    }

    Closed_ = true;

    PendingOperationFuture_ = PendingOperationFuture_.Apply(
        BIND(&IFlushableAsyncOutputStream::Close, Underlying_));
    return PendingOperationFuture_;
}

TFuture<void> TFramingAsyncOutputStream::DoWriteFrame(TString header, const std::optional<TSharedRef>& frame) {
    TGuard<TSpinLock> guard(SpinLock_);
    if (Closed_) {
        return MakeFuture(TError("Can not write to closed framing stream"));
    }

    PendingOperationFuture_ = PendingOperationFuture_.Apply(
        BIND(&IFlushableAsyncOutputStream::Write, Underlying_, TSharedRef::FromString(std::move(header))));
    if (frame) {
        PendingOperationFuture_ = PendingOperationFuture_.Apply(
            BIND(&IFlushableAsyncOutputStream::Write, Underlying_, *frame));
    }
    return PendingOperationFuture_;
}

} // namespace NYT::NHttpProxy