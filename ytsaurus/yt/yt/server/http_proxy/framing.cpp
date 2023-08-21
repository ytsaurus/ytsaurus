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

TFramingAsyncOutputStream::TFramingAsyncOutputStream(
    IFlushableAsyncOutputStreamPtr underlying,
    IInvokerPtr invoker)
    : Underlying_(std::move(underlying))
    , Invoker_(std::move(invoker))
{ }

TFuture<void> TFramingAsyncOutputStream::WriteDataFrame(const TSharedRef& buffer)
{
    if (buffer.Size() > std::numeric_limits<ui32>::max()) {
        THROW_ERROR_EXCEPTION("Data frame is too large: got %v bytes, limit is %v",
            buffer.Size(),
            std::numeric_limits<ui32>::max());
    }
    auto header = TString::Uninitialized(1 + sizeof(ui32));
    header[0] = static_cast<char>(EFrameType::Data);
    auto littleEndianSize = HostToLittle(static_cast<ui32>(buffer.Size()));
    WriteUnaligned<ui32>(header.Detach() + 1, littleEndianSize);
    return DoWriteFrame(std::move(header), buffer);
}

TFuture<void> TFramingAsyncOutputStream::WriteKeepAliveFrame()
{
    auto header = TString::Uninitialized(1);
    header[0] = static_cast<char>(EFrameType::KeepAlive);
    return DoWriteFrame(std::move(header), std::nullopt);
}

TFuture<void> TFramingAsyncOutputStream::Write(const TSharedRef& buffer)
{
    return WriteDataFrame(buffer);
}

TFuture<void> TFramingAsyncOutputStream::Flush()
{
    auto guard = Guard(SpinLock_);
    if (Closed_) {
        return MakeFuture(TError("Can not flush closed framing stream"));
    }

    AddAction(BIND(&IFlushableAsyncOutputStream::Flush, Underlying_));
    return PendingOperationFuture_;
}

TFuture<void> TFramingAsyncOutputStream::Close()
{
    auto guard = Guard(SpinLock_);
    if (Closed_) {
        return MakeFuture(TError("Framing stream is already closed (or is being closed)"));
    }

    Closed_ = true;

    AddAction(BIND(&IFlushableAsyncOutputStream::Close, Underlying_));
    return PendingOperationFuture_;
}

TFuture<void> TFramingAsyncOutputStream::DoWriteFrame(TString header, const std::optional<TSharedRef>& frame)
{
    auto guard = Guard(SpinLock_);
    if (Closed_) {
        return MakeFuture(TError("Can not write to closed framing stream"));
    }

    auto headerRef = TSharedRef::FromString(std::move(header));
    AddAction(BIND(&IFlushableAsyncOutputStream::Write, Underlying_, std::move(headerRef)));
    if (frame) {
        AddAction(BIND(&IFlushableAsyncOutputStream::Write, Underlying_, *frame));
    }
    return PendingOperationFuture_;
}

void TFramingAsyncOutputStream::AddAction(TCallback<TFuture<void>()> action)
{
    auto asyncAction = BIND(action).AsyncVia(Invoker_);
    PendingOperationFuture_ = PendingOperationFuture_.Apply(std::move(asyncAction));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
