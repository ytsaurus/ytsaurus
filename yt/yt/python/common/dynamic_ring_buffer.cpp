#include "dynamic_ring_buffer.h"

#include <library/cpp/yt/assert/assert.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/system/sanitizers.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

TDynamicRingBuffer::TDynamicRingBuffer()
    : Begin_(0)
    , Size_(0)
    , Buf_(1)
{ }

void TDynamicRingBuffer::Push(TStringBuf data)
{
    if (Avail() < data.length()) {
        Relocate(Max(Capacity() * 2, Size() + data.length()));
    }

    while (data.length() > 0) {
        size_t pos = NormalizeIndex(Begin_ + Size_);
        size_t bytesAvailable = BytesContinuouslyAvailable(pos, data.length());
        MemCopy(Buf_.Data() + pos, data.Data(), bytesAvailable);
        NSan::Unpoison(Buf_.Data() + pos, bytesAvailable);
        Size_ += bytesAvailable;
        data.Skip(bytesAvailable);
    }
}

void TDynamicRingBuffer::Pop(TBuffer* destination, size_t count)
{
    YT_ASSERT(Size() >= count);

    while (count > 0) {
        size_t bytesAvailable = BytesContinuouslyAvailable(Begin_, count);
        destination->Append(Buf_.Data() + Begin_, bytesAvailable);
        Begin_ = NormalizeIndex(Begin_ + bytesAvailable);
        Size_ -= bytesAvailable;
        count -= bytesAvailable;
    }
}

void TDynamicRingBuffer::Clear()
{
    Begin_ = Size_ = 0;
    Buf_.Clear();
}

size_t TDynamicRingBuffer::Size() const
{
    return Size_;
}

bool TDynamicRingBuffer::Empty() const
{
    return Size() == 0;
}

size_t TDynamicRingBuffer::Capacity() const
{
    return Buf_.Capacity();
}

void TDynamicRingBuffer::Reserve(size_t count)
{
    if (Capacity() < count) {
        Relocate(count);
    }
}

size_t TDynamicRingBuffer::Avail() const
{
    return Buf_.Capacity() - Size_;
}

// Maps index from [0, 2 * capacity) in [0, capacity)
size_t TDynamicRingBuffer::NormalizeIndex(size_t index) const
{
    YT_ASSERT(index < 2 * Buf_.Capacity());

    return index < Buf_.Capacity() ? index : index - Buf_.Capacity();
}

size_t TDynamicRingBuffer::BytesContinuouslyAvailable(size_t pos, size_t limit) const
{
    return Min(Buf_.Capacity() - pos, limit);
}

void TDynamicRingBuffer::Relocate(size_t size)
{
    TBuffer newBuf(size);

    size_t bytesCopied = 0;
    while (bytesCopied < Size_) {
        size_t bytesAvailable = BytesContinuouslyAvailable(Begin_, Size_ - bytesCopied);
        newBuf.Append(Buf_.Data() + Begin_, bytesAvailable);
        Begin_ = NormalizeIndex(Begin_ + bytesAvailable);
        bytesCopied += bytesAvailable;
    }

    Buf_.Swap(newBuf);
    Begin_ = 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
