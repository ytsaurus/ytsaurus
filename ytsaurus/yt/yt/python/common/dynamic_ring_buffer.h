#pragma once

#include <util/generic/buffer.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

class TDynamicRingBuffer
{
public:
    TDynamicRingBuffer();

    void Push(TStringBuf data);

    void Pop(TBuffer* destination, size_t count);

    void Clear();

    size_t Size() const;

    bool Empty() const;

    size_t Capacity() const;

    void Reserve(size_t count);

private:
    size_t Avail() const;

    size_t NormalizeIndex(size_t index) const;

    size_t BytesContiniouslyAvailable(size_t pos, size_t limit = Max<size_t>()) const;

    void Relocate(size_t size);

private:
    size_t Begin_;
    size_t Size_;
    TBuffer Buf_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
