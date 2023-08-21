#pragma once

#include <yt/yt/python/common/dynamic_ring_buffer.h>

#include <util/stream/zerocopy.h>
#include <util/stream/input.h>

namespace NYT::NPython {

////////////////////////////////////////////////////////////////////////////////

class TTeeInputStream : public IZeroCopyInput
{
public:
    explicit TTeeInputStream(IZeroCopyInput* underlying);

    ~TTeeInputStream() override;

    size_t Size() const;

    void ExtractFromBuffer(TBuffer* destination, size_t count);

    void Flush();

protected:
    size_t DoNext(const void** ptr, size_t len) override;

private:
    TDynamicRingBuffer RingBuffer_;
    IZeroCopyInput* Underlying_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPython
