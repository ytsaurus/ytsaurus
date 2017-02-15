#pragma once

#include "proxy_input.h"

#include <util/stream/buffered.h>
#include <util/stream/file.h>
#include <util/system/file.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TJobReader
    : public TProxyInput
{
public:
    explicit TJobReader(int fd);

    bool OnStreamError(
        const yexception& e,
        bool keepRanges,
        ui32 rangeIndex,
        ui64 rowIndex) override;

    bool HasRangeIndices() const override { return false; }

protected:
    size_t DoRead(void* buf, size_t len) override;

private:
    int Fd_;
    TFile FdFile_;
    TFileInput FdInput_;
    TBufferedInput BufferedInput_;

    static const size_t BUFFER_SIZE = 64 << 10;
};

////////////////////////////////////////////////////////////////////////////////


} // namespace NYT
