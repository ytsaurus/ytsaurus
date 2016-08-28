#include "private.h"
#include "stderr_writer.h"

#include <yt/core/misc/finally.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

void TStderrWriter::DoWrite(const void* buf_, size_t len)
{
    if (Failed_) {
        return;
    }

    auto buf = static_cast<const char*>(buf_);

    // Limit for tail and head are the half of total limit.
    i64 limit = SizeLimit_ / 2;

    if (WrittenSize_ < limit) {
        try {
            TFileChunkOutput::DoWrite(buf, len);
        } catch (const std::exception& ex) {
            Failed_ = true;
            LOG_WARNING(ex, "Writing stderr data to chunk failed");
            return;
        }
        WrittenSize_ += len;
    } else {
        if (!BufferInitialized_) {
            CyclicBuffer_ = TBlob(TDefaultBlobTag(), limit);
            BufferInitialized_ = true;
        }

        if (Position_ + len <= CyclicBuffer_.Size()) {
            std::copy(buf, buf + len, CyclicBuffer_.Begin() + Position_);
            Position_ += len;
        } else {
            BufferOverflowed_ = true;
            if (len >= limit) {
                Position_ = 0;
                std::copy(buf + len - limit, buf + len, CyclicBuffer_.Begin());
            } else {
                std::copy(buf, buf + CyclicBuffer_.Size() - Position_, CyclicBuffer_.Begin() + Position_);
                std::copy(buf + CyclicBuffer_.Size() - Position_, buf + len, CyclicBuffer_.Begin());
                Position_ += len - CyclicBuffer_.Size();
            }
        }
    }
}

void TStderrWriter::DoFinish()
{
    try {
        if (!Failed_) {
            static const auto skipped = STRINGBUF("\n...skipped...\n");
            if (BufferOverflowed_) {
                TFileChunkOutput::DoWrite(skipped.data(), skipped.length());
                TFileChunkOutput::DoWrite(CyclicBuffer_.Begin() + Position_, CyclicBuffer_.Size() - Position_);
                TFileChunkOutput::DoWrite(CyclicBuffer_.Begin(), Position_);
            } else {
                TFileChunkOutput::DoWrite(CyclicBuffer_.Begin(), Position_);
            }
        }
        TFileChunkOutput::DoFinish();
    } catch (const std::exception& ex) {
        LOG_WARNING(ex, "Writing stderr data to chunk failed");
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT

