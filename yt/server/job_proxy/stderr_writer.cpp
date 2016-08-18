#include "private.h"
#include "stderr_writer.h"

#include <yt/core/misc/finally.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

void TStderrWriter::DoWrite(const void* buf, size_t len)
{
    if (Failed_) {
        return;
    }

    if (WrittenSize_ < SizeLimit_ / 2) {
        try {
            TFileChunkOutput::DoWrite(buf, len);
        } catch (const std::exception& ex) {
            Failed_ = true;
            LOG_WARNING(ex, "Writing stderr data to chunk failed");
            return;
        }
        WrittenSize_ += len;
    } else {
        AccumulatedSize_ += len;
        Blobs_.push_back(TBlob(TDefaultBlobTag(), buf, len));
        while (AccumulatedSize_ - Blobs_.front().Size() > SizeLimit_ / 2) {
            AccumulatedSize_ -= Blobs_.front().Size();
            Blobs_.pop_front();
            HasSkippedData_ = true;
        }
    }
}

void TStderrWriter::DoFinish()
{
    try {
        if (!Failed_) {
            static const auto skipped = STRINGBUF("\n...skipped...\n");
            if (HasSkippedData_) {
                TFileChunkOutput::DoWrite(skipped.data(), skipped.length());
            }
            for (const auto& ref : Blobs_) {
                TFileChunkOutput::DoWrite(ref.Begin(), ref.Size());
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

