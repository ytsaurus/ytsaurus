#pragma once

#include <memory>

#include <util/generic/buffer.h>
#include <util/generic/size_literals.h>

#include <util/stream/buffer.h>
#include <util/stream/file.h>
#include <util/stream/zlib.h>

namespace NYT {
namespace NLogging {

////////////////////////////////////////////////////////////////////////////////

class TRandomAccessGZipFile
    : public IOutputStream
{
public:
    explicit TRandomAccessGZipFile(TFile* file, size_t compressionLevel = 6, size_t blockSize = 256_KB);

private:
    const size_t CompressionLevel_;

    TFile* const File_;

    i64 OutputPosition_ = 0;

    TBufferOutput Output_;
    std::unique_ptr<TZLibCompress> Compressor_;

    void DoWrite(const void* buf, size_t len) override;
    void DoFlush() override;
    void DoFinish() override;

    void Repair();
    void Reset();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging
} // namespace NYT
