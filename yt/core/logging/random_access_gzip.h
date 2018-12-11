#pragma once

#include <memory>

#include <util/generic/buffer.h>

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
    explicit TRandomAccessGZipFile(const TString& path, int blockSize = 256 * 1024);

private:
    TFile File_;
    const int BlockSize_;
    i64 OutputPosition_ = 0;

    TBufferOutput Output_;
    std::unique_ptr<TZLibCompress> Compressor_;

    virtual void DoWrite(const void* buf, size_t len) override;
    virtual void DoFlush() override;
    virtual void DoFinish() override;

    void Repair();
    void Reset();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging
} // namespace NYT
