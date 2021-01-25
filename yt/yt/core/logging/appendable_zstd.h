#pragma once

#include <util/generic/buffer.h>
#include <util/generic/size_literals.h>

#include <util/stream/buffer.h>
#include <util/stream/file.h>

#include <util/system/types.h>

#include <vector>

namespace NYT {
namespace NLogging {

constexpr const int DefaultZstdCompressionLevel = 3;

////////////////////////////////////////////////////////////////////////////////

class TAppendableZstdFile
    : public IOutputStream
{
public:
    explicit TAppendableZstdFile(
        TFile* file,
        int compressionLevel = DefaultZstdCompressionLevel,
        bool writeTruncateMessage = true);
    ~TAppendableZstdFile();

private:
    struct ZstdContext;

    int CompressionLevel_;

    TFile* const File_;
    i64 OutputPosition_ = 0;

    TBuffer Input_;
    TBuffer Output_;

    std::unique_ptr<ZstdContext> Context_;

    virtual void DoWrite(const void* buf, size_t len) override;
    virtual void DoFlush() override;
    virtual void DoFinish() override;

    void FlushOutput();
    void CompressOneFrame();
    void ScanTail();
    void Repair(bool writeTruncateMessage);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NLogging
} // namespace NYT
