#pragma once

#include <util/stream/input.h>
#include <util/stream/output.h>

#include <util/generic/ptr.h>

#include <memory>

namespace NYT::NCompression {

////////////////////////////////////////////////////////////////////////////////

class TBrotliCompress
    : public IOutputStream
{
public:
    TBrotliCompress(IOutputStream* underlying, int level);
    ~TBrotliCompress() override;

protected:
    virtual void DoWrite(const void* buffer, size_t length);

    virtual void DoFinish();

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

class TBrotliDecompress
    : public IInputStream
{
public:
    explicit TBrotliDecompress(IInputStream* underlying, size_t buflen = 8 * 1024, bool trusted = false);
    ~TBrotliDecompress() override;

private:
    virtual size_t DoRead(void* buffer, size_t length);

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCompression

