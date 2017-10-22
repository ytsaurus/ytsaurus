#pragma once

#include <util/stream/input.h>
#include <util/stream/output.h>

#include <util/generic/ptr.h>

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

class TBrotliCompress
    : public IOutputStream
{
public:
    TBrotliCompress(IOutputStream* underlying, int level);

protected:
    virtual void DoWrite(const void* buffer, size_t length);

    virtual void DoFinish();

private:
    class TImpl;
    THolder<TImpl> Impl_;
};

class TBrotliDecompress
    : public IInputStream
{
public:
    explicit TBrotliDecompress(IInputStream* underlying, size_t buflen = 8 * 1024, bool trusted = false);

private:
    virtual size_t DoRead(void* buffer, size_t length);

private:
    class TImpl;
    THolder<TImpl> Impl_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT

