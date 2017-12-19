#include "stream_stack.h"

#include <library/streams/lzop/lzop.h>
#include <library/streams/lz/lz.h>
#include <library/streams/brotli/brotli.h>

#include <util/stream/zlib.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

template <class TStream>
class TLazyInput
    : public IInputStream
{
public:
    TLazyInput(IInputStream* input)
        : Input_(input)
    { }

protected:
    virtual size_t DoRead(void* data, size_t length) override
    {
        ConstructSlave();
        return Slave_->Read(data, length);
    }

private:
    inline void ConstructSlave()
    {
        if (!Slave_) {
            Slave_.reset(new TStream(Input_));
        }
    }

private:
    IInputStream* Input_;
    std::unique_ptr<IInputStream> Slave_;
};

////////////////////////////////////////////////////////////////////////////////

void AddCompressionToStack(TGrowingInputStreamStack& stack, ECompression compression)
{
    switch (compression) {
        case ECompression::None:
            break;
        case ECompression::Gzip:
        case ECompression::Deflate:
            stack.Add<TZLibDecompress>();
            break;
        case ECompression::LZOP:
            stack.Add<TLzopDecompress>();
            break;
        case ECompression::LZO:
            stack.Add<TLazyInput<TLzoDecompress>>();
            break;
        case ECompression::LZF:
            stack.Add<TLazyInput<TLzfDecompress>>();
            break;
        case ECompression::Snappy:
            stack.Add<TLazyInput<TSnappyDecompress>>();
            break;
        case ECompression::Brotli:
            stack.Add<TBrotliDecompress>();
            break;
        default:
            Y_UNREACHABLE();
    }
}

void AddCompressionToStack(TGrowingOutputStreamStack& stack, ECompression compression)
{
    switch (compression) {
        case ECompression::None:
            break;
        case ECompression::Gzip:
            stack.Add<TZLibCompress>(ZLib::GZip, 4, DefaultStreamBufferSize);
            break;
        case ECompression::Deflate:
            stack.Add<TZLibCompress>(ZLib::ZLib, 4, DefaultStreamBufferSize);
            break;
        case ECompression::LZOP:
            stack.Add<TLzopCompress>(DefaultStreamBufferSize);
            break;
        case ECompression::LZO:
            stack.Add<TLzoCompress>(DefaultStreamBufferSize);
            break;
        case ECompression::LZF:
            stack.Add<TLzfCompress>(DefaultStreamBufferSize);
            break;
        case ECompression::Snappy:
            stack.Add<TSnappyCompress>(DefaultStreamBufferSize);
            break;
        case ECompression::Brotli:
            stack.Add<TBrotliCompress>(3);
            break;
        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
