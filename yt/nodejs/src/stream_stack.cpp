#include "common.h"
#include "stream_stack.h"

#include <util/stream/input.h>
#include <util/stream/output.h>
#include <util/stream/zlib.h>
#include <util/stream/lz.h>
#include <util/stream/lzop.h>

namespace NYT {
namespace NNodeJS {

////////////////////////////////////////////////////////////////////////////////

template <class TStream>
class TLazyInput
    : public TInputStream
{
public:
    TLazyInput(TInputStream* input)
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
    TInputStream* Input_;
    std::unique_ptr<TInputStream> Slave_;
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
        default:
            YUNREACHABLE();
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
        default:
            YUNREACHABLE();
    }

    stack.Add<TBufferedOutput>()->SetPropagateMode(true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
