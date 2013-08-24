#include "input_stack.h"

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
    TLazyInput(TInputStream* in)
        : Input_(in)
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

TNodeJSInputStack::TNodeJSInputStack(TInputStreamWrap* base)
    : TGrowingStreamStack(base)
{
    THREAD_AFFINITY_IS_V8();
    YASSERT(base == Bottom());
    GetBaseStream()->AsyncRef(true);
}

TNodeJSInputStack::~TNodeJSInputStack() throw()
{
    GetBaseStream()->AsyncUnref();
}

TInputStreamWrap* TNodeJSInputStack::GetBaseStream()
{
    return static_cast<TInputStreamWrap*>(Bottom());
}

void TNodeJSInputStack::AddCompression(ECompression compression)
{
    switch (compression) {
        case ECompression::None:
            break;
        case ECompression::Gzip:
        case ECompression::Deflate:
            Add<TZLibDecompress>();
            break;
        case ECompression::LZOP:
            Add<TLzopDecompress>();
            break;
        case ECompression::LZO:
            Add< TLazyInput<TLzoDecompress> >();
            break;
        case ECompression::LZF:
            Add< TLazyInput<TLzfDecompress> >();
            break;
        case ECompression::Snappy:
            Add< TLazyInput<TSnappyDecompress> >();
            break;
        default:
            YUNREACHABLE();
    }
}

size_t TNodeJSInputStack::DoRead(void* data, size_t length)
{
    return Top()->Read(data, length);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeJS
} // namespace NYT
