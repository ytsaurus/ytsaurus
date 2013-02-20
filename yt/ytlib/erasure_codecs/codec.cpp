#include "codec.h"
#include "reed_solomon.h"
#include "lrc.h"

#include <ytlib/misc/lazy_ptr.h>
#include <ytlib/misc/singleton.h>

namespace NYT {

namespace NErasure {

////////////////////////////////////////////////////////////////////////////////

namespace {

struct TCauchyReedSolomonWrapper
    : public TCauchyReedSolomon
    , public TRefCounted
{
    TCauchyReedSolomonWrapper(int blockCount, int parityCount, int wordSize)
        : TCauchyReedSolomon(blockCount, parityCount, wordSize)
    { }
};

struct TLrcWrapper
    : public TLrc
    , public TRefCounted
{
    TLrcWrapper(int blockCount)
        : TLrc(blockCount)
    { }
};

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

int ICodec::GetTotalBlockCount() const
{
    return GetDataBlockCount() + GetParityBlockCount();
}

////////////////////////////////////////////////////////////////////////////////

ICodec* GetCodec(ECodec id)
{
    static TLazyPtr<TCauchyReedSolomonWrapper> CauchyReedSolomon(
        BIND([] () {
            return New<TCauchyReedSolomonWrapper>(6, 3, 8);
        })
    );
    
    static TLazyPtr<TLrcWrapper> Lrc(
        BIND([] () {
            return New<TLrcWrapper>(12);
        })
    );

    switch (id) {
        case ECodec::ReedSolomon_6_3:
            return CauchyReedSolomon.Get();
        case ECodec::Lrc_12_2_2:
            return Lrc.Get();
        default:
            YUNREACHABLE();
    }
}
    
////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure

} // namespace NYT
