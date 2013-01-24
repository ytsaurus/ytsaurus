#include "codec.h"
#include "reed_solomon.h"

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

} // anonymous namespace

ICodec* GetCodec(ECodec id)
{
    static TLazyPtr<TCauchyReedSolomonWrapper> CauchyReedSolomon(
        BIND([] () {
            return New<TCauchyReedSolomonWrapper>(6, 3, 8);
        })
    );

    switch (id) {
        case ECodec::ReedSolomon3:
            return CauchyReedSolomon.Get();
        default:
            YUNREACHABLE();
    }
}
    
////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure

} // namespace NYT
