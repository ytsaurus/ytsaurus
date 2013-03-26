#include "codec.h"
#include "reed_solomon.h"
#include "lrc.h"

#include <util/generic/singleton.h>

namespace NYT {
namespace NErasure {

////////////////////////////////////////////////////////////////////////////////

int ICodec::GetTotalBlockCount()
{
    return GetDataBlockCount() + GetParityBlockCount();
}

////////////////////////////////////////////////////////////////////////////////

ICodec* GetCodec(ECodec id)
{
    class TCauchyReedSolomonWrapper
        : public TCauchyReedSolomon
    {
    public:
        TCauchyReedSolomonWrapper()
            : TCauchyReedSolomon(6, 3, 8)
        { }
    };

    class TLrcWrapper
        : public TLrc
    {
    public:
        TLrcWrapper()
            : TLrc(12)
        { }
    };

    switch (id) {
        case ECodec::ReedSolomon_6_3:
            return Singleton<TCauchyReedSolomonWrapper>();

        case ECodec::Lrc_12_2_2:
            return Singleton<TLrcWrapper>();

        default:
            YUNREACHABLE();
    }
}
    
////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT
