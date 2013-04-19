#include "codec.h"
#include "reed_solomon.h"
#include "lrc.h"

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
    switch (id) {
        case ECodec::ReedSolomon: {
            static TCauchyReedSolomon result(6, 3, 8);
            return &result;
        }

        case ECodec::Lrc: {
            static TLrc result(12);
            return &result;
        }

        default:
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT
