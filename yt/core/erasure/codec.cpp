#include "codec.h"
#include "lrc.h"
#include "reed_solomon.h"

namespace NYT {
namespace NErasure {

////////////////////////////////////////////////////////////////////////////////

int ICodec::GetTotalPartCount() const
{
    return GetDataPartCount() + GetParityPartCount();
}

////////////////////////////////////////////////////////////////////////////////

ICodec* GetCodec(ECodec id)
{
    switch (id) {
        case ECodec::ReedSolomon_6_3: {
            static TCauchyReedSolomon result(6, 3, 8);
            return &result;
        }

        case ECodec::Lrc_12_2_2: {
            static TLrc result(12);
            return &result;
        }

        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT
