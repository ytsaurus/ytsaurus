#include "codec.h"
#include "reed_solomon.h"
#include "lrc.h"

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
            YUNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT
