#include "codec.h"

using namespace NErasure;

namespace NYT::NErasure {

////////////////////////////////////////////////////////////////////////////////

ICodec* GetCodec(ECodec id)
{
    switch (id) {
        // NB: This codec uses Jerasure as a backend.
        case ECodec::ReedSolomon_6_3: {
            static TCauchyReedSolomonJerasure<6, 3, 8, TCodecTraits> result;
            return &result;
        }
        // NB: This codec uses ISA-l as a backend.
        case ECodec::ReedSolomon_3_3: {
            static TReedSolomonIsa<3, 3, 8, TCodecTraits> result;
            return &result;
        }
        // NB: This codec uses Jerasure as a backend.
        case ECodec::JerasureLrc_12_2_2: {
            static TLrcJerasure<12, 4, 8, TCodecTraits> result;
            return &result;
        }
        // NB: This codec uses ISA-l as a backend.
        case ECodec::IsaLrc_12_2_2: {
            static TLrcIsa<12, 4, 8, TCodecTraits> result;
            return &result;
        }
        default:
            Y_UNREACHABLE();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NErasure
