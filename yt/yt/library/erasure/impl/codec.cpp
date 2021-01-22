#include "codec.h"

#include <yt/core/misc/assert.h>

namespace NYT::NErasure {

using namespace ::NErasure;

////////////////////////////////////////////////////////////////////////////////

struct TJerasureBlobTag
{ };

struct TLrcBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

void TCodecTraits::Check(bool expr, const char* strExpr, const char* file, int line)
{
    if (Y_UNLIKELY(!expr)) {
        ::NYT::NDetail::AssertTrapImpl("YT_VERIFY", strExpr, file, line);
        Y_UNREACHABLE();
    }
}

TCodecTraits::TMutableBlobType TCodecTraits::AllocateBlob(size_t size)
{
    return TMutableBlobType::Allocate<TJerasureBlobTag>(size, false);
}

TCodecTraits::TBufferType TCodecTraits::AllocateBuffer(size_t size)
{
    // Only LRC now uses buffer allocation.
    return TBufferType(TLrcBufferTag(), size);
}

TCodecTraits::TBlobType TCodecTraits::FromBufferToBlob(TBufferType&& blob)
{
    return TBlobType::FromBlob(std::move(blob));
}

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
        case ECodec::IsaReedSolomon_6_3: {
            static TReedSolomonIsa<6, 3, 8, TCodecTraits> result;
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
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

