#pragma once

#include <yt/core/misc/assert.h>
#include <yt/core/misc/blob.h>
#include <yt/core/misc/ref.h>

#include <library/erasure/codec.h>

#include <util/system/compiler.h>

#include <bitset>

namespace NYT::NErasure {

////////////////////////////////////////////////////////////////////////////////

struct TJerasureTag {};
struct TLrcTag {};

using ::NErasure::TPartIndexList;
using ::NErasure::TPartIndexSet;

DEFINE_ENUM_WITH_UNDERLYING_TYPE(ECodec, i8,
    ((None)           (0))
    ((ReedSolomon_6_3)(1))
    ((Lrc_12_2_2)     (2))
);

struct TCodecTraits {
    using TBlobType = TSharedRef;
    using TMutableBlobType = TSharedMutableRef;
    using TBufferType = NYT::TBlob;
    using ECodecType = ECodec;

    static inline void Check(bool expr)
    {
        YCHECK(expr);
    }

    static inline TMutableBlobType AllocateBlob(size_t size)
    {
        return TMutableBlobType::Allocate<TJerasureTag>(size, false);
    }

    static inline TBufferType AllocateBuffer(size_t size)
    {
        // Only Lrc now uses buffer allocation
        return TBufferType(TLrcTag(), size);
    }

    static inline TBlobType FromBufferToBlob(TBufferType&& blob)
    {
        return TBlobType::FromBlob(std::move(blob));
    }
};

using ICodec = ::NErasure::ICodec<typename TCodecTraits::TBlobType>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NErasure
