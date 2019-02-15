#pragma once

#include <yt/core/misc/assert.h>
#include <yt/core/misc/blob.h>
#include <yt/core/misc/ref.h>

#include <library/erasure/codec.h>

#include <util/system/compiler.h>

#include <bitset>

namespace NYT::NErasure {

////////////////////////////////////////////////////////////////////////////////

using TPartIndexList = ::NErasure::TPartIndexList;
using TPartIndexSet = ::NErasure::TPartIndexSet;

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

    template <class Tag>
    static inline TMutableBlobType AllocateBlob(size_t size, bool initializeStorage)
    {
        return TMutableBlobType::Allocate<Tag>(size, initializeStorage);
    }

    template <class Tag>
    static inline TBufferType AllocateBuffer(Tag tag, size_t size)
    {
        return TBufferType(tag, size);
    }

    static inline TBlobType FromBufferToBlob(TBufferType&& blob)
    {
        return TBlobType::FromBlob(std::move(blob));
    }
};

using ICodec = ::NErasure::ICodec<typename TCodecTraits::TBlobType>;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NErasure
