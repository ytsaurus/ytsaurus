#pragma once

#include "public.h"

#include <yt/core/misc/blob.h>
#include <yt/core/misc/ref.h>

#include <library/cpp/erasure/codec.h>

#include <bitset>

namespace NYT::NErasure {

////////////////////////////////////////////////////////////////////////////////

struct TCodecTraits
{
    using TBlobType = TSharedRef;
    using TMutableBlobType = TSharedMutableRef;
    using TBufferType = TBlob;
    using ECodecType = ECodec;

    static void Check(bool expr, const char* strExpr, const char* file, int line);
    static TMutableBlobType AllocateBlob(size_t size);
    static TBufferType AllocateBuffer(size_t size);
    static TBlobType FromBufferToBlob(TBufferType&& blob);
};

using ICodec = ::NErasure::ICodec<typename TCodecTraits::TBlobType>;

ICodec* GetCodec(ECodec id);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NErasure
