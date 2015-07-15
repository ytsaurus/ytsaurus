#pragma once

#include "public.h"

#include <core/misc/public.h>
#include <core/misc/ref.h>

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

std::vector<TSharedRef> CompressWithEnvelope(
    const TSharedRef& uncompressedData,
    ECodec codecId = ECodec::None,
    i64 maxPartSize = DefaultEnvelopePartSize);

std::vector<TSharedRef> CompressWithEnvelope(
    const std::vector<TSharedRef>& uncompressedData,
    ECodec codecId = ECodec::None,
    i64 maxPartSize = DefaultEnvelopePartSize);

TSharedRef DecompressWithEnvelope(
    const std::vector<TSharedRef>& compressedData);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT

