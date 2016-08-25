#pragma once

#include "details.h"

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

void ZstdCompress(int level, StreamSource* source, TBlob* output);

void ZstdDecompress(StreamSource* source, TBlob* output);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT

