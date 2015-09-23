#pragma once

#include "details.h"

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

void BrotliCompress(int level, StreamSource* source, TBlob* output);

void BrotliDecompress(StreamSource* source, TBlob* output);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT

