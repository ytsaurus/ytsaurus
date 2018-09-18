#pragma once

#include "details.h"

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

void LzmaCompress(int level, StreamSource* source, TBlob* output);

void LzmaDecompress(StreamSource* source, TBlob* output);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT


