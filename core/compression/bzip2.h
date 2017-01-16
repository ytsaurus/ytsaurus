#pragma once

#include "details.h"

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

void Bzip2Compress(int level, StreamSource* source, TBlob* output);

void Bzip2Decompress(StreamSource* source, TBlob* output);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT
