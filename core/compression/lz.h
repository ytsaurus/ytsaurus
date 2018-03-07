#pragma once

#include "details.h"

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

void Lz4Compress(bool highCompression, StreamSource* source, TBlob* sink);

void Lz4Decompress(StreamSource* source, TBlob* sink);

////////////////////////////////////////////////////////////////////////////////

void QuickLzCompress(StreamSource* source, TBlob* sink);

void QuickLzDecompress(StreamSource* source, TBlob* sink);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT

