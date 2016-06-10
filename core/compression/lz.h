#pragma once

#include "details.h"

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

size_t Lz4CompressionBound(const std::vector<int>& lengths);

void Lz4Compress(bool highCompression, StreamSource* source, TBlob* output);

void Lz4Decompress(StreamSource* source, TBlob* output);

////////////////////////////////////////////////////////////////////////////////

void QuickLzCompress(StreamSource* source, TBlob* output);

void QuickLzDecompress(StreamSource* source, TBlob* output);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT

