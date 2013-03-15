#pragma once

#include "helpers.h"

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

void Lz4Compress(bool highCompression, StreamSource* source, std::vector<char>* output);

void Lz4Decompress(StreamSource* source, std::vector<char>* output);

////////////////////////////////////////////////////////////////////////////////

void QuickLzCompress(StreamSource* source, std::vector<char>* output);

void QuickLzDecompress(StreamSource* source, std::vector<char>* output);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT

