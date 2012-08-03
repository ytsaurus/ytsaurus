#pragma once

#include "source.h"

namespace NYT {
namespace NCodec {

////////////////////////////////////////////////////////////////////////////////

void Lz4Compress(bool highCompression, StreamSource* source, std::vector<char>* output);

void Lz4Decompress(StreamSource* source, std::vector<char>* output);

////////////////////////////////////////////////////////////////////////////////
        
} // namespace NCodec
} // namespace NYT

