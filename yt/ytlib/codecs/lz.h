#pragma once

#include "source.h"

namespace NYT {

void Lz4Compress(StreamSource* source, std::vector<char>* output);

void Lz4Decompress(StreamSource* source, std::vector<char>* output);

} // namespace NYT
