#pragma once

#include "source.h"

namespace NYT {

void SnappyCompress(StreamSource* source, std::vector<char>* output);

void SnappyDecompress(StreamSource* source, std::vector<char>* output);

} // namespace NYT
