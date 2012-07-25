#pragma once

#include "source.h"

namespace NYT {

void ZlibCompress(StreamSource* source, std::vector<char>* output);

void ZlibDecompress(StreamSource* source, std::vector<char>* output);
        
} // namespace NYT

