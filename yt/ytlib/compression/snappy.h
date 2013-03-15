#pragma once

#include "helpers.h"

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

void SnappyCompress(StreamSource* source, std::vector<char>* output);

void SnappyDecompress(StreamSource* source, std::vector<char>* output);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT
