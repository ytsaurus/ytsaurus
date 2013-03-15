#pragma once

#include "helpers.h"

namespace NYT {
namespace NCompression {

////////////////////////////////////////////////////////////////////////////////

void ZlibCompress(int level, StreamSource* source, std::vector<char>* output);

void ZlibDecompress(StreamSource* source, std::vector<char>* output);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCompression
} // namespace NYT

