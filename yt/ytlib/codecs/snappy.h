#pragma once

#include "helpers.h"

namespace NYT {
namespace NCodec {

////////////////////////////////////////////////////////////////////////////////

void SnappyCompress(StreamSource* source, std::vector<char>* output);

void SnappyDecompress(StreamSource* source, std::vector<char>* output);

////////////////////////////////////////////////////////////////////////////////

} // namespace NCodec
} // namespace NYT
