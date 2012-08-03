#pragma once

#include "source.h"

namespace NYT {
namespace NCodec {

////////////////////////////////////////////////////////////////////////////////

void ZlibCompress(int level, StreamSource* source, std::vector<char>* output);

void ZlibDecompress(StreamSource* source, std::vector<char>* output);

////////////////////////////////////////////////////////////////////////////////
        
}} // namespace NYT::NCodec

