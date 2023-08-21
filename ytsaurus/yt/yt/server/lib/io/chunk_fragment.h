#pragma once

#include "public.h"

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

struct TChunkFragmentDescriptor
{
    //! Length of the fragment.
    int Length;
    //! Chunk-wise block index.
    int BlockIndex;
    //! Block-wise offset.
    i64 BlockOffset;
};

void FormatValue(TStringBuilderBase* builder, const TChunkFragmentDescriptor& descriptor, TStringBuf spec);
TString ToString(const TChunkFragmentDescriptor& descriptor);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
