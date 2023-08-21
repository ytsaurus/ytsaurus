#include "chunk_fragment.h"

#include <library/cpp/yt/string/format.h>
#include <library/cpp/yt/string/string_builder.h>

namespace NYT::NIO {

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TChunkFragmentDescriptor& descriptor, TStringBuf /*spec*/)
{
    builder->AppendFormat("{%v,%v,%v}",
        descriptor.Length,
        descriptor.BlockIndex,
        descriptor.BlockOffset);
}

TString ToString(const TChunkFragmentDescriptor& descriptor)
{
    return ToStringViaBuilder(descriptor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO
