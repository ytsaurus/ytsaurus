#pragma once

#include "public.h"

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TBlock
{
    std::vector<TSharedRef> Data;
    NProto::TDataBlockMeta Meta;
    std::optional<int> GroupIndex;
    std::optional<ui32> SegmentMetaOffset;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
