#pragma once

#include <yt/yt/client/ypath/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NFileClient {

////////////////////////////////////////////////////////////////////////////////

// TODO(achains): Move to chunk_client/helpers.h?
NChunkClient::TMultiChunkWriterOptionsPtr GetWriterOptions(
    const NYTree::IAttributeDictionaryPtr& attributes,
    const NYPath::TRichYPath& path,
    const IMemoryUsageTrackerPtr& tracker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFileClient
