#pragma once

#include "public.h"

#include <yt/yt/library/query/base/public.h>

#include <yt/yt/client/table_client/public.h>

#include <yt/yt/client/chunk_client/public.h>

#include <yt/yt/ytlib/sequoia_client/chunk_meta_extensions.record.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

TChunkMetaExtensionsKey GetChunkMetaExtensionsKey(NChunkClient::TChunkId chunkId);

NTableClient::TColumnFilter GetChunkMetaExtensionsColumnFilter(const THashSet<int>& extensionTags);

void ToProto(
    NYT::NProto::TExtensionSet* protoExtensions,
    const TChunkMetaExtensions& extensions);
void FromProto(
    TChunkMetaExtensions* extensions,
    const NYT::NProto::TExtensionSet& protoExtensions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
