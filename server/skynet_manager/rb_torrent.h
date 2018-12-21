#pragma once

#include "public.h"

#include <yt/server/skynet_manager/resource.pb.h>

#include <yt/core/crypto/crypto.h>

#include <yt/core/yson/consumer.h>

#include <yt/client/table_client/unversioned_row.h>

#include <util/stream/buffer.h>

namespace NYT::NSkynetManager {

////////////////////////////////////////////////////////////////////////////////

struct TResourceDescription
{
    TResourceId ResourceId;
    NYTree::INodePtr TorrentMeta;
};

TResourceDescription ConvertResource(const NProto::TResource& resource, bool needHash, bool needMeta);

////////////////////////////////////////////////////////////////////////////////

struct TTableShard
{
    NTableClient::TOwningKey Key;
    NProto::TResource Resource;
};

////////////////////////////////////////////////////////////////////////////////

struct TRowRangeLocation
{
    i64 RowIndex;
    i64 RowCount = 0;

    std::optional<i64> LowerLimit;

    NChunkClient::TChunkId ChunkId;

    std::vector<TString> Nodes;

    friend bool operator < (const TRowRangeLocation& rhs, const TRowRangeLocation& lhs);
};

NYTree::INodePtr MakeLinks(
    const NProto::TResource& resource,
    const std::vector<TRowRangeLocation>& locations);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSkynetManager
