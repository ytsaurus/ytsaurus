#pragma once

#include "public.h"

#include <yt/yt/server/node/tablet_node/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/query_client/public.h>

#include <yt/yt/library/query/distributed/public.h>

#include <yt/yt/library/query/engine_api/join_profiler.h>

#include <yt/yt/client/table_client/unversioned_reader.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/compression/public.h>

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

struct TSessionRowset
{
    TFuture<TSharedRange<NTableClient::TUnversionedRow>> AsyncRowset;
    NTableClient::TTableSchemaPtr Schema;
};

////////////////////////////////////////////////////////////////////////////////

struct IDistributedSession
    : public TRefCounted
{
    virtual void InsertOrThrow(
        NQueryClient::TRowsetId id,
        NTableClient::ISchemafulUnversionedReaderPtr rowset,
        NTableClient::TTableSchemaPtr schema) = 0;

    virtual TSessionRowset GetOrThrow(NQueryClient::TRowsetId id) const = 0;

    virtual void RenewLease() const = 0;

    virtual std::vector<std::string> GetPropagationAddresses() const = 0;

    virtual void ErasePropagationAddresses(const std::vector<std::string>& addresses) = 0;

    virtual NCompression::ECodec GetCodecId() const = 0;

    virtual const IMemoryChunkProviderPtr& GetMemoryChunkProvider() const = 0;

    virtual TFuture<void> PushRowset(
        const std::string& nodeAddress,
        const NQueryClient::TShufflePart& shufflePart,
        NTableClient::TTableSchemaPtr schema,
        NNodeTrackerClient::INodeChannelFactoryPtr channelFactory,
        i64 desiredUncompressedResponseBlockSize) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedSession)

////////////////////////////////////////////////////////////////////////////////

IDistributedSessionPtr CreateDistributedSession(
    NQueryClient::TDistributedSessionId sessionId,
    NConcurrency::TLease lease,
    NCompression::ECodec codecId,
    TDuration retentionTime,
    std::optional<i64> memoryLimitPerNode,
    IMemoryChunkProviderPtr memoryChunkProvider);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
