#pragma once

#include "public.h"

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/query_client/public.h>

#include <yt/yt/client/table_client/unversioned_reader.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/compression/public.h>

namespace NYT::NQueryAgent {

////////////////////////////////////////////////////////////////////////////////

struct IDistributedSession
    : public TRefCounted
{
    virtual void InsertOrThrow(
        NTableClient::ISchemafulUnversionedReaderPtr reader,
        NQueryClient::TRowsetId id) = 0;

    virtual NTableClient::ISchemafulUnversionedReaderPtr GetOrThrow(NQueryClient::TRowsetId id) const = 0;

    virtual void RenewLease() const = 0;

    virtual std::vector<TString> GetPropagationAddresses() const = 0;

    virtual void ErasePropagationAddresses(const std::vector<TString>& addresses) = 0;

    virtual NCompression::ECodec GetCodecId() const = 0;

    virtual TFuture<void> PushRowset(
        const TString& nodeAddress,
        NQueryClient::TRowsetId rowsetId,
        NTableClient::TTableSchemaPtr schema,
        const std::vector<TRange<NTableClient::TUnversionedRow>>& subranges,
        NNodeTrackerClient::INodeChannelFactoryPtr channelFactory,
        size_t desiredUncompressedResponseBlockSize) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedSession)

////////////////////////////////////////////////////////////////////////////////

IDistributedSessionPtr CreateDistributedSession(
    NQueryClient::TDistributedSessionId sessionId,
    NConcurrency::TLease lease,
    NCompression::ECodec codecId,
    TDuration retentionTime);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryAgent
