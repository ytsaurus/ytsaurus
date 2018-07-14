#pragma once

#include "public.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/chunk_client/public.h>
#include <yt/ytlib/chunk_client/chunk_reader.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/actions/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

struct IUserJobIOFactory
    : public virtual TRefCounted
{
    virtual NTableClient::ISchemalessMultiChunkReaderPtr CreateReader(
        NApi::NNative::IClientPtr client,
        const NNodeTrackerClient::TNodeDescriptor& nodeDescriptor,
        TClosure onNetworkReleased,
        NTableClient::TNameTablePtr nameTable,
        const NTableClient::TColumnFilter& columnFilter) = 0;

    virtual NTableClient::ISchemalessMultiChunkWriterPtr CreateWriter(
        NApi::NNative::IClientPtr client,
        NTableClient::TTableWriterConfigPtr config,
        NTableClient::TTableWriterOptionsPtr options,
        const NChunkClient::TChunkListId& chunkListId,
        const NTransactionClient::TTransactionId& transactionId,
        const NTableClient::TTableSchema& tableSchema,
        const NTableClient::TChunkTimestamps& chunkTimestamps) = 0;
};
DEFINE_REFCOUNTED_TYPE(IUserJobIOFactory)

struct TUserJobIOFactoryBase
    : public IUserJobIOFactory
{
    TUserJobIOFactoryBase(
        const NChunkClient::TClientBlockReadOptions& blockReadOptions,
        NChunkClient::TTrafficMeterPtr trafficMeter);

protected:
    const NChunkClient::TClientBlockReadOptions BlockReadOptions_;
    const NChunkClient::TTrafficMeterPtr TrafficMeter_;
};

////////////////////////////////////////////////////////////////////////////////

IUserJobIOFactoryPtr CreateUserJobIOFactory(
    const IJobSpecHelperPtr& jobSpecHelper,
    const NChunkClient::TClientBlockReadOptions& blockReadOptions,
    NChunkClient::TTrafficMeterPtr trafficMeter);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
