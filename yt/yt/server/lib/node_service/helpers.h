#pragma once

#include <yt/yt/ytlib/table_client/public.h>
#include <yt/yt/ytlib/table_client/samples_fetcher.h>

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/proto/chunk_slice.pb.h>
#include <yt/yt/ytlib/chunk_client/proto/data_node_service.pb.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

std::vector<TError> ProcessGetChunkSlicesRequest(
    const NChunkClient::NProto::TReqGetChunkSlices& request,
    NRpc::TTypedServiceResponse<NChunkClient::NProto::TRspGetChunkSlices>& response,
    int requestCount,
    const std::vector<TErrorOr<NChunkClient::TRefCountedChunkMetaPtr>>& chunkMetas);

////////////////////////////////////////////////////////////////////////////////

std::vector<TError> ProcessGetTableSamplesRequest(
    const NChunkClient::NProto::TReqGetTableSamples& request,
    NRpc::TTypedServiceResponse<NChunkClient::NProto::TRspGetTableSamples>& response,
    int requestCount,
    NTableClient::ESamplingPolicy samplingPolicy,
    const NTableClient::TKeyColumns& keyColumns,
    i32 maxSampleSize,
    const std::vector<TErrorOr<NChunkClient::TRefCountedChunkMetaPtr>>& chunkMetas);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
