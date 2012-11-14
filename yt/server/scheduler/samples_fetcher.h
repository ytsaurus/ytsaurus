#pragma once

#include "public.h"

#include <ytlib/misc/error.h>

#include <ytlib/logging/tagged_logger.h>

#include <ytlib/chunk_client/data_node_service_proxy.h>

#include <ytlib/table_client/public.h>
#include <ytlib/table_client/table_chunk_meta.pb.h>
#include <ytlib/table_client/table_reader.pb.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

//! Fetches samples for a bunch of table chunks by requesting
//! them directly from data nodes.
class TSamplesFetcher
    : public TRefCounted
{
public:
    typedef NChunkClient::TDataNodeServiceProxy::TRspGetTableSamplesPtr TResponsePtr;

    TSamplesFetcher(
        TSchedulerConfigPtr config,
        TSortOperationSpecPtr spec,
        const TOperationId& operationId);

    void SetDesiredSampleCount(int desiredSampleCount);

    bool Prepare(const std::vector<NTableClient::TRefCountedInputChunkPtr>& chunks);

    void CreateNewRequest(const Stroka& address);

    // If False is returned then samples from this chunk are not required.
    bool AddChunkToRequest(NTableClient::TRefCountedInputChunkPtr inputChunk);

    TFuture<TResponsePtr> InvokeRequest();

    TError ProcessResponseItem(
        TResponsePtr rsp, 
        int index,
        NTableClient::TRefCountedInputChunkPtr inputChunk);

    const std::vector<NTableClient::NProto::TKey>& GetSamples() const;

    NLog::TTaggedLogger& GetLogger();

private:
    TSchedulerConfigPtr Config;
    TSortOperationSpecPtr Spec;
    int DesiredSampleCount;

    i64 SizeBetweenSamples;
    i64 CurrentSize;
    i64 CurrentSampleCount;

    NLog::TTaggedLogger Logger;

    //! All samples fetched so far.
    std::vector<NTableClient::NProto::TKey> Samples;

    NChunkClient::TDataNodeServiceProxy::TReqGetTableSamplesPtr CurrentRequest;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
