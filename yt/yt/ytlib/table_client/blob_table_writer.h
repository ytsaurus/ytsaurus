#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/client.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/data_sink.h>
#include <yt/yt/ytlib/chunk_client/chunk_writer.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/blob_reader.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/misc/blob_output.h>

#include <yt/yt/core/yson/string.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>

#include <util/stream/output.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

//
// TBlobTableWriter allows to split blob to the parts of specified size
// (size is configured in blobTableWriterConfig) and write this parts into a table.
//
// Each row of the table will contain
//   - BlobIdColumns: bunch of string columns that identify blob (blobIdColumnValues)
//   - PartIndexColumn: int64 column that shows part index inside blob
//   - DataColumn: string column that contains actual data from blob
//
// IMPORTANT:
//   `Finish()` ought to be called once all writes are complete.
//   Destructor doesn't call Finish, since it involves complicated logic including WaitFor
//   that is not good to call from destructor.
class TBlobTableWriter
    : public IOutputStream
{
public:
    TBlobTableWriter(
        const TBlobTableSchema& schema,
        const std::vector<NYson::TYsonString>& blobIdColumnValues,
        NApi::NNative::IClientPtr client,
        TBlobTableWriterConfigPtr blobTableWriterConfig,
        TTableWriterOptionsPtr tableWriterOptions,
        NTransactionClient::TTransactionId transactionId,
        TMasterTableSchemaId schemaId,
        const std::optional<NChunkClient::TDataSink>& dataSink,
        NChunkClient::TChunkListId chunkListId,
        NChunkClient::TTrafficMeterPtr trafficMeter,
        NConcurrency::IThroughputThrottlerPtr throttler,
        NChunkClient::IChunkWriter::TWriteBlocksOptions writeBlocksOptions);

    NControllerAgent::NProto::TOutputResult GetOutputResult(bool withChunkSpecs = false) const;

private:
    void DoWrite(const void* buf, size_t size) override;
    void DoFlush() override;
    void DoFinish() override;

private:
    TUnversionedOwningRow BlobIdColumnValues_;

    ISchemalessMultiChunkWriterPtr MultiChunkWriter_;
    TBlobOutput Buffer_;
    const size_t PartSize_;
    int WrittenPartCount_ = 0;
    bool Finished_ = false;
    std::atomic<bool> Failed_ =  false ;

    // Table column ids.
    std::vector<int> BlobIdColumnIds_;
    int PartIndexColumnId_ = -1;
    int DataColumnId_ = -1;

    NLogging::TLogger Logger;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
