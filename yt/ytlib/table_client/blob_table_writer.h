#pragma once

#include "unversioned_row.h"

#include <yt/ytlib/api/client.h>

#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/core/misc/blob_output.h>
#include <yt/core/misc/chunked_memory_pool.h>

#include <yt/core/yson/string.h>

#include <util/stream/output.h>

namespace NYT {
namespace NTableClient {

////////////////////////////////////////////////////////////////////////////////

struct TBlobTableSchema
{
    // Names of special blob columns.
    static const TString PartIndexColumn;
    static const TString DataColumn;

    // Do not specify anything except name and value
    // type in all column schemas.
    std::vector<TColumnSchema> BlobIdColumns;

    TTableSchema ToTableSchema() const;
};

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
        NApi::INativeClientPtr client,
        TBlobTableWriterConfigPtr blobTableWriterConfig,
        TTableWriterOptionsPtr tableWriterOptions,
        const NCypressClient::TTransactionId& transactionId,
        const NChunkClient::TChunkListId& chunkListId,
        NChunkClient::TTrafficMeterPtr trafficMeter);

    NScheduler::NProto::TOutputResult GetOutputResult() const;

private:
    virtual void DoWrite(const void* buf, size_t size) override;
    virtual void DoFlush() override;
    virtual void DoFinish() override;

private:
    TUnversionedOwningRow BlobIdColumnValues_;

    ISchemalessMultiChunkWriterPtr MultiChunkWriter_;
    TBlobOutput Buffer_;
    const size_t PartSize_;
    int WrittenPartCount_ = 0;
    bool Finished_ = false;

    // Table column ids.
    std::vector<int> BlobIdColumnIds_;
    int PartIndexColumnId_ = -1;
    int DataColumnId_ = -1;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
