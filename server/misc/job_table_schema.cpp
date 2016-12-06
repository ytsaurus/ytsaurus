#include "job_table_schema.h"

#include <yt/ytlib/table_client/row_base.h>

namespace NYT {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

NTableClient::TBlobTableSchema GetStderrBlobTableSchema()
{
    TBlobTableSchema result;
    result.BlobIdColumns.emplace_back("job_id", EValueType::String);
    return result;
}

NTableClient::TBlobTableSchema GetCoreBlobTableSchema()
{
    TBlobTableSchema result;
    result.BlobIdColumns.emplace_back("job_id", EValueType::String);
    result.BlobIdColumns.emplace_back("core_id", EValueType::Int64);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
