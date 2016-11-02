#include "stderr_table_schema.h"

namespace NYT {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

NTableClient::TBlobTableSchema GetStderrBlobTableSchema()
{
    TBlobTableSchema result;
    result.BlobIdColumns.emplace_back("job_id");
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
