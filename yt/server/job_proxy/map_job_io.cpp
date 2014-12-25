#include "stdafx.h"

#include "map_job_io.h"
#include "user_job_io_detail.h"

#include <ytlib/new_table_client/schemaless_chunk_writer.h>
#include <ytlib/new_table_client/schemaless_chunk_reader.h>

#include <ytlib/scheduler/config.h>

namespace NYT {
namespace NJobProxy {

using namespace NVersionedTableClient;
using namespace NChunkClient;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////

class TMapJobIO
    : public TUserJobIOBase
{
public:
    TMapJobIO(IJobHost* host)
        : TUserJobIOBase(host)
    { }


private:
    virtual ISchemalessMultiChunkWriterPtr DoCreateWriter(
        TTableWriterOptionsPtr options,
        const TChunkListId& chunkListId,
        const TTransactionId& transactionId,
        const TKeyColumns& keyColumns) override
    {
        return CreateTableWriter(options, chunkListId, transactionId, keyColumns);
    }

    virtual std::vector<ISchemalessMultiChunkReaderPtr> DoCreateReaders() override
    {
        return CreateRegularReaders(true);
    }

};

std::unique_ptr<IUserJobIO> CreateMapJobIO(IJobHost* host)
{
    return std::unique_ptr<IUserJobIO>(new TMapJobIO(host));
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
