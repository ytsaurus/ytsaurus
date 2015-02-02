#include "stdafx.h"

#include "map_job_io.h"
#include "user_job_io_detail.h"

#include <ytlib/table_client/sync_writer.h>
#include <ytlib/table_client/sync_reader.h>

#include <ytlib/scheduler/config.h>

namespace NYT {
namespace NJobProxy {

using namespace NTableClient;
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
    virtual ISyncWriterUnsafePtr DoCreateWriter(
        TTableWriterOptionsPtr options,
        const TChunkListId& chunkListId,
        const TTransactionId& transactionId) override
    {
        return CreateTableWriter(options, chunkListId, transactionId);
    }

    virtual std::vector<ISyncReaderPtr> DoCreateReaders() override
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
