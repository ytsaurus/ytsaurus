#include "stdafx.h"

#include "map_job_io.h"
#include "user_job_io_detail.h"

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
    TMapJobIO(IJobHost* host, bool isParallel)
        : TUserJobIOBase(host)
        , IsParallel_(isParallel)
    { }

private:
    bool IsParallel_;

    virtual ISchemalessMultiChunkWriterPtr DoCreateWriter(
        TTableWriterOptionsPtr options,
        const TChunkListId& chunkListId,
        const TTransactionId& transactionId,
        const TKeyColumns& keyColumns) override
    {
        return CreateTableWriter(options, chunkListId, transactionId, keyColumns);
    }

    virtual ISchemalessMultiChunkReaderPtr DoCreateReader(
        TNameTablePtr nameTable,
        const TColumnFilter& columnFilter) override
    {
        return CreateRegularReader(IsParallel_, std::move(nameTable), columnFilter);
    }

};

std::unique_ptr<IUserJobIO> CreateMapJobIO(IJobHost* host)
{
    return std::unique_ptr<IUserJobIO>(new TMapJobIO(host, true));
}

std::unique_ptr<IUserJobIO> CreateOrderedMapJobIO(IJobHost* host)
{
    return std::unique_ptr<IUserJobIO>(new TMapJobIO(host, false));
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
