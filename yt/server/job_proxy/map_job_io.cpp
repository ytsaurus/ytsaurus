#include "map_job_io.h"
#include "user_job_io_detail.h"

#include <yt/ytlib/scheduler/config.h>

#include <yt/ytlib/table_client/schemaless_chunk_reader.h>

namespace NYT {
namespace NJobProxy {

using namespace NTableClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NTransactionClient;

using NChunkClient::TDataSliceDescriptor;
using NYT::TRange;

////////////////////////////////////////////////////////////////////

class TMapJobIO
    : public TUserJobIOBase
{
public:
    TMapJobIO(IJobHostPtr host, bool useParallelReader)
        : TUserJobIOBase(host)
        , UseParallelReader_(useParallelReader)
    { }

    virtual void InterruptReader() override
    {
        if (Reader_) {
            Reader_->Interrupt();
        }
    }

    virtual std::vector<TDataSliceDescriptor> GetUnreadDataSliceDescriptors() const override
    {
        if (Reader_) {
            return Reader_->GetUnreadDataSliceDescriptors(TRange<TUnversionedRow>());
        }
        return std::vector<TDataSliceDescriptor>();
    }

private:
    const bool UseParallelReader_;


    virtual ISchemalessMultiChunkWriterPtr DoCreateWriter(
        TTableWriterConfigPtr config,
        TTableWriterOptionsPtr options,
        const TChunkListId& chunkListId,
        const TTransactionId& transactionId,
        const TTableSchema& tableSchema) override
    {
        return CreateTableWriter(config, options, chunkListId, transactionId, tableSchema);
    }

    virtual ISchemalessMultiChunkReaderPtr DoCreateReader(
        TNameTablePtr nameTable,
        const TColumnFilter& columnFilter) override
    {
        return CreateRegularReader(UseParallelReader_, std::move(nameTable), columnFilter);
    }
};

////////////////////////////////////////////////////////////////////

std::unique_ptr<IUserJobIO> CreateMapJobIO(IJobHostPtr host)
{
    return std::unique_ptr<IUserJobIO>(new TMapJobIO(host, true));
}

std::unique_ptr<IUserJobIO> CreateOrderedMapJobIO(IJobHostPtr host)
{
    return std::unique_ptr<IUserJobIO>(new TMapJobIO(host, false));
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
