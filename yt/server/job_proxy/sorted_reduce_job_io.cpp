#include "sorted_reduce_job_io.h"
#include "config.h"
#include "job.h"
#include "user_job_io_detail.h"

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>
#include <yt/ytlib/table_client/schemaless_sorted_merging_reader.h>

namespace NYT {
namespace NJobProxy {

using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NTableClient;
using namespace NChunkClient;
using namespace NTransactionClient;
using namespace NChunkClient::NProto;
using namespace NJobTrackerClient::NProto;
using namespace NYTree;
using namespace NYson;

using NChunkClient::TDataSliceDescriptor;
using NYT::TRange;

////////////////////////////////////////////////////////////////////

class TSortedReduceJobIO
    : public TUserJobIOBase
{
public:
    TSortedReduceJobIO(IJobHostPtr host, bool interruptAtKeyEdge)
        : TUserJobIOBase(host)
        , InterruptAtKeyEdge_(interruptAtKeyEdge)
    {
        const auto& reduceJobSpecExt = Host_->GetJobSpec().GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
        PrimaryKeyColumnCount_ = reduceJobSpecExt.key_columns_size();
        ReduceKeyColumnCount_ = reduceJobSpecExt.reduce_key_column_count();
        ForeignKeyColumnCount_ = reduceJobSpecExt.join_key_column_count();
    }

    virtual int GetKeySwitchColumnCount() const override
    {
        return ForeignKeyColumnCount_ != 0 ? ForeignKeyColumnCount_ : ReduceKeyColumnCount_;
    }

    ISchemalessMultiChunkReaderPtr CreateTableReader(
        NTableClient::TTableReaderOptionsPtr options,
        std::vector<TDataSliceDescriptor> dataSliceDescriptors,
        const TNameTablePtr& nameTable,
        const TColumnFilter& columnFilter,
        const TKeyColumns& keyColumns)
    {
        return CreateSchemalessSequentialMultiChunkReader(
            JobIOConfig_->TableReader,
            options,
            Host_->GetClient(),
            Host_->LocalDescriptor(),
            Host_->GetBlockCache(),
            Host_->GetInputNodeDirectory(),
            std::move(dataSliceDescriptors),
            nameTable,
            columnFilter,
            keyColumns);
    }

    virtual ISchemalessMultiChunkReaderPtr DoCreateReader(
        TNameTablePtr nameTable,
        const TColumnFilter& columnFilter) override
    {
        YCHECK(nameTable->GetSize() == 0 && columnFilter.All);

        const auto& jobSpec = Host_->GetJobSpec();
        const auto& jobSpecExt = jobSpec.GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
        auto keyColumns = FromProto<TKeyColumns>(jobSpecExt.key_columns());

        std::vector<ISchemalessMultiChunkReaderPtr> primaryReaders;
        nameTable = TNameTable::FromKeyColumns(keyColumns);
        auto options = ConvertTo<TTableReaderOptionsPtr>(TYsonString(
            SchedulerJobSpec_.input_table_specs(0).table_reader_options()));

        for (const auto& inputSpec : SchedulerJobSpec_.input_table_specs()) {
            // ToDo(psushin): validate that input chunks are sorted.
            auto dataSliceDescriptors = FromProto<std::vector<TDataSliceDescriptor>>(inputSpec.data_slice_descriptors());
            auto reader = CreateTableReader(options, std::move(dataSliceDescriptors), nameTable, columnFilter, keyColumns);
            primaryReaders.emplace_back(std::move(reader));
        }

        std::vector<ISchemalessMultiChunkReaderPtr> foreignReaders;
        keyColumns.resize(ForeignKeyColumnCount_);

        for (const auto& inputSpec : SchedulerJobSpec_.foreign_input_table_specs()) {
            auto dataSliceDescriptors = FromProto<std::vector<TDataSliceDescriptor>>(inputSpec.data_slice_descriptors());
            auto reader = CreateTableReader(options, std::move(dataSliceDescriptors), nameTable, columnFilter, keyColumns);
            foreignReaders.push_back(reader);
        }

        auto readerFactory = (InterruptAtKeyEdge_) ? CreateSchemalessSortedJoiningReader : CreateSchemalessJoinReduceJoiningReader;
        Reader_ = readerFactory(primaryReaders, PrimaryKeyColumnCount_, foreignReaders, ForeignKeyColumnCount_);
        return Reader_;
    }

    virtual ISchemalessMultiChunkWriterPtr DoCreateWriter(
        TTableWriterConfigPtr config,
        TTableWriterOptionsPtr options,
        const TChunkListId& chunkListId,
        const TTransactionId& transactionId,
        const TTableSchema& tableSchema) override
    {
        return CreateTableWriter(config, options, chunkListId, transactionId, tableSchema);
    }

    virtual void InterruptReader() override
    {
        if (Reader_) {
            Reader_->Interrupt();
        }
    }

    virtual std::vector<TDataSliceDescriptor> GetUnreadDataSliceDescriptors() const override
    {
        return Reader_->GetUnreadDataSliceDescriptors(TRange<TUnversionedRow>());
    }

private:
    bool InterruptAtKeyEdge_;
    int PrimaryKeyColumnCount_;
    int ReduceKeyColumnCount_;
    int ForeignKeyColumnCount_;
    ISchemalessMultiChunkReaderPtr Reader_;
};

////////////////////////////////////////////////////////////////////

std::unique_ptr<IUserJobIO> CreateSortedReduceJobIO(IJobHostPtr host)
{
    return std::unique_ptr<IUserJobIO>(new TSortedReduceJobIO(host, true));
}

std::unique_ptr<IUserJobIO> CreateJoinReduceJobIO(IJobHostPtr host)
{
    return std::unique_ptr<IUserJobIO>(new TSortedReduceJobIO(host, false));
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
