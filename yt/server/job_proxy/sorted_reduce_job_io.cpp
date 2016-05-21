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

////////////////////////////////////////////////////////////////////

class TSortedReduceJobIO
    : public TUserJobIOBase
{
public:
    explicit TSortedReduceJobIO(IJobHostPtr host)
        : TUserJobIOBase(host)
    {
        const auto& reduceJobSpecExt = Host_->GetJobSpec().GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
        ReduceKeyColumnCount_ = reduceJobSpecExt.reduce_key_column_count();
        ForeignKeyColumnCount_ = reduceJobSpecExt.join_key_column_count();
    }

    virtual int GetKeySwitchColumnCount() const override
    {
        return ForeignKeyColumnCount_ != 0 ? ForeignKeyColumnCount_ : ReduceKeyColumnCount_;
    }

    virtual ISchemalessMultiChunkReaderPtr DoCreateReader(
        TNameTablePtr nameTable,
        const TColumnFilter& columnFilter) override
    {
        YCHECK(nameTable->GetSize() == 0 && columnFilter.All);

        const auto& jobSpec = Host_->GetJobSpec();
        const auto& jobSpecExt = jobSpec.GetExtension(TReduceJobSpecExt::reduce_job_spec_ext);
        auto keyColumns = FromProto<TKeyColumns>(jobSpecExt.key_columns());

        std::vector<ISchemalessMultiChunkReaderPtr> readers;
        nameTable = TNameTable::FromKeyColumns(keyColumns);
        auto options = ConvertTo<TTableReaderOptionsPtr>(TYsonString(
            SchedulerJobSpec_.input_specs(0).table_reader_options()));

        for (const auto& inputSpec : SchedulerJobSpec_.input_specs()) {
            // ToDo(psushin): validate that input chunks are sorted.
            std::vector<TChunkSpec> chunks(inputSpec.chunks().begin(), inputSpec.chunks().end());

            auto reader = CreateSchemalessSequentialMultiChunkReader(
                JobIOConfig_->TableReader,
                options,
                Host_->GetClient(),
                Host_->LocalDescriptor(),
                Host_->GetBlockCache(),
                Host_->GetInputNodeDirectory(),
                chunks,
                nameTable,
                columnFilter,
                keyColumns);

            readers.push_back(reader);
        }

        auto reduceReader = CreateSchemalessSortedMergingReader(readers, keyColumns.size());
        if (ForeignKeyColumnCount_ == 0) {
            return reduceReader;
        }

        std::vector<ISchemalessMultiChunkReaderPtr> joinReaders;
        keyColumns.resize(ForeignKeyColumnCount_);

        for (const auto& inputSpec : SchedulerJobSpec_.foreign_input_specs()) {
            std::vector<TChunkSpec> chunks(inputSpec.chunks().begin(), inputSpec.chunks().end());

            auto reader = CreateSchemalessSequentialMultiChunkReader(
                JobIOConfig_->TableReader,
                options,
                Host_->GetClient(),
                Host_->LocalDescriptor(),
                Host_->GetBlockCache(),
                Host_->GetInputNodeDirectory(),
                chunks,
                nameTable,
                columnFilter,
                keyColumns);

            joinReaders.push_back(reader);
        }
        joinReaders.push_back(reduceReader);

        return CreateSchemalessSortedMergingReader(joinReaders, ForeignKeyColumnCount_);
    }

    virtual ISchemalessMultiChunkWriterPtr DoCreateWriter(
        TTableWriterOptionsPtr options,
        const TChunkListId& chunkListId,
        const TTransactionId& transactionId,
        const TKeyColumns& keyColumns) override
    {
        return CreateTableWriter(options, chunkListId, transactionId, keyColumns);
    }

private:
    int ReduceKeyColumnCount_;
    int ForeignKeyColumnCount_;
};

std::unique_ptr<IUserJobIO> CreateSortedReduceJobIO(IJobHostPtr host)
{
    return std::unique_ptr<IUserJobIO>(new TSortedReduceJobIO(host));
}

////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
