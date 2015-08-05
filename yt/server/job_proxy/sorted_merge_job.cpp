#include "stdafx.h"

#include "sorted_merge_job.h"

#include "config.h"
#include "job_detail.h"

#include <ytlib/chunk_client/chunk_spec.h>

#include <ytlib/table_client/name_table.h>
#include <ytlib/table_client/schemaless_chunk_reader.h>
#include <ytlib/table_client/schemaless_chunk_writer.h>
#include <ytlib/table_client/schemaless_sorted_merging_reader.h>

namespace NYT {
namespace NJobProxy {

using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NScheduler::NProto;
using namespace NTransactionClient;
using namespace NTableClient;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TSortedMergeJob
    : public TSimpleJobBase
{
public:
    explicit TSortedMergeJob(IJobHost* host)
        : TSimpleJobBase(host)
        , MergeJobSpecExt_(JobSpec_.GetExtension(TMergeJobSpecExt::merge_job_spec_ext))
    {
        auto config = host->GetConfig();

        YCHECK(SchedulerJobSpecExt_.output_specs_size() == 1);
        const auto& outputSpec = SchedulerJobSpecExt_.output_specs(0);

        auto keyColumns = FromProto<Stroka>(MergeJobSpecExt_.key_columns());

        auto nameTable = TNameTable::FromKeyColumns(keyColumns);
        std::vector<ISchemalessMultiChunkReaderPtr> readers;

        for (const auto& inputSpec : SchedulerJobSpecExt_.input_specs()) {
            std::vector<TChunkSpec> chunkSpecs(inputSpec.chunks().begin(), inputSpec.chunks().end());

            TotalRowCount_ += GetCumulativeRowCount(chunkSpecs);

            auto reader = CreateSchemalessSequentialMultiChunkReader(
                config->JobIO->TableReader,
                New<TMultiChunkReaderOptions>(),
                host->GetMasterChannel(),
                host->GetBlockCache(),
                host->GetNodeDirectory(),
                std::move(chunkSpecs),
                nameTable,
                TColumnFilter(),
                keyColumns);

            readers.push_back(reader);
        }

        // Read without table index.
        Reader_ = CreateSchemalessSortedMergingReader(readers, keyColumns.size(), false);


        auto transactionId = FromProto<TTransactionId>(SchedulerJobSpecExt_.output_transaction_id());
        auto chunkListId = FromProto<TChunkListId>(outputSpec.chunk_list_id());
        auto options = ConvertTo<TTableWriterOptionsPtr>(TYsonString(outputSpec.table_writer_options()));

        Writer_ = CreateSchemalessMultiChunkWriter(
            config->JobIO->TableWriter,
            options,
            nameTable,
            keyColumns,
            TOwningKey(),
            host->GetMasterChannel(),
            transactionId,
            chunkListId,
            false); // Value reordering not required.
    }

private:
    const TMergeJobSpecExt& MergeJobSpecExt_;

    virtual void CreateReader() override
    { }

    virtual void CreateWriter() override
    { }
};

IJobPtr CreateSortedMergeJob(IJobHost* host)
{
    return New<TSortedMergeJob>(host);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
