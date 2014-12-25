#pragma once

#include "public.h"

#include "user_job_io.h"

#include <ytlib/new_table_client/public.h>

#include <ytlib/transaction_client/public.h>

#include <core/logging/log.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TUserJobIOBase
    : public IUserJobIO
{
public:
    TUserJobIOBase(IJobHost* host);

    virtual void Init() override;

    virtual const std::vector<NVersionedTableClient::ISchemalessMultiChunkWriterPtr>& GetWriters() const override;
    virtual const std::vector<NVersionedTableClient::ISchemalessMultiChunkReaderPtr>& GetReaders() const override;

    virtual void PopulateResult(NScheduler::NProto::TSchedulerJobResultExt* schedulerJobResultExt) override;

protected:
    IJobHost* Host_;

    const NScheduler::NProto::TSchedulerJobSpecExt& SchedulerJobSpec_;
    NScheduler::TJobIOConfigPtr JobIOConfig_;

    std::vector<NVersionedTableClient::ISchemalessMultiChunkReaderPtr> Readers_;
    std::vector<NVersionedTableClient::ISchemalessMultiChunkWriterPtr> Writers_;

    NLog::TLogger Logger;


    virtual NVersionedTableClient::ISchemalessMultiChunkWriterPtr DoCreateWriter(
        NVersionedTableClient::TTableWriterOptionsPtr options,
        const NChunkClient::TChunkListId& chunkListId,
        const NTransactionClient::TTransactionId& transactionId,
        const NVersionedTableClient::TKeyColumns& keyColumns) = 0;

    virtual std::vector<NVersionedTableClient::ISchemalessMultiChunkReaderPtr> DoCreateReaders() = 0;


    std::vector<NVersionedTableClient::ISchemalessMultiChunkReaderPtr> CreateRegularReaders(bool isParallel);

    NVersionedTableClient::ISchemalessMultiChunkReaderPtr CreateTableReader(
        NChunkClient::TMultiChunkReaderOptionsPtr options,
        const std::vector<NChunkClient::NProto::TChunkSpec>& chunkSpecs, 
        NVersionedTableClient::TNameTablePtr nameTable,
        bool isParallel);

    NVersionedTableClient::ISchemalessMultiChunkWriterPtr CreateTableWriter(
        NVersionedTableClient::TTableWriterOptionsPtr options,
        const NChunkClient::TChunkListId& chunkListId,
        const NTransactionClient::TTransactionId& transactionId,
        const NVersionedTableClient::TKeyColumns& keyColumns); 
    
    NVersionedTableClient::NProto::TBoundaryKeysExt GetBoundaryKeys(
        NVersionedTableClient::ISchemalessMultiChunkWriterPtr writer) const;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
