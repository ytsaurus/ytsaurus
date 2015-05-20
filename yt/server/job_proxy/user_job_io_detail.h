#pragma once

#include "public.h"

#include "user_job_io.h"

#include <ytlib/new_table_client/public.h>

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
    virtual const NVersionedTableClient::ISchemalessMultiChunkReaderPtr& GetReader() const override;

    virtual void PopulateResult(NScheduler::NProto::TSchedulerJobResultExt* schedulerJobResultExt) override;

    virtual bool IsKeySwitchEnabled() const override;

protected:
    IJobHost* Host_;

    const NScheduler::NProto::TSchedulerJobSpecExt& SchedulerJobSpec_;
    NScheduler::TJobIOConfigPtr JobIOConfig_;

    NVersionedTableClient::ISchemalessMultiChunkReaderPtr Reader_;
    std::vector<NVersionedTableClient::ISchemalessMultiChunkWriterPtr> Writers_;

    NLogging::TLogger Logger;


    virtual NVersionedTableClient::ISchemalessMultiChunkWriterPtr DoCreateWriter(
        NVersionedTableClient::TTableWriterOptionsPtr options,
        const NChunkClient::TChunkListId& chunkListId,
        const NTransactionClient::TTransactionId& transactionId,
        const NVersionedTableClient::TKeyColumns& keyColumns) = 0;

    virtual NVersionedTableClient::ISchemalessMultiChunkReaderPtr DoCreateReader(
        NVersionedTableClient::TNameTablePtr nameTable,
        const NVersionedTableClient::TColumnFilter& columnFilter) = 0;


    NVersionedTableClient::ISchemalessMultiChunkReaderPtr CreateRegularReader(
        bool isParallel,
        NVersionedTableClient::TNameTablePtr nameTable,
        const NVersionedTableClient::TColumnFilter& columnFilter);

    NVersionedTableClient::ISchemalessMultiChunkReaderPtr CreateTableReader(
        NChunkClient::TMultiChunkReaderOptionsPtr options,
        const std::vector<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
        NVersionedTableClient::TNameTablePtr nameTable,
        const NVersionedTableClient::TColumnFilter& columnFilter,
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
