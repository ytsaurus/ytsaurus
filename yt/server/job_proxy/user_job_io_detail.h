#pragma once

#include "public.h"

#include "user_job_io.h"

#include <ytlib/table_client/public.h>

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

    virtual const std::vector<NTableClient::ISchemalessMultiChunkWriterPtr>& GetWriters() const override;
    virtual const NTableClient::ISchemalessMultiChunkReaderPtr& GetReader() const override;

    virtual int GetReduceKeyColumnCount() const override;

    virtual void PopulateResult(NScheduler::NProto::TSchedulerJobResultExt* schedulerJobResultExt) override;

    virtual void CreateReader() override;

    virtual NTableClient::TSchemalessReaderFactory GetReaderFactory() override;

protected:
    IJobHost* Host_;

    const NScheduler::NProto::TSchedulerJobSpecExt& SchedulerJobSpec_;
    NScheduler::TJobIOConfigPtr JobIOConfig_;

    NTableClient::ISchemalessMultiChunkReaderPtr Reader_;
    std::vector<NTableClient::ISchemalessMultiChunkWriterPtr> Writers_;

    NLogging::TLogger Logger;


    virtual NTableClient::ISchemalessMultiChunkWriterPtr DoCreateWriter(
        NTableClient::TTableWriterOptionsPtr options,
        const NChunkClient::TChunkListId& chunkListId,
        const NTransactionClient::TTransactionId& transactionId,
        const NTableClient::TKeyColumns& keyColumns) = 0;

    virtual NTableClient::ISchemalessMultiChunkReaderPtr DoCreateReader(
        NTableClient::TNameTablePtr nameTable,
        const NTableClient::TColumnFilter& columnFilter) = 0;


    NTableClient::ISchemalessMultiChunkReaderPtr CreateRegularReader(
        bool isParallel,
        NTableClient::TNameTablePtr nameTable,
        const NTableClient::TColumnFilter& columnFilter);

    NTableClient::ISchemalessMultiChunkReaderPtr CreateTableReader(
        NChunkClient::TMultiChunkReaderOptionsPtr options,
        const std::vector<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
        NTableClient::TNameTablePtr nameTable,
        const NTableClient::TColumnFilter& columnFilter,
        bool isParallel);

    NTableClient::ISchemalessMultiChunkWriterPtr CreateTableWriter(
        NTableClient::TTableWriterOptionsPtr options,
        const NChunkClient::TChunkListId& chunkListId,
        const NTransactionClient::TTransactionId& transactionId,
        const NTableClient::TKeyColumns& keyColumns);

    NTableClient::NProto::TBoundaryKeysExt GetBoundaryKeys(
        NTableClient::ISchemalessMultiChunkWriterPtr writer) const;

private:
    void InitReader(
        NTableClient::TNameTablePtr nameTable,
        const NTableClient::TColumnFilter& columnFilter);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
