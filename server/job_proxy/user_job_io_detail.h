#pragma once

#include "public.h"
#include "user_job_io.h"

#include <yt/ytlib/table_client/public.h>

#include <yt/core/logging/log.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

class TUserJobIOBase
    : public IUserJobIO
{
public:
    explicit TUserJobIOBase(IJobHostPtr host);

    virtual void Init() override;

    virtual std::vector<NTableClient::ISchemalessMultiChunkWriterPtr> GetWriters() const override;
    virtual NTableClient::ISchemalessMultiChunkReaderPtr GetReader() const override;

    virtual int GetKeySwitchColumnCount() const override;

    virtual void PopulateResult(NScheduler::NProto::TSchedulerJobResultExt* schedulerJobResultExt) override;

    virtual void CreateReader() override;

    virtual NTableClient::TSchemalessReaderFactory GetReaderFactory() override;

protected:
    const IJobHostPtr Host_;

    std::atomic<bool> Initialized_ = { false };

    const NScheduler::NProto::TSchedulerJobSpecExt& SchedulerJobSpec_;
    NScheduler::TJobIOConfigPtr JobIOConfig_;

    NTableClient::ISchemalessMultiChunkReaderPtr Reader_;
    std::vector<NTableClient::ISchemalessMultiChunkWriterPtr> Writers_;

    NLogging::TLogger Logger;


    virtual NTableClient::ISchemalessMultiChunkWriterPtr DoCreateWriter(
        NTableClient::TTableWriterOptionsPtr options,
        const NChunkClient::TChunkListId& chunkListId,
        const NTransactionClient::TTransactionId& transactionId,
        const NTableClient::TTableSchema& tableSchema) = 0;

    virtual NTableClient::ISchemalessMultiChunkReaderPtr DoCreateReader(
        NTableClient::TNameTablePtr nameTable,
        const NTableClient::TColumnFilter& columnFilter) = 0;


    NTableClient::ISchemalessMultiChunkReaderPtr CreateRegularReader(
        bool isParallel,
        NTableClient::TNameTablePtr nameTable,
        const NTableClient::TColumnFilter& columnFilter);

    NTableClient::ISchemalessMultiChunkReaderPtr CreateTableReader(
        NTableClient::TTableReaderOptionsPtr options,
        const std::vector<NChunkClient::NProto::TChunkSpec>& chunkSpecs,
        NTableClient::TNameTablePtr nameTable,
        const NTableClient::TColumnFilter& columnFilter,
        bool isParallel);

    NTableClient::ISchemalessMultiChunkWriterPtr CreateTableWriter(
        NTableClient::TTableWriterOptionsPtr options,
        const NChunkClient::TChunkListId& chunkListId,
        const NTransactionClient::TTransactionId& transactionId,
        const NTableClient::TTableSchema& tableSchema);

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
