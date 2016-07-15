#pragma once

#include "public.h"
#include "user_job_io.h"

#include <yt/ytlib/chunk_client/data_slice_descriptor.h>

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
    ~TUserJobIOBase();

    virtual void Init() override;

    virtual std::vector<NTableClient::ISchemalessMultiChunkWriterPtr> GetWriters() const override;
    virtual NTableClient::ISchemalessMultiChunkReaderPtr GetReader() const override;
    virtual TOutputStream* GetStderrTableWriter() const override;

    virtual int GetKeySwitchColumnCount() const override;

    virtual void PopulateResult(NScheduler::NProto::TSchedulerJobResultExt* schedulerJobResultExt) override;
    virtual void PopulateStderrResult(NScheduler::NProto::TSchedulerJobResultExt* schedulerJobResultExt) override;

    virtual void CreateReader() override;

    virtual NTableClient::TSchemalessReaderFactory GetReaderFactory() override;

    virtual void InterruptReader() override;

    virtual std::vector<NChunkClient::TDataSliceDescriptor> GetUnreadDataSliceDescriptors() const override;

protected:
    const IJobHostPtr Host_;

    const NScheduler::NProto::TSchedulerJobSpecExt& SchedulerJobSpec_;
    const NScheduler::TJobIOConfigPtr JobIOConfig_;

    std::atomic<bool> Initialized_ = {false};

    NTableClient::ISchemalessMultiChunkReaderPtr Reader_;
    std::vector<NTableClient::ISchemalessMultiChunkWriterPtr> Writers_;
    std::unique_ptr<NTableClient::TBlobTableWriter> StderrTableWriter_;

    NLogging::TLogger Logger;


    virtual NTableClient::ISchemalessMultiChunkWriterPtr DoCreateWriter(
        NTableClient::TTableWriterConfigPtr config,
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
        std::vector<NChunkClient::TDataSliceDescriptor> dataSliceDescriptors,
        NTableClient::TNameTablePtr nameTable,
        const NTableClient::TColumnFilter& columnFilter,
        bool isParallel);

    NTableClient::ISchemalessMultiChunkWriterPtr CreateTableWriter(
        NTableClient::TTableWriterConfigPtr config,
        NTableClient::TTableWriterOptionsPtr options,
        const NChunkClient::TChunkListId& chunkListId,
        const NTransactionClient::TTransactionId& transactionId,
        const NTableClient::TTableSchema& tableSchema);

private:
    void InitReader(
        NTableClient::TNameTablePtr nameTable,
        const NTableClient::TColumnFilter& columnFilter);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
