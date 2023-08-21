#include "user_job_read_controller.h"

#include "any_to_composite_converter.h"
#include "helpers.h"
#include "job_spec_helper.h"
#include "user_job_io_factory.h"

#include <yt/yt/ytlib/table_client/helpers.h>
#include <yt/yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/yt/client/table_client/adapters.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/ytlib/table_client/schemaless_multi_chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/async_stream.h>

#include <yt/yt/core/ytree/convert.h>

#include <algorithm>

namespace NYT::NJobProxy {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NFormats;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;
using namespace NScheduler;
using namespace NScheduler::NProto;

////////////////////////////////////////////////////////////////////////////////

class TUserJobReadController
    : public IUserJobReadController
{
public:
    TUserJobReadController(
        IJobSpecHelperPtr jobSpecHelper,
        IInvokerPtr invoker,
        TClosure onNetworkRelease,
        IUserJobIOFactoryPtr userJobIOFactory,
        std::optional<TString> udfDirectory)
        : JobSpecHelper_(std::move(jobSpecHelper))
        , SerializedInvoker_(CreateSerializedInvoker(std::move(invoker), "user_job_read_controller"))
        , OnNetworkRelease_(onNetworkRelease)
        , UserJobIOFactory_(userJobIOFactory)
        , UdfDirectory_(std::move(udfDirectory))
    { }

    //! Returns closure that launches data transfer to given async output.
    TCallback<TFuture<void>()> PrepareJobInputTransfer(const IAsyncOutputStreamPtr& asyncOutput) override
    {
        const auto& schedulerJobSpecExt = JobSpecHelper_->GetSchedulerJobSpecExt();

        const auto& userJobSpec = schedulerJobSpecExt.user_job_spec();

        auto format = ConvertTo<TFormat>(TYsonString(userJobSpec.input_format()));
        if (schedulerJobSpecExt.has_input_query_spec()) {
            return PrepareInputActionsQuery(schedulerJobSpecExt.input_query_spec(), format, asyncOutput);
        } else {
            return PrepareInputActionsPassthrough(format, asyncOutput);
        }
    }

    double GetProgress() const override
    {
        if (!Initialized_) {
            return 0;
        }

        i64 total = Reader_->GetTotalRowCount();
        i64 current = Reader_->GetSessionRowIndex();

        if (total == 0) {
            return 0.0;
        }

        return std::clamp(current / static_cast<double>(total), 0.0, 1.0);
    }

    TFuture<std::vector<TBlob>> GetInputContext() const override
    {
        if (!Initialized_) {
            return MakeFuture(std::vector<TBlob>());
        }

        return BIND([=, this, this_ = MakeStrong(this)] {
            std::vector<TBlob> result;
            for (const auto& input : FormatWriters_) {
                result.push_back(input->GetContext());
            }
            return result;
        })
        .AsyncVia(SerializedInvoker_)
        .Run();
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return Initialized_ ? Reader_->GetFailedChunkIds() : std::vector<TChunkId>();
    }

    std::optional<NChunkClient::NProto::TDataStatistics> GetDataStatistics() const override
    {
        if (!Initialized_) {
            return std::nullopt;
        }
        return Reader_->GetDataStatistics();
    }

    std::optional<TCodecStatistics> GetDecompressionStatistics() const override
    {
        if (!Initialized_) {
            return std::nullopt;
        }
        return Reader_->GetDecompressionStatistics();
    }

    std::optional<TTimingStatistics> GetTimingStatistics() const override
    {
        if (!Initialized_) {
            return std::nullopt;
        }
        return Reader_->GetTimingStatistics();
    }

    void InterruptReader() override
    {
        if (!Initialized_) {
            THROW_ERROR_EXCEPTION(EErrorCode::JobNotPrepared, "Cannot interrupt uninitialized reader");
        }

        if (JobSpecHelper_->IsReaderInterruptionSupported() && !Interrupted_) {
            YT_VERIFY(Reader_);

            if (Reader_->GetDataStatistics().row_count() > 0) {
                Interrupted_ = true;
                Reader_->Interrupt();
            } else {
                THROW_ERROR_EXCEPTION(EErrorCode::JobNotPrepared, "Cannot interrupt reader that didn't start reading");
            }
        }
    }

    TInterruptDescriptor GetInterruptDescriptor() const override
    {
        if (Interrupted_) {
            YT_VERIFY(Reader_);
            return Reader_->GetInterruptDescriptor(TRange<TUnversionedRow>());
        } else {
            return {};
        }
    }

private:
    const IJobSpecHelperPtr JobSpecHelper_;
    const IInvokerPtr SerializedInvoker_;
    const TClosure OnNetworkRelease_;
    const IUserJobIOFactoryPtr UserJobIOFactory_;

    ISchemalessMultiChunkReaderPtr Reader_;
    std::vector<ISchemalessFormatWriterPtr> FormatWriters_;
    std::optional<TString> UdfDirectory_;
    std::atomic<bool> Initialized_ = {false};
    std::atomic<bool> Interrupted_ = {false};


private:
    TCallback<TFuture<void>()> PrepareInputActionsPassthrough(
        const TFormat& format,
        const IAsyncOutputStreamPtr& asyncOutput)
    {
        InitializeReader();

        std::vector<TTableSchemaPtr> schemas;
        if (JobSpecHelper_->GetSchedulerJobSpecExt().input_stream_schemas_size() > 0) {
            for (const auto& schemaProto : JobSpecHelper_->GetSchedulerJobSpecExt().input_stream_schemas()) {
                DeserializeFromWireProto(&schemas.emplace_back(), schemaProto);
            }
        } else {
            auto dataSourceDirectory = JobSpecHelper_->GetDataSourceDirectory();
            for (const auto& dataSource : dataSourceDirectory->DataSources()) {
                schemas.emplace_back(dataSource.Schema() ? dataSource.Schema() : New<TTableSchema>());
            }
        }

        auto writer = CreateStaticTableWriterForFormat(
            format,
            Reader_->GetNameTable(),
            schemas,
            asyncOutput,
            true,
            JobSpecHelper_->GetJobIOConfig()->ControlAttributes,
            JobSpecHelper_->GetKeySwitchColumnCount());

        if (JobSpecHelper_->GetSchedulerJobSpecExt().user_job_spec().cast_input_any_to_composite()) {
            // Intermediate chunks have incomplete schema, so Composite value type is not
            // restored in block reader. We need to restore it here.
            writer = New<TAnyToCompositeConverter>(std::move(writer), schemas, Reader_->GetNameTable());
        }

        FormatWriters_.push_back(writer);

        TPipeReaderToWriterOptions options;
        options.BufferRowCount = JobSpecHelper_->GetJobIOConfig()->BufferRowCount;
        options.PipeDelay = JobSpecHelper_->GetJobIOConfig()->Testing->PipeDelay;
        return BIND([=, this, this_ = MakeStrong(this)] {
            PipeReaderToWriter(
                Reader_,
                writer,
                options);
            WaitFor(asyncOutput->Close())
                .ThrowOnError();
        }).AsyncVia(SerializedInvoker_);
    }

    TCallback<TFuture<void>()> PrepareInputActionsQuery(
        const TQuerySpec& querySpec,
        const TFormat& format,
        const IAsyncOutputStreamPtr& asyncOutput)
    {
        if (JobSpecHelper_->GetJobIOConfig()->ControlAttributes->EnableKeySwitch) {
            THROW_ERROR_EXCEPTION("enable_key_switch is not supported when query is set");
        }

        auto readerFactory = [&] (TNameTablePtr nameTable, TColumnFilter columnFilter) -> ISchemalessUnversionedReaderPtr {
            InitializeReader(std::move(nameTable), std::move(columnFilter));
            return Reader_;
        };

        return BIND([=, this, this_ = MakeStrong(this)] {
            RunQuery(
                querySpec,
                readerFactory,
                [&] (TNameTablePtr nameTable, TTableSchemaPtr schema) {
                    auto schemalessWriter = CreateStaticTableWriterForFormat(
                        format,
                        std::move(nameTable),
                        {std::move(schema)},
                        asyncOutput,
                        true,
                        JobSpecHelper_->GetJobIOConfig()->ControlAttributes,
                        0);

                    FormatWriters_.push_back(schemalessWriter);

                    return schemalessWriter;
                },
                UdfDirectory_);
            WaitFor(asyncOutput->Close())
                .ThrowOnError();
        }).AsyncVia(SerializedInvoker_);
    }

    void InitializeReader()
    {
        InitializeReader(New<TNameTable>(), TColumnFilter());
    }

    void InitializeReader(TNameTablePtr nameTable, const TColumnFilter& columnFilter)
    {
        YT_VERIFY(!Reader_);
        Reader_ = UserJobIOFactory_->CreateReader(
            OnNetworkRelease_,
            std::move(nameTable),
            columnFilter);
        Initialized_ = true;
    }
};

DEFINE_REFCOUNTED_TYPE(TUserJobReadController)

////////////////////////////////////////////////////////////////////////////////

class TVanillaUserJobReadController
    : public IUserJobReadController
{
public:
    TCallback<TFuture<void>()> PrepareJobInputTransfer(const IAsyncOutputStreamPtr& /*asyncOutput*/) override
    {
        return BIND([] { return VoidFuture; });
    }

    double GetProgress() const override
    {
        return 0.0;
    }

    TFuture<std::vector<TBlob>> GetInputContext() const override
    {
        THROW_ERROR_EXCEPTION("Input context is not supported for vanilla jobs");
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

    std::optional<NChunkClient::NProto::TDataStatistics> GetDataStatistics() const override
    {
        return std::nullopt;
    }

    std::optional<TCodecStatistics> GetDecompressionStatistics() const override
    {
        return std::nullopt;
    }

    std::optional<TTimingStatistics> GetTimingStatistics() const override
    {
        return std::nullopt;
    }

    void InterruptReader() override
    { }

    TInterruptDescriptor GetInterruptDescriptor() const override
    {
        return {};
    }
};

DEFINE_REFCOUNTED_TYPE(TVanillaUserJobReadController)

////////////////////////////////////////////////////////////////////////////////

IUserJobReadControllerPtr CreateUserJobReadController(
    IJobSpecHelperPtr jobSpecHelper,
    TChunkReaderHostPtr chunkReaderHost,
    IInvokerPtr invoker,
    TClosure onNetworkRelease,
    std::optional<TString> udfDirectory,
    const TClientChunkReadOptions& chunkReadOptions,
    TString localHostName)
{
    if (jobSpecHelper->GetJobType() != EJobType::Vanilla) {
        return New<TUserJobReadController>(
            jobSpecHelper,
            invoker,
            onNetworkRelease,
            CreateUserJobIOFactory(
                jobSpecHelper,
                chunkReadOptions,
                std::move(chunkReaderHost),
                std::move(localHostName),
                nullptr),
            udfDirectory);
    } else {
        return New<TVanillaUserJobReadController>();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
