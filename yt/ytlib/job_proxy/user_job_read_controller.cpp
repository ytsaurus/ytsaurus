#include "user_job_read_controller.h"

#include "helpers.h"
#include "job_spec_helper.h"
#include "user_job_io_factory.h"

#include <yt/ytlib/scheduler/proto/job.pb.h>

#include <yt/ytlib/table_client/name_table.h>
#include <yt/ytlib/table_client/schemaless_chunk_reader.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/async_stream.h>

#include <yt/core/misc/finally.h>
#include <yt/core/misc/numeric_helpers.h>

#include <yt/core/ytree/convert.h>

namespace NYT {
namespace NJobProxy {

using NApi::INativeClientPtr;

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
        INativeClientPtr client,
        IInvokerPtr invoker,
        TNodeDescriptor nodeDescriptor,
        TClosure onNetworkRelease,
        IUserJobIOFactoryPtr userJobIOFactory,
        TNullable<TString> udfDirectory)
        : JobSpecHelper_(std::move(jobSpecHelper))
        , Client_(std::move(client))
        , SerializedInvoker_(CreateSerializedInvoker(std::move(invoker)))
        , NodeDescriptor_(std::move(nodeDescriptor))
        , OnNetworkRelease_(onNetworkRelease)
        , UserJobIOFactory_(userJobIOFactory)
        , UdfDirectory_(std::move(udfDirectory))
    { }

    //! Returns closure that launches data transfer to given async output.
    virtual TCallback<TFuture<void>()> PrepareJobInputTransfer(const IAsyncOutputStreamPtr& asyncOutput) override
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

    virtual double GetProgress() const override
    {
        if (!Initialized_) {
            return 0;
        }

        i64 total = Reader_->GetTotalRowCount();
        i64 current = Reader_->GetSessionRowIndex();

        if (total == 0) {
            return 0.0;
        }

        return Clamp(current / static_cast<double>(total), 0.0, 1.0);
    }

    virtual TFuture<std::vector<TBlob>> GetInputContext() const override
    {
        if (!Initialized_) {
            return MakeFuture(std::vector<TBlob>());
        }

        return BIND([=, this_ = MakeStrong(this)] {
            std::vector<TBlob> result;
            for (const auto& input : FormatWriters_) {
                result.push_back(input->GetContext());
            }
            return result;
        })
        .AsyncVia(SerializedInvoker_)
        .Run();
    }

    virtual std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return Initialized_ ? Reader_->GetFailedChunkIds() : std::vector<TChunkId>();
    }

    virtual TNullable<NChunkClient::NProto::TDataStatistics> GetDataStatistics() const override
    {
        if (!Initialized_) {
            return Null;
        }
        return Reader_->GetDataStatistics();
    }

    virtual void InterruptReader() override
    {
        if (!Initialized_) {
            THROW_ERROR_EXCEPTION(EErrorCode::JobNotPrepared, "Cannot interrupt uninitialized reader");
        }

        if (JobSpecHelper_->IsReaderInterruptionSupported() && !Interrupted_) {
            YCHECK(Reader_);
            Interrupted_ = true;

            if (Reader_->GetDataStatistics().row_count() > 0) {
                Reader_->Interrupt();
            } else {
                THROW_ERROR_EXCEPTION(EErrorCode::JobNotPrepared, "Cannot interrupt reader that didn't start reading");
            }
        }
    }

    virtual TInterruptDescriptor GetInterruptDescriptor() const override
    {
        if (Interrupted_) {
            YCHECK(Reader_);
            return Reader_->GetInterruptDescriptor(TRange<TUnversionedRow>());
        } else {
            return {};
        }
    }

private:
    TCallback<TFuture<void>()> PrepareInputActionsPassthrough(
        const TFormat& format,
        const IAsyncOutputStreamPtr& asyncOutput)
    {
        InitializeReader();

        auto writer = CreateSchemalessWriterForFormat(
            format,
            Reader_->GetNameTable(),
            asyncOutput,
            true,
            JobSpecHelper_->GetJobIOConfig()->ControlAttributes,
            JobSpecHelper_->GetKeySwitchColumnCount());

        FormatWriters_.push_back(writer);

        auto bufferRowCount = JobSpecHelper_->GetJobIOConfig()->BufferRowCount;

        return BIND([=, this_ = MakeStrong(this)] () {
            PipeReaderToWriter(
                Reader_,
                writer,
                bufferRowCount);
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

        auto readerFactory = [&] (TNameTablePtr nameTable, TColumnFilter columnFilter) -> ISchemalessReaderPtr {
            InitializeReader(std::move(nameTable), std::move(columnFilter));
            return Reader_;
        };

        return BIND([=, this_ = MakeStrong(this)] {
            RunQuery(
                querySpec,
                readerFactory,
                [&] (TNameTablePtr nameTable) {
                    auto schemalessWriter = CreateSchemalessWriterForFormat(
                        format,
                        nameTable,
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
        YCHECK(!Reader_);
        Reader_ = CreateUserJobIOFactory(JobSpecHelper_)->CreateReader(
            Client_,
            NodeDescriptor_,
            OnNetworkRelease_,
            std::move(nameTable),
            columnFilter);
        Initialized_ = true;
    }

private:
    const IJobSpecHelperPtr JobSpecHelper_;
    const INativeClientPtr Client_;
    const IInvokerPtr SerializedInvoker_;
    const TNodeDescriptor NodeDescriptor_;
    const TClosure OnNetworkRelease_;
    const IUserJobIOFactoryPtr UserJobIOFactory_;
    ISchemalessMultiChunkReaderPtr Reader_;
    std::vector<ISchemalessFormatWriterPtr> FormatWriters_;
    TNullable<TString> UdfDirectory_;
    std::atomic<bool> Initialized_ = {false};
    std::atomic<bool> Interrupted_ = {false};

    NLogging::TLogger Logger;
};

DEFINE_REFCOUNTED_TYPE(TUserJobReadController)

////////////////////////////////////////////////////////////////////////////////

class TVanillaUserJobReadController
    : public IUserJobReadController
{
public:
    TCallback<TFuture<void>()> PrepareJobInputTransfer(const IAsyncOutputStreamPtr& asyncOutput) override
    {
        return BIND([] { return VoidFuture; });
    }

    double GetProgress() const override
    {
        return 0.0;
    }

    TFuture<std::vector<TBlob>> GetInputContext() const override
    {
        return MakeFuture(std::vector<TBlob>{});
    }

    std::vector<TChunkId> GetFailedChunkIds() const override
    {
        return {};
    }

    TNullable<NChunkClient::NProto::TDataStatistics> GetDataStatistics() const override
    {
        return Null;
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
    INativeClientPtr client,
    IInvokerPtr invoker,
    TNodeDescriptor nodeDescriptor,
    TClosure onNetworkRelease,
    TNullable<TString> udfDirectory)
{
    if (jobSpecHelper->GetJobType() != EJobType::Vanilla) {
        return New<TUserJobReadController>(
            jobSpecHelper,
            client,
            invoker,
            nodeDescriptor,
            onNetworkRelease,
            CreateUserJobIOFactory(jobSpecHelper),
            udfDirectory);
    } else {
        return New<TVanillaUserJobReadController>();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
