#include "user_job_read_controller.h"

#include "helpers.h"
#include "job_spec_helper.h"
#include "user_job_io_factory.h"

#include <yt/ytlib/scheduler/job.pb.h>

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
using namespace NChunkClient::NProto;
using namespace NConcurrency;
using namespace NFormats;
using namespace NNodeTrackerClient;
using namespace NTableClient;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TUserJobReadController::TUserJobReadController(
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

TCallback<TFuture<void>()> TUserJobReadController::PrepareJobInputTransfer(const IAsyncOutputStreamPtr& asyncOutput)
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

TCallback<TFuture<void>()> TUserJobReadController::PrepareInputActionsPassthrough(
    const NFormats::TFormat& format,
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

TCallback<TFuture<void>()> TUserJobReadController::PrepareInputActionsQuery(
    const NScheduler::NProto::TQuerySpec& querySpec,
    const NFormats::TFormat& format,
    const IAsyncOutputStreamPtr& asyncOutput)
{
    if (JobSpecHelper_->GetJobIOConfig()->ControlAttributes->EnableKeySwitch) {
        THROW_ERROR_EXCEPTION("enable_key_switch is not supported when query is set");
    }

    auto readerFactory = [&] (TNameTablePtr nameTable, TColumnFilter columnFilter) -> ISchemalessReaderPtr {
        InitializeReader(std::move(nameTable), std::move(columnFilter));
        return Reader_;
    };

    return BIND([=, this_ = MakeStrong(this)] () {
        RunQuery(querySpec, readerFactory, [&] (TNameTablePtr nameTable) {
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

void TUserJobReadController::InitializeReader(TNameTablePtr nameTable, const TColumnFilter& columnFilter)
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

void TUserJobReadController::InitializeReader()
{
    InitializeReader(New<TNameTable>(), TColumnFilter());
}

double TUserJobReadController::GetProgress() const
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

TFuture<std::vector<TBlob>> TUserJobReadController::GetInputContext() const
{
    if (!Initialized_) {
        return MakeFuture(std::vector<TBlob>());
    }

    return BIND([=, this_ = MakeStrong(this)]() {
        std::vector<TBlob> result;
        for (const auto& input : FormatWriters_) {
            result.push_back(input->GetContext());
        }
        return result;
    })
    .AsyncVia(SerializedInvoker_)
    .Run();
}

std::vector<TChunkId> TUserJobReadController::GetFailedChunkIds() const
{
    return Initialized_ ? Reader_->GetFailedChunkIds() : std::vector<TChunkId>();
}

TNullable<TDataStatistics> TUserJobReadController::GetDataStatistics() const
{
    if (!Initialized_) {
        return Null;
    }
    return Reader_->GetDataStatistics();
}

void TUserJobReadController::InterruptReader()
{
    if (JobSpecHelper_->IsReaderInterruptionSupported() && !Interrupted_) {
        YCHECK(Reader_);
        Reader_->Interrupt();
        Interrupted_ = true;
    }
}

std::vector<NChunkClient::TDataSliceDescriptor> TUserJobReadController::GetUnreadDataSliceDescriptors() const
{
    if (Interrupted_) {
        YCHECK(Reader_);
        return Reader_->GetUnreadDataSliceDescriptors(NYT::TRange<TUnversionedRow>());
    } else {
        return {};
    }
}

////////////////////////////////////////////////////////////////////////////////

TUserJobReadControllerPtr CreateUserJobReadController(
    IJobSpecHelperPtr jobSpecHelper,
    NApi::INativeClientPtr client,
    IInvokerPtr invoker,
    NNodeTrackerClient::TNodeDescriptor nodeDescriptor,
    TClosure onNetworkRelease,
    TNullable<TString> udfDirectory)
{
    return New<TUserJobReadController>(
        jobSpecHelper,
        client,
        invoker,
        nodeDescriptor,
        onNetworkRelease,
        CreateUserJobIOFactory(jobSpecHelper),
        udfDirectory);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
