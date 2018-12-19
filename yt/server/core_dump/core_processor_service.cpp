#include "core_processor_service.h"

#include "core_processor_service_proxy.h"

#include <yt/server/misc/job_table_schema.h>

#include <yt/client/api/client.h>

#include <yt/ytlib/chunk_client/dispatcher.h>

#include <yt/ytlib/job_tracker_client/public.h>

#include <yt/core/concurrency/public.h>
#include <yt/core/concurrency/async_semaphore.h>
#include <yt/core/concurrency/async_stream.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/misc/fs.h>

#include <yt/core/net/connection.h>

#include <yt/core/pipes/pipe.h>

#include <yt/core/ytree/convert.h>

namespace NYT::NCoreDump {

using namespace NApi;
using namespace NChunkClient;
using namespace NConcurrency;
using namespace NExecAgent;
using namespace NFS;
using namespace NJobProxy;
using namespace NObjectClient;
using namespace NPipes;
using namespace NNet;
using namespace NRpc;
using namespace NProto;
using namespace NScheduler;
using namespace NScheduler::NProto;
using namespace NTableClient;
using namespace NYTree;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

class TCoreProcessorService::TCoreProcessor
    : public TRefCounted
{
public:
    TCoreProcessor(
        const IJobHostPtr& jobHost,
        const TBlobTableWriterConfigPtr& blobTableWriterConfig,
        const TTableWriterOptionsPtr& tableWriterOptions,
        TTransactionId transaction,
        TChunkListId chunkList,
        const IInvokerPtr& controlInvoker,
        TDuration readWriteTimeout,
        const TLogger& logger)
        : JobId_(jobHost->GetJobId())
        , Client_(jobHost->GetClient())
        , AsyncSemaphore_(New<TAsyncSemaphore>(1 /* totalSlots */))
        , ControlInvoker_(controlInvoker)
        , IOInvoker_(NChunkClient::TDispatcher::Get()->GetReaderInvoker())
        , BlobTableWriterConfig_(blobTableWriterConfig)
        , TableWriterOptions_(tableWriterOptions)
        , Transaction_(transaction)
        , ChunkList_(chunkList)
        , ReadWriteTimeout_(readWriteTimeout)
        , TrafficMeter_(jobHost->GetTrafficMeter())
        , OutThrottler_(jobHost->GetOutBandwidthThrottler())
        , Logger(logger)
    {
        BoundaryKeys_.set_empty(true);
        CoreResultPromise_ = MakePromise<TCoreResult>({CoreInfos_, BoundaryKeys_});
    }

    //! Prepares everything for writing the new core dump and returns path of
    //! the named pipe the core should be written to.
    TString ProcessCore(i32 processId, const TString& executableName)
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        if (ActiveCoreCount_ == 0) {
            CoreResultPromise_ = NewPromise<TCoreResult>();
        }
        ++ActiveCoreCount_;

        YT_LOG_INFO("Registering core dump (ProcessId: %v, ExecutableName: %v, ActiveCoreCount: %v)",
            processId,
            executableName,
            ActiveCoreCount_);

        auto namedPipePath = GetRealPath(CombinePaths("./pipes", Format("core-%v-%v", processId, executableName)));
        auto namedPipe = TNamedPipe::Create(namedPipePath);

        AsyncSemaphore_->AsyncAcquire(
            BIND(&TCoreProcessor::DoWriteCore, MakeStrong(this), namedPipe, processId, executableName),
            IOInvoker_);

        return namedPipePath;
    }

    TCoreResult Finalize(TDuration timeout) const
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        YT_LOG_INFO("Finalizing core processor");

        TCoreResult coreResult;

        TError coreAppearedWaitResult;
        if (timeout != TDuration::Zero()) {
            YT_LOG_INFO("Waiting for core to appear (Timeout: %v)",
                timeout);
            coreAppearedWaitResult = WaitFor(GetCoreAppearedEvent()
                .WithTimeout(timeout));
        }

        if (!coreAppearedWaitResult.IsOK()) {
            YCHECK(coreAppearedWaitResult.FindMatching(NYT::EErrorCode::Timeout));
            YT_LOG_INFO("Core did not appear within timeout, creating a dummy core info entry (Timeout: %v)", timeout);
            // Even though the core file we have been waiting for didn't appear, we create an entity node in Cypress related to it.
            TCoreInfo dummyCoreInfo;
            dummyCoreInfo.set_process_id(-1);
            dummyCoreInfo.set_executable_name("n/a");
            ToProto(dummyCoreInfo.mutable_error(), TError("Timeout while waiting for a core dump"));
            coreResult.CoreInfos = { dummyCoreInfo };
            coreResult.BoundaryKeys.set_empty(true);
        } else {
            YT_LOG_INFO("At least one core dump appeared within timeout, waiting until no core dump is active "
                     "(NumberOfActiveCores: %v)",
                ActiveCoreCount_);
            coreResult = WaitFor(GetCoreResult())
                .ValueOrThrow();
        }

        YT_LOG_INFO("Core processor finished");

        return coreResult;
    }

private:
    const TJobId JobId_;
    const NNative::IClientPtr Client_;
    TAsyncSemaphorePtr AsyncSemaphore_;
    const IInvokerPtr ControlInvoker_;
    const IInvokerPtr IOInvoker_;
    const TBlobTableWriterConfigPtr BlobTableWriterConfig_;
    const TTableWriterOptionsPtr TableWriterOptions_;
    const TTransactionId Transaction_;
    const TChunkListId ChunkList_;
    const TDuration ReadWriteTimeout_;
    const TTrafficMeterPtr TrafficMeter_;
    const IThroughputThrottlerPtr OutThrottler_;

    // Promise that is set when there are no cores that are currently processed.
    TPromise<TCoreResult> CoreResultPromise_;

    // Promise that is set when at least one core starts being processed.
    TPromise<void> CoreAppereadPromise_ = NewPromise<void>();

    TOutputResult BoundaryKeys_;
    std::vector<TCoreInfo> CoreInfos_;

    int ActiveCoreCount_ = 0;

    TLogger Logger;

    TFuture<TCoreResult> GetCoreResult() const
    {
        return CoreResultPromise_.ToFuture();
    }

    TFuture<void> GetCoreAppearedEvent() const
    {
        return CoreAppereadPromise_.ToFuture();
    }

    // This method retrieves the core from the named pipe, writes
    // it to the core table and returns the size of a core or throws
    // an exception in case of some error.
    void DoWriteCore(TNamedPipePtr namedPipe, i32 processId, TString executableName, TAsyncSemaphoreGuard /* guard */)
    {
        VERIFY_INVOKER_AFFINITY(IOInvoker_);

        int coreId = CoreInfos_.size();
        auto Logger = TLogger(this->Logger)
            .AddTag("CoreId: %v, ProcessId: %v, ExecutableName: %v, NamedPipe: %v",
                coreId,
                processId,
                executableName,
                namedPipe->GetPath());

        YT_LOG_INFO("Started processing core dump");

        CoreInfos_.emplace_back();
        CoreInfos_[coreId].set_process_id(processId);
        CoreInfos_[coreId].set_executable_name(executableName);

        if (coreId == 0) {
            CoreAppereadPromise_.Set();
        }

        try {
            TBlobTableWriter blobWriter(
                GetCoreBlobTableSchema(),
                {ConvertToYsonString(JobId_), ConvertToYsonString(coreId)} /* blobIdColumnValues */,
                Client_,
                BlobTableWriterConfig_,
                TableWriterOptions_,
                Transaction_,
                ChunkList_,
                TrafficMeter_,
                OutThrottler_);

            auto reader = CreateZeroCopyAdapter(namedPipe->CreateAsyncReader(), 1_MB /* blockSize */);
            auto writer = CreateZeroCopyAdapter(CreateAsyncAdapter(&blobWriter));

            i64 coreSize = 0;
            while (auto block = WaitFor(reader->Read().WithTimeout(ReadWriteTimeout_))
                .ValueOrThrow())
            {
                coreSize += block.Size();
                WaitFor(writer->Write(block))
                    .ThrowOnError();
            }

            blobWriter.Finish();

            auto outputResult = blobWriter.GetOutputResult();

            // A sanity check.
            YCHECK(!outputResult.empty() || coreSize == 0);

            if (BoundaryKeys_.empty()) {
                BoundaryKeys_.MergeFrom(outputResult);
            } else if (!outputResult.empty()) {
                BoundaryKeys_.mutable_max()->swap(*outputResult.mutable_max());
            }

            YT_LOG_INFO("Finished processing core dump (Size: %v)", coreSize);

            CoreInfos_[coreId].set_size(coreSize);
        } catch (const std::exception& ex) {
            YT_LOG_ERROR(ex, "Error while piping core to Cypress");
            auto error = TError("Error while piping core to Cypress")
                << ex;
            ToProto(CoreInfos_[coreId].mutable_error(), error);
        }

        WaitFor(BIND(&TCoreProcessor::OnCoreWritten, MakeStrong(this))
            .AsyncVia(ControlInvoker_)
            .Run())
            .ThrowOnError();
    }

    void OnCoreWritten()
    {
        VERIFY_INVOKER_AFFINITY(ControlInvoker_);

        --ActiveCoreCount_;
        YT_LOG_INFO("Finished processing core dump (ActiveCoreCount: %v)",
            ActiveCoreCount_);

        if (ActiveCoreCount_ == 0) {
            CoreResultPromise_.Set({CoreInfos_, BoundaryKeys_});
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TCoreProcessorService::TCoreProcessorService(
    const IJobHostPtr& jobHost,
    const TBlobTableWriterConfigPtr& blobTableWriterConfig,
    const TTableWriterOptionsPtr& tableWriterOptions,
    TTransactionId transaction,
    TChunkListId chunkList,
    const IInvokerPtr& controlInvoker,
    TDuration readWriteTimeout)
    : TServiceBase(
        controlInvoker,
        TCoreProcessorServiceProxy::GetDescriptor(),
        jobHost->GetLogger())
    , CoreProcessor_(New<TCoreProcessor>(
        jobHost,
        blobTableWriterConfig,
        tableWriterOptions,
        transaction,
        chunkList,
        controlInvoker,
        readWriteTimeout,
        jobHost->GetLogger()))
{
    RegisterMethod(RPC_SERVICE_METHOD_DESC(StartCoreDump));
}

TCoreProcessorService::~TCoreProcessorService() = default;

TCoreResult TCoreProcessorService::Finalize(TDuration timeout) const
{
    return WaitFor(BIND(&TCoreProcessor::Finalize, CoreProcessor_, timeout)
        .AsyncVia(GetDefaultInvoker())
        .Run())
        .ValueOrThrow();
}

DEFINE_RPC_SERVICE_METHOD(TCoreProcessorService, StartCoreDump)
{
    auto namedPipePath = CoreProcessor_->ProcessCore(
        request->process_id(),
        request->executable_name());
    context->SetRequestInfo("ProcessId: %v, ExecutableName: %v", request->process_id(), request->executable_name());
    response->set_named_pipe_path(namedPipePath);
    context->SetResponseInfo("NamedPipePath: %v", namedPipePath);
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCoreDump

