#include "client_impl.h"

#include "connection.h"
#include "helpers.h"

#include <yt/yt/client/api/file_reader.h>
#include <yt/yt/client/api/operations_archive_schema.h>
#include <yt/yt/client/api/rowset.h>
#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/job_tracker_client/helpers.h>

#include <yt/yt/client/query_client/query_builder.h>

#include <yt/yt/client/table_client/helpers.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/record_helpers.h>

#include <yt/yt/ytlib/api/native/config.h>

#include <yt/yt/ytlib/chunk_client/client_block_cache.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_host.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_options.h>
#include <yt/yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/yt/ytlib/chunk_client/combine_data_slices.h>
#include <yt/yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/yt/ytlib/chunk_client/data_source.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>
#include <yt/yt/ytlib/chunk_client/job_spec_extensions.h>

#include <yt/yt/ytlib/controller_agent/helpers.h>

#include <yt/yt/ytlib/controller_agent/proto/job.pb.h>

#include <yt/yt/ytlib/job_proxy/job_spec_helper.h>
#include <yt/yt/ytlib/job_proxy/helpers.h>
#include <yt/yt/ytlib/job_proxy/user_job_read_controller.h>

#include <yt/yt/ytlib/job_prober_client/job_shell_descriptor_cache.h>

#include <yt/yt/ytlib/node_tracker_client/channel.h>

#include <yt/yt/ytlib/object_client/object_service_proxy.h>

#include <yt/yt/ytlib/scheduler/helpers.h>
#include <yt/yt/ytlib/scheduler/records/job.record.h>
#include <yt/yt/ytlib/scheduler/records/job_fail_context.record.h>
#include <yt/yt/ytlib/scheduler/records/operation_id.record.h>
#include <yt/yt/ytlib/scheduler/records/job_stderr.record.h>
#include <yt/yt/ytlib/scheduler/records/job_spec.record.h>

#include <yt/yt/client/chunk_client/config.h>

#include <yt/yt/core/compression/codec.h>

#include <yt/yt/core/concurrency/async_stream.h>
#include <yt/yt/core/concurrency/async_stream_pipe.h>
#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/ytree/ypath_resolver.h>

#include <util/string/join.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NControllerAgent;
using namespace NControllerAgent::NProto;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NFileClient;
using namespace NRpc;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NTransactionClient;
using namespace NSecurityClient;
using namespace NQueryClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NScheduler;
using namespace NNodeTrackerClient;
using namespace NJobTrackerClient;

using NChunkClient::TDataSliceDescriptor;
using NNodeTrackerClient::TNodeDescriptor;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static const THashSet<TString> DefaultListJobsAttributes = {
    "job_id",
    "type",
    "state",
    "start_time",
    "finish_time",
    "address",
    "has_spec",
    "progress",
    "stderr_size",
    "fail_context_size",
    "error",
    "interruption_info",
    "brief_statistics",
    "job_competition_id",
    "has_competitors",
    "probing_job_competition_id",
    "task_name",
    "pool",
    "pool_tree",
    "monitoring_descriptor",
    "core_infos",
    "job_cookie",
    "controller_state",
};

static const auto DefaultGetJobAttributes = [] {
    auto attributes = DefaultListJobsAttributes;
    attributes.insert("operation_id");
    attributes.insert("statistics");
    attributes.insert("events");
    attributes.insert("exec_attributes");
    return attributes;
}();

static const auto SupportedJobAttributes = DefaultGetJobAttributes;

static const auto FinishedJobStatesString = [] {
    TCompactVector<TString, TEnumTraits<EJobState>::GetDomainSize()> finishedJobStates;
    for (const auto& jobState : TEnumTraitsImpl_EJobState::GetDomainValues()) {
        if (IsJobFinished(jobState)) {
            finishedJobStates.push_back("\"" + FormatEnum(jobState) + "\"");
        }
    }
    return JoinSeq(", ", finishedJobStates);
}();

////////////////////////////////////////////////////////////////////////////////

class TJobInputReader
    : public NConcurrency::IAsyncZeroCopyInputStream
{
public:
    TJobInputReader(NJobProxy::IUserJobReadControllerPtr userJobReadController, IInvokerPtr invoker)
        : Invoker_(std::move(invoker))
        , UserJobReadController_(std::move(userJobReadController))
        , AsyncStreamPipe_(New<TAsyncStreamPipe>())
    { }

    ~TJobInputReader()
    {
        if (TransferResultFuture_) {
            TransferResultFuture_.Cancel(TError("Reader destroyed"));
        }
    }

    void Open()
    {
        auto transferClosure = UserJobReadController_->PrepareJobInputTransfer(AsyncStreamPipe_);
        TransferResultFuture_ = BIND(transferClosure)
            .AsyncVia(Invoker_)
            .Run();

        TransferResultFuture_.Subscribe(BIND([pipe = AsyncStreamPipe_] (const TError& error) {
            if (!error.IsOK()) {
                YT_UNUSED_FUTURE(pipe->Abort(TError("Failed to get job input") << error));
            }
        }));
    }

    TFuture<TSharedRef> Read() override
    {
        return AsyncStreamPipe_->Read();
    }

private:
    const IInvokerPtr Invoker_;
    const NJobProxy::IUserJobReadControllerPtr UserJobReadController_;
    const NConcurrency::TAsyncStreamPipePtr AsyncStreamPipe_;

    TFuture<void> TransferResultFuture_;
};

DECLARE_REFCOUNTED_CLASS(TJobInputReader)
DEFINE_REFCOUNTED_TYPE(TJobInputReader)

////////////////////////////////////////////////////////////////////////////////

static TYPath GetControllerAgentOrchidRunningJobsPath(TStringBuf controllerAgentAddress, TOperationId operationId)
{
    return GetControllerAgentOrchidOperationPath(controllerAgentAddress, operationId) + "/running_jobs";
}

static TYPath GetControllerAgentOrchidRetainedFinishedJobsPath(TStringBuf controllerAgentAddress, TOperationId operationId)
{
    return GetControllerAgentOrchidOperationPath(controllerAgentAddress, operationId) + "/retained_finished_jobs";
}

////////////////////////////////////////////////////////////////////////////////

static void ValidateJobSpecVersion(
    TJobId jobId,
    const TJobSpec& jobSpec)
{
    if (!jobSpec.has_version() || jobSpec.version() != GetJobSpecVersion()) {
        THROW_ERROR_EXCEPTION("Job spec found in operation archive is of unsupported version")
            << TErrorAttribute("job_id", jobId)
            << TErrorAttribute("found_version", jobSpec.version())
            << TErrorAttribute("supported_version", GetJobSpecVersion());
    }
}

static bool IsNoSuchJobOrOperationError(const TError& error)
{
    return
        error.FindMatching(NScheduler::EErrorCode::NoSuchAllocation) ||
        error.FindMatching(NScheduler::EErrorCode::NoSuchOperation) ||
        error.FindMatching(NControllerAgent::EErrorCode::NoSuchJob) ||
        error.FindMatching(NExecNode::EErrorCode::NoSuchJob);
}

// Get job node descriptor from scheduler and check that user has |requiredPermissions|
// for accessing the corresponding operation.
TErrorOr<TNodeDescriptor> TClient::TryGetJobNodeDescriptor(
    TJobId jobId,
    EPermissionSet requiredPermissions)
{
    try {
        auto allocationId = AllocationIdFromJobId(jobId);

        auto allocationBriefInfoOrError = WaitFor(GetAllocationBriefInfo(
            *SchedulerOperationProxy_,
            allocationId,
            TAllocationInfoToRequest{
                .OperationId = true,
                .OperationAcl = true,
                .NodeDescriptor = true,
            }));

        if (!allocationBriefInfoOrError.IsOK()) {
            return std::move(allocationBriefInfoOrError).Wrap();
        }

        const auto& allocationBriefInfo = allocationBriefInfoOrError.Value();

        ValidateOperationAccess(
            allocationBriefInfo.OperationId,
            *allocationBriefInfo.OperationAcl,
            jobId,
            requiredPermissions);

        return allocationBriefInfo.NodeDescriptor;
    } catch (const std::exception& ex) {
        return ex;
    }
}

TErrorOr<IChannelPtr> TClient::TryCreateChannelToJobNode(
    TOperationId operationId,
    TJobId jobId,
    EPermissionSet requiredPermissions)
{
    auto jobNodeDescriptorOrError = TryGetJobNodeDescriptor(jobId, requiredPermissions);
    if (jobNodeDescriptorOrError.IsOK()) {
        return ChannelFactory_->CreateChannel(jobNodeDescriptorOrError.ValueOrThrow());
    }

    YT_LOG_DEBUG(
        jobNodeDescriptorOrError,
        "Failed to get job node descriptor from scheduler (OperationId: %v, JobId: %v)",
        operationId,
        jobId);

    if (!IsNoSuchJobOrOperationError(jobNodeDescriptorOrError)) {
        THROW_ERROR_EXCEPTION("Failed to get job node descriptor from scheduler")
            << jobNodeDescriptorOrError;
    }

    try {
        ValidateOperationAccess(operationId, jobId, requiredPermissions);

        TGetJobOptions options;
        options.Attributes = {TString("address")};
        // TODO(ignat): support structured return value in GetJob.
        auto jobYsonString = WaitFor(GetJob(operationId, jobId, options))
            .ValueOrThrow();
        auto address = ConvertToNode(jobYsonString)->AsMap()->GetChildValueOrThrow<TString>("address");
        return ChannelFactory_->CreateChannel(address);
    } catch (const std::exception& ex) {
        auto error = TError(ex);
        YT_LOG_DEBUG(error, "Failed to create node channel to job using address from archive (OperationId: %v, JobId: %v)",
            operationId,
            jobId);
        return error;
    }
}

TErrorOr<TJobSpec> TClient::TryFetchJobSpecFromJobNode(
    TJobId jobId,
    NRpc::IChannelPtr nodeChannel)
{
    NJobProberClient::TJobProberServiceProxy jobProberServiceProxy(std::move(nodeChannel));
    jobProberServiceProxy.SetDefaultTimeout(Connection_->GetConfig()->JobProberRpcTimeout);

    auto req = jobProberServiceProxy.GetSpec();
    ToProto(req->mutable_job_id(), jobId);

    auto rspOrError = WaitFor(req->Invoke());
    if (!rspOrError.IsOK()) {
        return TError("Failed to get job spec from job node")
            << std::move(rspOrError)
            << TErrorAttribute("job_id", jobId);
    }

    const auto& rsp = rspOrError.Value();
    const auto& spec = rsp->spec();
    ValidateJobSpecVersion(jobId, spec);
    return spec;
}

TErrorOr<TJobSpec> TClient::TryFetchJobSpecFromJobNode(
    TJobId jobId,
    EPermissionSet requiredPermissions)
{
    // TODO(pogorelov): Do not get operation id from archive here, if it is already got from scheduler.
    if (auto operationId = TryGetOperationId(jobId)) {
        auto nodeChannelOrError = TryCreateChannelToJobNode(operationId, jobId, requiredPermissions);
        if (nodeChannelOrError.IsOK()) {
            return TryFetchJobSpecFromJobNode(jobId, nodeChannelOrError.ValueOrThrow());
        }
        YT_LOG_DEBUG(
            nodeChannelOrError,
            "Failed to create channel to job node using archive info (OperationId: %v, JobId: %v)",
            operationId,
            jobId);
    }
    auto jobNodeDescriptorOrError = TryGetJobNodeDescriptor(jobId, requiredPermissions);
    if (!jobNodeDescriptorOrError.IsOK()) {
        return TError(std::move(jobNodeDescriptorOrError));
    }
    const auto& nodeDescriptor = jobNodeDescriptorOrError.Value();
    auto nodeChannel = ChannelFactory_->CreateChannel(nodeDescriptor);
    return TryFetchJobSpecFromJobNode(jobId, nodeChannel);
}

TJobSpec TClient::FetchJobSpecFromArchive(TJobId jobId)
{
    auto jobIdAsGuid = jobId.Underlying();

    NRecords::TOperationIdKey recordKey{
        .JobIdHi = jobIdAsGuid.Parts64[0],
        .JobIdLo = jobIdAsGuid.Parts64[1]
    };
    auto keys = FromRecordKeys(MakeRange(std::array{recordKey}));

    auto resultOrError = WaitFor(LookupRows(
        GetOperationsArchiveJobSpecsPath(),
        NRecords::TJobSpecDescriptor::Get()->GetNameTable(),
        keys,
        /*options*/ {}));

    if (!resultOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Failed to get job spec from operation archive")
            << TErrorAttribute("job_id", jobId)
            << resultOrError;
    }

    const auto& result = resultOrError.Value();
    const auto& rowset = result.Rowset;

    auto records = ToRecords<NRecords::TJobSpec>(rowset);
    YT_VERIFY(records.size() <= 1);

    std::optional<TString> jobSpecStr;
    if (!records.empty()) {
        jobSpecStr = records[0].Spec;
    }

    if (!jobSpecStr) {
        THROW_ERROR_EXCEPTION("Missing job spec in job archive table")
            << TErrorAttribute("job_id", jobId);
    }

    TJobSpec jobSpec;
    if (!TryDeserializeProto(&jobSpec, TRef::FromString(*jobSpecStr))) {
        THROW_ERROR_EXCEPTION("Failed to parse job spec fetched from operation archive")
            << TErrorAttribute("job_id", jobId);
    }

    ValidateJobSpecVersion(jobId, jobSpec);

    return jobSpec;
}

TOperationId TClient::TryGetOperationId(
    TJobId jobId)
{
    auto jobIdAsGuid = jobId.Underlying();

    NRecords::TOperationIdKey recordKey{
        .JobIdHi = jobIdAsGuid.Parts64[0],
        .JobIdLo = jobIdAsGuid.Parts64[1]
    };
    auto keys = FromRecordKeys(MakeRange(std::array{recordKey}));

    auto rowsetOrError = WaitFor(LookupRows(
        GetOperationsArchiveOperationIdsPath(),
        NRecords::TOperationIdDescriptor::Get()->GetNameTable(),
        keys,
        /*options*/ {}));

    if (rowsetOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
        return {};
    }

    auto rowset = rowsetOrError
        .ValueOrThrow()
        .Rowset;

    auto records = ToRecords<NRecords::TOperationId>(rowset);
    YT_VERIFY(records.size() <= 1);
    if (records.empty()) {
        return {};
    }

    const auto& record = records[0];
    return TOperationId(TGuid(record.OperationIdHi, record.OperationIdLo));
}

void TClient::ValidateOperationAccess(
    TOperationId operationId,
    TJobId jobId,
    EPermissionSet permissions)
{
    TGetOperationOptions getOperationOptions;
    getOperationOptions.Attributes = {TString("runtime_parameters")};
    auto operationOrError = WaitFor(GetOperation(operationId, getOperationOptions));

    TSerializableAccessControlList acl;
    if (operationOrError.IsOK()) {
        auto operation = std::move(operationOrError).Value();
        auto aclYson = TryGetAny(operation.RuntimeParameters.AsStringBuf(), "/acl");
        if (aclYson) {
            acl = ConvertTo<TSerializableAccessControlList>(TYsonStringBuf(*aclYson));
        } else {
            // We check against an empty ACL to allow only "superusers" and "root" access.
            YT_LOG_WARNING(
                "Failed to get ACL from operation attributes; "
                "validating against empty ACL (OperationId: %v, JobId: %v)",
                operationId,
                jobId);
        }
    } else {
        // We check against an empty ACL to allow only "superusers" and "root" access.
        YT_LOG_WARNING(
            operationOrError,
            "Failed to get operation to validate access; "
            "validating against empty ACL (OperationId: %v, JobId: %v)",
            operationId,
            jobId);
    }

    NScheduler::ValidateOperationAccess(
        Options_.GetAuthenticatedUser(),
        operationId,
        AllocationIdFromJobId(jobId),
        permissions,
        acl,
        StaticPointerCast<IClient>(MakeStrong(this)),
        Logger);
}

void TClient::ValidateOperationAccess(
    TJobId jobId,
    const TJobSpec& jobSpec,
    EPermissionSet permissions)
{
    const auto extensionId = NControllerAgent::NProto::TJobSpecExt::job_spec_ext;
    TSerializableAccessControlList acl;
    if (jobSpec.HasExtension(extensionId) && jobSpec.GetExtension(extensionId).has_acl()) {
        TYsonString aclYson(jobSpec.GetExtension(extensionId).acl());
        acl = ConvertTo<TSerializableAccessControlList>(aclYson);
    } else {
        // We check against an empty ACL to allow only "superusers" and "root" access.
        YT_LOG_WARNING(
            "Job spec has no sheduler_job_spec_ext or the extension has no ACL; "
            "validating against empty ACL (JobId: %v)",
            jobId);
    }

    NScheduler::ValidateOperationAccess(
        /*user*/ std::nullopt,
        TOperationId(),
        AllocationIdFromJobId(jobId),
        permissions,
        acl,
        StaticPointerCast<IClient>(MakeStrong(this)),
        Logger);
}

void TClient::ValidateOperationAccess(
    NScheduler::TOperationId operationId,
    const NSecurityClient::TSerializableAccessControlList& operationAcl,
    TJobId jobId,
    NYTree::EPermissionSet permissions)
{
    NScheduler::ValidateOperationAccess(
        Options_.GetAuthenticatedUser(),
        operationId,
        AllocationIdFromJobId(jobId),
        permissions,
        operationAcl,
        StaticPointerCast<IClient>(MakeStrong(this)),
        Logger);
}

TJobSpec TClient::FetchJobSpec(
    TJobId jobId,
    NApi::EJobSpecSource specSource,
    NYTree::EPermissionSet requiredPermissions)
{
    if (Any(specSource & EJobSpecSource::Node)) {
        auto jobSpecFromProxyOrError = TryFetchJobSpecFromJobNode(jobId, requiredPermissions);
        if (!jobSpecFromProxyOrError.IsOK() && !IsNoSuchJobOrOperationError(jobSpecFromProxyOrError)) {
            THROW_ERROR jobSpecFromProxyOrError;
        }

        if (jobSpecFromProxyOrError.IsOK()) {
            return std::move(jobSpecFromProxyOrError).Value();
        }

        YT_LOG_DEBUG(jobSpecFromProxyOrError, "Failed to fetch job spec from job node (JobId: %v)",
            jobId);
    }

    if (Any(specSource & EJobSpecSource::Archive)) {
        auto jobSpec = FetchJobSpecFromArchive(jobId);

        auto operationId = TryGetOperationId(jobId);
        if (operationId) {
            ValidateOperationAccess(operationId, jobId, requiredPermissions);
        } else {
            ValidateOperationAccess(jobId, jobSpec, requiredPermissions);
        }


        return jobSpec;
    }
    THROW_ERROR_EXCEPTION("Failed to get job spec")
        << TErrorAttribute("job_id", jobId)
        << TErrorAttribute("spec_source", specSource);
}

////////////////////////////////////////////////////////////////////////////////

void TClient::DoDumpJobContext(
    TJobId jobId,
    const TYPath& path,
    const TDumpJobContextOptions& /*options*/)
{
    auto allocationId = AllocationIdFromJobId(jobId);

    auto allocationBriefInfo = WaitFor(GetAllocationBriefInfo(
        *SchedulerOperationProxy_,
        allocationId,
        TAllocationInfoToRequest{
            .OperationId = true,
            .OperationAcl = true,
            .NodeDescriptor = true,
        }))
        .ValueOrThrow();

    ValidateOperationAccess(
        allocationBriefInfo.OperationId,
        *allocationBriefInfo.OperationAcl,
        jobId,
        EPermissionSet(EPermission::Read));

    auto transaction = [&] {
        auto attributes = CreateEphemeralAttributes();
        attributes->Set("title", Format("Dump input context of job %v of operation %v", jobId, allocationBriefInfo.OperationId));

        NApi::TTransactionStartOptions options{
            .Attributes = std::move(attributes)
        };

        return WaitFor(StartTransaction(ETransactionType::Master, options))
            .ValueOrThrow();
    }();

    auto nodeChannel = ChannelFactory_->CreateChannel(allocationBriefInfo.NodeDescriptor);
    NJobProberClient::TJobProberServiceProxy jobProberServiceProxy(std::move(nodeChannel));
    jobProberServiceProxy.SetDefaultTimeout(Connection_->GetConfig()->JobProberRpcTimeout);

    auto req = jobProberServiceProxy.DumpInputContext();
    ToProto(req->mutable_job_id(), jobId);
    ToProto(req->mutable_transaction_id(), transaction->GetId());

    YT_LOG_DEBUG("Requesting node to dump job input context (TransactionId: %v)", transaction->GetId());

    auto rsp = WaitFor(req->Invoke()).
        ValueOrThrow();

    auto chunkIds = FromProto<std::vector<TChunkId>>(rsp->chunk_ids());
    YT_VERIFY(chunkIds.size() == 1);

    auto chunkId = chunkIds[0];

    YT_LOG_DEBUG("Received job input context dump from node (ChunkId: %v)", chunkId);

    try {
        TJobFile file{
            jobId,
            path,
            chunkId,
            "input_context"
        };
        SaveJobFiles(
            MakeStrong(this),
            allocationBriefInfo.OperationId,
            {file},
            transaction->GetId());

        WaitFor(transaction->Commit())
            .ThrowOnError();
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(
            "Error saving input context for job %v into %v",
            jobId,
            path)
            << ex;
    }

    YT_LOG_DEBUG(
        "Job input context attached (ChunkId: %v, Path: %v)",
        chunkId,
        path);
}

////////////////////////////////////////////////////////////////////////////////

IAsyncZeroCopyInputStreamPtr TClient::DoGetJobInput(
    TJobId jobId,
    const TGetJobInputOptions& options)
{
    auto jobSpec = FetchJobSpec(jobId, options.JobSpecSource, EPermissionSet(EPermission::Read));

    auto* schedulerJobSpecExt = jobSpec.MutableExtension(NControllerAgent::NProto::TJobSpecExt::job_spec_ext);

    auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();

    auto locateChunksResult = WaitFor(BIND([=, this, this_ = MakeStrong(this)] {
        std::vector<TChunkSpec*> chunkSpecList;
        for (auto& tableSpec : *schedulerJobSpecExt->mutable_input_table_specs()) {
            for (auto& chunkSpec : *tableSpec.mutable_chunk_specs()) {
                chunkSpecList.push_back(&chunkSpec);
            }
        }

        for (auto& tableSpec : *schedulerJobSpecExt->mutable_foreign_input_table_specs()) {
            for (auto& chunkSpec : *tableSpec.mutable_chunk_specs()) {
                chunkSpecList.push_back(&chunkSpec);
            }
        }

        LocateChunks(
            MakeStrong(this),
            New<TMultiChunkReaderConfig>()->MaxChunksPerLocateRequest,
            chunkSpecList,
            nodeDirectory,
            Logger);
        nodeDirectory->DumpTo(schedulerJobSpecExt->mutable_input_node_directory());
    })
        .AsyncVia(GetConnection()->GetInvoker())
        .Run());

    if (!locateChunksResult.IsOK()) {
        THROW_ERROR_EXCEPTION("Failed to locate chunks used in job input")
            << TErrorAttribute("job_id", jobId)
            << locateChunksResult;
    }

    auto jobSpecHelper = NJobProxy::CreateJobSpecHelper(jobSpec);
    GetNativeConnection()->GetNodeDirectory()->MergeFrom(
        jobSpecHelper->GetJobSpecExt().input_node_directory());

    auto userJobReadController = CreateUserJobReadController(
        jobSpecHelper,
        TChunkReaderHost::FromClient(MakeStrong(this)),
        GetConnection()->GetInvoker(),
        /*onNetworkRelease*/ BIND([] { }),
        /*udfDirectory*/ {},
        TClientChunkReadOptions{
            .WorkloadDescriptor = TWorkloadDescriptor(EWorkloadCategory::UserInteractive)
        },
        /*localHostName*/ {});

    auto jobInputReader = New<TJobInputReader>(std::move(userJobReadController), GetConnection()->GetInvoker());
    jobInputReader->Open();
    return jobInputReader;
}

////////////////////////////////////////////////////////////////////////////////

TYsonString TClient::DoGetJobInputPaths(
    TJobId jobId,
    const TGetJobInputPathsOptions& options)
{
    auto jobSpec = FetchJobSpec(jobId, options.JobSpecSource, EPermissionSet(EPermissionSet::Read));

    const auto& jobSpecExt = jobSpec.GetExtension(NControllerAgent::NProto::TJobSpecExt::job_spec_ext);

    auto optionalDataSourceDirectoryExt = FindProtoExtension<TDataSourceDirectoryExt>(jobSpecExt.extensions());
    if (!optionalDataSourceDirectoryExt) {
        THROW_ERROR_EXCEPTION("Cannot build job input paths; job is either too old or has intermediate input")
            << TErrorAttribute("job_id", jobId);
    }

    const auto& dataSourceDirectoryExt = *optionalDataSourceDirectoryExt;
    auto dataSourceDirectory = FromProto<TDataSourceDirectoryPtr>(dataSourceDirectoryExt);

    for (const auto& dataSource : dataSourceDirectory->DataSources()) {
        if (!dataSource.GetPath()) {
            THROW_ERROR_EXCEPTION("Cannot build job input paths; job has intermediate input")
                << TErrorAttribute("job_id", jobId);
        }
    }

    std::vector<std::vector<TDataSliceDescriptor>> slicesByTable(dataSourceDirectory->DataSources().size());
    for (const auto& inputSpec : jobSpecExt.input_table_specs()) {
        auto dataSliceDescriptors = NJobProxy::UnpackDataSliceDescriptors(inputSpec);
        for (const auto& slice : dataSliceDescriptors) {
            slicesByTable[slice.GetDataSourceIndex()].push_back(slice);
        }
    }

    for (const auto& inputSpec : jobSpecExt.foreign_input_table_specs()) {
        auto dataSliceDescriptors = NJobProxy::UnpackDataSliceDescriptors(inputSpec);
        for (const auto& slice : dataSliceDescriptors) {
            slicesByTable[slice.GetDataSourceIndex()].push_back(slice);
        }
    }

    auto pathByTable = CombineDataSlices(dataSourceDirectory, slicesByTable);

    return ConvertToYsonString(pathByTable, EYsonFormat::Pretty);
}

////////////////////////////////////////////////////////////////////////////////

TYsonString TClient::DoGetJobSpec(
    TJobId jobId,
    const TGetJobSpecOptions& options)
{
    auto jobSpec = FetchJobSpec(jobId, options.JobSpecSource, EPermissionSet(EPermissionSet::Read));

    NControllerAgent::SanitizeJobSpec(&jobSpec);

    auto* jobSpecExt = jobSpec.MutableExtension(NControllerAgent::NProto::TJobSpecExt::job_spec_ext);

    if (options.OmitNodeDirectory) {
        jobSpecExt->clear_input_node_directory();
    }

    if (options.OmitInputTableSpecs) {
        jobSpecExt->clear_input_table_specs();
        jobSpecExt->clear_foreign_input_table_specs();
    }

    if (options.OmitOutputTableSpecs) {
        jobSpecExt->clear_output_table_specs();
    }

    TString jobSpecYsonBytes;
    TStringOutput jobSpecYsonBytesOutput(jobSpecYsonBytes);
    TYsonWriter jobSpecYsonWriter(&jobSpecYsonBytesOutput);

    TProtobufParserOptions parserOptions{
        .SkipUnknownFields = true,
    };
    WriteProtobufMessage(&jobSpecYsonWriter, jobSpec, parserOptions);

    return TYsonString(jobSpecYsonBytes);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TFun>
auto RetryJobIsNotRunning(
    TOperationId operationId,
    TJobId jobId,
    TFun invokeRequest,
    const NLogging::TLogger& Logger)
{
    constexpr int RetryCount = 10;
    constexpr TDuration RetryBackoff = TDuration::MilliSeconds(100);

    auto needRetry = [] (auto rspOrError) {
        auto jobIsNotRunning = rspOrError.FindMatching(NJobProberClient::EErrorCode::JobIsNotRunning);
        if (!jobIsNotRunning) {
            return false;
        }
        auto jobState = jobIsNotRunning->Attributes().template Find<EJobState>("job_state");
        return jobState && *jobState == EJobState::Running;
    };

    auto rspOrError = invokeRequest();
    for (int retry = 0; needRetry(rspOrError) && retry < RetryCount; ++retry) {
        YT_LOG_DEBUG("Job state is \"running\" but job phase is not, retrying "
            "(OperationId: %v, JobId: %v, Retry: %d, RetryCount: %d, RetryBackoff: %v, Error: %v)",
            operationId,
            jobId,
            retry,
            RetryCount,
            RetryBackoff,
            rspOrError);
        TDelayedExecutor::WaitForDuration(RetryBackoff);
        rspOrError = invokeRequest();
    }
    return rspOrError;
}

TSharedRef TClient::DoGetJobStderrFromNode(
    TOperationId operationId,
    TJobId jobId)
{
    auto nodeChannelOrError = TryCreateChannelToJobNode(operationId, jobId, EPermissionSet(EPermission::Read));
    if (!nodeChannelOrError.IsOK()) {
        return TSharedRef();
    }
    auto nodeChannel = std::move(nodeChannelOrError).Value();

    NJobProberClient::TJobProberServiceProxy jobProberServiceProxy(std::move(nodeChannel));
    jobProberServiceProxy.SetDefaultTimeout(Connection_->GetConfig()->JobProberRpcTimeout);

    auto rspOrError = RetryJobIsNotRunning(
        operationId,
        jobId,
        [&] {
            auto req = jobProberServiceProxy.GetStderr();
            req->SetMultiplexingBand(EMultiplexingBand::Heavy);
            ToProto(req->mutable_job_id(), jobId);
            return WaitFor(req->Invoke());
        },
        Logger);

    if (!rspOrError.IsOK()) {
        if (IsNoSuchJobOrOperationError(rspOrError) ||
            rspOrError.FindMatching(NJobProberClient::EErrorCode::JobIsNotRunning))
        {
            return TSharedRef();
        }
        THROW_ERROR_EXCEPTION("Failed to get job stderr from job proxy")
            << TErrorAttribute("operation_id", operationId)
            << TErrorAttribute("job_id", jobId)
            << std::move(rspOrError);
    }
    auto rsp = rspOrError.Value();
    return TSharedRef::FromString(rsp->stderr_data());
}

TSharedRef TClient::DoGetJobStderrFromArchive(
    TOperationId operationId,
    TJobId jobId)
{
    try {
        auto operationIdAsGuid = operationId.Underlying();
        auto jobIdAsGuid = jobId.Underlying();
        NRecords::TJobStderrKey recordKey{
            .OperationIdHi = operationIdAsGuid.Parts64[0],
            .OperationIdLo = operationIdAsGuid.Parts64[1],
            .JobIdHi = jobIdAsGuid.Parts64[0],
            .JobIdLo = jobIdAsGuid.Parts64[1]
        };
        auto keys = FromRecordKeys(MakeRange(std::array{recordKey}));

        auto rowset = WaitFor(LookupRows(
            GetOperationsArchiveJobStderrsPath(),
            NRecords::TJobStderrDescriptor::Get()->GetNameTable(),
            keys,
            /*options*/ {}))
            .ValueOrThrow()
            .Rowset;

        auto records = ToRecords<NRecords::TJobStderr>(rowset);
        YT_VERIFY(records.size() <= 1);
        if (records.empty()) {
            return {};
        }

        const auto& record = records[0];
        return TSharedRef::FromString(record.Stderr);
    } catch (const TErrorException& ex) {
        auto matchedError = ex.Error().FindMatching(NYTree::EErrorCode::ResolveError);
        if (!matchedError) {
            THROW_ERROR_EXCEPTION("Failed to get job stderr from archive")
                << TErrorAttribute("operation_id", operationId)
                << TErrorAttribute("job_id", jobId)
                << ex;
        }
        return {};
    }
}

TSharedRef TClient::DoGetJobStderr(
    const TOperationIdOrAlias& operationIdOrAlias,
    TJobId jobId,
    const TGetJobStderrOptions& options)
{
    auto timeout = options.Timeout.value_or(Connection_->GetConfig()->DefaultGetOperationTimeout);
    auto deadline = timeout.ToDeadLine();

    TOperationId operationId;
    Visit(operationIdOrAlias.Payload,
        [&] (const TOperationId& id) {
            operationId = id;
        },
        [&] (const TString& alias) {
            operationId = ResolveOperationAlias(alias, options, deadline);
        });

    ValidateOperationAccess(operationId, jobId, EPermissionSet(EPermission::Read));

    auto stderrRef = DoGetJobStderrFromNode(operationId, jobId);
    if (stderrRef) {
        return stderrRef;
    }

    stderrRef = DoGetJobStderrFromArchive(operationId, jobId);
    if (stderrRef) {
        return stderrRef;
    }

    THROW_ERROR_EXCEPTION(NControllerAgent::EErrorCode::NoSuchJob, "Job stderr is not found")
        << TErrorAttribute("operation_id", operationId)
        << TErrorAttribute("job_id", jobId);
}

////////////////////////////////////////////////////////////////////////////////

TSharedRef TClient::DoGetJobFailContextFromNode(
    TOperationId operationId,
    TJobId jobId)
{
    auto nodeChannelOrError = TryCreateChannelToJobNode(operationId, jobId, EPermissionSet(EPermission::Read));
    if (!nodeChannelOrError.IsOK()) {
        return TSharedRef();
    }
    auto nodeChannel = std::move(nodeChannelOrError).Value();

    NJobProberClient::TJobProberServiceProxy jobProberServiceProxy(std::move(nodeChannel));
    jobProberServiceProxy.SetDefaultTimeout(Connection_->GetConfig()->JobProberRpcTimeout);

    auto rspOrError = RetryJobIsNotRunning(
        operationId,
        jobId,
        [&] {
            auto req = jobProberServiceProxy.GetFailContext();
            req->SetMultiplexingBand(EMultiplexingBand::Heavy);
            ToProto(req->mutable_job_id(), jobId);
            return WaitFor(req->Invoke());
        },
        Logger);

    if (!rspOrError.IsOK()) {
        if (IsNoSuchJobOrOperationError(rspOrError) ||
            rspOrError.FindMatching(NJobProberClient::EErrorCode::JobIsNotRunning))
        {
            return TSharedRef();
        }
        THROW_ERROR_EXCEPTION("Failed to get job job fail context from node")
            << TErrorAttribute("operation_id", operationId)
            << TErrorAttribute("job_id", jobId)
            << std::move(rspOrError);
    }
    auto rsp = rspOrError.Value();
    return TSharedRef::FromString(rsp->fail_context_data());
}

TSharedRef TClient::DoGetJobFailContextFromArchive(
    TOperationId operationId,
    TJobId jobId)
{
    try {
        auto operationIdAsGuid = operationId.Underlying();
        auto jobIdAsGuid = jobId.Underlying();
        NRecords::TJobFailContextKey recordKey{
            .OperationIdHi = operationIdAsGuid.Parts64[0],
            .OperationIdLo = operationIdAsGuid.Parts64[1],
            .JobIdHi = jobIdAsGuid.Parts64[0],
            .JobIdLo = jobIdAsGuid.Parts64[1]
        };

        auto keys = FromRecordKeys(MakeRange(std::array{recordKey}));

        TLookupRowsOptions lookupOptions;
        const auto& idMapping = NRecords::TJobFailContextDescriptor::Get()->GetIdMapping();
        lookupOptions.ColumnFilter = NTableClient::TColumnFilter({*idMapping.FailContext});
        lookupOptions.KeepMissingRows = true;

        auto rowset = WaitFor(LookupRows(
            GetOperationsArchiveJobFailContextsPath(),
            NRecords::TJobFailContextDescriptor::Get()->GetNameTable(),
            std::move(keys),
            lookupOptions))
            .ValueOrThrow()
            .Rowset;

        auto optionalRecords = ToOptionalRecords<NRecords::TJobFailContextPartial>(rowset);
        YT_VERIFY(optionalRecords.size() == 1);

        if (optionalRecords[0] && optionalRecords[0]->FailContext) {
            return TSharedRef::FromString(*optionalRecords[0]->FailContext);
        }
    } catch (const TErrorException& ex) {
        auto matchedError = ex.Error().FindMatching(NYTree::EErrorCode::ResolveError);
        if (!matchedError) {
            THROW_ERROR_EXCEPTION("Failed to get job fail_context from archive")
                << TErrorAttribute("operation_id", operationId)
                << TErrorAttribute("job_id", jobId)
                << ex.Error();
        }
    }

    return {};
}

TSharedRef TClient::DoGetJobFailContext(
    const TOperationIdOrAlias& operationIdOrAlias,
    TJobId jobId,
    const TGetJobFailContextOptions& options)
{
    auto timeout = options.Timeout.value_or(Connection_->GetConfig()->DefaultGetOperationTimeout);
    auto deadline = timeout.ToDeadLine();

    TOperationId operationId;
    Visit(operationIdOrAlias.Payload,
        [&] (const TOperationId& id) {
            operationId = id;
        },
        [&] (const TString& alias) {
            operationId = ResolveOperationAlias(alias, options, deadline);
        });

    ValidateOperationAccess(operationId, jobId, EPermissionSet(EPermission::Read));

    if (auto failContextRef = DoGetJobFailContextFromNode(operationId, jobId)) {
        return failContextRef;
    }
    if (auto failContextRef = DoGetJobFailContextFromArchive(operationId, jobId)) {
        return failContextRef;
    }
    THROW_ERROR_EXCEPTION(
        NControllerAgent::EErrorCode::NoSuchJob,
        "Job fail context is not found")
        << TErrorAttribute("operation_id", operationId)
        << TErrorAttribute("job_id", jobId);
}

////////////////////////////////////////////////////////////////////////////////

static void ValidateNonNull(
    const TUnversionedValue& value,
    TStringBuf name,
    TOperationId operationId,
    TJobId jobId = {})
{
    if (Y_UNLIKELY(value.Type == EValueType::Null)) {
        auto error = TError("Unexpected null value in column %Qv in job archive", name)
            << TErrorAttribute("operation_id", operationId);
        if (jobId.Underlying()) {
            error = error << TErrorAttribute("job_id", jobId);
        }
        THROW_ERROR error;
    }
}

static TQueryBuilder GetListJobsQueryBuilder(
    TOperationId operationId,
    const TListJobsOptions& options)
{
    NQueryClient::TQueryBuilder builder;
    auto operationIdAsGuid = operationId.Underlying();
    builder.SetSource(GetOperationsArchiveJobsPath());

    builder.AddWhereConjunct(Format(
        "(operation_id_hi, operation_id_lo) = (%vu, %vu)",
        operationIdAsGuid.Parts64[0],
        operationIdAsGuid.Parts64[1]));

    builder.AddWhereConjunct(Format(
        "controller_state IN (%v) OR node_state IN (%v) "
        "OR ((NOT is_null(update_time)) AND update_time >= %v)",
        FinishedJobStatesString,
        FinishedJobStatesString,
        (TInstant::Now() - options.RunningJobsLookbehindPeriod).MicroSeconds()));

    if (options.Address) {
        builder.AddWhereConjunct(Format("is_prefix(%Qv, address)", *options.Address));
    }

    return builder;
}

// Get statistics for jobs.
TFuture<TListJobsStatistics> TClient::ListJobsStatisticsFromArchiveAsync(
    TOperationId operationId,
    TInstant deadline,
    const TListJobsOptions& options)
{
    auto builder = GetListJobsQueryBuilder(operationId, options);

    auto jobTypeIndex = builder.AddSelectExpression("type", "job_type");
    auto jobStateIndex = builder.AddSelectExpression("if(is_null(state), transient_state, state)", "node_state");
    auto countIndex = builder.AddSelectExpression("sum(1)", "count");

    builder.AddGroupByExpression("job_type");
    builder.AddGroupByExpression("node_state");

    TSelectRowsOptions selectRowsOptions;
    selectRowsOptions.Timestamp = AsyncLastCommittedTimestamp;
    selectRowsOptions.Timeout = deadline - Now();
    selectRowsOptions.InputRowLimit = std::numeric_limits<i64>::max();
    selectRowsOptions.MemoryLimitPerNode = 100_MB;

    return SelectRows(builder.Build(), selectRowsOptions).Apply(BIND([=] (const TSelectRowsResult& result) {
        TListJobsStatistics statistics;
        for (auto row : result.Rowset->GetRows()) {
            // Skip jobs that was not fully written (usually it is written only by controller).
            if (row[jobTypeIndex].Type == EValueType::Null || row[jobStateIndex].Type == EValueType::Null) {
                continue;
            }

            ValidateNonNull(row[jobTypeIndex], "type", operationId);
            auto jobType = ParseEnum<EJobType>(FromUnversionedValue<TStringBuf>(row[jobTypeIndex]));
            ValidateNonNull(row[jobStateIndex], "state", operationId);
            auto jobState = ParseEnum<EJobState>(FromUnversionedValue<TStringBuf>(row[jobStateIndex]));
            auto count = FromUnversionedValue<i64>(row[countIndex]);

            statistics.TypeCounts[jobType] += count;
            if (options.Type && *options.Type != jobType) {
                continue;
            }

            statistics.StateCounts[jobState] += count;
            if (options.State && *options.State != jobState) {
                continue;
            }
        }
        return statistics;
    }));
}

static std::vector<TJob> ParseJobsFromArchiveResponse(
    TOperationId operationId,
    const std::vector<NRecords::TJobPartial>& records,
    bool needFullStatistics)
{
    std::vector<TJob> jobs;
    jobs.reserve(records.size());
    for (const auto& record : records) {
        auto jobType = record.Type;
        if (!jobType) {
            jobType = record.JobType;
        }

        auto nodeState = record.TransientState;
        if (!nodeState) {
            nodeState = record.NodeState;
        }

        // Skip jobs that was not fully written (usually it is written only by controller).
        if (!jobType || !nodeState) {
            continue;
        }

        auto job = TJob{
            .Id = TJobId(TGuid(record.Key.JobIdHi, record.Key.JobIdLo)),
            .OperationId = operationId,
            // This field previously was non-optional.
            .Address = record.Address.value_or(""),
            .StderrSize = record.StderrSize,
            .FailContextSize = record.FailContextSize,
            // This field previously was non-optional.
            .HasSpec = record.HasSpec.value_or(false),
            .HasCompetitors = record.HasCompetitors.value_or(false),
            .HasProbingCompetitors = record.HasProbingCompetitors.value_or(false),
            .Error = record.Error.value_or(TYsonString()),
            .InterruptionInfo = record.InterruptionInfo.value_or(TYsonString()),
            .BriefStatistics = record.BriefStatistics.value_or(TYsonString()),
            .CoreInfos = record.CoreInfos.value_or(TYsonString()),
            .Events = record.Events.value_or(TYsonString()),
            .ExecAttributes = record.ExecAttributes.value_or(TYsonString()),
            .TaskName = record.TaskName,
            .PoolTree = record.PoolTree,
            .MonitoringDescriptor = record.MonitoringDescriptor,
            .JobCookie = record.JobCookie,
        };

        if (jobType) {
            job.Type = ParseEnum<EJobType>(*jobType);
        }

        if (record.ControllerState) {
            job.ControllerState = ParseEnum<EJobState>(*record.ControllerState);
        }

        if (nodeState) {
            job.ArchiveState = ParseEnum<EJobState>(*nodeState);
        }

        if (record.StartTime) {
            job.StartTime = TInstant::MicroSeconds(*record.StartTime);
        } else {
            // This field previously was non-optional.
            job.StartTime.emplace();
        }

        if (record.FinishTime) {
            job.FinishTime = TInstant::MicroSeconds(*record.FinishTime);
        }

        if (record.JobCompetitionId) {
            job.JobCompetitionId = TJobId(TGuid::FromString(*record.JobCompetitionId));
        }

        if (record.ProbingJobCompetitionId) {
            job.ProbingJobCompetitionId = TJobId(TGuid::FromString(*record.ProbingJobCompetitionId));
        }

        if ((needFullStatistics || !job.BriefStatistics) &&
            record.Statistics)
        {
            auto statisticsYson = *record.Statistics;
            if (needFullStatistics) {
                job.Statistics = statisticsYson;
            }
            auto statistics = ConvertToNode(statisticsYson);
            job.BriefStatistics = BuildBriefStatistics(statistics);
        }

        if ((needFullStatistics || !job.BriefStatistics) &&
            record.StatisticsLz4)
        {
            auto statisticsLz4 = TStringBuf(*record.StatisticsLz4);
            auto codec = NCompression::GetCodec(NCompression::ECodec::Lz4);
            auto decompressed = codec->Decompress(TSharedRef(statisticsLz4.data(), statisticsLz4.size(), nullptr));
            auto statisticsYson = TYsonStringBuf(TStringBuf(decompressed.Begin(), decompressed.Size()));
            if (needFullStatistics) {
                job.Statistics = TYsonString(statisticsYson);
            }
            auto statistics = ConvertToNode(statisticsYson);
            job.BriefStatistics = BuildBriefStatistics(statistics);
        }

        // We intentionally mark stderr as missing if job has no spec since
        // it is impossible to check permissions without spec.
        if (job.GetState() && IsJobFinished(*job.GetState()) && !job.HasSpec) {
            job.StderrSize = std::nullopt;
        }

        jobs.push_back(std::move(job));
    }
    return jobs;
}

TFuture<std::vector<TJob>> TClient::DoListJobsFromArchiveAsync(
    TOperationId operationId,
    TInstant deadline,
    const TListJobsOptions& options)
{
    auto builder = GetListJobsQueryBuilder(operationId, options);

    builder.SetLimit(options.Limit + options.Offset);

    builder.AddSelectExpression("job_id_hi");
    builder.AddSelectExpression("job_id_lo");
    builder.AddSelectExpression("type", "job_type");
    builder.AddSelectExpression("start_time");
    builder.AddSelectExpression("finish_time");
    builder.AddSelectExpression("address");
    builder.AddSelectExpression("error");
    builder.AddSelectExpression("interruption_info");
    builder.AddSelectExpression("statistics");
    builder.AddSelectExpression("statistics_lz4");
    builder.AddSelectExpression("stderr_size");
    builder.AddSelectExpression("has_spec");
    builder.AddSelectExpression("fail_context_size");
    builder.AddSelectExpression("job_competition_id");
    builder.AddSelectExpression("probing_job_competition_id");
    builder.AddSelectExpression("has_competitors");
    builder.AddSelectExpression("exec_attributes");
    builder.AddSelectExpression("task_name");
    builder.AddSelectExpression("pool_tree");
    builder.AddSelectExpression("monitoring_descriptor");
    builder.AddSelectExpression("core_infos");
    builder.AddSelectExpression("job_cookie");
    builder.AddSelectExpression("if(is_null(state), transient_state, state)", "node_state");
    builder.AddSelectExpression("controller_state");
    builder.AddSelectExpression(
        Format(
            "if(NOT is_null(node_state) AND NOT is_null(controller_state), "
            "   if(node_state IN (%v), node_state, controller_state), "
            "if(is_null(node_state), controller_state, node_state))",
            FinishedJobStatesString),
        "job_state");

    if (options.WithStderr) {
        if (*options.WithStderr) {
            builder.AddWhereConjunct("stderr_size != 0 AND NOT is_null(stderr_size)");
        } else {
            builder.AddWhereConjunct("stderr_size = 0 OR is_null(stderr_size)");
        }
    }

    if (options.WithSpec) {
        if (*options.WithSpec) {
            builder.AddWhereConjunct("has_spec");
        } else {
            builder.AddWhereConjunct("NOT has_spec OR is_null(has_spec)");
        }
    }

    if (options.WithFailContext) {
        if (*options.WithFailContext) {
            builder.AddWhereConjunct("fail_context_size != 0 AND NOT is_null(fail_context_size)");
        } else {
            builder.AddWhereConjunct("fail_context_size = 0 OR is_null(fail_context_size)");
        }
    }

    if (options.Type) {
        builder.AddWhereConjunct(Format("job_type = %Qv", FormatEnum(*options.Type)));
    }

    if (options.State) {
        builder.AddWhereConjunct(Format("job_state = %Qv", FormatEnum(*options.State)));
    }

    if (options.JobCompetitionId) {
        builder.AddWhereConjunct(Format("job_competition_id = %Qv", options.JobCompetitionId));
    }

    if (options.WithCompetitors) {
        if (*options.WithCompetitors) {
            builder.AddWhereConjunct("has_competitors");
        } else {
            builder.AddWhereConjunct("is_null(has_competitors) OR NOT has_competitors");
        }
    }

    if (options.WithMonitoringDescriptor) {
        if (*options.WithMonitoringDescriptor) {
            builder.AddWhereConjunct("not is_null(monitoring_descriptor)");
        } else {
            builder.AddWhereConjunct("is_null(monitoring_descriptor)");
        }
    }

    if (options.TaskName) {
        builder.AddWhereConjunct(Format("task_name = %Qv", *options.TaskName));
    }

    if (options.SortField != EJobSortField::None) {
        auto orderByDirection = [&] {
            switch (options.SortOrder) {
                case EJobSortDirection::Ascending:
                    return EOrderByDirection::Ascending;
                case EJobSortDirection::Descending:
                    return EOrderByDirection::Descending;
            }
            YT_ABORT();
        }();
        auto orderByFieldExpressions = [&] () -> std::vector<TString> {
            switch (options.SortField) {
                case EJobSortField::Type:
                    return {"job_type"};
                case EJobSortField::State:
                    return {"job_state"};
                case EJobSortField::StartTime:
                    return {"start_time"};
                case EJobSortField::FinishTime:
                    return {"finish_time"};
                case EJobSortField::Address:
                    return {"address"};
                case EJobSortField::Duration:
                    return {Format("if(is_null(finish_time), %v, finish_time) - start_time", TInstant::Now().MicroSeconds())};
                case EJobSortField::Id:
                case EJobSortField::None:
                    // We sort by id anyway.
                    return {};
                case EJobSortField::Progress:
                    // XXX: progress is not present in archive table.
                    return {};
            }
            YT_ABORT();
        }();
        orderByFieldExpressions.push_back("format_guid(job_id_hi, job_id_lo)");
        builder.AddOrderByExpression(JoinSeq(",", orderByFieldExpressions), orderByDirection);
    }

    TSelectRowsOptions selectRowsOptions;
    selectRowsOptions.Timestamp = AsyncLastCommittedTimestamp;
    selectRowsOptions.Timeout = deadline - Now();
    selectRowsOptions.InputRowLimit = std::numeric_limits<i64>::max();
    selectRowsOptions.MemoryLimitPerNode = 100_MB;

    return SelectRows(builder.Build(), selectRowsOptions).Apply(BIND([operationId, this_ = MakeStrong(this)] (const TSelectRowsResult& result) {
        auto records = ToRecords<NRecords::TJobPartial>(result.Rowset);
        return ParseJobsFromArchiveResponse(operationId, records, /*needFullStatistics*/ false);
    }));
}

static void ParseJobsFromControllerAgentResponse(
    TOperationId operationId,
    const std::vector<std::pair<TString, INodePtr>>& jobNodes,
    const std::function<bool(const INodePtr&)>& filter,
    const THashSet<TString>& attributes,
    std::vector<TJob>* jobs)
{
    auto needJobId = attributes.contains("job_id");
    auto needOperationId = attributes.contains("operation_id");
    auto needType = attributes.contains("type");
    auto needState = attributes.contains("state");
    auto needStartTime = attributes.contains("start_time");
    auto needFinishTime = attributes.contains("finish_time");
    auto needAddress = attributes.contains("address");
    auto needHasSpec = attributes.contains("has_spec");
    auto needProgress = attributes.contains("progress");
    auto needStderrSize = attributes.contains("stderr_size");
    auto needBriefStatistics = attributes.contains("brief_statistics");
    auto needJobCompetitionId = attributes.contains("job_competition_id");
    auto needProbingJobCompetitionId = attributes.contains("probing_job_competition_id");
    auto needHasCompetitors = attributes.contains("has_competitors");
    auto needHasProbingCompetitors = attributes.contains("has_probing_competitors");
    auto needError = attributes.contains("error");
    auto needTaskName = attributes.contains("task_name");
    auto needCoreInfos = attributes.contains("core_infos");
    auto needJobCookie = attributes.contains("job_cookie");

    for (const auto& [jobIdString, jobNode] : jobNodes) {
        if (!filter(jobNode)) {
            continue;
        }

        const auto& jobMapNode = jobNode->AsMap();
        auto& job = jobs->emplace_back();
        if (needJobId) {
            job.Id = TJobId(TGuid::FromString(jobIdString));
        }
        if (needOperationId) {
            job.OperationId =operationId;
        }
        if (needType) {
            job.Type = jobMapNode->GetChildValueOrThrow<EJobType>("job_type");
        }
        if (needState) {
            job.ControllerState = jobMapNode->GetChildValueOrThrow<EJobState>("state");
        }
        if (needStartTime) {
            job.StartTime = jobMapNode->GetChildValueOrThrow<TInstant>("start_time");
        }
        if (needFinishTime) {
            if (auto childNode = jobMapNode->FindChild("finish_time")) {
                job.FinishTime = childNode->GetValue<TInstant>();
            }
        }
        if (needAddress) {
            job.Address = jobMapNode->GetChildValueOrThrow<TString>("address");
        }
        if (needHasSpec) {
            job.HasSpec = true;
        }
        if (needProgress) {
            job.Progress = jobMapNode->GetChildValueOrThrow<double>("progress");
        }
        if (auto stderrSize = jobMapNode->GetChildValueOrThrow<i64>("stderr_size"); stderrSize > 0 && needStderrSize) {
            job.StderrSize = stderrSize;
        }
        if (needBriefStatistics) {
            job.BriefStatistics = ConvertToYsonString(jobMapNode->GetChildOrThrow("brief_statistics"));
        }
        if (needJobCompetitionId) {
            job.JobCompetitionId = jobMapNode->GetChildValueOrThrow<TJobId>("job_competition_id");
        }
        if (needProbingJobCompetitionId) {
            job.ProbingJobCompetitionId = jobMapNode->GetChildValueOrThrow<TJobId>("probing_job_competition_id");
        }
        if (needHasCompetitors) {
            job.HasCompetitors = jobMapNode->GetChildValueOrThrow<bool>("has_competitors");
        }
        if (needHasProbingCompetitors) {
            job.HasProbingCompetitors = jobMapNode->GetChildValueOrThrow<bool>("has_probing_competitors");
        }
        if (needError) {
            if (auto childNode = jobMapNode->FindChild("error")) {
                job.Error = ConvertToYsonString(ConvertTo<TError>(childNode));
            }
        }
        if (needTaskName) {
            job.TaskName = jobMapNode->GetChildValueOrThrow<TString>("task_name");
        }
        if (needCoreInfos) {
            if (auto childNode = jobMapNode->FindChild("core_infos")) {
                job.CoreInfos = ConvertToYsonString(childNode);
            }
        }
        if (needJobCookie) {
            // COMPAT(renadeen): remove this condition in 23.1.
            if (auto childNode = jobMapNode->FindChild("job_cookie")) {
                job.JobCookie = jobMapNode->GetChildValueOrThrow<ui64>("job_cookie");
            }
        }
    }
}

static void ParseJobsFromControllerAgentResponse(
    TOperationId operationId,
    const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp,
    const TString& key,
    const THashSet<TString>& attributes,
    const TListJobsOptions& options,
    std::vector<TJob>* jobs,
    int* totalCount,
    const NLogging::TLogger& Logger)
{
    auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>(key);
    if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
        return;
    }
    if (!rspOrError.IsOK()) {
        THROW_ERROR_EXCEPTION(EErrorCode::UncertainOperationControllerState,
            "Error obtaining %Qv of operation %v from controller agent orchid",
            key,
            operationId)
            << rspOrError;
    }

    auto rsp = rspOrError.Value();
    auto items = ConvertToNode(NYson::TYsonString(rsp->value()))->AsMap();
    *totalCount += items->GetChildren().size();

    YT_LOG_DEBUG("Received %Qv jobs from controller agent (Count: %v)",
        key,
        items->GetChildren().size());

    auto filter = [&] (const INodePtr& jobNode) -> bool {
        const auto& jobMap = jobNode->AsMap();
        auto address = jobMap->GetChildValueOrThrow<TString>("address");
        auto type = ConvertTo<EJobType>(jobMap->GetChildOrThrow("job_type"));
        auto state = ConvertTo<EJobState>(jobMap->GetChildOrThrow("state"));
        auto stderrSize = jobMap->GetChildValueOrThrow<i64>("stderr_size");
        auto failContextSize = jobMap->GetChildValueOrDefault<i64>("fail_context_size", 0);
        auto jobCompetitionId = jobMap->GetChildValueOrThrow<TJobId>("job_competition_id");
        auto hasCompetitors = jobMap->GetChildValueOrThrow<bool>("has_competitors");
        auto taskName = jobMap->GetChildValueOrThrow<TString>("task_name");
        return
            (!options.Address || options.Address == address) &&
            (!options.Type || options.Type == type) &&
            (!options.State || options.State == state) &&
            (!options.WithStderr || *options.WithStderr == (stderrSize > 0)) &&
            (!options.WithFailContext || *options.WithFailContext == (failContextSize > 0)) &&
            (!options.JobCompetitionId || options.JobCompetitionId == jobCompetitionId) &&
            (!options.WithCompetitors || options.WithCompetitors == hasCompetitors) &&
            (!options.TaskName || options.TaskName == taskName);
    };

    ParseJobsFromControllerAgentResponse(
        operationId,
        items->GetChildren(),
        filter,
        attributes,
        jobs);
}

TFuture<TListJobsFromControllerAgentResult> TClient::DoListJobsFromControllerAgentAsync(
    TOperationId operationId,
    const std::optional<TString>& controllerAgentAddress,
    TInstant deadline,
    const TListJobsOptions& options)
{
    if (!controllerAgentAddress) {
        return MakeFuture(TListJobsFromControllerAgentResult{});
    }

    TMasterReadOptions readOptions{
        .ReadFrom = EMasterChannelKind::Follower,
    };
    auto proxy = CreateObjectServiceReadProxy(readOptions);
    proxy.SetDefaultTimeout(deadline - Now());
    auto batchReq = proxy.ExecuteBatch();

    batchReq->AddRequest(
        TYPathProxy::Get(GetControllerAgentOrchidOperationPath(*controllerAgentAddress, operationId) + "/state"),
        "controller_state");

    batchReq->AddRequest(
        TYPathProxy::Get(GetControllerAgentOrchidRunningJobsPath(*controllerAgentAddress, operationId)),
        "running_jobs");

    batchReq->AddRequest(
        TYPathProxy::Get(GetControllerAgentOrchidRetainedFinishedJobsPath(*controllerAgentAddress, operationId)),
        "retained_finished_jobs");

    return batchReq->Invoke().Apply(
        BIND([operationId, options, this, this_ = MakeStrong(this)] (const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp) {
            auto operationStateRspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("controller_state");
            if (!operationStateRspOrError.IsOK()) {
                THROW_ERROR_EXCEPTION(EErrorCode::UncertainOperationControllerState,
                    "Error obtaining state of operation %v from controller agent",
                    operationId)
                    << operationStateRspOrError;
            }
            auto state = ConvertTo<EControllerState>(TYsonStringBuf(operationStateRspOrError.Value()->value()));
            if (state == EControllerState::Preparing) {
                THROW_ERROR_EXCEPTION(EErrorCode::UncertainOperationControllerState,
                    "Operation controller of operation %v is in %Qlv state",
                    operationId,
                    EControllerState::Preparing);
            }
            TListJobsFromControllerAgentResult result;
            ParseJobsFromControllerAgentResponse(
                operationId,
                batchRsp,
                "running_jobs",
                DefaultListJobsAttributes,
                options,
                &result.InProgressJobs,
                &result.TotalInProgressJobCount,
                Logger);
            ParseJobsFromControllerAgentResponse(
                operationId,
                batchRsp,
                "retained_finished_jobs",
                DefaultListJobsAttributes,
                options,
                &result.FinishedJobs,
                &result.TotalFinishedJobCount,
                Logger);
            return result;
        }));
}

using TJobComparator = std::function<bool(const TJob&, const TJob&)>;

static TJobComparator GetJobsComparator(
    EJobSortField sortField,
    EJobSortDirection sortOrder)
{
    auto makeLessBy = [sortOrder] (auto key) -> TJobComparator {
        switch (sortOrder) {
            case EJobSortDirection::Ascending:
                return [=] (const TJob& lhs, const TJob& rhs) {
                    auto lhsKey = key(lhs);
                    auto rhsKey = key(rhs);
                    return lhsKey < rhsKey || (lhsKey == rhsKey && lhs.Id < rhs.Id);
                };
            case EJobSortDirection::Descending:
                return [=] (const TJob& lhs, const TJob& rhs) {
                    auto lhsKey = key(lhs);
                    auto rhsKey = key(rhs);
                    return rhsKey < lhsKey || (rhsKey == lhsKey && rhs.Id < lhs.Id);
                };
        }
        YT_ABORT();
    };

    auto makeLessByField = [&] (auto TJob::* field) {
        return makeLessBy([field] (const TJob& job) {
            return job.*field;
        });
    };

    switch (sortField) {
        case EJobSortField::Type:
            return makeLessBy([] (const TJob& job) -> std::optional<TString> {
                if (auto type = job.Type) {
                    return FormatEnum(*type);
                } else {
                    return std::nullopt;
                }
            });
        case EJobSortField::State:
            return makeLessBy([] (const TJob& job) -> std::optional<TString> {
                if (auto state = job.GetState()) {
                    return FormatEnum(*state);
                } else {
                    return std::nullopt;
                }
            });
        case EJobSortField::StartTime:
            return makeLessByField(&TJob::StartTime);
        case EJobSortField::FinishTime:
            return makeLessByField(&TJob::FinishTime);
        case EJobSortField::Address:
            return makeLessByField(&TJob::Address);
        case EJobSortField::Progress:
            return makeLessByField(&TJob::Progress);
        case EJobSortField::None:
            return makeLessByField(&TJob::Id);
        case EJobSortField::Id:
            return makeLessBy([] (const TJob& job) {
                return ToString(job.Id);
            });
        case EJobSortField::Duration:
            return makeLessBy([now = TInstant::Now()] (const TJob& job) -> std::optional<TDuration> {
                if (job.StartTime) {
                    return (job.FinishTime ? *job.FinishTime : now) - *job.StartTime;
                } else {
                    return std::nullopt;
                }
            });
    }
    YT_ABORT();
}

static void MergeJobs(TJob&& controllerAgentJob, TJob* archiveJob)
{
    if (auto archiveState = archiveJob->ArchiveState; archiveState && IsJobFinished(*archiveState)) {
        // Archive job is most recent, it will not change anymore.
        return;
    }

    auto mergeNullableField = [&] (auto TJob::* field) {
        if (controllerAgentJob.*field) {
            archiveJob->*field = std::move(controllerAgentJob.*field);
        }
    };

    mergeNullableField(&TJob::Type);
    mergeNullableField(&TJob::ControllerState);
    mergeNullableField(&TJob::ArchiveState);
    mergeNullableField(&TJob::Progress);
    mergeNullableField(&TJob::StartTime);
    mergeNullableField(&TJob::FinishTime);
    mergeNullableField(&TJob::Address);
    mergeNullableField(&TJob::Progress);
    mergeNullableField(&TJob::Error);
    mergeNullableField(&TJob::BriefStatistics);
    mergeNullableField(&TJob::InputPaths);
    mergeNullableField(&TJob::CoreInfos);
    mergeNullableField(&TJob::JobCompetitionId);
    mergeNullableField(&TJob::ProbingJobCompetitionId);
    mergeNullableField(&TJob::HasCompetitors);
    mergeNullableField(&TJob::HasProbingCompetitors);
    mergeNullableField(&TJob::ExecAttributes);
    mergeNullableField(&TJob::TaskName);
    mergeNullableField(&TJob::PoolTree);
    mergeNullableField(&TJob::JobCookie);
    if (controllerAgentJob.StderrSize && archiveJob->StderrSize.value_or(0) < controllerAgentJob.StderrSize) {
        archiveJob->StderrSize = controllerAgentJob.StderrSize;
    }
}

static void UpdateStalenessInRunningOperationJobs(const TListJobsFromControllerAgentResult& controllerAgentJobs, std::vector<TJob>* archiveJobs)
{
    THashSet<TJobId> controllerJobIds;
    auto insertJobs = [&] (const std::vector<TJob>& controllerJobs) {
        for (const auto& job : controllerJobs) {
            controllerJobIds.insert(job.Id);
        }
    };
    insertJobs(controllerAgentJobs.InProgressJobs);
    insertJobs(controllerAgentJobs.FinishedJobs);

    for (auto& job : *archiveJobs) {
        auto jobState = job.GetState();
        job.IsStale = jobState && IsJobInProgress(*jobState) && !controllerJobIds.contains(job.Id);
    }
}

static void UpdateJobsAndAddMissing(TListJobsFromControllerAgentResult&& controllerAgentJobs, std::vector<TJob>* archiveJobs)
{
    THashMap<TJobId, TJob*> jobIdToArchiveJob;
    for (auto& job : *archiveJobs) {
        jobIdToArchiveJob.emplace(job.Id, &job);
    }
    std::vector<TJob> newJobs;
    auto mergeOrInsertControllerJobs = [&] (std::vector<TJob>* controllerJobs) {
        for (auto& job : *controllerJobs) {
            if (auto it = jobIdToArchiveJob.find(job.Id); it != jobIdToArchiveJob.end()) {
                MergeJobs(std::move(job), it->second);
            } else {
                newJobs.push_back(std::move(job));
            }
        }
    };
    mergeOrInsertControllerJobs(&controllerAgentJobs.InProgressJobs);
    mergeOrInsertControllerJobs(&controllerAgentJobs.FinishedJobs);

    archiveJobs->insert(
        archiveJobs->end(),
        std::make_move_iterator(newJobs.begin()),
        std::make_move_iterator(newJobs.end()));
}

static TError TryFillJobPools(
    const IClientPtr& client,
    TOperationId operationId,
    TMutableRange<TJob> jobs,
    const NLogging::TLogger& Logger)
{
    TGetOperationOptions getOperationOptions;
    getOperationOptions.Attributes = {TString("runtime_parameters")};

    auto operationOrError = WaitFor(client->GetOperation(operationId, getOperationOptions));
    if (!operationOrError.IsOK()) {
        YT_LOG_DEBUG(operationOrError, "Failed to fetch operation to extract pools (OperationId: %v)",
            operationId);
        return operationOrError;
    }

    auto path = "/scheduling_options_per_pool_tree";
    auto schedulingOptionsPerPoolTreeYson = TryGetAny(operationOrError.Value().RuntimeParameters.AsStringBuf(), path);
    if (!schedulingOptionsPerPoolTreeYson) {
        YT_LOG_DEBUG("Operation runtime_parameters miss scheduling_options_per_pool_tree (OperationId: %v)",
            operationId);
        return TError("Operation %v runtime_parameters miss scheduling_options_per_pool_tree",
            operationId);
    }

    auto schedulingOptionPerPoolTree = ConvertTo<THashMap<TString, INodePtr>>(
        TYsonStringBuf(*schedulingOptionsPerPoolTreeYson));

    for (auto& job : jobs) {
        if (!job.PoolTree) {
            return TError(Format("Pool tree is missing in job %v", job.Id));
        }
        auto optionsIt = schedulingOptionPerPoolTree.find(*job.PoolTree);
        if (optionsIt == schedulingOptionPerPoolTree.end()) {
            return TError(Format("Pool tree %Qv is not found in scheduling_options_per_pool_tree", *job.PoolTree));
        }
        const auto& optionsNode = optionsIt->second;
        auto poolNode = optionsNode->AsMap()->FindChild("pool");
        if (!poolNode) {
            return TError(Format("%Qv field is missing in scheduling_options_per_pool_tree for tree %Qv", "pool", *job.PoolTree));
        }
        job.Pool = ConvertTo<TString>(poolNode);
    }

    return TError();
}

TListJobsResult TClient::DoListJobs(
    const TOperationIdOrAlias& operationIdOrAlias,
    const TListJobsOptions& options)
{
    if (options.Offset < 0 || options.Limit < 0) {
        THROW_ERROR_EXCEPTION("offset and limit must be nonnegative numbers");
    }

    auto timeout = options.Timeout.value_or(Connection_->GetConfig()->DefaultListJobsTimeout);
    auto deadline = timeout.ToDeadLine();

    TOperationId operationId;
    Visit(operationIdOrAlias.Payload,
        [&] (const TOperationId& id) {
            operationId = id;
        },
        [&] (const TString& alias) {
            operationId = ResolveOperationAlias(alias, options, deadline);
        });

    // Issue the requests in parallel.
    TFuture<std::vector<TJob>> archiveResultFuture;
    TFuture<TListJobsStatistics> statisticsFuture;
    if (DoesOperationsArchiveExist()) {
        archiveResultFuture = DoListJobsFromArchiveAsync(
            operationId,
            deadline,
            options);
        statisticsFuture = ListJobsStatisticsFromArchiveAsync(operationId, deadline, options);
    }

    auto controllerAgentAddress = FindControllerAgentAddressFromCypress(
        operationId,
        MakeStrong(this));
    auto controllerAgentResultFuture = DoListJobsFromControllerAgentAsync(
        operationId,
        controllerAgentAddress,
        deadline,
        options);

    auto operationInfo = DoGetOperation(operationId, TGetOperationOptions{
        .Attributes = {{"state"}},
        .IncludeRuntime = true,
    });
    auto operationFinished = operationInfo.State && IsOperationFinished(*operationInfo.State);

    // Wait for results and extract them.
    TListJobsResult result;
    TListJobsFromControllerAgentResult controllerAgentResult;
    auto controllerAgentResultOrError = WaitFor(controllerAgentResultFuture);
    if (controllerAgentResultOrError.IsOK()) {
        controllerAgentResult = std::move(controllerAgentResultOrError.Value());
        result.ControllerAgentJobCount =
            controllerAgentResult.TotalFinishedJobCount + controllerAgentResult.TotalInProgressJobCount;
    } else {
        if (operationFinished && controllerAgentResultOrError.FindMatching(EErrorCode::UncertainOperationControllerState)) {
            // No such operation in the controller agent.
            result.ControllerAgentJobCount = 0;
        } else {
            result.Errors.push_back(std::move(controllerAgentResultOrError));
        }
    }

    std::vector<TJob> archiveResult;
    if (archiveResultFuture) {
        auto archiveResultOrError = WaitFor(archiveResultFuture);
        if (archiveResultOrError.IsOK()) {
            archiveResult = std::move(archiveResultOrError.Value());
        } else {
            result.Errors.push_back(TError(
                EErrorCode::JobArchiveUnavailable,
                "Job archive is unavailable")
                << archiveResultOrError);
        }
    }

    // Combine the results if necessary.
    if (!controllerAgentAddress) {
        result.Jobs = std::move(archiveResult);
    } else {
        if (!operationFinished) {
            UpdateStalenessInRunningOperationJobs(controllerAgentResult, &archiveResult);
        }

        UpdateJobsAndAddMissing(std::move(controllerAgentResult), &archiveResult);
        result.Jobs = std::move(archiveResult);
        auto jobComparator = GetJobsComparator(options.SortField, options.SortOrder);
        std::sort(result.Jobs.begin(), result.Jobs.end(), jobComparator);
    }

    if (operationFinished) {
        for (auto& job : result.Jobs) {
            auto jobState = job.GetState();
            job.IsStale = jobState && IsJobInProgress(*jobState);
        }
    }

    // Take the correct range [offset, offset + limit).
    result.Jobs.resize(std::min<int>(result.Jobs.size(), options.Offset + options.Limit));
    auto beginIt = std::min(result.Jobs.end(), result.Jobs.begin() + options.Offset);
    result.Jobs.erase(result.Jobs.begin(), beginIt);

    // Extract statistics if available.
    if (statisticsFuture) {
        auto statisticsOrError = WaitFor(statisticsFuture);
        if (!statisticsOrError.IsOK()) {
            result.Errors.push_back(TError(
                EErrorCode::JobArchiveUnavailable,
                "Failed to fetch statistics from job archive")
                << statisticsOrError);
        } else {
            result.Statistics = std::move(statisticsOrError).Value();
            result.ArchiveJobCount = 0;
            for (auto count : result.Statistics.TypeCounts) {
                *result.ArchiveJobCount += count;
            }
        }
    }

    // Compute pools.
    auto error = TryFillJobPools(this, operationId, TMutableRange(result.Jobs), Logger);
    if (!error.IsOK()) {
        YT_LOG_DEBUG(error, "Failed to fill job pools (OperationId: %v)",
            operationId);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

static std::vector<TString> MakeJobArchiveAttributes(const THashSet<TString>& attributes)
{
    std::vector<TString> result;
    // Plus 2 as operation_id and job_id are split into hi and lo.
    result.reserve(attributes.size() + 2);
    for (const auto& attribute : attributes) {
        if (!SupportedJobAttributes.contains(attribute)) {
            THROW_ERROR_EXCEPTION(
                NApi::EErrorCode::NoSuchAttribute,
                "Job attribute %Qv is not supported",
                attribute)
                << TErrorAttribute("attribute_name", attribute);
        }
        if (attribute == "operation_id" || attribute == "job_id") {
            result.push_back(attribute + "_hi");
            result.push_back(attribute + "_lo");
        } else if (attribute == "state") {
            result.emplace_back("state");
            result.emplace_back("transient_state");
        } else if (attribute == "statistics") {
            result.emplace_back("statistics");
            result.emplace_back("statistics_lz4");
        } else if (attribute == "progress" || attribute == "pool") {
            // Progress and pool are missing from job archive.
        } else {
            result.push_back(attribute);
        }
    }
    return result;
}

std::optional<TJob> TClient::DoGetJobFromArchive(
    TOperationId operationId,
    TJobId jobId,
    TInstant deadline,
    const THashSet<TString>& attributes)
{
    auto operationIdAsGuid = operationId.Underlying();
    auto jobIdAsGuid = jobId.Underlying();

    NRecords::TJobKey recordKey{
        .OperationIdHi = operationIdAsGuid.Parts64[0],
        .OperationIdLo = operationIdAsGuid.Parts64[1],
        .JobIdHi = jobIdAsGuid.Parts64[0],
        .JobIdLo = jobIdAsGuid.Parts64[1],
    };
    auto keys = FromRecordKeys(MakeRange(std::array{recordKey}));

    const auto& jobsTable = NRecords::TJobDescriptor::Get()->GetNameTable();

    std::vector<int> columnIndexes;
    auto fields = MakeJobArchiveAttributes(attributes);
    for (const auto& field : fields) {
        columnIndexes.push_back(jobsTable->GetIdOrThrow(field));
    }

    TLookupRowsOptions lookupOptions;
    lookupOptions.ColumnFilter = NTableClient::TColumnFilter(columnIndexes);
    lookupOptions.KeepMissingRows = true;
    lookupOptions.Timeout = deadline - Now();

    auto rowset = WaitFor(LookupRows(
        GetOperationsArchiveJobsPath(),
        jobsTable,
        keys,
        lookupOptions))
        .ValueOrThrow()
        .Rowset;

    auto rows = rowset->GetRows();
    YT_VERIFY(!rows.Empty());
    if (!rows[0]) {
        return {};
    }

    auto records = ToRecords<NRecords::TJobPartial>(rowset);
    auto jobs = ParseJobsFromArchiveResponse(operationId, records, /*needFullStatistics*/ true);
    if (jobs.empty()) {
        return {};
    }

    return std::move(jobs.front());
}

std::optional<TJob> TClient::DoGetJobFromControllerAgent(
    TOperationId operationId,
    TJobId jobId,
    TInstant deadline,
    const THashSet<TString>& attributes)
{
    auto controllerAgentAddress = FindControllerAgentAddressFromCypress(
        operationId,
        MakeStrong(this));
    if (!controllerAgentAddress) {
        return {};
    }

    TMasterReadOptions readOptions{
        .ReadFrom = EMasterChannelKind::Follower,
    };
    auto proxy = CreateObjectServiceReadProxy(readOptions);
    proxy.SetDefaultTimeout(deadline - Now());
    auto batchReq = proxy.ExecuteBatch();

    auto operationStatePath =
        GetControllerAgentOrchidOperationPath(*controllerAgentAddress, operationId) + "/state";
    batchReq->AddRequest(TYPathProxy::Get(operationStatePath), "get_controller_state");

    auto runningJobPath =
        GetControllerAgentOrchidRunningJobsPath(*controllerAgentAddress, operationId) + "/" + ToString(jobId);
    batchReq->AddRequest(TYPathProxy::Get(runningJobPath), "get_job");

    auto finishedJobPath =
        GetControllerAgentOrchidRetainedFinishedJobsPath(*controllerAgentAddress, operationId) + "/" + ToString(jobId);
    batchReq->AddRequest(TYPathProxy::Get(finishedJobPath), "get_job");

    auto batchRspOrError = WaitFor(batchReq->Invoke());

    if (!batchRspOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Cannot get jobs from controller agent")
            << batchRspOrError;
    }
    const auto& batchRsp = batchRspOrError.Value();

    for (const auto& rspOrError : batchRsp->GetResponses<TYPathProxy::TRspGet>("get_job")) {
        if (rspOrError.IsOK()) {
            std::vector<TJob> jobs;
            ParseJobsFromControllerAgentResponse(
                operationId,
                {{ToString(jobId), ConvertToNode(TYsonStringBuf(rspOrError.Value()->value()))}},
                /*filter*/ [] (const INodePtr&) {
                    return true;
                },
                attributes,
                &jobs);
            YT_VERIFY(jobs.size() == 1);
            return jobs[0];
        } else if (!rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            THROW_ERROR_EXCEPTION(EErrorCode::UncertainOperationControllerState,
                "Error obtaining job %v of operation %v from controller agent",
                jobId,
                operationId)
                << rspOrError;
        }
    }

    auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_controller_state");
    if (!rspOrError.IsOK()) {
        THROW_ERROR_EXCEPTION(EErrorCode::UncertainOperationControllerState,
            "Error obtaining state of operation %v from controller agent",
            operationId)
            << rspOrError;
    }
    auto state = ConvertTo<EControllerState>(TYsonStringBuf(rspOrError.Value()->value()));
    if (state == EControllerState::Preparing) {
        THROW_ERROR_EXCEPTION(EErrorCode::UncertainOperationControllerState,
            "Operation controller of operation %v is in %Qlv state",
            operationId,
            EControllerState::Preparing);
    }

    return {};
}

TYsonString TClient::DoGetJob(
    const TOperationIdOrAlias& operationIdOrAlias,
    TJobId jobId,
    const TGetJobOptions& options)
{
    auto timeout = options.Timeout.value_or(Connection_->GetConfig()->DefaultGetJobTimeout);
    auto deadline = timeout.ToDeadLine();

    TOperationId operationId;
    Visit(operationIdOrAlias.Payload,
        [&] (const TOperationId& id) {
            operationId = id;
        },
        [&] (const TString& alias) {
            operationId = ResolveOperationAlias(alias, options, deadline);
        });

    const auto& attributes = options.Attributes.value_or(DefaultGetJobAttributes);

    auto operationInfoFuture = GetOperation(operationId, TGetOperationOptions{
        .Attributes = {{"state"}},
        .IncludeRuntime = true,
    });

    std::optional<TJob> controllerAgentJob;
    TError controllerAgentError;
    try {
        controllerAgentJob = DoGetJobFromControllerAgent(operationId, jobId, deadline, attributes);
    } catch (const std::exception& ex) {
        controllerAgentError = TError(ex);
    }

    std::optional<TJob> archiveJob;
    if (DoesOperationsArchiveExist()) {
        archiveJob = DoGetJobFromArchive(operationId, jobId, deadline, attributes);
    }

    auto operationInfo = WaitFor(operationInfoFuture).ValueOrThrow();
    auto operationFinished = operationInfo.State && IsOperationFinished(*operationInfo.State);

    if (!controllerAgentError.IsOK() && !operationFinished) {
        // Operation is running but controller agent request failed, it is bad.
        THROW_ERROR controllerAgentError;
    }

    bool jobInControllerAgent = controllerAgentJob.has_value();

    TJob job;
    if (archiveJob && controllerAgentJob) {
        job = std::move(*archiveJob);
        MergeJobs(std::move(*controllerAgentJob), &job);
    } else if (archiveJob) {
        job = std::move(*archiveJob);
    } else if (controllerAgentJob) {
        job = std::move(*controllerAgentJob);
    } else {
        THROW_ERROR_EXCEPTION(
            EErrorCode::NoSuchJob,
            "Job %v or operation %v not found neither in archive nor in controller agent",
            jobId,
            operationId);
    }

    job.IsStale = [&] {
        auto jobState = job.GetState();
        return jobState && IsJobInProgress(*jobState) && (operationFinished || !jobInControllerAgent);
    }();

    if (attributes.contains("pool")) {
        auto error = TryFillJobPools(this, operationId, TMutableRange(&job, 1), Logger);
        if (!error.IsOK()) {
            YT_LOG_DEBUG(error, "Failed to fill job pools (OperationId: %v, JobId: %v)",
                operationId,
                jobId);
        }
    }

    return BuildYsonStringFluently()
        .Do([&] (TFluentAny fluent) {
            Serialize(job, fluent.GetConsumer(), "job_id");
        });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NNative
