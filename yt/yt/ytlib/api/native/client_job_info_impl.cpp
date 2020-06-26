#include "client_impl.h"

#include "connection.h"
#include "private.h"

#include <yt/client/api/file_reader.h>
#include <yt/client/api/operation_archive_schema.h>
#include <yt/client/api/rowset.h>

#include <yt/client/job_tracker_client/helpers.h>

#include <yt/client/table_client/helpers.h>
#include <yt/client/table_client/name_table.h>

#include <yt/ytlib/chunk_client/chunk_reader.h>
#include <yt/ytlib/chunk_client/chunk_reader_statistics.h>
#include <yt/ytlib/chunk_client/data_slice_descriptor.h>
#include <yt/ytlib/chunk_client/data_source.h>
#include <yt/ytlib/chunk_client/helpers.h>
#include <yt/ytlib/chunk_client/job_spec_extensions.h>

#include <yt/ytlib/job_proxy/job_spec_helper.h>
#include <yt/ytlib/job_proxy/helpers.h>
#include <yt/ytlib/job_proxy/user_job_read_controller.h>

#include <yt/ytlib/job_prober_client/job_node_descriptor_cache.h>

#include <yt/ytlib/node_tracker_client/channel.h>

#include <yt/ytlib/object_client/object_service_proxy.h>

#include <yt/core/concurrency/async_stream.h>
#include <yt/core/concurrency/async_stream_pipe.h>
#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/throughput_throttler.h>

namespace NYT::NApi::NNative {

using namespace NConcurrency;
using namespace NYPath;
using namespace NYTree;
using namespace NYson;
using namespace NCypressClient;
using namespace NObjectClient;
using namespace NFileClient;
using namespace NRpc;
using namespace NTableClient;
using namespace NTableClient::NProto;
using namespace NSecurityClient;
using namespace NQueryClient;
using namespace NChunkClient;
using namespace NChunkClient::NProto;
using namespace NScheduler;
using namespace NJobTrackerClient;
using namespace NNodeTrackerClient;

using NChunkClient::TChunkReaderStatistics;
using NChunkClient::TReadLimit;
using NChunkClient::TReadRange;
using NChunkClient::TDataSliceDescriptor;
using NNodeTrackerClient::TNodeDescriptor;

////////////////////////////////////////////////////////////////////////////////

static constexpr i64 ListJobsFromArchiveInProgressJobLimit = 100000;

// Attribute names allowed for 'get_job' and 'list_jobs' commands.
static const THashSet<TString> SupportedJobAttributes = {
    "operation_id",
    "job_id",
    "type",
    "state",
    "start_time",
    "finish_time",
    "address",
    "error",
    "statistics",
    "events",
    "has_spec",
    "job_competition_id",
    "has_competitors",
    "exec_attributes",
    "task_name",
};

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
                pipe->Abort(TError("Failed to get job input") << error);
            }
        }));
    }

    virtual TFuture<TSharedRef> Read() override
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

static TUnversionedOwningRow CreateJobKey(TJobId jobId, const TNameTablePtr& nameTable)
{
    TOwningRowBuilder keyBuilder(2);

    keyBuilder.AddValue(MakeUnversionedUint64Value(jobId.Parts64[0], nameTable->GetIdOrRegisterName("job_id_hi")));
    keyBuilder.AddValue(MakeUnversionedUint64Value(jobId.Parts64[1], nameTable->GetIdOrRegisterName("job_id_lo")));

    return keyBuilder.FinishRow();
}

static TYPath GetControllerAgentOrchidRunningJobsPath(TStringBuf controllerAgentAddress, TOperationId operationId)
{
    return GetControllerAgentOrchidOperationPath(controllerAgentAddress, operationId) + "/running_jobs";
}

static TYPath GetControllerAgentOrchidRetainedFinishedJobsPath(TStringBuf controllerAgentAddress, TOperationId operationId)
{
    return GetControllerAgentOrchidOperationPath(controllerAgentAddress, operationId) + "/retained_finished_jobs";
}

void TClient::DoDumpJobContext(
    TJobId jobId,
    const TYPath& path,
    const TDumpJobContextOptions& /*options*/)
{
    auto req = JobProberProxy_->DumpInputContext();
    ToProto(req->mutable_job_id(), jobId);
    ToProto(req->mutable_path(), path);

    WaitFor(req->Invoke())
        .ThrowOnError();
}

static void ValidateJobSpecVersion(
    TJobId jobId,
    const NYT::NJobTrackerClient::NProto::TJobSpec& jobSpec)
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
        error.FindMatching(NScheduler::EErrorCode::NoSuchJob) ||
        error.FindMatching(NScheduler::EErrorCode::NoSuchOperation);
}

// Get job node descriptor from scheduler and check that user has |requiredPermissions|
// for accessing the corresponding operation.
TErrorOr<TNodeDescriptor> TClient::TryGetJobNodeDescriptor(
    TJobId jobId,
    EPermissionSet requiredPermissions)
{
    const auto& cache = Connection_->GetJobNodeDescriptorCache();
    NJobProberClient::TJobNodeDescriptorKey key{
        .User = Options_.GetAuthenticatedUser(),
        .JobId = jobId,
        .Permissions = requiredPermissions
    };
    return WaitFor(cache->Get(key));
}

IChannelPtr TClient::TryCreateChannelToJobNode(
    TOperationId operationId,
    TJobId jobId,
    EPermissionSet requiredPermissions)
{
    auto jobNodeDescriptorOrError = TryGetJobNodeDescriptor(jobId, requiredPermissions);
    if (jobNodeDescriptorOrError.IsOK()) {
        return ChannelFactory_->CreateChannel(jobNodeDescriptorOrError.ValueOrThrow());
    }

    if (!IsNoSuchJobOrOperationError(jobNodeDescriptorOrError)) {
        THROW_ERROR_EXCEPTION("Failed to get job node descriptor from scheduler")
            << jobNodeDescriptorOrError;
    }

    try {
        TGetJobOptions options;
        options.Attributes = {TString("address")};
        // TODO(ignat): support structured return value in GetJob.
        auto jobYsonString = WaitFor(GetJob(operationId, jobId, options))
            .ValueOrThrow();
        auto address = ConvertToNode(jobYsonString)->AsMap()->GetChild("address")->GetValue<TString>();

        auto nodeChannel = ChannelFactory_->CreateChannel(address);
        auto jobSpecOrError = TryFetchJobSpecFromJobNode(jobId, nodeChannel);
        if (!jobSpecOrError.IsOK()) {
            return nullptr;
        }

        const auto& jobSpec = jobSpecOrError.Value();
        ValidateJobSpecVersion(jobId, jobSpec);
        ValidateOperationAccess(jobId, jobSpec, requiredPermissions);

        return nodeChannel;
    } catch (const TErrorException& ex) {
        YT_LOG_DEBUG(ex, "Failed create node channel to job using address from archive (JobId: %v)", jobId);
        return nullptr;
    }
}

TErrorOr<NJobTrackerClient::NProto::TJobSpec> TClient::TryFetchJobSpecFromJobNode(
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

TErrorOr<NJobTrackerClient::NProto::TJobSpec> TClient::TryFetchJobSpecFromJobNode(
    TJobId jobId,
    EPermissionSet requiredPermissions)
{
    auto jobNodeDescriptorOrError = TryGetJobNodeDescriptor(jobId, requiredPermissions);
    if (!jobNodeDescriptorOrError.IsOK()) {
        return TError(std::move(jobNodeDescriptorOrError));
    }

    const auto& nodeDescriptor = jobNodeDescriptorOrError.Value();
    auto nodeChannel = ChannelFactory_->CreateChannel(nodeDescriptor);
    return TryFetchJobSpecFromJobNode(jobId, nodeChannel);
}

NJobTrackerClient::NProto::TJobSpec TClient::FetchJobSpecFromArchive(
    TJobId jobId,
    EPermissionSet requiredPermissions)
{
    auto nameTable = New<TNameTable>();

    TLookupRowsOptions lookupOptions;
    lookupOptions.ColumnFilter = NTableClient::TColumnFilter({nameTable->RegisterName("spec")});
    lookupOptions.KeepMissingRows = true;

    auto owningKey = CreateJobKey(jobId, nameTable);

    std::vector<TUnversionedRow> keys;
    keys.push_back(owningKey);

    auto lookupResult = WaitFor(LookupRows(
        GetOperationsArchiveJobSpecsPath(),
        nameTable,
        MakeSharedRange(keys, owningKey),
        lookupOptions));

    if (!lookupResult.IsOK()) {
        THROW_ERROR_EXCEPTION(lookupResult)
            .Wrap("Lookup job spec in operation archive failed")
            << TErrorAttribute("job_id", jobId);
    }

    auto rows = lookupResult.Value()->GetRows();
    YT_VERIFY(!rows.Empty());

    if (!rows[0]) {
        THROW_ERROR_EXCEPTION("Missing job spec in job archive table")
            << TErrorAttribute("job_id", jobId);
    }

    auto value = rows[0][0];

    if (value.Type != EValueType::String) {
        THROW_ERROR_EXCEPTION("Found job spec has unexpected value type")
            << TErrorAttribute("job_id", jobId)
            << TErrorAttribute("value_type", value.Type);
    }

    NJobTrackerClient::NProto::TJobSpec jobSpec;
    bool ok = jobSpec.ParseFromArray(value.Data.String, value.Length);
    if (!ok) {
        THROW_ERROR_EXCEPTION("Cannot parse job spec")
            << TErrorAttribute("job_id", jobId);
    }

    ValidateJobSpecVersion(jobId, jobSpec);
    ValidateOperationAccess(jobId, jobSpec, requiredPermissions);

    return jobSpec;
}

NJobTrackerClient::NProto::TJobSpec TClient::FetchJobSpec(
    NScheduler::TJobId jobId,
    NYTree::EPermissionSet requiredPermissions)
{
    auto jobSpecFromProxyOrError = TryFetchJobSpecFromJobNode(jobId, requiredPermissions);
    if (!jobSpecFromProxyOrError.IsOK() && !IsNoSuchJobOrOperationError(jobSpecFromProxyOrError)) {
        THROW_ERROR jobSpecFromProxyOrError;
    }

    NJobTrackerClient::NProto::TJobSpec jobSpec;
    if (jobSpecFromProxyOrError.IsOK()) {
        jobSpec = std::move(jobSpecFromProxyOrError.Value());
    } else {
        jobSpec = FetchJobSpecFromArchive(jobId, EPermissionSet(EPermission::Read));
    }

    return jobSpec;
}

IAsyncZeroCopyInputStreamPtr TClient::DoGetJobInput(
    TJobId jobId,
    const TGetJobInputOptions& /*options*/)
{
    auto jobSpec = FetchJobSpec(jobId, EPermissionSet(EPermission::Read));

    auto* schedulerJobSpecExt = jobSpec.MutableExtension(NScheduler::NProto::TSchedulerJobSpecExt::scheduler_job_spec_ext);

    auto nodeDirectory = New<NNodeTrackerClient::TNodeDirectory>();
    auto locateChunks = BIND([=] {
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
    });

    auto locateChunksResult = WaitFor(locateChunks
        .AsyncVia(GetConnection()->GetInvoker())
        .Run());

    if (!locateChunksResult.IsOK()) {
        THROW_ERROR_EXCEPTION("Failed to locate chunks used in job input")
            << TErrorAttribute("job_id", jobId);
    }

    auto jobSpecHelper = NJobProxy::CreateJobSpecHelper(jobSpec);

    TClientBlockReadOptions blockReadOptions;
    blockReadOptions.ChunkReaderStatistics = New<TChunkReaderStatistics>();

    auto userJobReadController = CreateUserJobReadController(
        jobSpecHelper,
        MakeStrong(this),
        GetConnection()->GetInvoker(),
        TNodeDescriptor(),
        BIND([] { }) /* onNetworkRelease */,
        std::nullopt /* udfDirectory */,
        blockReadOptions,
        nullptr /* trafficMeter */,
        NConcurrency::GetUnlimitedThrottler() /* bandwidthThrottler */,
        NConcurrency::GetUnlimitedThrottler() /* rpsThrottler */);

    auto jobInputReader = New<TJobInputReader>(std::move(userJobReadController), GetConnection()->GetInvoker());
    jobInputReader->Open();
    return jobInputReader;
}

TYsonString TClient::DoGetJobInputPaths(
    TJobId jobId,
    const TGetJobInputPathsOptions& /*options*/)
{
    auto jobSpec = FetchJobSpec(jobId, EPermissionSet(EPermissionSet::Read));

    auto schedulerJobSpecExt = jobSpec.GetExtension(NScheduler::NProto::TSchedulerJobSpecExt::scheduler_job_spec_ext);

    auto optionalDataSourceDirectoryExt = FindProtoExtension<TDataSourceDirectoryExt>(schedulerJobSpecExt.extensions());
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
    for (const auto& inputSpec : schedulerJobSpecExt.input_table_specs()) {
        auto dataSliceDescriptors = NJobProxy::UnpackDataSliceDescriptors(inputSpec);
        for (const auto& slice : dataSliceDescriptors) {
            slicesByTable[slice.GetDataSourceIndex()].push_back(slice);
        }
    }

    for (const auto& inputSpec : schedulerJobSpecExt.foreign_input_table_specs()) {
        auto dataSliceDescriptors = NJobProxy::UnpackDataSliceDescriptors(inputSpec);
        for (const auto& slice : dataSliceDescriptors) {
            slicesByTable[slice.GetDataSourceIndex()].push_back(slice);
        }
    }

    auto compareAbsoluteReadLimits = [] (const TReadLimit& lhs, const TReadLimit& rhs) -> bool {
        YT_VERIFY(lhs.HasRowIndex() == rhs.HasRowIndex());

        if (lhs.HasRowIndex() && lhs.GetRowIndex() != rhs.GetRowIndex()) {
            return lhs.GetRowIndex() < rhs.GetRowIndex();
        }

        if (lhs.HasKey() && rhs.HasKey()) {
            return lhs.GetKey() < rhs.GetKey();
        } else if (lhs.HasKey()) {
            // rhs is less
            return false;
        } else if (rhs.HasKey()) {
            // lhs is less
            return true;
        } else {
            // These read limits are effectively equal.
            return false;
        }
    };

    auto canMergeSlices = [] (const TDataSliceDescriptor& lhs, const TDataSliceDescriptor& rhs, bool versioned) {
        if (lhs.GetRangeIndex() != rhs.GetRangeIndex()) {
            return false;
        }

        auto lhsUpperLimit = GetAbsoluteUpperReadLimit(lhs, versioned);
        auto rhsLowerLimit = GetAbsoluteLowerReadLimit(rhs, versioned);

        YT_VERIFY(lhsUpperLimit.HasRowIndex() == rhsLowerLimit.HasRowIndex());
        if (lhsUpperLimit.HasRowIndex() && lhsUpperLimit.GetRowIndex() < rhsLowerLimit.GetRowIndex()) {
            return false;
        }

        if (lhsUpperLimit.HasKey() != rhsLowerLimit.HasKey()) {
            return false;
        }

        if (lhsUpperLimit.HasKey() && lhsUpperLimit.GetKey() < rhsLowerLimit.GetKey()) {
            return false;
        }

        return true;
    };

    std::vector<std::vector<std::pair<TDataSliceDescriptor, TDataSliceDescriptor>>> rangesByTable(dataSourceDirectory->DataSources().size());
    for (int tableIndex = 0; tableIndex < dataSourceDirectory->DataSources().size(); ++tableIndex) {
        bool versioned = dataSourceDirectory->DataSources()[tableIndex].GetType() == EDataSourceType::VersionedTable;
        auto& tableSlices = slicesByTable[tableIndex];
        std::sort(
            tableSlices.begin(),
            tableSlices.end(),
            [&] (const TDataSliceDescriptor& lhs, const TDataSliceDescriptor& rhs) {
                if (lhs.GetRangeIndex() != rhs.GetRangeIndex()) {
                    return lhs.GetRangeIndex() < rhs.GetRangeIndex();
                }

                auto lhsLowerLimit = GetAbsoluteLowerReadLimit(lhs, versioned);
                auto rhsLowerLimit = GetAbsoluteLowerReadLimit(rhs, versioned);

                return compareAbsoluteReadLimits(lhsLowerLimit, rhsLowerLimit);
            });

        int firstSlice = 0;
        while (firstSlice < static_cast<int>(tableSlices.size())) {
            int lastSlice = firstSlice + 1;
            while (lastSlice < static_cast<int>(tableSlices.size())) {
                if (!canMergeSlices(tableSlices[lastSlice - 1], tableSlices[lastSlice], versioned)) {
                    break;
                }
                ++lastSlice;
            }
            rangesByTable[tableIndex].emplace_back(
                tableSlices[firstSlice],
                tableSlices[lastSlice - 1]);

            firstSlice = lastSlice;
        }
    }

    auto buildSliceLimit = [](const TReadLimit& limit, TFluentAny fluent) {
        fluent.BeginMap()
              .DoIf(limit.HasRowIndex(), [&] (TFluentMap fluent) {
                  fluent
                      .Item("row_index").Value(limit.GetRowIndex());
              })
              .DoIf(limit.HasKey(), [&] (TFluentMap fluent) {
                  fluent
                      .Item("key").Value(limit.GetKey());
              })
              .EndMap();
    };

    return BuildYsonStringFluently(EYsonFormat::Pretty)
        .DoListFor(rangesByTable, [&] (TFluentList fluent, const std::vector<std::pair<TDataSliceDescriptor, TDataSliceDescriptor>>& tableRanges) {
            fluent
                .DoIf(!tableRanges.empty(), [&] (TFluentList fluent) {
                    int dataSourceIndex = tableRanges[0].first.GetDataSourceIndex();
                    const auto& dataSource =  dataSourceDirectory->DataSources()[dataSourceIndex];
                    bool versioned = dataSource.GetType() == EDataSourceType::VersionedTable;
                    fluent
                        .Item()
                            .BeginAttributes()
                        .DoIf(dataSource.GetForeign(), [&] (TFluentMap fluent) {
                            fluent
                                .Item("foreign").Value(true);
                        })
                        .Item("ranges")
                        .DoListFor(tableRanges, [&] (TFluentList fluent, const std::pair<TDataSliceDescriptor, TDataSliceDescriptor>& range) {
                            fluent
                                .Item()
                                .BeginMap()
                                .Item("lower_limit").Do(BIND(
                                    buildSliceLimit,
                                    GetAbsoluteLowerReadLimit(range.first, versioned)))
                                .Item("upper_limit").Do(BIND(
                                    buildSliceLimit,
                                    GetAbsoluteUpperReadLimit(range.second, versioned)))
                                .EndMap();
                        })
                        .EndAttributes()
                        .Value(dataSource.GetPath());
                });
        });
}

TYsonString TClient::DoGetJobSpec(
    TJobId jobId,
    const TGetJobSpecOptions& options)
{
    auto jobSpec = FetchJobSpec(jobId, EPermissionSet(EPermissionSet::Read));
    auto* schedulerJobSpecExt = jobSpec.MutableExtension(NScheduler::NProto::TSchedulerJobSpecExt::scheduler_job_spec_ext);

    if (options.OmitNodeDirectory) {
        schedulerJobSpecExt->clear_input_node_directory();
    }

    if (options.OmitInputTableSpecs) {
        schedulerJobSpecExt->clear_input_table_specs();
        schedulerJobSpecExt->clear_foreign_input_table_specs();
    }

    if (options.OmitOutputTableSpecs) {
        schedulerJobSpecExt->clear_output_table_specs();
    }

    TString jobSpecYsonBytes;
    TStringOutput jobSpecYsonBytesOutput(jobSpecYsonBytes);
    TYsonWriter jobSpecYsonWriter(&jobSpecYsonBytesOutput);
    WriteProtobufMessage(&jobSpecYsonWriter, jobSpec);

    auto jobSpecNode = ConvertToNode(TYsonString(jobSpecYsonBytes));

    return ConvertToYsonString(jobSpecNode);
}

TSharedRef TClient::DoGetJobStderrFromNode(
    TOperationId operationId,
    TJobId jobId)
{
    auto nodeChannel = TryCreateChannelToJobNode(operationId, jobId, EPermissionSet(EPermission::Read));
    if (!nodeChannel) {
        return TSharedRef();
    }

    NJobProberClient::TJobProberServiceProxy jobProberServiceProxy(std::move(nodeChannel));
    jobProberServiceProxy.SetDefaultTimeout(Connection_->GetConfig()->JobProberRpcTimeout);

    auto req = jobProberServiceProxy.GetStderr();
    req->SetMultiplexingBand(EMultiplexingBand::Heavy);
    ToProto(req->mutable_job_id(), jobId);
    auto rspOrError = WaitFor(req->Invoke());
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

TSharedRef TClient::DoGetJobStderrFromCypress(
    TOperationId operationId,
    TJobId jobId)
{
    auto createFileReader = [&] (const NYPath::TYPath& path) {
        return WaitFor(static_cast<IClientBase*>(this)->CreateFileReader(path));
    };

    try {
        auto fileReader = createFileReader(NScheduler::GetStderrPath(operationId, jobId))
            .ValueOrThrow();

        std::vector<TSharedRef> blocks;
        while (true) {
            auto block = WaitFor(fileReader->Read())
                .ValueOrThrow();

            if (!block) {
                break;
            }

            blocks.push_back(std::move(block));
        }

        i64 size = GetByteSize(blocks);
        YT_VERIFY(size);
        auto stderrFile = TSharedMutableRef::Allocate(size);
        auto memoryOutput = TMemoryOutput(stderrFile.Begin(), size);

        for (const auto& block : blocks) {
            memoryOutput.Write(block.Begin(), block.Size());
        }

        return stderrFile;
    } catch (const TErrorException& exception) {
        auto matchedError = exception.Error().FindMatching(NYTree::EErrorCode::ResolveError);

        if (!matchedError) {
            THROW_ERROR_EXCEPTION("Failed to get job stderr from Cypress")
                << TErrorAttribute("operation_id", operationId)
                << TErrorAttribute("job_id", jobId)
                << exception.Error();
        }
    }

    return TSharedRef();
}

TSharedRef TClient::DoGetJobStderrFromArchive(
    TOperationId operationId,
    TJobId jobId)
{
    // Check permissions.
    FetchJobSpecFromArchive(jobId, EPermissionSet(EPermission::Read));

    try {
        TJobStderrTableDescriptor tableDescriptor;

        auto rowBuffer = New<TRowBuffer>();

        std::vector<TUnversionedRow> keys;
        auto key = rowBuffer->AllocateUnversioned(4);
        key[0] = MakeUnversionedUint64Value(operationId.Parts64[0], tableDescriptor.Index.OperationIdHi);
        key[1] = MakeUnversionedUint64Value(operationId.Parts64[1], tableDescriptor.Index.OperationIdLo);
        key[2] = MakeUnversionedUint64Value(jobId.Parts64[0], tableDescriptor.Index.JobIdHi);
        key[3] = MakeUnversionedUint64Value(jobId.Parts64[1], tableDescriptor.Index.JobIdLo);
        keys.push_back(key);

        TLookupRowsOptions lookupOptions;
        lookupOptions.ColumnFilter = NTableClient::TColumnFilter({tableDescriptor.Index.Stderr});
        lookupOptions.KeepMissingRows = true;

        auto rowset = WaitFor(LookupRows(
            GetOperationsArchiveJobStderrsPath(),
            tableDescriptor.NameTable,
            MakeSharedRange(keys, rowBuffer),
            lookupOptions))
            .ValueOrThrow();

        auto rows = rowset->GetRows();
        YT_VERIFY(!rows.Empty());

        if (rows[0]) {
            auto value = rows[0][0];

            YT_VERIFY(value.Type == EValueType::String);
            return TSharedRef::MakeCopy<char>(TRef(value.Data.String, value.Length));
        }
    } catch (const TErrorException& exception) {
        auto matchedError = exception.Error().FindMatching(NYTree::EErrorCode::ResolveError);

        if (!matchedError) {
            THROW_ERROR_EXCEPTION("Failed to get job stderr from archive")
                << TErrorAttribute("operation_id", operationId)
                << TErrorAttribute("job_id", jobId)
                << exception.Error();
        }
    }

    return TSharedRef();
}

TSharedRef TClient::DoGetJobStderr(
    TOperationId operationId,
    TJobId jobId,
    const TGetJobStderrOptions& /*options*/)
{
    auto stderrRef = DoGetJobStderrFromNode(operationId, jobId);
    if (stderrRef) {
        return stderrRef;
    }

    stderrRef = DoGetJobStderrFromCypress(operationId, jobId);
    if (stderrRef) {
        return stderrRef;
    }

    stderrRef = DoGetJobStderrFromArchive(operationId, jobId);
    if (stderrRef) {
        return stderrRef;
    }

    THROW_ERROR_EXCEPTION(NScheduler::EErrorCode::NoSuchJob, "Job stderr is not found")
        << TErrorAttribute("operation_id", operationId)
        << TErrorAttribute("job_id", jobId);
}

TSharedRef TClient::DoGetJobFailContextFromArchive(
    TOperationId operationId,
    TJobId jobId)
{
    // Check permissions.
    FetchJobSpecFromArchive(jobId, EPermissionSet(EPermission::Read));

    try {
        TJobFailContextTableDescriptor tableDescriptor;

        auto rowBuffer = New<TRowBuffer>();

        std::vector<TUnversionedRow> keys;
        auto key = rowBuffer->AllocateUnversioned(4);
        key[0] = MakeUnversionedUint64Value(operationId.Parts64[0], tableDescriptor.Index.OperationIdHi);
        key[1] = MakeUnversionedUint64Value(operationId.Parts64[1], tableDescriptor.Index.OperationIdLo);
        key[2] = MakeUnversionedUint64Value(jobId.Parts64[0], tableDescriptor.Index.JobIdHi);
        key[3] = MakeUnversionedUint64Value(jobId.Parts64[1], tableDescriptor.Index.JobIdLo);
        keys.push_back(key);

        TLookupRowsOptions lookupOptions;
        lookupOptions.ColumnFilter = NTableClient::TColumnFilter({tableDescriptor.Index.FailContext});
        lookupOptions.KeepMissingRows = true;

        auto rowset = WaitFor(LookupRows(
            GetOperationsArchiveJobFailContextsPath(),
            tableDescriptor.NameTable,
            MakeSharedRange(keys, rowBuffer),
            lookupOptions))
            .ValueOrThrow();

        auto rows = rowset->GetRows();
        YT_VERIFY(!rows.Empty());

        if (rows[0]) {
            auto value = rows[0][0];

            YT_VERIFY(value.Type == EValueType::String);
            return TSharedRef::MakeCopy<char>(TRef(value.Data.String, value.Length));
        }
    } catch (const TErrorException& exception) {
        auto matchedError = exception.Error().FindMatching(NYTree::EErrorCode::ResolveError);

        if (!matchedError) {
            THROW_ERROR_EXCEPTION("Failed to get job fail_context from archive")
                << TErrorAttribute("operation_id", operationId)
                << TErrorAttribute("job_id", jobId)
                << exception.Error();
        }
    }

    return TSharedRef();
}

TSharedRef TClient::DoGetJobFailContextFromCypress(
    TOperationId operationId,
    TJobId jobId)
{
    auto createFileReader = [&] (const NYPath::TYPath& path) {
        return WaitFor(static_cast<IClientBase*>(this)->CreateFileReader(path));
    };

    try {
        auto fileReader = createFileReader(NScheduler::GetFailContextPath(operationId, jobId))
            .ValueOrThrow();

        std::vector<TSharedRef> blocks;
        while (true) {
            auto block = WaitFor(fileReader->Read())
                .ValueOrThrow();

            if (!block) {
                break;
            }

            blocks.push_back(std::move(block));
        }

        i64 size = GetByteSize(blocks);
        YT_VERIFY(size);
        auto failContextFile = TSharedMutableRef::Allocate(size);
        auto memoryOutput = TMemoryOutput(failContextFile.Begin(), size);

        for (const auto& block : blocks) {
            memoryOutput.Write(block.Begin(), block.Size());
        }

        return failContextFile;
    } catch (const TErrorException& exception) {
        auto matchedError = exception.Error().FindMatching(NYTree::EErrorCode::ResolveError);

        if (!matchedError) {
            THROW_ERROR_EXCEPTION("Failed to get job fail context from Cypress")
                << TErrorAttribute("operation_id", operationId)
                << TErrorAttribute("job_id", jobId)
                << exception.Error();
        }
    }

    return TSharedRef();
}

TSharedRef TClient::DoGetJobFailContext(
    TOperationId operationId,
    TJobId jobId,
    const TGetJobFailContextOptions& /*options*/)
{
    if (auto failContextRef = DoGetJobFailContextFromCypress(operationId, jobId)) {
        return failContextRef;
    }
    if (auto failContextRef = DoGetJobFailContextFromArchive(operationId, jobId)) {
        return failContextRef;
    }
    THROW_ERROR_EXCEPTION(
        NScheduler::EErrorCode::NoSuchJob,
        "Job fail context is not found")
        << TErrorAttribute("operation_id", operationId)
        << TErrorAttribute("job_id", jobId);
}

static void ValidateNonNull(
    const TUnversionedValue& value,
    TStringBuf name,
    TOperationId operationId,
    TJobId jobId = {})
{
    if (Y_UNLIKELY(value.Type == EValueType::Null)) {
        auto error = TError("Unexpected null value in column %Qv in job archive", name)
            << TErrorAttribute("operation_id", operationId);
        if (jobId) {
            error = error << TErrorAttribute("job_id", jobId);
        }
        THROW_ERROR error;
    }
}

static TQueryBuilder GetListJobsQueryBuilder(
    TOperationId operationId,
    const std::optional<std::vector<EJobState>>& states,
    const TListJobsOptions& options)
{
    NQueryClient::TQueryBuilder builder;
    builder.SetSource(GetOperationsArchiveJobsPath());

    builder.AddWhereConjunct(Format(
        "(operation_id_hi, operation_id_lo) = (%vu, %vu)",
        operationId.Parts64[0],
        operationId.Parts64[1]));

    builder.AddWhereConjunct(Format(
        R""(job_state IN ("aborted", "failed", "completed", "lost") )""
        "OR (NOT is_null(update_time) AND update_time >= %v)",
        (TInstant::Now() - options.RunningJobsLookbehindPeriod).MicroSeconds()));

    if (options.Address) {
        builder.AddWhereConjunct(Format("address = %Qv", *options.Address));
    }

    if (states) {
        std::vector<TString> stateStrings;
        for (auto state : *states) {
            stateStrings.push_back(Format("%Qv", FormatEnum(state)));
        }
        builder.AddWhereConjunct(Format("job_state IN (%v)", JoinToString(stateStrings, AsStringBuf(", "))));
    }

    return builder;
}

// Asynchronously perform "select_rows" from job archive and parse result.
//
// |Offset| and |Limit| fields in |options| are ignored, |limit| is used instead.
// Jobs are additionally filtered by |states|.
TFuture<std::vector<TJob>> TClient::DoListJobsFromArchiveAsyncImpl(
    TOperationId operationId,
    const std::vector<EJobState>& states,
    i64 limit,
    const TSelectRowsOptions& selectRowsOptions,
    const TListJobsOptions& options)
{
    auto builder = GetListJobsQueryBuilder(operationId, states, options);

    builder.SetLimit(limit);

    auto jobIdHiIndex = builder.AddSelectExpression("job_id_hi");
    auto jobIdLoIndex = builder.AddSelectExpression("job_id_lo");
    auto typeIndex = builder.AddSelectExpression("type", "job_type");
    auto stateIndex = builder.AddSelectExpression("if(is_null(state), transient_state, state)", "job_state");
    auto startTimeIndex = builder.AddSelectExpression("start_time");
    auto finishTimeIndex = builder.AddSelectExpression("finish_time");
    auto addressIndex = builder.AddSelectExpression("address");
    auto errorIndex = builder.AddSelectExpression("error");
    auto statisticsIndex = builder.AddSelectExpression("statistics");
    auto stderrSizeIndex = builder.AddSelectExpression("stderr_size");
    auto hasSpecIndex = builder.AddSelectExpression("has_spec");
    auto failContextSizeIndex = builder.AddSelectExpression("fail_context_size");
    auto jobCompetitionIdIndex = builder.AddSelectExpression("job_competition_id");
    auto hasCompetitorsIndex = builder.AddSelectExpression("has_competitors");
    auto execAttributesIndex = builder.AddSelectExpression("exec_attributes");
    auto taskNameIndex = builder.AddSelectExpression("task_name");

    int coreInfosIndex = -1;
    {
        constexpr int requiredVersion = 31;
        if (DoGetOperationsArchiveVersion() >= requiredVersion) {
            coreInfosIndex = builder.AddSelectExpression("core_infos");
        }
    }

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

    if (options.TaskName) {
        builder.AddWhereConjunct(Format("task_name = %Qv", *options.TaskName));
    }

    if (options.SortField != EJobSortField::None) {
        EOrderByDirection orderByDirection;
        switch (options.SortOrder) {
            case EJobSortDirection::Ascending:
                orderByDirection = EOrderByDirection::Ascending;
                break;
            case EJobSortDirection::Descending:
                orderByDirection = EOrderByDirection::Descending;
                break;
            default:
                YT_ABORT();
        }
        switch (options.SortField) {
            case EJobSortField::Type:
                builder.AddOrderByExpression("job_type", orderByDirection);
                break;
            case EJobSortField::State:
                builder.AddOrderByExpression("job_state", orderByDirection);
                break;
            case EJobSortField::StartTime:
                builder.AddOrderByExpression("start_time", orderByDirection);
                break;
            case EJobSortField::FinishTime:
                builder.AddOrderByExpression("finish_time", orderByDirection);
                break;
            case EJobSortField::Address:
                builder.AddOrderByExpression("address", orderByDirection);
                break;
            case EJobSortField::Duration:
                builder.AddOrderByExpression(
                    Format(
                        "if(is_null(finish_time), %v, finish_time) - start_time",
                        TInstant::Now().MicroSeconds()),
                    orderByDirection);
                break;
            case EJobSortField::Id:
                builder.AddOrderByExpression("format_guid(job_id_hi, job_id_lo)", orderByDirection);
                break;
            case EJobSortField::Progress:
                // XXX: progress is not present in archive table.
                break;
            default:
                YT_ABORT();
        }
    }

    return SelectRows(builder.Build(), selectRowsOptions).Apply(BIND([=] (const TSelectRowsResult& result) {
        std::vector<TJob> jobs;
        auto rows = result.Rowset->GetRows();
        jobs.reserve(rows.Size());
        for (auto row : rows) {
            ValidateNonNull(row[jobIdHiIndex], "job_id_hi", operationId, TJobId());
            ValidateNonNull(row[jobIdLoIndex], "job_id_lo", operationId, TJobId());

            TJobId jobId(FromUnversionedValue<ui64>(row[jobIdHiIndex]), FromUnversionedValue<ui64>(row[jobIdLoIndex]));

            jobs.emplace_back();
            auto& job = jobs.back();

            job.Id = jobId;

            ValidateNonNull(row[typeIndex], "type", operationId, jobId);
            job.Type = ParseEnum<EJobType>(FromUnversionedValue<TStringBuf>(row[typeIndex]));

            ValidateNonNull(row[stateIndex], "state", operationId, jobId);
            job.ArchiveState = ParseEnum<EJobState>(FromUnversionedValue<TStringBuf>(row[stateIndex]));

            if (row[startTimeIndex].Type != EValueType::Null) {
                job.StartTime = TInstant::MicroSeconds(FromUnversionedValue<i64>(row[startTimeIndex]));
            } else {
                // This field previously was non-optional.
                job.StartTime.emplace();
            }

            if (row[finishTimeIndex].Type != EValueType::Null) {
                job.FinishTime = TInstant::MicroSeconds(FromUnversionedValue<i64>(row[finishTimeIndex]));
            }

            if (row[addressIndex].Type != EValueType::Null) {
                job.Address = FromUnversionedValue<TString>(row[addressIndex]);
            } else {
                // This field previously was non-optional.
                job.Address.emplace();
            }

            if (row[stderrSizeIndex].Type != EValueType::Null) {
                job.StderrSize = FromUnversionedValue<ui64>(row[stderrSizeIndex]);
            }

            if (row[failContextSizeIndex].Type != EValueType::Null) {
                job.FailContextSize = FromUnversionedValue<ui64>(row[failContextSizeIndex]);
            }

            if (row[jobCompetitionIdIndex].Type != EValueType::Null) {
                job.JobCompetitionId = FromUnversionedValue<TGuid>(row[jobCompetitionIdIndex]);
            }

            if (row[hasCompetitorsIndex].Type != EValueType::Null) {
                job.HasCompetitors = FromUnversionedValue<bool>(row[hasCompetitorsIndex]);
            } else {
                job.HasCompetitors = false;
            }

            if (row[hasSpecIndex].Type != EValueType::Null) {
                job.HasSpec = FromUnversionedValue<bool>(row[hasSpecIndex]);
            } else {
                // This field previously was non-optional.
                job.HasSpec = false;
            }

            if (row[errorIndex].Type != EValueType::Null) {
                job.Error = FromUnversionedValue<TYsonString>(row[errorIndex]);
            }

            if (coreInfosIndex != -1 && row[coreInfosIndex].Type != EValueType::Null) {
                job.CoreInfos = FromUnversionedValue<TYsonString>(row[coreInfosIndex]);
            }

            if (row[statisticsIndex].Type != EValueType::Null) {
                auto briefStatisticsYson = FromUnversionedValue<TYsonString>(row[statisticsIndex]);
                auto briefStatistics = ConvertToNode(briefStatisticsYson);

                // See BuildBriefStatistics.
                auto rowCount = FindNodeByYPath(briefStatistics, "/data/input/row_count/sum");
                auto uncompressedDataSize = FindNodeByYPath(briefStatistics, "/data/input/uncompressed_data_size/sum");
                auto compressedDataSize = FindNodeByYPath(briefStatistics, "/data/input/compressed_data_size/sum");
                auto dataWeight = FindNodeByYPath(briefStatistics, "/data/input/data_weight/sum");
                auto inputPipeIdleTime = FindNodeByYPath(briefStatistics, "/user_job/pipes/input/idle_time/sum");
                auto jobProxyCpuUsage = FindNodeByYPath(briefStatistics, "/job_proxy/cpu/user/sum");

                job.BriefStatistics = BuildYsonStringFluently()
                    .BeginMap()
                        .DoIf(static_cast<bool>(rowCount), [&] (TFluentMap fluent) {
                            fluent.Item("processed_input_row_count").Value(rowCount->AsInt64()->GetValue());
                        })
                        .DoIf(static_cast<bool>(uncompressedDataSize), [&] (TFluentMap fluent) {
                            fluent.Item("processed_input_uncompressed_data_size").Value(uncompressedDataSize->AsInt64()->GetValue());
                        })
                        .DoIf(static_cast<bool>(compressedDataSize), [&] (TFluentMap fluent) {
                            fluent.Item("processed_input_compressed_data_size").Value(compressedDataSize->AsInt64()->GetValue());
                        })
                        .DoIf(static_cast<bool>(dataWeight), [&] (TFluentMap fluent) {
                            fluent.Item("processed_input_data_weight").Value(dataWeight->AsInt64()->GetValue());
                        })
                        .DoIf(static_cast<bool>(inputPipeIdleTime), [&] (TFluentMap fluent) {
                            fluent.Item("input_pipe_idle_time").Value(inputPipeIdleTime->AsInt64()->GetValue());
                        })
                        .DoIf(static_cast<bool>(jobProxyCpuUsage), [&] (TFluentMap fluent) {
                            fluent.Item("job_proxy_cpu_usage").Value(jobProxyCpuUsage->AsInt64()->GetValue());
                        })
                    .EndMap();
            }

            if (row[execAttributesIndex].Type != EValueType::Null) {
                job.ExecAttributes = FromUnversionedValue<TYsonString>(row[execAttributesIndex]);
            }

            if (row[taskNameIndex].Type != EValueType::Null) {
                job.TaskName = FromUnversionedValue<TString>(row[taskNameIndex]);
            }

            // We intentionally mark stderr as missing if job has no spec since
            // it is impossible to check permissions without spec.
            if (job.GetState() && NJobTrackerClient::IsJobFinished(*job.GetState()) && !job.HasSpec) {
                job.StderrSize = std::nullopt;
            }
        }
        return jobs;
    }));
}

// Get statistics for jobs.
TFuture<TListJobsStatistics> TClient::ListJobsStatisticsFromArchiveAsync(
    TOperationId operationId,
    TInstant deadline,
    const TListJobsOptions& options)
{
    auto builder = GetListJobsQueryBuilder(operationId, /* states */ {}, options);

    auto jobTypeIndex = builder.AddSelectExpression("type", "job_type");
    auto jobStateIndex = builder.AddSelectExpression("if(is_null(state), transient_state, state)", "job_state");
    auto countIndex = builder.AddSelectExpression("sum(1)", "count");

    builder.AddGroupByExpression("job_type");
    builder.AddGroupByExpression("job_state");

    TSelectRowsOptions selectRowsOptions;
    selectRowsOptions.Timestamp = AsyncLastCommittedTimestamp;
    selectRowsOptions.Timeout = deadline - Now();
    selectRowsOptions.InputRowLimit = std::numeric_limits<i64>::max();
    selectRowsOptions.MemoryLimitPerNode = 100_MB;

    return SelectRows(builder.Build(), selectRowsOptions).Apply(BIND([=] (const TSelectRowsResult& result) {
        TListJobsStatistics statistics;
        for (auto row : result.Rowset->GetRows()) {
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

// Retrieves:
// 1) Filtered finished jobs (with limit).
// 2) All (non-filtered and without limit) in-progress jobs (if |includeInProgressJobs == true|).
TFuture<TClient::TListJobsFromArchiveResult> TClient::DoListJobsFromArchiveAsync(
    TOperationId operationId,
    TInstant deadline,
    bool includeInProgressJobs,
    const TListJobsOptions& options)
{
    std::vector<EJobState> inProgressJobStates;
    std::vector<EJobState> finishedJobStates;
    for (auto state : TEnumTraits<EJobState>::GetDomainValues()) {
        if (IsJobInProgress(state)) {
            inProgressJobStates.push_back(state);
        } else {
            finishedJobStates.push_back(state);
        }
    }

    TSelectRowsOptions selectRowsOptions;
    selectRowsOptions.Timestamp = AsyncLastCommittedTimestamp;
    selectRowsOptions.Timeout = deadline - Now();
    selectRowsOptions.InputRowLimit = std::numeric_limits<i64>::max();
    selectRowsOptions.MemoryLimitPerNode = 100_MB;

    TFuture<std::vector<TJob>> jobsInProgressFuture;
    if (includeInProgressJobs) {
        jobsInProgressFuture = DoListJobsFromArchiveAsyncImpl(
            operationId,
            inProgressJobStates,
            ListJobsFromArchiveInProgressJobLimit,
            selectRowsOptions,
            options);
    } else {
        jobsInProgressFuture = MakeFuture(std::vector<TJob>{});
    }

    auto finishedJobsFuture = DoListJobsFromArchiveAsyncImpl(
        operationId,
        finishedJobStates,
        options.Limit + options.Offset,
        selectRowsOptions,
        options);

    return AllSet(std::vector<TFuture<void>>{
            jobsInProgressFuture.As<void>(),
            finishedJobsFuture.As<void>()})
        .Apply(BIND([jobsInProgressFuture, finishedJobsFuture, this, this_=MakeStrong(this)] (const std::vector<TError>&) {
            const auto& jobsInProgressOrError = jobsInProgressFuture.Get();
            const auto& finishedJobsOrError = finishedJobsFuture.Get();

            if (!jobsInProgressOrError.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to get jobs in progress from the operation archive")
                    << jobsInProgressOrError;
            }
            if (!finishedJobsOrError.IsOK()) {
                THROW_ERROR_EXCEPTION("Failed to get finished jobs from the operation archive")
                    << finishedJobsOrError;
            }

            int filteredOutAsFinished = 0;
            auto difference = [&filteredOutAsFinished] (std::vector<TJob> origin, const std::vector<TJob>& blacklist) {
                THashSet<TJobId> idBlacklist;
                for (const auto& job : blacklist) {
                    YT_VERIFY(job.Id);
                    idBlacklist.emplace(job.Id);
                }
                origin.erase(
                    std::remove_if(
                        origin.begin(),
                        origin.end(),
                        [&idBlacklist, &filteredOutAsFinished] (const TJob& job) {
                            YT_VERIFY(job.Id);

                            if (idBlacklist.contains(job.Id)) {
                                ++filteredOutAsFinished;
                                return true;
                            }
                            return false;
                        }),
                    origin.end());
                return origin;
            };

            TListJobsFromArchiveResult result;
            result.FinishedJobs = finishedJobsOrError.Value();
            // If a job is present in both lists, we give priority
            // to |FinishedJobs| and remove it from |InProgressJobs|.
            result.InProgressJobs = difference(jobsInProgressOrError.Value(), result.FinishedJobs);

            YT_LOG_DEBUG("Received finished jobs from archive (Count: %v)", result.FinishedJobs.size());
            YT_LOG_DEBUG("Received in-progress jobs from archive (Count: %v, FilteredOutAsFinished: %v)", result.InProgressJobs.size(), filteredOutAsFinished);

            return result;
        }));
}

TFuture<std::pair<std::vector<TJob>, int>> TClient::DoListJobsFromCypressAsync(
    TOperationId operationId,
    TInstant deadline,
    const TListJobsOptions& options)
{
    TObjectServiceProxy proxy(GetOperationArchiveChannel(options.ReadFrom));

    auto attributeFilter = std::vector<TString>{
        "job_type",
        "state",
        "start_time",
        "finish_time",
        "address",
        "error",
        "brief_statistics",
        "input_paths",
        "core_infos",
        "uncompressed_data_size",
        "job_competition_id",
        "has_competitors",
    };

    auto batchReq = proxy.ExecuteBatch();
    batchReq->SetTimeout(deadline - Now());

    {
        auto getReq = TYPathProxy::Get(GetJobsPath(operationId));
        ToProto(getReq->mutable_attributes()->mutable_keys(), attributeFilter);
        batchReq->AddRequest(getReq, "get_jobs");
    }

    return batchReq->Invoke().Apply(BIND([&options, this, this_=MakeStrong(this)] (
        const TErrorOr<TObjectServiceProxy::TRspExecuteBatchPtr>& batchRspOrError)
    {
        const auto& batchRsp = batchRspOrError.ValueOrThrow();
        auto getReqRsp = batchRsp->GetResponse<TYPathProxy::TRspGet>("get_jobs");

        const auto& rsp = getReqRsp.ValueOrThrow();

        std::pair<std::vector<TJob>, int> result;
        auto& jobs = result.first;

        auto items = ConvertToNode(NYson::TYsonString(rsp->value()))->AsMap();
        result.second = items->GetChildren().size();

        YT_LOG_DEBUG("Received jobs from cypress (Count: %v)", items->GetChildren().size());

        for (const auto& item : items->GetChildren()) {
            const auto& attributes = item.second->Attributes();
            auto children = item.second->AsMap();

            auto id = TGuid::FromString(item.first);

            auto type = ParseEnum<NJobTrackerClient::EJobType>(attributes.Get<TString>("job_type"));
            auto state = ParseEnum<NJobTrackerClient::EJobState>(attributes.Get<TString>("state"));
            auto address = attributes.Get<TString>("address");

            i64 stderrSize = -1;
            if (auto stderrNode = children->FindChild("stderr")) {
                stderrSize = stderrNode->Attributes().Get<i64>("uncompressed_data_size");
            }

            if (options.WithStderr) {
                if (*options.WithStderr && stderrSize <= 0) {
                    continue;
                }
                if (!(*options.WithStderr) && stderrSize > 0) {
                    continue;
                }
            }

            i64 failContextSize = -1;
            if (auto failContextNode = children->FindChild("fail_context")) {
                failContextSize = failContextNode->Attributes().Get<i64>("uncompressed_data_size");
            }

            if (options.WithFailContext) {
                if (*options.WithFailContext && failContextSize <= 0) {
                    continue;
                }
                if (!(*options.WithFailContext) && failContextSize > 0) {
                    continue;
                }
            }

            if (options.JobCompetitionId) {
                if (options.JobCompetitionId != attributes.Find<TJobId>("job_competition_id")) {
                    continue;
                }
            }

            if (options.WithCompetitors) {
                if (options.WithCompetitors != attributes.Find<bool>("has_competitors")) {
                    continue;
                }
            }

            if (options.TaskName) {
                if (options.TaskName != attributes.Find<TString>("task_name")) {
                    continue;
                }
            }

            jobs.emplace_back();
            auto& job = jobs.back();

            job.Id = id;
            job.Type = type;

            // XXX(levysotsky): It's a hack, will be removed simultaneously with the whole function :).
            job.ControllerAgentState = state;

            job.StartTime = ConvertTo<TInstant>(attributes.Get<TString>("start_time"));
            job.FinishTime = ConvertTo<TInstant>(attributes.Get<TString>("finish_time"));
            job.Address = address;
            if (stderrSize >= 0) {
                job.StderrSize = stderrSize;
            }
            if (failContextSize >= 0) {
                job.FailContextSize = failContextSize;
            }
            job.HasSpec = true;
            job.Error = attributes.FindYson("error");
            job.BriefStatistics = attributes.FindYson("brief_statistics");
            job.InputPaths = attributes.FindYson("input_paths");
            job.CoreInfos = attributes.FindYson("core_infos");
            job.JobCompetitionId = attributes.Find<TJobId>("job_competition_id").value_or(TJobId());
            job.HasCompetitors = attributes.Find<bool>("has_competitors").value_or(false);
        }

        return result;
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
    auto needHasCompetitors = attributes.contains("has_competitors");
    auto needError = attributes.contains("error");
    auto needTaskName = attributes.contains("task_name");

    for (const auto& [jobIdString, jobNode] : jobNodes) {
        if (!filter(jobNode)) {
            continue;
        }

        const auto& jobMapNode = jobNode->AsMap();
        auto& job = jobs->emplace_back();
        if (needJobId) {
            job.Id = TJobId::FromString(jobIdString);
        }
        if (needOperationId) {
            job.OperationId = operationId;
        }
        if (needType) {
            job.Type = ParseEnum<EJobType>(jobMapNode->GetChild("job_type")->GetValue<TString>());
        }
        if (needState) {
            job.ControllerAgentState = ParseEnum<EJobState>(jobMapNode->GetChild("state")->GetValue<TString>());
        }
        if (needStartTime) {
            job.StartTime = ConvertTo<TInstant>(jobMapNode->GetChild("start_time")->GetValue<TString>());
        }
        if (needFinishTime) {
            if (auto child = jobMapNode->FindChild("finish_time")) {
                job.FinishTime = ConvertTo<TInstant>(child->GetValue<TString>());
            }
        }
        if (needAddress) {
            job.Address = jobMapNode->GetChild("address")->GetValue<TString>();
        }
        if (needHasSpec) {
            job.HasSpec = true;
        }
        if (needProgress) {
            job.Progress = jobMapNode->GetChild("progress")->GetValue<double>();
        }

        auto stderrSize = jobMapNode->GetChild("stderr_size")->GetValue<i64>();
        if (stderrSize > 0 && needStderrSize) {
            job.StderrSize = stderrSize;
        }

        if (needBriefStatistics) {
            job.BriefStatistics = ConvertToYsonString(jobMapNode->GetChild("brief_statistics"));
        }
        if (needJobCompetitionId) {
            //COMPAT(renadeen): can remove this check when 19.8 will be on all clusters
            if (auto child = jobMapNode->FindChild("job_competition_id")) {
                job.JobCompetitionId = ConvertTo<TJobId>(child);
            }
        }
        if (needHasCompetitors) {
            //COMPAT(renadeen): can remove this check when 19.8 will be on all clusters
            if (auto child = jobMapNode->FindChild("has_competitors")) {
                job.HasCompetitors = ConvertTo<bool>(child);
            }
        }
        if (needError) {
            if (auto child = jobMapNode->FindChild("error")) {
                job.Error = ConvertToYsonString(ConvertTo<TError>(child));
            }
        }
        if (needTaskName) {
            if (auto child = jobMapNode->FindChild("task_name")) {
                job.TaskName = ConvertTo<TString>(child);
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
    NLogging::TLogger Logger)
{
    auto rspOrError = batchRsp->GetResponse<TYPathProxy::TRspGet>(key);
    if (rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
        return;
    }
    if (!rspOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Cannot get %Qv from controller agent", key)
            << rspOrError;
    }

    auto rsp = rspOrError.Value();
    auto items = ConvertToNode(NYson::TYsonString(rsp->value()))->AsMap();
    *totalCount += items->GetChildren().size();

    YT_LOG_DEBUG("Received %v jobs from controller agent (Count: %v)", key, items->GetChildren().size());

    auto filter = [&] (const INodePtr& jobNode) -> bool {
        auto stderrSize = jobNode->AsMap()->GetChild("stderr_size")->GetValue<i64>();
        auto failContextSizeNode = jobNode->AsMap()->FindChild("fail_context_size");
        auto failContextSize = failContextSizeNode
            ? failContextSizeNode->GetValue<i64>()
            : 0;
        auto jobCompetitionIdNode = jobNode->AsMap()->FindChild("job_competition_id");
        auto jobCompetitionId = jobCompetitionIdNode  //COMPAT(renadeen): can remove this check when 19.8 will be on all clusters
            ? ConvertTo<TJobId>(jobCompetitionIdNode)
            : TJobId();
        auto hasCompetitorsNode = jobNode->AsMap()->FindChild("has_competitors");
        auto hasCompetitors = hasCompetitorsNode  //COMPAT(renadeen): can remove this check when 19.8 will be on all clusters
            ? ConvertTo<bool>(hasCompetitorsNode)
            : false;
        auto taskNameNode = jobNode->AsMap()->FindChild("task_name");
        auto taskName = taskNameNode
            ? ConvertTo<TString>(taskNameNode)
            : "";
        return
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

TFuture<TClient::TListJobsFromControllerAgentResult> TClient::DoListJobsFromControllerAgentAsync(
    TOperationId operationId,
    const std::optional<TString>& controllerAgentAddress,
    TInstant deadline,
    const TListJobsOptions& options)
{
    if (!controllerAgentAddress) {
        return MakeFuture(TListJobsFromControllerAgentResult{});
    }

    // TODO(levysotskiy): extract this list to some common place.
    static const THashSet<TString> DefaultAttributes = {
        "job_id",
        "type",
        "state",
        "start_time",
        "finish_time",
        "address",
        "has_spec",
        "progress",
        "stderr_size",
        "error",
        "brief_statistics",
        "job_competition_id",
        "has_competitors",
        "task_name",
    };

    TObjectServiceProxy proxy(GetMasterChannelOrThrow(EMasterChannelKind::Follower));
    proxy.SetDefaultTimeout(deadline - Now());
    auto batchReq = proxy.ExecuteBatch();

    batchReq->AddRequest(
        TYPathProxy::Get(GetControllerAgentOrchidRunningJobsPath(*controllerAgentAddress, operationId)),
        "running_jobs");

    batchReq->AddRequest(
        TYPathProxy::Get(GetControllerAgentOrchidRetainedFinishedJobsPath(*controllerAgentAddress, operationId)),
        "retained_finished_jobs");

    return batchReq->Invoke().Apply(BIND([operationId, options, this, this_=MakeStrong(this)] (const TObjectServiceProxy::TRspExecuteBatchPtr& batchRsp) {
        TListJobsFromControllerAgentResult result;
        ParseJobsFromControllerAgentResponse(
            operationId,
            batchRsp,
            "running_jobs",
            DefaultAttributes,
            options,
            &result.InProgressJobs,
            &result.TotalInProgressJobCount,
            Logger);
        ParseJobsFromControllerAgentResponse(
            operationId,
            batchRsp,
            "retained_finished_jobs",
            DefaultAttributes,
            options,
            &result.FinishedJobs,
            &result.TotalFinishedJobCount,
            Logger);
        return result;
    }));
}

static std::function<bool(const TJob&, const TJob&)> GetJobsComparator(
    EJobSortField sortField,
    EJobSortDirection sortOrder)
{
    auto makeLessBy = [sortOrder] (auto transform) -> std::function<bool(const TJob&, const TJob&)> {
        switch (sortOrder) {
            case EJobSortDirection::Ascending:
                return [=] (const TJob& lhs, const TJob& rhs) {
                    return transform(lhs) < transform(rhs);
                };
            case EJobSortDirection::Descending:
                return [=] (const TJob& lhs, const TJob& rhs) {
                    return transform(rhs) < transform(lhs);
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
        default:
            YT_ABORT();
    }
}

template <typename TSourceJob>
static void MergeJob(TSourceJob&& source, TJob* target)
{
#define MERGE_NULLABLE_FIELD(name) \
    if (source.name) { \
        target->name = std::forward<decltype(std::forward<TSourceJob>(source).name)>(source.name); \
    }
    MERGE_NULLABLE_FIELD(Type);
    MERGE_NULLABLE_FIELD(ControllerAgentState);
    MERGE_NULLABLE_FIELD(ArchiveState);
    MERGE_NULLABLE_FIELD(Progress);
    MERGE_NULLABLE_FIELD(StartTime);
    MERGE_NULLABLE_FIELD(FinishTime);
    MERGE_NULLABLE_FIELD(Address);
    MERGE_NULLABLE_FIELD(Progress);
    MERGE_NULLABLE_FIELD(Error);
    MERGE_NULLABLE_FIELD(BriefStatistics);
    MERGE_NULLABLE_FIELD(InputPaths);
    MERGE_NULLABLE_FIELD(CoreInfos);
    MERGE_NULLABLE_FIELD(JobCompetitionId);
    MERGE_NULLABLE_FIELD(HasCompetitors);
    MERGE_NULLABLE_FIELD(ExecAttributes);
    MERGE_NULLABLE_FIELD(TaskName);
#undef MERGE_NULLABLE_FIELD
    if (source.StderrSize && target->StderrSize.value_or(0) < source.StderrSize) {
        target->StderrSize = source.StderrSize;
    }
}

static THashMap<TJobId, TJob*> CreateJobIdToJobMap(std::vector<TJob>& jobs)
{
    THashMap<TJobId, TJob*> result;
    for (auto& job : jobs) {
        YT_VERIFY(job.Id);
        result.emplace(job.Id, &job);
    }
    return result;
}

template <typename TJobs>
static void UpdateJobs(TJobs&& patch, std::vector<TJob>* origin)
{
    auto originMap = CreateJobIdToJobMap(*origin);
    for (auto& job : patch) {
        YT_VERIFY(job.Id);
        if (auto originMapIt = originMap.find(job.Id); originMapIt != originMap.end()) {
            if constexpr (std::is_rvalue_reference_v<TJobs>) {
                MergeJob(std::move(job), originMapIt->second);
            } else {
                MergeJob(job, originMapIt->second);
            }
        }
    }
}

static void UpdateJobsAndAddMissing(std::vector<TJob>&& delta, std::vector<TJob>* origin)
{
    auto originMap = CreateJobIdToJobMap(*origin);
    std::vector<TJob> newJobs;
    for (auto& job : delta) {
        YT_VERIFY(job.Id);
        if (auto originMapIt = originMap.find(job.Id); originMapIt != originMap.end()) {
            MergeJob(std::move(job), originMapIt->second);
        } else {
            newJobs.push_back(std::move(job));
        }
    }
    origin->insert(
        origin->end(),
        std::make_move_iterator(newJobs.begin()),
        std::make_move_iterator(newJobs.end()));
}

template <typename T, typename TComparator>
static void MergeThreeVectors(
    std::vector<T>&& first,
    std::vector<T>&& second,
    std::vector<T>&& third,
    std::vector<T>* result,
    TComparator comparator)
{
    YT_VERIFY(result);
    std::vector<T> firstAndSecond;
    firstAndSecond.reserve(first.size() + second.size());
    std::merge(
        std::make_move_iterator(first.begin()),
        std::make_move_iterator(first.end()),
        std::make_move_iterator(second.begin()),
        std::make_move_iterator(second.end()),
        std::back_inserter(firstAndSecond),
        comparator);
    result->reserve(firstAndSecond.size() + third.size());
    std::merge(
        std::make_move_iterator(firstAndSecond.begin()),
        std::make_move_iterator(firstAndSecond.end()),
        std::make_move_iterator(third.begin()),
        std::make_move_iterator(third.end()),
        std::back_inserter(*result),
        comparator);
}

TListJobsResult TClient::DoListJobs(
    TOperationId operationId,
    const TListJobsOptions& options)
{
    auto timeout = options.Timeout.value_or(Connection_->GetConfig()->DefaultListJobsTimeout);
    auto deadline = timeout.ToDeadLine();

    auto controllerAgentAddress = GetControllerAgentAddressFromCypress(
        operationId,
        GetMasterChannelOrThrow(EMasterChannelKind::Follower));

    auto dataSource = options.DataSource;
    if (dataSource == EDataSource::Auto) {
        if (controllerAgentAddress) {
            dataSource = EDataSource::Runtime;
        } else {
            dataSource = EDataSource::Archive;
        }
    }

    bool includeCypress;
    bool includeControllerAgent;
    bool includeArchive;

    switch (dataSource) {
        case EDataSource::Archive:
            includeCypress = false;
            includeControllerAgent = true;
            includeArchive = true;
            break;
        case EDataSource::Runtime:
            includeCypress = true;
            includeControllerAgent = true;
            includeArchive = false;
            break;
        case EDataSource::Manual:
            THROW_ERROR_EXCEPTION("\"manual\" mode is deprecated and forbidden");
        default:
            YT_ABORT();
    }

    TFuture<std::pair<std::vector<TJob>, int>> cypressResultFuture;
    TFuture<TListJobsFromControllerAgentResult> controllerAgentResultFuture;
    TFuture<TListJobsFromArchiveResult> archiveResultFuture;
    TFuture<TListJobsStatistics> statisticsFuture;

    // Issue the requests in parallel.

    auto tryListJobsFromArchiveAsync = [&] {
        if (DoesOperationsArchiveExist()) {
            return DoListJobsFromArchiveAsync(
                operationId,
                deadline,
                /* includeInProgressJobs */ controllerAgentAddress.has_value(),
                options);
        }
        return MakeFuture(TListJobsFromArchiveResult{});
    };

    if (includeArchive) {
        archiveResultFuture = tryListJobsFromArchiveAsync();
    }

    if (includeCypress) {
        cypressResultFuture = DoListJobsFromCypressAsync(operationId, deadline, options);
    }

    if (includeControllerAgent) {
        controllerAgentResultFuture = DoListJobsFromControllerAgentAsync(
            operationId,
            controllerAgentAddress,
            deadline,
            options);
    }

    if (DoesOperationsArchiveExist()) {
        statisticsFuture = ListJobsStatisticsFromArchiveAsync(operationId, deadline, options);
    }

    // Wait for results and combine them.

    TListJobsResult result;

    TListJobsFromControllerAgentResult controllerAgentResult;
    if (includeControllerAgent) {
        auto controllerAgentResultOrError = WaitFor(controllerAgentResultFuture);
        if (controllerAgentResultOrError.IsOK()) {
            controllerAgentResult = std::move(controllerAgentResultOrError.Value());
            result.ControllerAgentJobCount =
                controllerAgentResult.TotalFinishedJobCount + controllerAgentResult.TotalInProgressJobCount;
        } else if (controllerAgentResultOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            // No such operation in the controller agent.
            result.ControllerAgentJobCount = 0;
        } else {
            result.Errors.push_back(std::move(controllerAgentResultOrError));
        }
    }

    auto filterJobs = [&options] (std::vector<TJob>&& jobs, TListJobsStatistics* statistics) {
        std::vector<TJob> filteredJobs;
        for (auto& job : jobs) {
            if (options.Address && job.Address != *options.Address ||
                options.Type && job.Type != *options.Type ||
                options.State && job.GetState() != *options.State) {
                continue;
            }
            filteredJobs.push_back(std::move(job));
        }
        return filteredJobs;
    };

    auto mergeWithArchiveJobs = [&] (
        TListJobsResult& result,
        TListJobsFromControllerAgentResult&& controllerAgentResult,
        const TFuture<TListJobsFromArchiveResult>& archiveResultFuture)
    {
        TListJobsFromArchiveResult archiveResult;

        auto archiveResultOrError = WaitFor(archiveResultFuture);
        if (archiveResultOrError.IsOK()) {
            archiveResult = std::move(archiveResultOrError.Value());
        } else {
            result.Errors.push_back(TError(
                EErrorCode::JobArchiveUnavailable,
                "Job archive is unavailable")
                << archiveResultOrError);
        }

        if (!controllerAgentAddress && archiveResult.InProgressJobs.empty()) {
            result.Jobs = std::move(archiveResult.FinishedJobs);
            return;
        }

        UpdateJobs(controllerAgentResult.InProgressJobs, &archiveResult.InProgressJobs);
        UpdateJobs(controllerAgentResult.FinishedJobs, &archiveResult.InProgressJobs);
        UpdateJobs(controllerAgentResult.InProgressJobs, &archiveResult.FinishedJobs);
        UpdateJobs(controllerAgentResult.FinishedJobs, &archiveResult.FinishedJobs);

        THashSet<TJobId> archiveJobIds;
        for (const auto& job : archiveResult.InProgressJobs) {
            YT_VERIFY(job.Id);
            archiveJobIds.insert(job.Id);
        }
        THashSet<TJobId> archiveFinishedJobIds;
        for (const auto& job : archiveResult.FinishedJobs) {
            YT_VERIFY(job.Id);
            archiveJobIds.insert(job.Id);
        }

        int filteredOutAsReceivedFromArchive = 0;
        controllerAgentResult.FinishedJobs.erase(
            std::remove_if(
                controllerAgentResult.FinishedJobs.begin(),
                controllerAgentResult.FinishedJobs.end(),
                [&] (const TJob& job) {
                    if (archiveJobIds.contains(job.Id)) {
                        ++filteredOutAsReceivedFromArchive;
                        return true;
                    }
                    return false;
                }),
            controllerAgentResult.FinishedJobs.end());

        YT_LOG_DEBUG("Finished jobs from controller agent filtered (FilteredOutAsReceivedFromArchive: %v)",
            filteredOutAsReceivedFromArchive);

        auto jobComparator = GetJobsComparator(options.SortField, options.SortOrder);
        auto filterAndSort = [&] (std::vector<TJob>& jobs) {
            jobs = filterJobs(std::move(jobs), &result.Statistics);
            std::sort(jobs.begin(), jobs.end(), jobComparator);
        };

        filterAndSort(controllerAgentResult.FinishedJobs);
        filterAndSort(archiveResult.InProgressJobs);
        std::sort(archiveResult.FinishedJobs.begin(), archiveResult.FinishedJobs.end(), jobComparator);

        MergeThreeVectors(
            std::move(controllerAgentResult.FinishedJobs),
            std::move(archiveResult.InProgressJobs),
            std::move(archiveResult.FinishedJobs),
            &result.Jobs,
            jobComparator);
    };

    switch (dataSource) {
        case EDataSource::Archive: {
            mergeWithArchiveJobs(result, std::move(controllerAgentResult), archiveResultFuture);
            break;
        }
        case EDataSource::Runtime: {
            auto cypressResultOrError = WaitFor(cypressResultFuture);
            std::vector<TJob> jobs;
            if (cypressResultOrError.IsOK()) {
                auto& [cypressJobs, cypressJobCount] = cypressResultOrError.Value();
                result.CypressJobCount = cypressJobCount;
                jobs = std::move(cypressJobs);
            } else if (cypressResultOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                // No such operation in Cypress.
                result.CypressJobCount = 0;
            } else {
                result.Errors.push_back(TError("Failed to get jobs from Cypress") << cypressResultOrError);
            }

            // No jobs fetched from Cypress, try to get them from archive.
            // (There might be no jobs in Cypress because we don't store them there in recent versions).
            if (result.CypressJobCount == 0 && options.DataSource == EDataSource::Auto) {
                mergeWithArchiveJobs(result, std::move(controllerAgentResult), tryListJobsFromArchiveAsync());
                break;
            }

            UpdateJobsAndAddMissing(std::move(controllerAgentResult.InProgressJobs), &jobs);
            UpdateJobsAndAddMissing(std::move(controllerAgentResult.FinishedJobs), &jobs);

            jobs = filterJobs(std::move(jobs), &result.Statistics);
            std::sort(jobs.begin(), jobs.end(), GetJobsComparator(options.SortField, options.SortOrder));
            result.Jobs = std::move(jobs);

            break;
        }
        default:
            YT_ABORT();
    }

    auto beginIt = std::min(result.Jobs.end(), result.Jobs.begin() + options.Offset);
    auto endIt = std::min(result.Jobs.end(), beginIt + options.Limit);
    result.Jobs = std::vector<TJob>(std::make_move_iterator(beginIt), std::make_move_iterator(endIt));

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

    for (auto& job : result.Jobs) {
        job.IsStale = !job.ControllerAgentState && job.ArchiveState && IsJobInProgress(*job.ArchiveState);
    }

    return result;
}

template <typename T>
static std::optional<T> FindValue(
    TUnversionedRow row,
    const NTableClient::TColumnFilter& columnFilter,
    int columnIndex)
{
    auto maybeIndex = columnFilter.FindPosition(columnIndex);
    if (maybeIndex && row[*maybeIndex].Type != EValueType::Null) {
        return FromUnversionedValue<T>(row[*maybeIndex]);
    }
    return {};
}

template <typename TValue>
void TryAddFluentItem(
    TFluentMap fluent,
    TStringBuf key,
    TUnversionedRow row,
    const NTableClient::TColumnFilter& columnFilter,
    int columnIndex)
{
    if (auto value = FindValue<TValue>(row, columnFilter, columnIndex)) {
        fluent.Item(key).Value(*value);
    }
}

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
    const THashSet<TString>& attributes,
    const TGetJobOptions& options)
{
    TJobTableDescriptor table;
    auto rowBuffer = New<TRowBuffer>();

    std::vector<TUnversionedRow> keys;
    auto key = rowBuffer->AllocateUnversioned(4);
    key[0] = MakeUnversionedUint64Value(operationId.Parts64[0], table.Index.OperationIdHi);
    key[1] = MakeUnversionedUint64Value(operationId.Parts64[1], table.Index.OperationIdLo);
    key[2] = MakeUnversionedUint64Value(jobId.Parts64[0], table.Index.JobIdHi);
    key[3] = MakeUnversionedUint64Value(jobId.Parts64[1], table.Index.JobIdLo);
    keys.push_back(key);


    std::vector<int> columnIndexes;
    auto fields = MakeJobArchiveAttributes(attributes);
    for (const auto& field : fields) {
        columnIndexes.push_back(table.NameTable->GetIdOrThrow(field));
    }

    TLookupRowsOptions lookupOptions;
    lookupOptions.ColumnFilter = NTableClient::TColumnFilter(columnIndexes);
    lookupOptions.KeepMissingRows = true;
    lookupOptions.Timeout = deadline - Now();

    auto rowset = WaitFor(LookupRows(
        GetOperationsArchiveJobsPath(),
        table.NameTable,
        MakeSharedRange(std::move(keys), std::move(rowBuffer)),
        lookupOptions))
        .ValueOrThrow();

    auto rows = rowset->GetRows();
    YT_VERIFY(!rows.Empty());
    auto row = rows[0];

    if (!row) {
        return {};
    }

    const auto& columnFilter = lookupOptions.ColumnFilter;

    TJob job;

    if (columnFilter.ContainsIndex(table.Index.JobIdHi)) {
        job.Id = jobId;
    }
    if (columnFilter.ContainsIndex(table.Index.OperationIdHi)) {
        job.OperationId = operationId;
    }

    auto state = FindValue<TStringBuf>(row, columnFilter, table.Index.State);
    if (!state) {
        state = FindValue<TStringBuf>(row, columnFilter, table.Index.TransientState);
    }
    if (state) {
        job.ArchiveState = ParseEnum<EJobState>(*state);
    }

    // NB: We need a separate function for |TInstant| because it has type "int64" in table
    // but |FromUnversionedValue<TInstant>| expects it to be "uint64".
    auto findInstant = [&] (int columnIndex) -> std::optional<TInstant> {
        if (auto microseconds = FindValue<i64>(row, columnFilter, columnIndex)) {
            return TInstant::MicroSeconds(*microseconds);
        }
        return {};
    };

    if (auto startTime = findInstant(table.Index.StartTime)) {
        job.StartTime = *startTime;
    }
    if (auto finishTime = findInstant(table.Index.FinishTime)) {
        job.FinishTime = *finishTime;
    }
    if (auto hasSpec = FindValue<bool>(row, columnFilter, table.Index.HasSpec)) {
        job.HasSpec = *hasSpec;
    }
    if (auto address = FindValue<TStringBuf>(row, columnFilter, table.Index.Address)) {
        job.Address = *address;
    }
    if (auto type = FindValue<TStringBuf>(row, columnFilter, table.Index.Type)) {
        job.Type = ParseEnum<EJobType>(*type);
    }
    if (auto error = FindValue<TYsonString>(row, columnFilter, table.Index.Error)) {
        job.Error = std::move(*error);
    }
    if (auto statistics = FindValue<TYsonString>(row, columnFilter, table.Index.Statistics)) {
        job.Statistics = std::move(*statistics);
    }
    if (auto events = FindValue<TYsonString>(row, columnFilter, table.Index.Events)) {
        job.Events = std::move(*events);
    }
    if (auto jobCompetitionId = FindValue<TJobId>(row, columnFilter, table.Index.JobCompetitionId)) {
        job.JobCompetitionId = *jobCompetitionId;
    }
    if (columnFilter.FindPosition(table.Index.HasCompetitors)) {
        if (auto hasCompetitors = FindValue<bool>(row, columnFilter, table.Index.HasCompetitors)) {
            job.HasCompetitors = *hasCompetitors;
        } else {
            job.HasCompetitors = false;
        }
    }
    if (auto execAttributes = FindValue<TYsonString>(row, columnFilter, table.Index.ExecAttributes)) {
        job.ExecAttributes = std::move(*execAttributes);
    }
    if (auto taskName = FindValue<TStringBuf>(row, columnFilter, table.Index.TaskName)) {
        job.TaskName = taskName;
    }

    return job;
}

std::optional<TJob> TClient::DoGetJobFromControllerAgent(
    TOperationId operationId,
    TJobId jobId,
    TInstant deadline,
    const THashSet<TString>& attributes,
    const TGetJobOptions& options)
{
    auto controllerAgentAddress = GetControllerAgentAddressFromCypress(
        operationId,
        GetMasterChannelOrThrow(EMasterChannelKind::Follower));
    if (!controllerAgentAddress) {
        return {};
    }

    TObjectServiceProxy proxy(GetMasterChannelOrThrow(EMasterChannelKind::Follower));
    proxy.SetDefaultTimeout(deadline - Now());
    auto batchReq = proxy.ExecuteBatch();

    auto runningJobPath =
        GetControllerAgentOrchidRunningJobsPath(*controllerAgentAddress, operationId) + "/" + ToString(jobId);
    batchReq->AddRequest(TYPathProxy::Get(runningJobPath));

    auto finishedJobPath =
        GetControllerAgentOrchidRetainedFinishedJobsPath(*controllerAgentAddress, operationId) + "/" + ToString(jobId);
    batchReq->AddRequest(TYPathProxy::Get(finishedJobPath));

    auto batchRspOrError = WaitFor(batchReq->Invoke());

    if (!batchRspOrError.IsOK()) {
        THROW_ERROR_EXCEPTION("Cannot get jobs from controller agent")
            << batchRspOrError;
    }

    for (const auto& rspOrError : batchRspOrError.Value()->GetResponses<TYPathProxy::TRspGet>()) {
        if (rspOrError.IsOK()) {
            std::vector<TJob> jobs;
            ParseJobsFromControllerAgentResponse(
                operationId,
                {{ToString(jobId), ConvertToNode(TYsonString(rspOrError.Value()->value()))}},
                [] (const INodePtr&) {
                    return true;
                },
                attributes,
                &jobs);
            YT_VERIFY(jobs.size() == 1);
            return jobs[0];
        } else if (!rspOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
            THROW_ERROR_EXCEPTION("Cannot get jobs from controller agent")
                << rspOrError;
        }
    }

    return {};
}

TYsonString TClient::DoGetJob(
    TOperationId operationId,
    TJobId jobId,
    const TGetJobOptions& options)
{
    auto timeout = options.Timeout.value_or(Connection_->GetConfig()->DefaultGetJobTimeout);
    auto deadline = timeout.ToDeadLine();

    static const THashSet<TString> DefaultAttributes = {
        "operation_id",
        "job_id",
        "type",
        "state",
        "start_time",
        "finish_time",
        "address",
        "error",
        "statistics",
        "events",
        "has_spec",
        "job_competition_id",
        "has_competitors",
        "exec_attributes",
        "task_name",
    };

    const auto& attributes = options.Attributes.value_or(DefaultAttributes);

    auto job = DoGetJobFromArchive(operationId, jobId, deadline, attributes, options);
    if (!job) {
        job = DoGetJobFromControllerAgent(operationId, jobId, deadline, attributes, options);
    }

    if (!job) {
        THROW_ERROR_EXCEPTION(
            EErrorCode::NoSuchJob,
            "Job %v or operation %v not found neither in archive nor in controller agent",
            jobId,
            operationId);
    }

    return BuildYsonStringFluently()
        .Do([&] (TFluentAny fluent) {
            Serialize(*job, fluent.GetConsumer(), "job_id");
        });
}

} // namespace NYT::NApi::NNative
