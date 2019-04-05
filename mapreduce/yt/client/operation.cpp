#include "operation.h"

#include "client.h"
#include "file_reader.h"
#include "file_writer.h"
#include "init.h"
#include "operation_tracker.h"
#include "retry_heavy_write_request.h"
#include "skiff.h"
#include "structured_table_formats.h"
#include "yt_poller.h"

#include <mapreduce/yt/common/abortable_registry.h>
#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/finally_guard.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/retry_lib.h>
#include <mapreduce/yt/common/wait_proxy.h>

#include <mapreduce/yt/interface/errors.h>
#include <mapreduce/yt/interface/fluent.h>
#include <mapreduce/yt/interface/job_statistics.h>

#include <mapreduce/yt/interface/logging/log.h>

#include <mapreduce/yt/node/serialize.h>

#include <mapreduce/yt/http/requests.h>
#include <mapreduce/yt/http/retry_request.h>

#include <mapreduce/yt/io/job_reader.h>
#include <mapreduce/yt/io/job_writer.h>
#include <mapreduce/yt/io/yamr_table_reader.h>
#include <mapreduce/yt/io/yamr_table_writer.h>
#include <mapreduce/yt/io/node_table_reader.h>
#include <mapreduce/yt/io/node_table_writer.h>
#include <mapreduce/yt/io/proto_table_reader.h>
#include <mapreduce/yt/io/proto_table_writer.h>
#include <mapreduce/yt/io/proto_helpers.h>
#include <mapreduce/yt/io/skiff_table_reader.h>

#include <mapreduce/yt/raw_client/raw_batch_request.h>
#include <mapreduce/yt/raw_client/raw_requests.h>

#include <library/yson/writer.h>
#include <library/yson/json_writer.h>

#include <util/folder/path.h>

#include <util/stream/buffer.h>
#include <util/stream/file.h>

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>

#include <util/system/execpath.h>
#include <util/system/mutex.h>
#include <util/system/rwlock.h>
#include <util/system/thread.h>

#include <library/digest/md5/md5.h>

namespace NYT {
namespace NDetail {

using namespace NRawClient;

static const ui64 DefaultExrtaTmpfsSize = 1024LL * 1024LL;
static const bool LockCachedFilesWhenStartingOperation = false;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TSimpleOperationIo
{
    TVector<TRichYPath> Inputs;
    TVector<TRichYPath> Outputs;

    TFormat InputFormat;
    TFormat OutputFormat;

    TVector<TSmallJobFile> JobFiles;
};

struct TMapReduceOperationIo
{
    TVector<TRichYPath> Inputs;
    TVector<TRichYPath> MapOutputs;
    TVector<TRichYPath> Outputs;

    TMaybe<TFormat> MapperInputFormat;
    TMaybe<TFormat> MapperOutputFormat;

    TMaybe<TFormat> ReduceCombinerInputFormat;
    TMaybe<TFormat> ReduceCombinerOutputFormat;

    TFormat ReducerInputFormat = TFormat::YsonBinary();
    TFormat ReducerOutputFormat = TFormat::YsonBinary();

    TVector<TSmallJobFile> MapperJobFiles;
    TVector<TSmallJobFile> ReduceCombinerJobFiles;
    TVector<TSmallJobFile> ReducerJobFiles;
};

ui64 RoundUpFileSize(ui64 size)
{
    constexpr ui64 roundUpTo = 4ull << 10;
    return (size + roundUpTo - 1) & ~(roundUpTo - 1);
}

bool UseLocalModeOptimization(const TAuth& auth)
{
    if (!TConfig::Get()->EnableLocalModeOptimization) {
        return false;
    }

    static THashMap<TString, bool> localModeMap;
    static TRWMutex mutex;

    {
        TReadGuard guard(mutex);
        auto it = localModeMap.find(auth.ServerName);
        if (it != localModeMap.end()) {
            return it->second;
        }
    }

    bool isLocalMode = false;
    TString localModeAttr("//sys/@local_mode_fqdn");
    if (Exists(auth, TTransactionId(), localModeAttr)) {
        auto fqdn = Get(auth, TTransactionId(), localModeAttr).AsString();
        isLocalMode = (fqdn == TProcessState::Get()->HostName);
    }

    {
        TWriteGuard guard(mutex);
        localModeMap[auth.ServerName] = isLocalMode;
    }

    return isLocalMode;
}

template <typename T>
void VerifyHasElements(const TVector<T>& paths, TStringBuf name)
{
    if (paths.empty()) {
        ythrow TApiUsageError() << "no " << name << " table is specified";
    }
}

////////////////////////////////////////////////////////////////////////////////

TVector<TSmallJobFile> CreateFormatConfig(
    TMaybe<TSmallJobFile> inputConfig,
    const TMaybe<TSmallJobFile>& outputConfig)
{
    TVector<TSmallJobFile> result;
    if (inputConfig) {
        result.push_back(std::move(*inputConfig));
    }
    if (outputConfig) {
        result.push_back(std::move(*outputConfig));
    }
    return result;
}

template <typename T>
ENodeReaderFormat NodeReaderFormatFromHintAndGlobalConfig(const TUserJobFormatHintsBase<T>& formatHints)
{
    auto result = TConfig::Get()->NodeReaderFormat;
    if (formatHints.InputFormatHints_ && formatHints.InputFormatHints_->SkipNullValuesForTNode_) {
        Y_ENSURE_EX(
            result != ENodeReaderFormat::Skiff,
            TApiUsageError() << "skiff format doesn't support SkipNullValuesForTNode format hint");
        result = ENodeReaderFormat::Yson;
    }
    return result;
}

template <class TSpec>
TSimpleOperationIo CreateSimpleOperationIo(
    const IStructuredJob& structuredJob,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TSpec& spec,
    const TOperationOptions& options,
    bool allowSkiff)
{
    VerifyHasElements(spec.Inputs_, "input");
    VerifyHasElements(spec.Outputs_, "output");

    const auto structuredInputs= CanonizeStructuredTableList(auth, spec.GetStructuredInputs());
    const auto structuredOutputs = CanonizeStructuredTableList(auth, spec.GetStructuredOutputs());

    ENodeReaderFormat nodeReaderFormat =
        allowSkiff
        ? NodeReaderFormatFromHintAndGlobalConfig(spec)
        : ENodeReaderFormat::Yson;

    TVector<TSmallJobFile> formatConfigList;
    TFormatBuilder formatBuilder(auth, transactionId, options);

    // Input format
    auto[inputFormat, inputFormatConfig] = formatBuilder.CreateFormat(
        structuredJob,
        EIODirection::Input,
        structuredInputs,
        spec.InputFormatHints_,
        nodeReaderFormat,
        /* allowFormatFromTableAttribute = */ true);

    // Output format
    auto[outputFormat, outputFormatConfig] = formatBuilder.CreateFormat(
        structuredJob,
        EIODirection::Output,
        structuredOutputs,
        spec.OutputFormatHints_,
        ENodeReaderFormat::Yson,
        /* allowFormatFromTableAttribute = */ false);

    const bool inferOutputSchema = options.InferOutputSchema_.GetOrElse(TConfig::Get()->InferTableSchema);
    auto outputPaths = GetPathList(structuredOutputs, inferOutputSchema);

    return TSimpleOperationIo {
        GetPathList(structuredInputs, /*inferSchema*/ false),
        outputPaths,

        inputFormat,
        outputFormat,

        CreateFormatConfig(inputFormatConfig, outputFormatConfig)
    };
}

template <class T>
TSimpleOperationIo CreateSimpleOperationIo(
    const TAuth& auth,
    const TSimpleRawOperationIoSpec<T>& spec)
{
    auto getFormatOrDefault = [&] (const TMaybe<TFormat>& maybeFormat, const char* formatName) {
        if (maybeFormat) {
            return *maybeFormat;
        } else if (spec.Format_) {
            return *spec.Format_;
        } else {
            ythrow TApiUsageError() << "Neither " << formatName << "format nor default format is specified for raw operation";
        }
    };

    VerifyHasElements(spec.GetInputs(), "input");
    VerifyHasElements(spec.GetOutputs(), "output");

    return TSimpleOperationIo {
        CanonizePaths(auth, spec.GetInputs()),
        CanonizePaths(auth, spec.GetOutputs()),

        getFormatOrDefault(spec.InputFormat_, "input"),
        getFormatOrDefault(spec.OutputFormat_, "output"),

        TVector<TSmallJobFile>{},
    };
}


////////////////////////////////////////////////////////////////////////////////

struct IItemToUpload
{
    virtual ~IItemToUpload() = default;

    virtual TString CalculateMD5() const = 0;
    virtual THolder<IInputStream> CreateInputStream() const = 0;
    virtual TString GetDescription() const = 0;
};

class TFileToUpload
    : public IItemToUpload
{
public:
    TFileToUpload(TString fileName)
        : FileName_(std::move(fileName))
    { }

    TString CalculateMD5() const override
    {
        constexpr size_t md5Size = 32;
        TString result;
        result.ReserveAndResize(md5Size);
        MD5::File(FileName_.data(), result.Detach());
        return result;
    }

    THolder<IInputStream> CreateInputStream() const override
    {
        return MakeHolder<TFileInput>(FileName_);
    }

    TString GetDescription() const override
    {
        return FileName_;
    }

private:
    TString FileName_;
};

class TDataToUpload
    : public IItemToUpload
{
public:
    TDataToUpload(TString data, TString description)
        : Data_(std::move(data))
        , Description_(std::move(description))
    { }

    TString CalculateMD5() const override
    {
        constexpr size_t md5Size = 32;
        TString result;
        result.ReserveAndResize(md5Size);
        MD5::Data(reinterpret_cast<const unsigned char*>(Data_.data()), Data_.size(), result.Detach());
        return result;
    }

    THolder<IInputStream> CreateInputStream() const override
    {
        return MakeHolder<TMemoryInput>(Data_.data(), Data_.size());
    }

    TString GetDescription() const override
    {
        return Description_;
    }

private:
    TString Data_;
    TString Description_;
};

class TJobPreparer
    : private TNonCopyable
{
public:
    TJobPreparer(
        TOperationPreparer& operationPreparer,
        const TUserJobSpec& spec,
        const IJob& job,
        size_t outputTableCount,
        const TVector<TSmallJobFile>& smallFileList,
        const TOperationOptions& options)
        : OperationPreparer_(operationPreparer)
        , Spec_(spec)
        , Options_(options)
    {
        auto jobBinary = TConfig::Get()->GetJobBinary();
        if (!HoldsAlternative<TJobBinaryDefault>(Spec_.GetJobBinary())) {
            jobBinary = Spec_.GetJobBinary();
        }
        TString binaryPathInsideJob;
        if (HoldsAlternative<TJobBinaryDefault>(jobBinary)) {
            if (GetInitStatus() != EInitStatus::FullInitialization) {
                ythrow yexception() << "NYT::Initialize() must be called prior to any operation";
            }
            jobBinary = TJobBinaryLocalPath{GetPersistentExecPath()};

            if (UseLocalModeOptimization(OperationPreparer_.GetAuth())) {
                binaryPathInsideJob = GetExecPath();
            }
        } else if (HoldsAlternative<TJobBinaryLocalPath>(jobBinary)) {
            if (UseLocalModeOptimization(OperationPreparer_.GetAuth())) {
                binaryPathInsideJob = TFsPath(::Get<TJobBinaryLocalPath>(jobBinary).Path).RealPath();
            }
        }
        Y_ASSERT(!HoldsAlternative<TJobBinaryDefault>(jobBinary));

        CreateStorage();
        auto cypressFileList = CanonizePaths(OperationPreparer_.GetAuth(), spec.Files_);
        for (const auto& file : cypressFileList) {
            UseFileInCypress(file);
        }
        for (const auto& localFile : spec.GetLocalFiles()) {
            UploadLocalFile(std::get<0>(localFile), std::get<1>(localFile));
        }
        auto jobStateSmallFile = GetJobState(job);
        if (jobStateSmallFile) {
            UploadSmallFile(*jobStateSmallFile);
        }
        for (const auto& smallFile : smallFileList) {
            UploadSmallFile(smallFile);
        }

        // binaryPathInsideJob is only set when LocalModeOptimization option is on, so upload is not needed
        if (!binaryPathInsideJob) {
            binaryPathInsideJob = "./cppbinary";
            UploadBinary(jobBinary);
        }

        TString jobCommandPrefix = options.JobCommandPrefix_;
        if (!spec.JobCommandPrefix_.empty()) {
            jobCommandPrefix = spec.JobCommandPrefix_;
        }

        TString jobCommandSuffix = options.JobCommandSuffix_;
        if (!spec.JobCommandSuffix_.empty()) {
            jobCommandSuffix = spec.JobCommandSuffix_;
        }

        ClassName_ = TJobFactory::Get()->GetJobName(&job);
        Command_ = TStringBuilder() <<
            jobCommandPrefix <<
            (TConfig::Get()->UseClientProtobuf ? "YT_USE_CLIENT_PROTOBUF=1" : "YT_USE_CLIENT_PROTOBUF=0") << " " <<
            binaryPathInsideJob << " " <<
            // This argument has no meaning, but historically is checked in job initialization.
            "--yt-map " <<
            "\"" << ClassName_ << "\" " <<
            outputTableCount << " " <<
            jobStateSmallFile.Defined() <<
            jobCommandSuffix;

        if (LockCachedFilesWhenStartingOperation) {
            operationPreparer.LockFiles(&CachedFiles_, LockedFileSignatures_, GetCachePath());
        }
    }

    TVector<TRichYPath> GetFiles() const
    {
        TVector<TRichYPath> allFiles = CypressFiles_;
        allFiles.insert(allFiles.end(), CachedFiles_.begin(), CachedFiles_.end());
        return allFiles;
    }

    const TString& GetClassName() const
    {
        return ClassName_;
    }

    const TString& GetCommand() const
    {
        return Command_;
    }

    const TUserJobSpec& GetSpec() const
    {
        return Spec_;
    }

    bool ShouldMountSandbox() const
    {
        return TConfig::Get()->MountSandboxInTmpfs || Options_.MountSandboxInTmpfs_;
    }

    ui64 GetTotalFileSize() const
    {
        return TotalFileSize_;
    }

private:
    TOperationPreparer& OperationPreparer_;
    TUserJobSpec Spec_;
    TOperationOptions Options_;

    TVector<TRichYPath> CypressFiles_;
    TVector<TRichYPath> CachedFiles_;
    mutable TVector<TString> LockedFileSignatures_;

    TString ClassName_;
    TString Command_;
    ui64 TotalFileSize_ = 0;

    TString GetFileStorage() const
    {
        return Options_.FileStorage_ ?
            *Options_.FileStorage_ :
            TConfig::Get()->RemoteTempFilesDirectory;
    }

    TYPath GetCachePath() const
    {
        return AddPathPrefix(TStringBuilder() << GetFileStorage() << "/new_cache");
    }

    void CreateStorage() const
    {
        Create(OperationPreparer_.GetAuth(), Options_.FileStorageTransactionId_, GetCachePath(), NT_MAP,
            TCreateOptions()
            .IgnoreExisting(true)
            .Recursive(true));
    }

    class TRetryPolicyIgnoringLockConflicts
        : public TAttemptLimitedRetryPolicy
    {
    public:
        using TAttemptLimitedRetryPolicy::TAttemptLimitedRetryPolicy;
        using TAttemptLimitedRetryPolicy::OnGenericError;

        TMaybe<TDuration> OnRetriableError(const TErrorResponse& e) override
        {
            if (IsAttemptLimitExceeded()) {
                return Nothing();
            }
            if (e.IsConcurrentTransactionLockConflict()) {
                return TConfig::Get()->RetryInterval;
            }
            return TAttemptLimitedRetryPolicy::OnRetriableError(e);
        }
    };

    TString UploadToRandomPath(const IItemToUpload& itemToUpload) const
    {
        TString uniquePath = AddPathPrefix(TStringBuilder() << GetFileStorage() << "/cpp_" << CreateGuidAsString());
        Create(OperationPreparer_.GetAuth(), Options_.FileStorageTransactionId_, uniquePath, NT_FILE, TCreateOptions()
            .IgnoreExisting(true)
            .Recursive(true));
        {
            TFileWriter writer(uniquePath, OperationPreparer_.GetAuth(), Options_.FileStorageTransactionId_);
            itemToUpload.CreateInputStream()->ReadAll(writer);
            writer.Finish();
        }
        return uniquePath;
    }

    TString UploadToCacheUsingApi(const IItemToUpload& itemToUpload) const
    {
        auto md5Signature = itemToUpload.CalculateMD5();
        Y_VERIFY(md5Signature.size() == 32);

        constexpr ui32 LockConflictRetryCount = 30;
        auto retryPolicy = MakeIntrusive<TRetryPolicyIgnoringLockConflicts>(LockConflictRetryCount);
        auto maybePath = GetFileFromCache(
            OperationPreparer_.GetAuth(),
            md5Signature,
            GetCachePath(),
            TGetFileFromCacheOptions(),
            retryPolicy);
        if (maybePath) {
            LOG_DEBUG("File is already in cache: %s", itemToUpload.GetDescription().c_str());
            return *maybePath;
        }

        TString uniquePath = AddPathPrefix(TStringBuilder() << GetFileStorage() << "/cpp_" << CreateGuidAsString());

        Create(OperationPreparer_.GetAuth(), TTransactionId(), uniquePath, NT_FILE,
            TCreateOptions()
            .IgnoreExisting(true)
            .Recursive(true));

        {
            TFileWriter writer(uniquePath, OperationPreparer_.GetAuth(), TTransactionId(),
                TFileWriterOptions().ComputeMD5(true));
            itemToUpload.CreateInputStream()->ReadAll(writer);
            writer.Finish();
        }

        auto cachePath = PutFileToCache(
            OperationPreparer_.GetAuth(),
            uniquePath,
            md5Signature,
            GetCachePath(),
            TPutFileToCacheOptions(),
            retryPolicy);

        Remove(OperationPreparer_.GetAuth(), TTransactionId(), uniquePath, TRemoveOptions().Force(true));

        LockedFileSignatures_.push_back(md5Signature);
        return cachePath;
    }

    TString UploadToCache(const IItemToUpload& itemToUpload) const
    {
        LOG_INFO("Uploading file (FileName: %s)", itemToUpload.GetDescription().c_str());
        TFinallyGuard g([&] {
            LOG_INFO("Complete uploading file (FileName: %s)", itemToUpload.GetDescription().c_str());
        });
        switch (Options_.FileCacheMode_) {
            case TOperationOptions::EFileCacheMode::ApiCommandBased:
                Y_ENSURE_EX(Options_.FileStorageTransactionId_.IsEmpty(), TApiUsageError() <<
                    "Default cache mode (API command-based) doesn't allow non-default 'FileStorageTransactionId_'");
                return UploadToCacheUsingApi(itemToUpload);
            case TOperationOptions::EFileCacheMode::CachelessRandomPathUpload:
                return UploadToRandomPath(itemToUpload);
            default:
                Y_FAIL("Unknown file cache mode: %d", static_cast<int>(Options_.FileCacheMode_));
        }
    }

    void UseFileInCypress(const TRichYPath& file)
    {
        if (!Exists(OperationPreparer_.GetAuth(), OperationPreparer_.GetTransactionId(), file.Path_)) {
            ythrow yexception() << "File " << file.Path_ << " does not exist";
        }

        if (ShouldMountSandbox()) {
            auto size = Get(
                OperationPreparer_.GetAuth(),
                OperationPreparer_.GetTransactionId(),
                file.Path_ + "/@uncompressed_data_size")
                .AsInt64();

            TotalFileSize_ += RoundUpFileSize(static_cast<ui64>(size));
        }
        CypressFiles_.push_back(file);
    }

    void UploadLocalFile(const TLocalFilePath& localPath, const TAddLocalFileOptions& options)
    {
        TFsPath fsPath(localPath);
        fsPath.CheckExists();

        TFileStat stat;
        fsPath.Stat(stat);

        bool isExecutable = stat.Mode & (S_IXUSR | S_IXGRP | S_IXOTH);
        auto cachePath = UploadToCache(TFileToUpload(localPath));

        TRichYPath cypressPath(cachePath);
        cypressPath.FileName(options.PathInJob_.GetOrElse(fsPath.Basename()));
        if (isExecutable) {
            cypressPath.Executable(true);
        }

        if (ShouldMountSandbox()) {
            TotalFileSize_ += RoundUpFileSize(stat.Size);
        }

        CachedFiles_.push_back(cypressPath);
    }

    void UploadBinary(const TJobBinaryConfig& jobBinary)
    {
        if (HoldsAlternative<TJobBinaryLocalPath>(jobBinary)) {
            auto binaryLocalPath = ::Get<TJobBinaryLocalPath>(jobBinary).Path;
            UploadLocalFile(binaryLocalPath, TAddLocalFileOptions().PathInJob("cppbinary"));
        } else if (HoldsAlternative<TJobBinaryCypressPath>(jobBinary)) {
            auto binaryCypressPath = ::Get<TJobBinaryCypressPath>(jobBinary).Path;
            UseFileInCypress(
                TRichYPath(binaryCypressPath)
                    .FileName("cppbinary")
                    .Executable(true));
        } else {
            Y_FAIL("%s", (TStringBuilder() << "Unexpected jobBinary tag: " << jobBinary.index()).data());
        }
    }

    TMaybe<TSmallJobFile> GetJobState(const IJob& job)
    {
        TString result;
        {
            TStringOutput output(result);
            job.Save(output);
            output.Finish();
        }
        if (result.empty()) {
            return Nothing();
        } else {
            return TSmallJobFile{"jobstate", result};
        }
    }

    void UploadSmallFile(const TSmallJobFile& smallFile)
    {
        auto cachePath = UploadToCache(TDataToUpload(smallFile.Data, smallFile.FileName + " [generated-file]"));
        CachedFiles_.push_back(TRichYPath(cachePath).FileName(smallFile.FileName));
        if (ShouldMountSandbox()) {
            TotalFileSize_ += RoundUpFileSize(smallFile.Data.size());
        }
    }
};

////////////////////////////////////////////////////////////////////

TVector<TFailedJobInfo> GetFailedJobInfo(
    const TAuth& auth,
    const TOperationId& operationId,
    const TGetFailedJobInfoOptions& options)
{
    const auto listJobsResult = ListJobs(
        auth,
        operationId,
        TListJobsOptions()
            .State(EJobState::Failed)
            .Limit(options.MaxJobCount_));

    const auto stderrTailSize = options.StderrTailSize_;

    TVector<TFailedJobInfo> result;
    for (const auto& job : listJobsResult.Jobs) {
        auto& info = result.emplace_back();
        Y_ENSURE(job.Id);
        info.JobId = *job.Id;
        info.Error = job.Error.GetOrElse(TYtError(TString("unknown error")));
        if (job.StderrSize.GetOrElse(0) != 0) {
            info.Stderr = GetJobStderrWithRetries(auth, operationId, *job.Id);
            if (info.Stderr.size() > stderrTailSize) {
                info.Stderr = info.Stderr.substr(info.Stderr.size() - stderrTailSize, stderrTailSize);
            }
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

EOperationBriefState CheckOperation(
    const TAuth& auth,
    const TOperationId& operationId)
{
    auto attributes = GetOperation(
        auth,
        operationId,
        TGetOperationOptions().AttributeFilter(TOperationAttributeFilter()
            .Add(EOperationAttribute::State)
            .Add(EOperationAttribute::Result)));
    Y_VERIFY(attributes.BriefState);
    if (*attributes.BriefState == EOperationBriefState::Completed) {
        return EOperationBriefState::Completed;
    } else if (*attributes.BriefState == EOperationBriefState::Aborted || *attributes.BriefState == EOperationBriefState::Failed) {
        LOG_ERROR("Operation %s %s (%s)",
            GetGuidAsString(operationId).data(),
            ::ToString(*attributes.BriefState).data(),
            ToString(TOperationExecutionTimeTracker::Get()->Finish(operationId)).data());

        auto failedJobInfoList = GetFailedJobInfo(auth, operationId, TGetFailedJobInfoOptions());

        Y_VERIFY(attributes.Result && attributes.Result->Error);
        ythrow TOperationFailedError(
            *attributes.BriefState == EOperationBriefState::Aborted
            ? TOperationFailedError::Aborted
            : TOperationFailedError::Failed,
            operationId,
            *attributes.Result->Error,
            failedJobInfoList);
    }
    return EOperationBriefState::InProgress;
}

void WaitForOperation(
    const TAuth& auth,
    const TOperationId& operationId)
{
    const TDuration checkOperationStateInterval =
        UseLocalModeOptimization(auth) ? TDuration::MilliSeconds(100) : TDuration::Seconds(1);

    while (true) {
        auto status = CheckOperation(auth, operationId);
        if (status == EOperationBriefState::Completed) {
            LOG_INFO("Operation %s completed (%s)",
                GetGuidAsString(operationId).data(),
                ToString(TOperationExecutionTimeTracker::Get()->Finish(operationId)).data());
            break;
        }
        TWaitProxy::Get()->Sleep(checkOperationStateInterval);
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

void BuildUserJobFluently1(
    const TJobPreparer& preparer,
    const TMaybe<TFormat>& inputFormat,
    const TMaybe<TFormat>& outputFormat,
    TFluentMap fluent)
{
    const auto& userJobSpec = preparer.GetSpec();
    TMaybe<i64> memoryLimit = userJobSpec.MemoryLimit_;
    TMaybe<double> cpuLimit = userJobSpec.CpuLimit_;

    // Use 1MB extra tmpfs size by default, it helps to detect job sandbox as tmp directory
    // for standard python libraries. See YTADMINREQ-14505 for more details.
    auto tmpfsSize = preparer.GetSpec().ExtraTmpfsSize_.GetOrElse(DefaultExrtaTmpfsSize);
    if (preparer.ShouldMountSandbox()) {
        tmpfsSize += preparer.GetTotalFileSize();
        if (tmpfsSize == 0) {
            // This can be a case for example when it is local mode and we don't upload binary.
            // NOTE: YT doesn't like zero tmpfs size.
            tmpfsSize = RoundUpFileSize(1);
        }
        memoryLimit = memoryLimit.GetOrElse(512ll << 20) + tmpfsSize;
    }

    fluent
        .Item("file_paths").List(preparer.GetFiles())
        .Item("command").Value(preparer.GetCommand())
        .Item("class_name").Value(preparer.GetClassName())
        .DoIf(!userJobSpec.Environment_.empty(), [&] (TFluentMap fluentMap) {
            TNode environment;
            for (const auto& item : userJobSpec.Environment_) {
                environment[item.first] = item.second;
            }
            fluentMap.Item("environment").Value(environment);
        })
        .DoIf(userJobSpec.DiskSpaceLimit_.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("disk_space_limit").Value(*userJobSpec.DiskSpaceLimit_);
        })
        .DoIf(inputFormat.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("input_format").Value(inputFormat->Config);
        })
        .DoIf(outputFormat.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("output_format").Value(outputFormat->Config);
        })
        .DoIf(memoryLimit.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("memory_limit").Value(*memoryLimit);
        })
        .DoIf(userJobSpec.MemoryReserveFactor_.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("memory_reserve_factor").Value(*userJobSpec.MemoryReserveFactor_);
        })
        .DoIf(cpuLimit.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("cpu_limit").Value(*cpuLimit);
        })
        .DoIf(preparer.ShouldMountSandbox(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("tmpfs_path").Value(".");
            fluentMap.Item("tmpfs_size").Value(tmpfsSize);
            fluentMap.Item("copy_files").Value(true);
        });
}

void BuildCommonOperationPart(const TOperationOptions& options, TFluentMap fluent)
{
    const TProcessState* properties = TProcessState::Get();
    const TString& pool = TConfig::Get()->Pool;

    fluent
        .Item("started_by")
        .BeginMap()
            .Item("hostname").Value(properties->HostName)
            .Item("pid").Value(properties->Pid)
            .Item("user").Value(properties->UserName)
            .Item("command").List(properties->CommandLine)
            .Item("wrapper_version").Value(properties->ClientVersion)
        .EndMap()
        .DoIf(!pool.empty(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("pool").Value(pool);
        })
        .DoIf(options.SecureVault_.Defined(), [&] (TFluentMap fluentMap) {
            Y_ENSURE(options.SecureVault_->IsMap(),
                "SecureVault must be a map node, got " << options.SecureVault_->GetType());
            fluentMap.Item("secure_vault").Value(*options.SecureVault_);
        });
}

template <typename TSpec>
void BuildCommonUserOperationPart(const TSpec& baseSpec, TNode* spec)
{
    if (baseSpec.MaxFailedJobCount_.Defined()) {
        (*spec)["max_failed_job_count"] = *baseSpec.MaxFailedJobCount_;
    }
    if (baseSpec.FailOnJobRestart_.Defined()) {
        (*spec)["fail_on_job_restart"] = *baseSpec.FailOnJobRestart_;
    }
    if (baseSpec.StderrTablePath_.Defined()) {
        (*spec)["stderr_table_path"] = *baseSpec.StderrTablePath_;
    }
    if (baseSpec.CoreTablePath_.Defined()) {
        (*spec)["core_table_path"] = *baseSpec.CoreTablePath_;
    }
}

template <typename TSpec>
void BuildJobCountOperationPart(const TSpec& spec, TNode* nodeSpec)
{
    if (spec.JobCount_.Defined()) {
        (*nodeSpec)["job_count"] = *spec.JobCount_;
    }
    if (spec.DataSizePerJob_.Defined()) {
        (*nodeSpec)["data_size_per_job"] = *spec.DataSizePerJob_;
    }
}

template <typename TSpec>
void BuildPartitionCountOperationPart(const TSpec& spec, TNode* nodeSpec)
{
    if (spec.PartitionCount_.Defined()) {
        (*nodeSpec)["partition_count"] = *spec.PartitionCount_;
    }
    if (spec.PartitionDataSize_.Defined()) {
        (*nodeSpec)["partition_data_size"] = *spec.PartitionDataSize_;
    }
}

template <typename TSpec>
void BuildDataSizePerSortJobPart(const TSpec& spec, TNode* nodeSpec)
{
    if (spec.DataSizePerSortJob_.Defined()) {
        (*nodeSpec)["data_size_per_sort_job"] = *spec.DataSizePerSortJob_;
    }
}

template <typename TSpec>
void BuildPartitionJobCountOperationPart(const TSpec& spec, TNode* nodeSpec)
{
    if (spec.PartitionJobCount_.Defined()) {
        (*nodeSpec)["partition_job_count"] = *spec.PartitionJobCount_;
    }
    if (spec.DataSizePerPartitionJob_.Defined()) {
        (*nodeSpec)["data_size_per_partition_job"] = *spec.DataSizePerPartitionJob_;
    }
}

template <typename TSpec>
void BuildMapJobCountOperationPart(const TSpec& spec, TNode* nodeSpec)
{
    if (spec.MapJobCount_.Defined()) {
        (*nodeSpec)["map_job_count"] = *spec.MapJobCount_;
    }
    if (spec.DataSizePerMapJob_.Defined()) {
        (*nodeSpec)["data_size_per_map_job"] = *spec.DataSizePerMapJob_;
    }
}

template <typename TSpec>
void BuildIntermediateDataReplicationFactorPart(const TSpec& spec, TNode* nodeSpec)
{
    if (spec.IntermediateDataReplicationFactor_.Defined()) {
        (*nodeSpec)["intermediate_data_replication_factor"] = *spec.IntermediateDataReplicationFactor_;
    }
}

////////////////////////////////////////////////////////////////////////////////

TNode MergeSpec(TNode dst, const TOperationOptions& options)
{
    MergeNodes(dst["spec"], TConfig::Get()->Spec);
    if (options.Spec_) {
        MergeNodes(dst["spec"], *options.Spec_);
    }
    return dst;
}

template <typename TSpec>
void CreateDebugOutputTables(const TSpec& spec, const TAuth& auth)
{
    if (spec.StderrTablePath_.Defined()) {
        NYT::NDetail::Create(auth, TTransactionId(), *spec.StderrTablePath_, NT_TABLE,
            TCreateOptions()
                .IgnoreExisting(true)
                .Recursive(true));
    }
    if (spec.CoreTablePath_.Defined()) {
        NYT::NDetail::Create(auth, TTransactionId(), *spec.CoreTablePath_, NT_TABLE,
            TCreateOptions()
                .IgnoreExisting(true)
                .Recursive(true));
    }
}

void CreateOutputTable(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TRichYPath& path)
{
    Y_ENSURE(path.Path_, "Output table is not set");
    NYT::NDetail::Create(auth, transactionId, path.Path_, NT_TABLE,
        TCreateOptions()
            .IgnoreExisting(true)
            .Recursive(true));
}

void CreateOutputTables(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TVector<TRichYPath>& paths)
{
    Y_ENSURE(!paths.empty(), "Output tables are not set");
    for (auto& path : paths) {
        CreateOutputTable(auth, transactionId, path);
    }
}

void CheckInputTablesExist(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TVector<TRichYPath>& paths)
{
    Y_ENSURE(!paths.empty(), "Input tables are not set");
    for (auto& path : paths) {
        auto curTransactionId =  path.TransactionId_.GetOrElse(transactionId);
        Y_ENSURE_EX(NYT::NDetail::Exists(auth, curTransactionId, path.Path_),
            TApiUsageError() << "Input table '" << path.Path_ << "' doesn't exist");
    }
}

void LogJob(const TOperationId& opId, const IJob* job, const char* type)
{
    if (job) {
        LOG_INFO("Operation %s; %s = %s",
            GetGuidAsString(opId).data(), type, TJobFactory::Get()->GetJobName(job).data());
    }
}

TString DumpYPath(const TRichYPath& path)
{
    TStringStream stream;
    TYsonWriter writer(&stream, YF_TEXT, YT_NODE);
    Serialize(path, &writer);
    return stream.Str();
}

void LogYPaths(const TOperationId& opId, const TVector<TRichYPath>& paths, const char* type)
{
    for (size_t i = 0; i < paths.size(); ++i) {
        LOG_INFO("Operation %s; %s[%" PRISZT "] = %s",
            GetGuidAsString(opId).data(), type, i, DumpYPath(paths[i]).data());
    }
}

void LogYPath(const TOperationId& opId, const TRichYPath& output, const char* type)
{
    LOG_INFO("Operation %s; %s = %s",
        GetGuidAsString(opId).data(), type, DumpYPath(output).data());
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TOperationId DoExecuteMap(
    TOperationPreparer& preparer,
    const TSimpleOperationIo& operationIo,
    const TMapOperationSpecBase<T>& spec,
    const IJob& mapper,
    const TOperationOptions& options)
{
    if (options.CreateDebugOutputTables_) {
        CreateDebugOutputTables(spec, preparer.GetAuth());
    }
    if (options.CreateOutputTables_) {
        CheckInputTablesExist(preparer.GetAuth(), preparer.GetTransactionId(), operationIo.Inputs);
        CreateOutputTables(preparer.GetAuth(), preparer.GetTransactionId(), operationIo.Outputs);
    }

    TJobPreparer map(
        preparer,
        spec.MapperSpec_,
        mapper,
        operationIo.Outputs.size(),
        operationIo.JobFiles,
        options);

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("mapper").DoMap(std::bind(
            BuildUserJobFluently1,
            std::cref(map),
            operationIo.InputFormat,
            operationIo.OutputFormat,
            std::placeholders::_1))
        .Item("input_table_paths").List(operationIo.Inputs)
        .Item("output_table_paths").List(operationIo.Outputs)
        .DoIf(spec.Ordered_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("ordered").Value(spec.Ordered_.GetRef());
        })
        .Do(std::bind(BuildCommonOperationPart, options, std::placeholders::_1))
    .EndMap().EndMap();

    specNode["spec"]["job_io"]["control_attributes"]["enable_row_index"] = TNode(true);
    if (!TConfig::Get()->TableWriter.Empty()) {
        specNode["spec"]["job_io"]["table_writer"] = TConfig::Get()->TableWriter;
    }
    specNode["spec"]["title"] = TNode(map.GetClassName());

    BuildCommonUserOperationPart(spec, &specNode["spec"]);
    BuildJobCountOperationPart(spec, &specNode["spec"]);

    auto operationId = preparer.StartOperation(
        "map",
        MergeSpec(std::move(specNode), options));

    LogJob(operationId, &mapper, "mapper");
    LogYPaths(operationId, operationIo.Inputs, "input");
    LogYPaths(operationId, operationIo.Outputs, "output");

    return operationId;
}

TOperationId ExecuteMap(
    TOperationPreparer& preparer,
    const TMapOperationSpec& spec,
    const IStructuredJob& mapper,
    const TOperationOptions& options)
{
    LOG_DEBUG("Starting map operation");
    return DoExecuteMap(
        preparer,
        CreateSimpleOperationIo(mapper, preparer.GetAuth(), preparer.GetTransactionId(), spec, options, /* allowSkiff = */ true),
        spec,
        mapper,
        options);
}

TOperationId ExecuteRawMap(
    TOperationPreparer& preparer,
    const TRawMapOperationSpec& spec,
    const IRawJob& mapper,
    const TOperationOptions& options)
{
    LOG_DEBUG("Starting raw map operation");
    return DoExecuteMap(
        preparer,
        CreateSimpleOperationIo(preparer.GetAuth(), spec),
        spec,
        mapper,
        options);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TOperationId DoExecuteReduce(
    TOperationPreparer& preparer,
    const TSimpleOperationIo& operationIo,
    const TReduceOperationSpecBase<T>& spec,
    const IJob& reducer,
    const TOperationOptions& options)
{
    if (options.CreateDebugOutputTables_) {
        CreateDebugOutputTables(spec, preparer.GetAuth());
    }
    if (options.CreateOutputTables_) {
        CheckInputTablesExist(preparer.GetAuth(), preparer.GetTransactionId(), operationIo.Inputs);
        CreateOutputTables(preparer.GetAuth(), preparer.GetTransactionId(), operationIo.Outputs);
    }

    TJobPreparer reduce(
        preparer,
        spec.ReducerSpec_,
        reducer,
        operationIo.Outputs.size(),
        operationIo.JobFiles,
        options);

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("reducer").DoMap(std::bind(
            BuildUserJobFluently1,
            std::cref(reduce),
            operationIo.InputFormat,
            operationIo.OutputFormat,
            std::placeholders::_1))
        .Item("sort_by").Value(spec.SortBy_)
        .Item("reduce_by").Value(spec.ReduceBy_)
        .DoIf(spec.JoinBy_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("join_by").Value(spec.JoinBy_.GetRef());
        })
        .DoIf(spec.EnableKeyGuarantee_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("enable_key_guarantee").Value(spec.EnableKeyGuarantee_.GetRef());
        })
        .Item("input_table_paths").List(operationIo.Inputs)
        .Item("output_table_paths").List(operationIo.Outputs)
        .Item("job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_key_switch").Value(true)
                .Item("enable_row_index").Value(true)
            .EndMap()
            .DoIf(!TConfig::Get()->TableWriter.Empty(), [&] (TFluentMap fluent) {
                fluent.Item("table_writer").Value(TConfig::Get()->TableWriter);
            })
        .EndMap()
        .Item("title").Value(reduce.GetClassName())
        .Do(std::bind(BuildCommonOperationPart, options, std::placeholders::_1))
    .EndMap().EndMap();

    BuildCommonUserOperationPart(spec, &specNode["spec"]);
    BuildJobCountOperationPart(spec, &specNode["spec"]);

    auto operationId = preparer.StartOperation(
        "reduce",
        MergeSpec(std::move(specNode), options));

    LogJob(operationId, &reducer, "reducer");
    LogYPaths(operationId, operationIo.Inputs, "input");
    LogYPaths(operationId, operationIo.Outputs, "output");

    return operationId;
}

TOperationId ExecuteReduce(
    TOperationPreparer& preparer,
    const TReduceOperationSpec& spec,
    const IStructuredJob& reducer,
    const TOperationOptions& options)
{
    LOG_DEBUG("Starting reduce operation");
    return DoExecuteReduce(
        preparer,
        CreateSimpleOperationIo(reducer, preparer.GetAuth(), preparer.GetTransactionId(), spec, options, /* allowSkiff = */ false),
        spec,
        reducer,
        options);
}

TOperationId ExecuteRawReduce(
    TOperationPreparer& preparer,
    const TRawReduceOperationSpec& spec,
    const IRawJob& reducer,
    const TOperationOptions& options)
{
    LOG_DEBUG("Starting raw reduce operation");
    return DoExecuteReduce(
        preparer,
        CreateSimpleOperationIo(preparer.GetAuth(), spec),
        spec,
        reducer,
        options);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TOperationId DoExecuteJoinReduce(
    TOperationPreparer& preparer,
    const TSimpleOperationIo& operationIo,
    const TJoinReduceOperationSpecBase<T>& spec,
    const IJob& reducer,
    const TOperationOptions& options)
{
    if (options.CreateDebugOutputTables_) {
        CreateDebugOutputTables(spec, preparer.GetAuth());
    }
    if (options.CreateOutputTables_) {
        CheckInputTablesExist(preparer.GetAuth(), preparer.GetTransactionId(), operationIo.Inputs);
        CreateOutputTables(preparer.GetAuth(), preparer.GetTransactionId(), operationIo.Outputs);
    }

    TJobPreparer reduce(
        preparer,
        spec.ReducerSpec_,
        reducer,
        operationIo.Outputs.size(),
        operationIo.JobFiles,
        options);

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("reducer").DoMap(std::bind(
            BuildUserJobFluently1,
            std::cref(reduce),
            operationIo.InputFormat,
            operationIo.OutputFormat,
            std::placeholders::_1))
        .Item("join_by").Value(spec.JoinBy_)
        .Item("input_table_paths").List(operationIo.Inputs)
        .Item("output_table_paths").List(operationIo.Outputs)
        .Item("job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_key_switch").Value(true)
                .Item("enable_row_index").Value(true)
            .EndMap()
            .DoIf(!TConfig::Get()->TableWriter.Empty(), [&] (TFluentMap fluent) {
                fluent.Item("table_writer").Value(TConfig::Get()->TableWriter);
            })
        .EndMap()
        .Item("title").Value(reduce.GetClassName())
        .Do(std::bind(BuildCommonOperationPart, options, std::placeholders::_1))
    .EndMap().EndMap();

    BuildCommonUserOperationPart(spec, &specNode["spec"]);
    BuildJobCountOperationPart(spec, &specNode["spec"]);

    auto operationId = preparer.StartOperation(
        "join_reduce",
        MergeSpec(std::move(specNode), options));

    LogJob(operationId, &reducer, "reducer");
    LogYPaths(operationId, operationIo.Inputs, "input");
    LogYPaths(operationId, operationIo.Outputs, "output");

    return operationId;
}

TOperationId ExecuteJoinReduce(
    TOperationPreparer& preparer,
    const TJoinReduceOperationSpec& spec,
    const IStructuredJob& reducer,
    const TOperationOptions& options)
{
    LOG_DEBUG("Starting join reduce operation");
    return DoExecuteJoinReduce(
        preparer,
        CreateSimpleOperationIo(reducer, preparer.GetAuth(), preparer.GetTransactionId(), spec, options, /* allowSkiff = */ false),
        spec,
        reducer,
        options);
}

TOperationId ExecuteRawJoinReduce(
    TOperationPreparer& preparer,
    const TRawJoinReduceOperationSpec& spec,
    const IRawJob& reducer,
    const TOperationOptions& options)
{
    LOG_DEBUG("Starting raw join reduce operation");
    return DoExecuteJoinReduce(
        preparer,
        CreateSimpleOperationIo(preparer.GetAuth(), spec),
        spec,
        reducer,
        options);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TOperationId DoExecuteMapReduce(
    TOperationPreparer& preparer,
    const TMapReduceOperationIo& operationIo,
    const TMapReduceOperationSpecBase<T>& spec,
    const IJob* mapper,
    const IJob* reduceCombiner,
    const IJob& reducer,
    const TOperationOptions& options)
{
    TVector<TRichYPath> allOutputs;
    allOutputs.insert(allOutputs.end(), operationIo.MapOutputs.begin(), operationIo.MapOutputs.end());
    allOutputs.insert(allOutputs.end(), operationIo.Outputs.begin(), operationIo.Outputs.end());

    if (options.CreateDebugOutputTables_) {
        CreateDebugOutputTables(spec, preparer.GetAuth());
    }
    if (options.CreateOutputTables_) {
        CheckInputTablesExist(preparer.GetAuth(), preparer.GetTransactionId(), operationIo.Inputs);
        CreateOutputTables(preparer.GetAuth(), preparer.GetTransactionId(), allOutputs);
    }

    TKeyColumns sortBy = spec.SortBy_;
    TKeyColumns reduceBy = spec.ReduceBy_;

    if (sortBy.Parts_.empty()) {
        sortBy = reduceBy;
    }

    const bool hasMapper = mapper != nullptr;
    const bool hasCombiner = reduceCombiner != nullptr;

    TVector<TRichYPath> files;

    TJobPreparer reduce(
        preparer,
        spec.ReducerSpec_,
        reducer,
        operationIo.Outputs.size(),
        operationIo.ReducerJobFiles,
        options);

    TString title;

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .DoIf(hasMapper, [&] (TFluentMap fluent) {
            TJobPreparer map(
                preparer,
                spec.MapperSpec_,
                *mapper,
                1 + operationIo.MapOutputs.size(),
                operationIo.MapperJobFiles,
                options);
            fluent.Item("mapper").DoMap(std::bind(
                BuildUserJobFluently1,
                std::cref(map),
                *operationIo.MapperInputFormat,
                *operationIo.MapperOutputFormat,
                std::placeholders::_1));

            title = "mapper:" + map.GetClassName() + " ";
        })
        .DoIf(hasCombiner, [&] (TFluentMap fluent) {
            TJobPreparer combine(
                preparer,
                spec.ReduceCombinerSpec_,
                *reduceCombiner,
                size_t(1),
                operationIo.ReduceCombinerJobFiles,
                options);
            fluent.Item("reduce_combiner").DoMap(std::bind(
                BuildUserJobFluently1,
                std::cref(combine),
                *operationIo.ReduceCombinerInputFormat,
                *operationIo.ReduceCombinerOutputFormat,
                std::placeholders::_1));
            title += "combiner:" + combine.GetClassName() + " ";
        })
        .Item("reducer").DoMap(std::bind(
            BuildUserJobFluently1,
            std::cref(reduce),
            operationIo.ReducerInputFormat,
            operationIo.ReducerOutputFormat,
            std::placeholders::_1))
        .Item("sort_by").Value(sortBy)
        .Item("reduce_by").Value(reduceBy)
        .Item("input_table_paths").List(operationIo.Inputs)
        .Item("output_table_paths").List(allOutputs)
        .Item("mapper_output_table_count").Value(operationIo.MapOutputs.size())
        .Item("map_job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_row_index").Value(true)
            .EndMap()
            .DoIf(!TConfig::Get()->TableWriter.Empty(), [&] (TFluentMap fluent) {
                fluent.Item("table_writer").Value(TConfig::Get()->TableWriter);
            })
        .EndMap()
        .Item("sort_job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_key_switch").Value(true)
            .EndMap()
            .DoIf(!TConfig::Get()->TableWriter.Empty(), [&] (TFluentMap fluent) {
                fluent.Item("table_writer").Value(TConfig::Get()->TableWriter);
            })
        .EndMap()
        .Item("reduce_job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_key_switch").Value(true)
            .EndMap()
            .DoIf(!TConfig::Get()->TableWriter.Empty(), [&] (TFluentMap fluent) {
                fluent.Item("table_writer").Value(TConfig::Get()->TableWriter);
            })
        .EndMap()
        .Item("title").Value(title + "reducer:" + reduce.GetClassName())
        .Do(std::bind(BuildCommonOperationPart, options, std::placeholders::_1))
    .EndMap().EndMap();

    if (spec.Ordered_) {
        specNode["spec"]["ordered"] = *spec.Ordered_;
    }

    BuildCommonUserOperationPart(spec, &specNode["spec"]);
    BuildMapJobCountOperationPart(spec, &specNode["spec"]);
    BuildPartitionCountOperationPart(spec, &specNode["spec"]);
    BuildIntermediateDataReplicationFactorPart(spec, &specNode["spec"]);
    BuildDataSizePerSortJobPart(spec, &specNode["spec"]);

    auto operationId = preparer.StartOperation(
        "map_reduce",
        MergeSpec(std::move(specNode), options));

    LogJob(operationId, mapper, "mapper");
    LogJob(operationId, reduceCombiner, "reduce_combiner");
    LogJob(operationId, &reducer, "reducer");
    LogYPaths(operationId, operationIo.Inputs, "input");
    LogYPaths(operationId, allOutputs, "output");

    return operationId;
}

TOperationId ExecuteMapReduce(
    TOperationPreparer& preparer,
    const TMapReduceOperationSpec& spec_,
    const IStructuredJob* mapper,
    const IStructuredJob* reduceCombiner,
    const IStructuredJob& reducer,
    const TOperationOptions& options)
{
    LOG_DEBUG("Starting map-reduce operation");
    TMapReduceOperationSpec spec = spec_;

    TMapReduceOperationIo operationIo;
    const auto structuredInputs = CanonizeStructuredTableList(preparer.GetAuth(), spec.GetStructuredInputs());
    const auto structuredMapOutputs = CanonizeStructuredTableList(preparer.GetAuth(), spec.GetStructuredMapOutputs());
    const auto structuredOutputs = CanonizeStructuredTableList(preparer.GetAuth(), spec.GetStructuredOutputs());

    const bool inferOutputSchema = options.InferOutputSchema_.GetOrElse(TConfig::Get()->InferTableSchema);

    operationIo.Inputs = GetPathList(structuredInputs, /*inferSchema*/ false);
    operationIo.MapOutputs = GetPathList(structuredMapOutputs, inferOutputSchema);
    operationIo.Outputs = GetPathList(structuredOutputs, inferOutputSchema);

    VerifyHasElements(operationIo.Inputs, "inputs");
    VerifyHasElements(operationIo.Outputs, "outputs");

    auto fixSpec = [&](const TFormat& format) {
        if (format.IsYamredDsv()) {
            spec.SortBy_.Parts_.clear();
            spec.ReduceBy_.Parts_.clear();

            const TYamredDsvAttributes attributes = format.GetYamredDsvAttributes();
            for (auto& column : attributes.KeyColumnNames) {
                spec.SortBy_.Parts_.push_back(column);
                spec.ReduceBy_.Parts_.push_back(column);
            }
            for (const auto& column : attributes.SubkeyColumnNames) {
                spec.SortBy_.Parts_.push_back(column);
            }
        }
    };

    TFormatBuilder formatBuilder(
       preparer.GetAuth(),
       preparer.GetTransactionId(),
       options);

    if (mapper) {
        auto nodeReaderFormat = NodeReaderFormatFromHintAndGlobalConfig(spec.MapperFormatHints_);

        auto [inputFormat, inputFormatConfig] = formatBuilder.CreateFormat(
           *mapper,
           EIODirection::Input,
           structuredInputs,
           spec.MapperFormatHints_.InputFormatHints_,
           nodeReaderFormat,
           /* allowFormatFromTableAttribute */ true);

        auto mapperOutputDescription =
            spec.GetIntermediateMapOutputDescription()
            .GetOrElse(TUnspecifiedTableStructure());
        TStructuredJobTableList mapperOutput = {
            TStructuredJobTable::Intermediate(mapperOutputDescription),
        };
        for (const auto& table : structuredMapOutputs) {
            mapperOutput.push_back(TStructuredJobTable{table.Description, table.RichYPath});
        }

        auto [outputFormat, outputFormatConfig] = formatBuilder.CreateFormat(
            *mapper,
            EIODirection::Output,
            mapperOutput,
            spec.MapperFormatHints_.OutputFormatHints_,
            ENodeReaderFormat::Yson,
            /* allowFormatFromTableAttribute */ false);

        operationIo.MapperJobFiles = CreateFormatConfig(inputFormatConfig, outputFormatConfig);
        operationIo.MapperInputFormat = inputFormat;
        operationIo.MapperOutputFormat = outputFormat;
    }

    if (reduceCombiner) {
        const bool isFirstStep = !mapper;
        TStructuredJobTableList inputs;
        if (isFirstStep) {
            inputs = structuredInputs;
        } else {
            auto reduceCombinerIntermediateInput =
                spec.GetIntermediateReduceCombinerInputDescription()
                .GetOrElse(TUnspecifiedTableStructure());
            inputs = {
                TStructuredJobTable::Intermediate(reduceCombinerIntermediateInput),
            };
        }
        auto [inputFormat, inputFormatConfig] = formatBuilder.CreateFormat(
            *reduceCombiner,
            EIODirection::Input,
            inputs,
            spec.ReduceCombinerFormatHints_.InputFormatHints_,
            ENodeReaderFormat::Yson,
            /* allowFormatFromTableAttribute = */ isFirstStep);

        auto reduceCombinerOutputDescription = spec.GetIntermediateReduceCombinerOutputDescription()
            .GetOrElse(TUnspecifiedTableStructure());

        TStructuredJobTableList outputs = {
            TStructuredJobTable::Intermediate(reduceCombinerOutputDescription),
        };
        auto [outputFormat, outputFormatConfig] = formatBuilder.CreateFormat(
            *reduceCombiner,
            EIODirection::Output,
            outputs,
            spec.ReduceCombinerFormatHints_.OutputFormatHints_,
            ENodeReaderFormat::Yson,
            /* allowFormatFromTableAttribute = */ false);

        operationIo.ReduceCombinerJobFiles = CreateFormatConfig(inputFormatConfig, outputFormatConfig);
        operationIo.ReduceCombinerInputFormat = inputFormat;
        operationIo.ReduceCombinerOutputFormat = outputFormat;

        if (isFirstStep) {
            fixSpec(*operationIo.ReduceCombinerInputFormat);
        }
    }

    const bool isFirstStep = (!mapper && !reduceCombiner);
    TStructuredJobTableList reducerInputs;
    if (isFirstStep) {
        reducerInputs = structuredInputs;
    } else {
        auto reducerInputDescription =
            spec.GetIntermediateReducerInputDescription()
            .GetOrElse(TUnspecifiedTableStructure());
        reducerInputs = {
            TStructuredJobTable::Intermediate(reducerInputDescription),
        };
    }
    auto [inputFormat, inputFormatConfig] = formatBuilder.CreateFormat(
        reducer,
        EIODirection::Input,
        reducerInputs,
        spec.ReducerFormatHints_.InputFormatHints_,
        ENodeReaderFormat::Yson,
        /* allowFormatFromTableAttribute = */ isFirstStep);

    auto [outputFormat, outputFormatConfig] = formatBuilder.CreateFormat(
        reducer,
        EIODirection::Output,
        ToStructuredJobTableList(spec.GetStructuredOutputs()),
        spec.ReducerFormatHints_.OutputFormatHints_,
        ENodeReaderFormat::Yson,
        /* allowFormatFromTableAttribute = */ false);
    operationIo.ReducerJobFiles = CreateFormatConfig(inputFormatConfig, outputFormatConfig);
    operationIo.ReducerInputFormat = inputFormat;
    operationIo.ReducerOutputFormat = outputFormat;

    if (isFirstStep) {
        fixSpec(operationIo.ReducerInputFormat);
    }

    return DoExecuteMapReduce(
        preparer,
        operationIo,
        spec,
        mapper,
        reduceCombiner,
        reducer,
        options);
}

TOperationId ExecuteRawMapReduce(
    TOperationPreparer& preparer,
    const TRawMapReduceOperationSpec& spec,
    const IRawJob* mapper,
    const IRawJob* reduceCombiner,
    const IRawJob& reducer,
    const TOperationOptions& options)
{
    LOG_DEBUG("Starting raw map-reduce operation");
    TMapReduceOperationIo operationIo;
    operationIo.Inputs = CanonizePaths(preparer.GetAuth(), spec.GetInputs());
    operationIo.MapOutputs = CanonizePaths(preparer.GetAuth(), spec.GetMapOutputs());
    operationIo.Outputs = CanonizePaths(preparer.GetAuth(), spec.GetOutputs());

    VerifyHasElements(operationIo.Inputs, "inputs");
    VerifyHasElements(operationIo.Outputs, "outputs");

    auto getFormatOrDefault = [&] (const TMaybe<TFormat>& maybeFormat, const TMaybe<TFormat> stageDefaultFormat, const char* formatName) {
        if (maybeFormat) {
            return *maybeFormat;
        } else if (stageDefaultFormat) {
            return *stageDefaultFormat;
        } else {
            ythrow TApiUsageError() << "Cannot derive " << formatName;
        }
    };

    if (mapper) {
        operationIo.MapperInputFormat = getFormatOrDefault(spec.MapperInputFormat_, spec.MapperFormat_, "mapper input format");
        operationIo.MapperOutputFormat = getFormatOrDefault(spec.MapperOutputFormat_, spec.MapperFormat_, "mapper output format");
    }

    if (reduceCombiner) {
        operationIo.ReduceCombinerInputFormat = getFormatOrDefault(spec.ReduceCombinerInputFormat_, spec.ReduceCombinerFormat_, "reduce combiner input format");
        operationIo.ReduceCombinerOutputFormat = getFormatOrDefault(spec.ReduceCombinerOutputFormat_, spec.ReduceCombinerFormat_, "reduce combiner output format");
    }

    operationIo.ReducerInputFormat = getFormatOrDefault(spec.ReducerInputFormat_, spec.ReducerFormat_, "reducer input format");
    operationIo.ReducerOutputFormat = getFormatOrDefault(spec.ReducerOutputFormat_, spec.ReducerFormat_, "reducer output format");

    return DoExecuteMapReduce(
        preparer,
        operationIo,
        spec,
        mapper,
        reduceCombiner,
        reducer,
        options);
}

TOperationId ExecuteSort(
    TOperationPreparer& preparer,
    const TSortOperationSpec& spec,
    const TOperationOptions& options)
{
    LOG_DEBUG("Starting sort operation");
    auto inputs = CanonizePaths(preparer.GetAuth(), spec.Inputs_);
    auto output = CanonizePath(preparer.GetAuth(), spec.Output_);

    if (options.CreateOutputTables_) {
        CheckInputTablesExist(preparer.GetAuth(), preparer.GetTransactionId(), inputs);
        CreateOutputTable(preparer.GetAuth(), preparer.GetTransactionId(), output);
    }

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("input_table_paths").List(inputs)
        .Item("output_table_path").Value(output)
        .Item("sort_by").Value(spec.SortBy_)
        .Do(std::bind(BuildCommonOperationPart, options, std::placeholders::_1))
    .EndMap().EndMap();

    BuildPartitionCountOperationPart(spec, &specNode["spec"]);
    BuildPartitionJobCountOperationPart(spec, &specNode["spec"]);
    BuildIntermediateDataReplicationFactorPart(spec, &specNode["spec"]);

    auto operationId = preparer.StartOperation(
        "sort",
        MergeSpec(std::move(specNode), options));

    LogYPaths(operationId, inputs, "input");
    LogYPath(operationId, output, "output");

    return operationId;
}

TOperationId ExecuteMerge(
    TOperationPreparer& preparer,
    const TMergeOperationSpec& spec,
    const TOperationOptions& options)
{
    LOG_DEBUG("Starting merge operation");
    auto inputs = CanonizePaths(preparer.GetAuth(), spec.Inputs_);
    auto output = CanonizePath(preparer.GetAuth(), spec.Output_);

    if (options.CreateOutputTables_) {
        CheckInputTablesExist(preparer.GetAuth(), preparer.GetTransactionId(), inputs);
        CreateOutputTable(preparer.GetAuth(), preparer.GetTransactionId(), output);
    }

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("input_table_paths").List(inputs)
        .Item("output_table_path").Value(output)
        .Item("mode").Value(::ToString(spec.Mode_))
        .Item("combine_chunks").Value(spec.CombineChunks_)
        .Item("force_transform").Value(spec.ForceTransform_)
        .Item("merge_by").Value(spec.MergeBy_)
        .Do(std::bind(BuildCommonOperationPart, options, std::placeholders::_1))
    .EndMap().EndMap();

    BuildJobCountOperationPart(spec, &specNode["spec"]);

    auto operationId = preparer.StartOperation(
        "merge",
        MergeSpec(std::move(specNode), options));

    LogYPaths(operationId, inputs, "input");
    LogYPath(operationId, output, "output");

    return operationId;
}

TOperationId ExecuteErase(
    TOperationPreparer& preparer,
    const TEraseOperationSpec& spec,
    const TOperationOptions& options)
{
    LOG_DEBUG("Starting erase operation");
    auto tablePath = CanonizePath(preparer.GetAuth(), spec.TablePath_);

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("table_path").Value(tablePath)
        .Item("combine_chunks").Value(spec.CombineChunks_)
        .Do(std::bind(BuildCommonOperationPart, options, std::placeholders::_1))
    .EndMap().EndMap();

    auto operationId = preparer.StartOperation(
        "erase",
        MergeSpec(std::move(specNode), options));

    LogYPath(operationId, tablePath, "table_path");

    return operationId;
}

TOperationId ExecuteRemoteCopy(
    TOperationPreparer& preparer,
    const TRemoteCopyOperationSpec& spec,
    const TOperationOptions& options)
{
    LOG_DEBUG("Starting remote copy operation");
    auto inputs = CanonizePaths(preparer.GetAuth(), spec.Inputs_);
    auto output = CanonizePath(preparer.GetAuth(), spec.Output_);

    if (options.CreateOutputTables_) {
        CreateOutputTable(preparer.GetAuth(), preparer.GetTransactionId(), output);
    }

    Y_ENSURE_EX(!spec.ClusterName_.empty(), TApiUsageError() << "ClusterName parameter is required");

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("cluster_name").Value(spec.ClusterName_)
        .Item("input_table_paths").List(inputs)
        .Item("output_table_path").Value(output)
        .DoIf(spec.NetworkName_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("network_name").Value(*spec.NetworkName_);
        })
        .DoIf(spec.SchemaInferenceMode_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("schema_inference_mode").Value(::ToString(*spec.SchemaInferenceMode_));
        })
        .Item("copy_attributes").Value(spec.CopyAttributes_)
        .DoIf(!spec.AttributeKeys_.empty(), [&] (TFluentMap fluent) {
            Y_ENSURE_EX(spec.CopyAttributes_, TApiUsageError() <<
                "Specifying nonempty AttributeKeys in RemoteCopy "
                "doesn't make sense without CopyAttributes == true");
            fluent.Item("attribute_keys").Value(spec.AttributeKeys_);
        })
        .Do(std::bind(BuildCommonOperationPart, options, std::placeholders::_1))
    .EndMap().EndMap();

    auto operationId = preparer.StartOperation(
        "remote_copy",
        MergeSpec(specNode, options));

    LogYPaths(operationId, inputs, "input");
    LogYPath(operationId, output, "output");

    return operationId;
}

TOperationId ExecuteVanilla(
    TOperationPreparer& preparer,
    const TVanillaOperationSpec& spec,
    const TOperationOptions& options)
{
    LOG_DEBUG("Starting vanilla operation");

    auto addTask = [&](TFluentMap fluent, const TVanillaTask& task) {
        TJobPreparer jobPreparer(
            preparer,
            task.Spec_,
            *task.Job_,
            /* outputTableCount = */ 0,
            /* smallFileList = */ {},
            options);
        fluent
            .Item(task.Name_).BeginMap()
                .Item("job_count").Value(task.JobCount_)
                .Do(std::bind(
                    BuildUserJobFluently1,
                    std::cref(jobPreparer),
                    /* inputFormat = */ Nothing(),
                    /* outputFormat = */ Nothing(),
                    std::placeholders::_1))
            .EndMap();
    };

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("tasks").DoMapFor(spec.Tasks_, addTask)
        .Do(std::bind(BuildCommonOperationPart, options, std::placeholders::_1))
    .EndMap().EndMap();

    BuildCommonUserOperationPart(spec, &specNode["spec"]);

    auto operationId = preparer.StartOperation(
        "vanilla",
        MergeSpec(std::move(specNode), options),
        /* useStartOperationRequest = */ true);

    return operationId;
}

////////////////////////////////////////////////////////////////////////////////

class TOperation::TOperationImpl
    : public TThrRefBase
{
public:
    TOperationImpl(TAuth auth, const TOperationId& operationId)
        : Auth_(std::move(auth))
        , Id_(operationId)
    { }

    const TOperationId& GetId() const;
    NThreading::TFuture<void> Watch(TYtPoller& ytPoller);

    EOperationBriefState GetBriefState();
    TMaybe<TYtError> GetError();
    TJobStatistics GetJobStatistics();
    TMaybe<TOperationBriefProgress> GetBriefProgress();
    void AbortOperation();
    void CompleteOperation();
    TOperationAttributes GetAttributes(const TGetOperationOptions& options);
    void UpdateParameters(const TUpdateOperationParametersOptions& options);
    TJobAttributes GetJob(const TJobId& jobId, const TGetJobOptions& options);
    TListJobsResult ListJobs(const TListJobsOptions& options);

    void AsyncFinishOperation(TOperationAttributes operationAttributes);
    void FinishWithException(std::exception_ptr exception);
    void UpdateBriefProgress(TMaybe<TOperationBriefProgress> briefProgress);

private:
    void UpdateAttributesAndCall(bool needJobStatistics, std::function<void(const TOperationAttributes&)> func);

    void SyncFinishOperationImpl(const TOperationAttributes&);
    static void* SyncFinishOperationProc(void* );

private:
    const TAuth Auth_;
    const TOperationId Id_;
    TMutex Lock_;
    TMaybe<NThreading::TPromise<void>> CompletePromise_;
    TOperationAttributes Attributes_;
};

////////////////////////////////////////////////////////////////////////////////

class TOperationPollerItem
    : public IYtPollerItem
{
public:
    TOperationPollerItem(::TIntrusivePtr<TOperation::TOperationImpl> operationImpl)
        : OperationImpl_(std::move(operationImpl))
    { }

    void PrepareRequest(TRawBatchRequest* batchRequest) override
    {
        OperationState_ = batchRequest->GetOperation(
            OperationImpl_->GetId(),
            TGetOperationOptions().AttributeFilter(TOperationAttributeFilter()
                .Add(EOperationAttribute::State)
                .Add(EOperationAttribute::BriefProgress)
                .Add(EOperationAttribute::Result)));
    }

    EStatus OnRequestExecuted() override
    {
        try {
            const auto& attributes = OperationState_.GetValue();
            Y_VERIFY(attributes.BriefState);
            if (*attributes.BriefState != EOperationBriefState::InProgress) {
                OperationImpl_->AsyncFinishOperation(attributes);
                return PollBreak;
            } else {
                OperationImpl_->UpdateBriefProgress(attributes.BriefProgress);
            }
        } catch (const TErrorResponse& e) {
            if (!IsRetriable(e)) {
                OperationImpl_->FinishWithException(std::current_exception());
                return PollBreak;
            }
        } catch (const yexception& e) {
            OperationImpl_->FinishWithException(std::current_exception());
            return PollBreak;
        }
        return PollContinue;
    }

private:
    ::TIntrusivePtr<TOperation::TOperationImpl> OperationImpl_;
    NThreading::TFuture<TOperationAttributes> OperationState_;
};

////////////////////////////////////////////////////////////////////////////////

const TOperationId& TOperation::TOperationImpl::GetId() const
{
    return Id_;
}

NThreading::TFuture<void> TOperation::TOperationImpl::Watch(TYtPoller& ytPoller)
{
    auto guard = Guard(Lock_);

    if (!CompletePromise_) {
        CompletePromise_ = NThreading::NewPromise<void>();
        ytPoller.Watch(::MakeIntrusive<TOperationPollerItem>(this));
    }

    auto operationId = GetId();
    TAbortableRegistry::Get()->Add(operationId, ::MakeIntrusive<TOperationAbortable>(Auth_, operationId));
    auto registry = TAbortableRegistry::Get();
    // We have to own an IntrusivePtr to registry to prevent use-after-free
    auto removeOperation = [registry, operationId](const NThreading::TFuture<void>&) {
        registry->Remove(operationId);
    };
    CompletePromise_->GetFuture().Subscribe(removeOperation);

    return *CompletePromise_;
}

EOperationBriefState TOperation::TOperationImpl::GetBriefState()
{
    EOperationBriefState result = EOperationBriefState::InProgress;
    UpdateAttributesAndCall(false, [&] (const TOperationAttributes& attributes) {
        Y_VERIFY(attributes.BriefState);
        result = *attributes.BriefState;
    });
    return result;
}

TMaybe<TYtError> TOperation::TOperationImpl::GetError()
{
    TMaybe<TYtError> result;
    UpdateAttributesAndCall(false, [&] (const TOperationAttributes& attributes) {
        Y_VERIFY(attributes.Result);
        result = attributes.Result->Error;
    });
    return result;
}

TJobStatistics TOperation::TOperationImpl::GetJobStatistics()
{
    TJobStatistics result;
    UpdateAttributesAndCall(true, [&] (const TOperationAttributes& attributes) {
        if (attributes.Progress) {
            result = attributes.Progress->JobStatistics;
        }
    });
    return result;
}

TMaybe<TOperationBriefProgress> TOperation::TOperationImpl::GetBriefProgress()
{
    {
        auto g = Guard(Lock_);
        if (CompletePromise_.Defined()) {
            // Poller do this job for us
            return Attributes_.BriefProgress;
        }
    }
    TMaybe<TOperationBriefProgress> result;
    UpdateAttributesAndCall(false, [&] (const TOperationAttributes& attributes) {
        result = attributes.BriefProgress;
    });
    return result;
}

void TOperation::TOperationImpl::UpdateBriefProgress(TMaybe<TOperationBriefProgress> briefProgress)
{
    auto g = Guard(Lock_);
    Attributes_.BriefProgress = std::move(briefProgress);
}

void TOperation::TOperationImpl::UpdateAttributesAndCall(bool needJobStatistics, std::function<void(const TOperationAttributes&)> func)
{
    {
        auto g = Guard(Lock_);
        if (Attributes_.BriefState
            && *Attributes_.BriefState != EOperationBriefState::InProgress
            && (!needJobStatistics || Attributes_.Progress))
        {
            func(Attributes_);
            return;
        }
    }

    TOperationAttributes attributes = NDetail::GetOperation(
        Auth_,
        Id_,
        TGetOperationOptions().AttributeFilter(TOperationAttributeFilter()
            .Add(EOperationAttribute::Result)
            .Add(EOperationAttribute::Progress)
            .Add(EOperationAttribute::State)
            .Add(EOperationAttribute::BriefProgress)));

    func(attributes);

    Y_ENSURE(attributes.BriefState);
    if (*attributes.BriefState != EOperationBriefState::InProgress) {
        auto g = Guard(Lock_);
        Attributes_ = std::move(attributes);
    }
}

void TOperation::TOperationImpl::FinishWithException(std::exception_ptr e)
{
    CompletePromise_->SetException(std::move(e));
}

void TOperation::TOperationImpl::AbortOperation() {
    NYT::NDetail::AbortOperation(Auth_, Id_);
}

void TOperation::TOperationImpl::CompleteOperation() {
    NYT::NDetail::CompleteOperation(Auth_, Id_);
}

TOperationAttributes TOperation::TOperationImpl::GetAttributes(const TGetOperationOptions& options) {
    return NYT::NDetail::GetOperation(Auth_, Id_, options);
}

void TOperation::TOperationImpl::UpdateParameters(const TUpdateOperationParametersOptions& options)
{
    return NYT::NDetail::UpdateOperationParameters(Auth_, Id_, options);
}

TJobAttributes TOperation::TOperationImpl::GetJob(const TJobId& jobId, const TGetJobOptions& options)
{
    return NYT::NDetail::GetJob(Auth_, Id_, jobId, options);
}

TListJobsResult TOperation::TOperationImpl::ListJobs(const TListJobsOptions& options)
{
    return NYT::NDetail::ListJobs(Auth_, Id_, options);
}

struct TAsyncFinishOperationsArgs
{
    ::TIntrusivePtr<TOperation::TOperationImpl> OperationImpl;
    TOperationAttributes OperationAttributes;
};

void TOperation::TOperationImpl::AsyncFinishOperation(TOperationAttributes operationAttributes)
{
    auto args = new TAsyncFinishOperationsArgs;
    args->OperationImpl = this;
    args->OperationAttributes = std::move(operationAttributes);

    TThread thread(TThread::TParams(&TOperation::TOperationImpl::SyncFinishOperationProc, args).SetName("finish operation"));
    thread.Start();
    thread.Detach();
}

void* TOperation::TOperationImpl::SyncFinishOperationProc(void* pArgs)
{
    THolder<TAsyncFinishOperationsArgs> args(static_cast<TAsyncFinishOperationsArgs*>(pArgs));
    args->OperationImpl->SyncFinishOperationImpl(args->OperationAttributes);
    return nullptr;
}

void TOperation::TOperationImpl::SyncFinishOperationImpl(const TOperationAttributes& attributes)
{
    Y_VERIFY(attributes.BriefState && *attributes.BriefState != EOperationBriefState::InProgress);

    {
        try {
            // `attributes' that came from poller don't have JobStatistics
            // so we call `GetJobStatistics' in order to get it from server
            // and cache inside object.
            GetJobStatistics();
        } catch (const TErrorResponse& ) {
            // But if for any reason we failed to get attributes
            // we complete operation using what we have.
            auto g = Guard(Lock_);
            Attributes_ = attributes;
        }
    }

    if (*attributes.BriefState == EOperationBriefState::Completed) {
        CompletePromise_->SetValue();
    } else if (*attributes.BriefState == EOperationBriefState::Aborted || *attributes.BriefState == EOperationBriefState::Failed) {
        Y_VERIFY(attributes.Result && attributes.Result->Error);
        const auto& error = *attributes.Result->Error;
        LOG_ERROR("Operation %s is `%s' with error: %s",
            GetGuidAsString(Id_).data(), ::ToString(*attributes.BriefState).data(), error.FullDescription().data());
        TString additionalExceptionText;
        TVector<TFailedJobInfo> failedJobStderrInfo;
        if (*attributes.BriefState == EOperationBriefState::Failed) {
            try {
                failedJobStderrInfo = NYT::NDetail::GetFailedJobInfo(Auth_, Id_, TGetFailedJobInfoOptions());
            } catch (const yexception& e) {
                additionalExceptionText = "Cannot get job stderrs: ";
                additionalExceptionText += e.what();
            }
        }
        CompletePromise_->SetException(
            std::make_exception_ptr(
                TOperationFailedError(
                    *attributes.BriefState == EOperationBriefState::Failed
                        ? TOperationFailedError::Failed
                        : TOperationFailedError::Aborted,
                    Id_,
                    error,
                    failedJobStderrInfo) << additionalExceptionText));
    }
}

////////////////////////////////////////////////////////////////////////////////

TOperation::TOperation(TOperationId id, TClientPtr client)
    : Client_(std::move(client))
    , Impl_(::MakeIntrusive<TOperationImpl>(Client_->GetAuth(), id))
{
}

const TOperationId& TOperation::GetId() const
{
    return Impl_->GetId();
}

NThreading::TFuture<void> TOperation::Watch()
{
    return Impl_->Watch(Client_->GetYtPoller());
}

TVector<TFailedJobInfo> TOperation::GetFailedJobInfo(const TGetFailedJobInfoOptions& options)
{
    return NYT::NDetail::GetFailedJobInfo(Client_->GetAuth(), GetId(), options);
}

EOperationBriefState TOperation::GetBriefState()
{
    return Impl_->GetBriefState();
}

TMaybe<TYtError> TOperation::GetError()
{
    return Impl_->GetError();
}

TJobStatistics TOperation::GetJobStatistics()
{
    return Impl_->GetJobStatistics();
}

TMaybe<TOperationBriefProgress> TOperation::GetBriefProgress()
{
    return Impl_->GetBriefProgress();
}

void TOperation::AbortOperation()
{
    Impl_->AbortOperation();
}

void TOperation::CompleteOperation()
{
    Impl_->CompleteOperation();
}

TOperationAttributes TOperation::GetAttributes(const TGetOperationOptions& options)
{
    return Impl_->GetAttributes(options);
}

void TOperation::UpdateParameters(const TUpdateOperationParametersOptions& options)
{
    Impl_->UpdateParameters(options);
}

TJobAttributes TOperation::GetJob(const TJobId& jobId, const TGetJobOptions& options)
{
    return Impl_->GetJob(jobId, options);
}

TListJobsResult TOperation::ListJobs(const TListJobsOptions& options)
{
    return Impl_->ListJobs(options);
}

////////////////////////////////////////////////////////////////////////////////

class TWaitOperationStartPollerItem
    : public IYtPollerItem
{
public:
    TWaitOperationStartPollerItem(TOperationId operationId, THolder<TPingableTransaction> transaction)
        : OperationId_(operationId)
        , Transaction_(std::move(transaction))
    { }

    void PrepareRequest(TRawBatchRequest* batchRequest) override
    {
        Future_ = batchRequest->GetOperation(
            OperationId_,
            TGetOperationOptions().AttributeFilter(
                TOperationAttributeFilter().Add(EOperationAttribute::State)));
    }

    EStatus OnRequestExecuted() override
    {
        try {
            auto attributes = Future_.GetValue();
            Y_ENSURE(attributes.State.Defined());
            bool operationHasLockedFiles =
                *attributes.State != "starting" &&
                *attributes.State != "pending" &&
                *attributes.State != "orphaned" &&
                *attributes.State != "waiting_for_agent" &&
                *attributes.State != "initializing";
            return operationHasLockedFiles ? EStatus::PollBreak : EStatus::PollContinue;
        } catch (const TErrorResponse& e) {
            LOG_ERROR("get_operation request %s failed: %s", e.GetRequestId().data(), e.GetError().GetMessage().data());
            return IsRetriable(e) ? PollContinue : PollBreak;
        } catch (const yexception& e) {
            LOG_ERROR("%s", e.what());
            return PollBreak;
        }
    }

private:
    TOperationId OperationId_;
    THolder<TPingableTransaction> Transaction_;
    NThreading::TFuture<TOperationAttributes> Future_;
};

TOperationPreparer::TOperationPreparer(TClientPtr client, TTransactionId transactionId)
    : Client_(std::move(client))
    , TransactionId_(transactionId)
    , FileTransaction_(new TPingableTransaction(Client_->GetAuth(), TransactionId_))
{ }

const TAuth& TOperationPreparer::GetAuth() const
{
    return Client_->GetAuth();
}

TTransactionId TOperationPreparer::GetTransactionId() const
{
    return TransactionId_;
}

class TCacheTouchingRetryPolicy
    : public IRequestRetryPolicy
{
public:
    TCacheTouchingRetryPolicy(
        IRequestRetryPolicyPtr baseRetryPolicy,
        const TAuth& auth,
        const TVector<TString>& md5Signatures,
        const TYPath& cachePath)
        : BasePolicy_(baseRetryPolicy)
        , Auth_(auth)
        , Signatures_(md5Signatures)
        , CachePath_(cachePath)
    { }

    void NotifyNewAttempt() override
    {
        TRawBatchRequest batchRequest;
        TVector<NThreading::TFuture<TMaybe<TYPath>>> results;
        results.reserve(Signatures_.size());
        for (const auto& md5Signature : Signatures_) {
            results.push_back(batchRequest.GetFileFromCache(
                md5Signature,
                CachePath_,
                TGetFileFromCacheOptions()));
        }
        ExecuteBatch(Auth_, batchRequest);
        for (const auto& future : results) {
            future.GetValueSync();
        }

        BasePolicy_->NotifyNewAttempt();
    }

    void OnIgnoredError(const TErrorResponse& e) override
    {
        BasePolicy_->OnIgnoredError(e);
    }

    TMaybe<TDuration> OnGenericError(const yexception& e) override
    {
        return BasePolicy_->OnGenericError(e);
    }

    TMaybe<TDuration> OnRetriableError(const TErrorResponse& e) override
    {
        return BasePolicy_->OnRetriableError(e);
    }

    TString GetAttemptDescription() const override
    {
        return BasePolicy_->GetAttemptDescription();
    }

private:
    IRequestRetryPolicyPtr BasePolicy_;
    const TAuth& Auth_;
    const TVector<TString>& Signatures_;
    TYPath CachePath_;
};

TOperationId TOperationPreparer::StartOperation(
    const TString& operationType,
    const TNode& spec,
    bool useStartOperationRequest)
{
    CheckValidity();

    THttpHeader header("POST", (useStartOperationRequest ? "start_op" : operationType));
    if (useStartOperationRequest) {
        header.AddParameter("operation_type", operationType);
    }
    header.AddTransactionId(TransactionId_);
    header.AddMutationId();

    auto retryPolicy = MakeIntrusive<TCacheTouchingRetryPolicy>(
        MakeIntrusive<TAttemptLimitedRetryPolicy>(static_cast<ui32>(TConfig::Get()->StartOperationRetryCount)),
        GetAuth(),
        LockedFileSignatures_,
        CachePath_);

    auto ysonSpec = NodeToYsonString(spec);
    auto responseInfo = RetryRequestWithPolicy(
        GetAuth(),
        header,
        ysonSpec,
        retryPolicy);
    TOperationId operationId = ParseGuidFromResponse(responseInfo.Response);

    LOG_INFO("Operation %s started (%s): http://%s/#page=operation&mode=detail&id=%s&tab=details",
        GetGuidAsString(operationId).data(), operationType.data(), GetAuth().ServerName.data(), GetGuidAsString(operationId).data());

    TOperationExecutionTimeTracker::Get()->Start(operationId);

    Client_->GetYtPoller().Watch(
        new TWaitOperationStartPollerItem(operationId, std::move(FileTransaction_)));

    return operationId;
}

// TODO(levysotsky): Get rid of lockedFileSignatures and cachePath after
// https://st.yandex-team.ru/YT-9951 is deployed on prod clusters.
void TOperationPreparer::LockFiles(
    TVector<TRichYPath>* paths,
    TVector<TString> lockedFileSignatures,
    const TYPath& cachePath)
{
    CheckValidity();

    LockedFileSignatures_ = std::move(lockedFileSignatures);
    CachePath_ = cachePath;

    TVector<NThreading::TFuture<TLockId>> lockIdFutures;
    lockIdFutures.reserve(paths->size());
    TRawBatchRequest lockRequest;
    for (const auto& path : *paths) {
        lockIdFutures.push_back(lockRequest.Lock(
            FileTransaction_->GetId(),
            path.Path_,
            ELockMode::LM_SNAPSHOT,
            TLockOptions().Waitable(true)));
    }
    ExecuteBatch(GetAuth(), lockRequest);

    TVector<NThreading::TFuture<TNode>> nodeIdFutures;
    nodeIdFutures.reserve(paths->size());
    TRawBatchRequest getNodeIdRequest;
    for (const auto& lockIdFuture : lockIdFutures) {
        nodeIdFutures.push_back(getNodeIdRequest.Get(
            FileTransaction_->GetId(),
            TStringBuilder() << '#' << GetGuidAsString(lockIdFuture.GetValue()) << "/@node_id",
            TGetOptions()));
    }
    ExecuteBatch(GetAuth(), getNodeIdRequest);

    for (size_t i = 0; i != paths->size(); ++i) {
        auto& richPath = (*paths)[i];
        richPath.OriginalPath(richPath.Path_);
        richPath.Path("#" + nodeIdFutures[i].GetValue().AsString());
        LOG_DEBUG("Locked file %s, new path is %s", richPath.OriginalPath_->data(), richPath.Path_.data());
    }
}

void TOperationPreparer::CheckValidity() const
{
    Y_ENSURE(
        FileTransaction_,
        "File transaction is already moved, are you trying to use preparer for more than one operation?");
}

////////////////////////////////////////////////////////////////////////////////

TOperationPtr CreateOperationAndWaitIfRequired(const TOperationId& operationId, TClientPtr client, const TOperationOptions& options)
{
    auto operation = ::MakeIntrusive<TOperation>(operationId, std::move(client));
    if (options.Wait_) {
        auto finishedFuture = operation->Watch();
        TWaitProxy::Get()->WaitFuture(finishedFuture);
        finishedFuture.GetValue();
    }
    return operation;
}

////////////////////////////////////////////////////////////////////////////////

void ResetUseClientProtobuf(const char* methodName)
{
    Cerr << "WARNING! OPTION `TConfig::UseClientProtobuf' IS RESET TO `true'; "
        << "IT CAN DETERIORIATE YOUR CODE PERFORMANCE!!! DON'T USE DEPRECATED METHOD `"
        << "TOperationIOSpec::" << methodName << "' TO AVOID THIS RESET" << Endl;
    // Give users some time to contemplate about usage of deprecated functions.
    Cerr << "Sleeping for 5 seconds..." << Endl;
    Sleep(TDuration::Seconds(5));
    TConfig::Get()->UseClientProtobuf = true;
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

::TIntrusivePtr<INodeReaderImpl> CreateJobNodeReader()
{
    if (auto schema = NDetail::GetJobInputSkiffSchema()) {
        return new NDetail::TSkiffTableReader(::MakeIntrusive<TJobReader>(0), schema);
    } else {
        return new TNodeTableReader(::MakeIntrusive<TJobReader>(0));
    }
}

::TIntrusivePtr<IYaMRReaderImpl> CreateJobYaMRReader()
{
    return new TYaMRTableReader(::MakeIntrusive<TJobReader>(0));
}

::TIntrusivePtr<IProtoReaderImpl> CreateJobProtoReader()
{
    if (TConfig::Get()->UseClientProtobuf) {
        return new TProtoTableReader(
            ::MakeIntrusive<TJobReader>(0),
            GetJobInputDescriptors());
    } else {
        return new TLenvalProtoTableReader(
            ::MakeIntrusive<TJobReader>(0),
            GetJobInputDescriptors());
    }
}

::TIntrusivePtr<INodeWriterImpl> CreateJobNodeWriter(size_t outputTableCount)
{
    return new TNodeTableWriter(MakeHolder<TJobWriter>(outputTableCount));
}

::TIntrusivePtr<IYaMRWriterImpl> CreateJobYaMRWriter(size_t outputTableCount)
{
    return new TYaMRTableWriter(MakeHolder<TJobWriter>(outputTableCount));
}

::TIntrusivePtr<IProtoWriterImpl> CreateJobProtoWriter(size_t outputTableCount)
{
    if (TConfig::Get()->UseClientProtobuf) {
        return new TProtoTableWriter(
            MakeHolder<TJobWriter>(outputTableCount),
            GetJobOutputDescriptors());
    } else {
        return new TLenvalProtoTableWriter(
            MakeHolder<TJobWriter>(outputTableCount),
            GetJobOutputDescriptors());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
