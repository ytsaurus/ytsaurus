#include "operation.h"

#include "client.h"
#include "file_reader.h"
#include "file_writer.h"
#include "init.h"
#include "prepare_operation.h"
#include "operation_tracker.h"
#include "retry_heavy_write_request.h"
#include "skiff.h"
#include "structured_table_formats.h"
#include "yt_poller.h"

#include <mapreduce/yt/common/abortable_registry.h>
#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/retry_lib.h>
#include <mapreduce/yt/common/wait_proxy.h>

#include <mapreduce/yt/interface/errors.h>
#include <mapreduce/yt/interface/fluent.h>
#include <mapreduce/yt/interface/format.h>
#include <mapreduce/yt/interface/job_statistics.h>
#include <mapreduce/yt/interface/protobuf_format.h>

#include <mapreduce/yt/interface/logging/log.h>

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
#include <mapreduce/yt/io/ydl_table_reader.h>
#include <mapreduce/yt/io/ydl_table_writer.h>
#include <mapreduce/yt/io/ydl_helpers.h>
#include <mapreduce/yt/io/skiff_table_reader.h>

#include <mapreduce/yt/raw_client/raw_batch_request.h>
#include <mapreduce/yt/raw_client/raw_requests.h>

#include <mapreduce/yt/util/batch.h>

#include <library/cpp/yson/writer.h>
#include <library/cpp/yson/json_writer.h>

#include <library/cpp/yson/node/serialize.h>

#include <util/folder/path.h>

#include <util/generic/hash_set.h>
#include <util/generic/scope.h>

#include <util/stream/buffer.h>
#include <util/stream/file.h>

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>

#include <util/system/execpath.h>
#include <util/system/mutex.h>
#include <util/system/rwlock.h>
#include <util/system/thread.h>

#include <library/cpp/digest/md5/md5.h>

namespace NYT {
namespace NDetail {

using namespace NRawClient;

static const ui64 DefaultExrtaTmpfsSize = 1024LL * 1024LL;

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

bool UseLocalModeOptimization(const TAuth& auth, const IClientRetryPolicyPtr& clientRetryPolicy)
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
    if (Exists(clientRetryPolicy->CreatePolicyForGenericRequest(),
        auth, TTransactionId(),
        localModeAttr))
    {
        auto fqdn = Get(
            clientRetryPolicy->CreatePolicyForGenericRequest(),
            auth,
            TTransactionId(),
            localModeAttr,
            TGetOptions()
        ).AsString();
        isLocalMode = (fqdn == TProcessState::Get()->FqdnHostName);
        LOG_DEBUG("Checking local mode; LocalModeFqdn: %s FqdnHostName: %s IsLocalMode: %s",
            fqdn.c_str(),
            TProcessState::Get()->FqdnHostName.c_str(),
            isLocalMode ? "true" : "false");
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
const TVector<TStructuredTablePath>& GetStructuredInputs(const TSpec& spec)
{
    if constexpr (std::is_same_v<TSpec, TVanillaTask>) {
        static const TVector<TStructuredTablePath> empty;
        return empty;
    } else {
        return spec.GetStructuredInputs();
    }
}

template <class TSpec>
const TVector<TStructuredTablePath>& GetStructuredOutputs(const TSpec& spec)
{
    return spec.GetStructuredOutputs();
}

template <class TSpec>
const TMaybe<TFormatHints>& GetInputFormatHints(const TSpec& spec)
{
    if constexpr (std::is_same_v<TSpec, TVanillaTask>) {
        static const TMaybe<TFormatHints> empty = Nothing();
        return empty;
    } else {
        return spec.InputFormatHints_;
    }
}

template <class TSpec>
const TMaybe<TFormatHints>& GetOutputFormatHints(const TSpec& spec)
{
    return spec.OutputFormatHints_;
}

template <class TSpec>
ENodeReaderFormat GetNodeReaderFormat(const TSpec& spec, bool allowSkiff)
{
    if constexpr (std::is_same<TSpec, TVanillaTask>::value) {
        return ENodeReaderFormat::Yson;
    } else {
        return allowSkiff
            ? NodeReaderFormatFromHintAndGlobalConfig(spec)
            : ENodeReaderFormat::Yson;
    }
}

THashSet<TString> GetColumnsUsedInOperation(const TJoinReduceOperationSpec& spec)
{
    return THashSet<TString>(spec.JoinBy_.Parts_.begin(), spec.JoinBy_.Parts_.end());
}

THashSet<TString> GetColumnsUsedInOperation(const TReduceOperationSpec& spec) {
    THashSet<TString> result(spec.SortBy_.Parts_.begin(), spec.SortBy_.Parts_.end());
    result.insert(spec.ReduceBy_.Parts_.begin(), spec.ReduceBy_.Parts_.end());
    if (spec.JoinBy_) {
        result.insert(spec.JoinBy_->Parts_.begin(), spec.JoinBy_->Parts_.end());
    }
    return result;
}

THashSet<TString> GetColumnsUsedInOperation(const TMapReduceOperationSpec& spec)
{
    THashSet<TString> result(spec.SortBy_.Parts_.begin(), spec.SortBy_.Parts_.end());
    result.insert(spec.ReduceBy_.Parts_.begin(), spec.ReduceBy_.Parts_.end());
    return result;
}

THashSet<TString> GetColumnsUsedInOperation(const TMapOperationSpec&)
{
    return THashSet<TString>();
}

THashSet<TString> GetColumnsUsedInOperation(const TVanillaTask&)
{
    return THashSet<TString>();
}

template <class TSpec>
TStructuredJobTableList ApplyProtobufColumnFilters(
    const TStructuredJobTableList& tableList,
    const TOperationPreparer& preparer,
    const TSpec& spec,
    const TOperationOptions& options)
{
    bool hasInputQuery = options.Spec_.Defined() && options.Spec_->IsMap() && options.Spec_->HasKey("input_query");
    if (hasInputQuery) {
        return tableList;
    }

    auto isDynamic = BatchTransform(
        CreateDefaultRequestRetryPolicy(),
        preparer.GetAuth(),
        tableList,
        [&] (TRawBatchRequest& batch, const auto& table) {
            return batch.Get(preparer.GetTransactionId(), table.RichYPath->Path_ + "/@dynamic", TGetOptions());
        });

    auto newTableList = tableList;
    auto columnsUsedInOperations = GetColumnsUsedInOperation(spec);
    for (size_t tableIndex = 0; tableIndex < tableList.size(); ++tableIndex) {
        if (isDynamic[tableIndex].AsBool()) {
            continue;
        }
        auto& table = newTableList[tableIndex];
        Y_VERIFY(table.RichYPath);
        if (table.RichYPath->Columns_) {
            continue;
        }
        if (!HoldsAlternative<TProtobufTableStructure>(table.Description)) {
            continue;
        }
        const auto& descriptor = ::Get<TProtobufTableStructure>(table.Description).Descriptor;
        if (!descriptor) {
            continue;
        }
        auto fromDescriptor = NDetail::InferColumnFilter(*descriptor);
        if (!fromDescriptor) {
            continue;
        }
        THashSet<TString> columns(fromDescriptor->begin(), fromDescriptor->end());
        columns.insert(columnsUsedInOperations.begin(), columnsUsedInOperations.end());
        table.RichYPath->Columns(TVector<TString>(columns.begin(), columns.end()));
    }
    return newTableList;
}

template <class TSpec>
TSimpleOperationIo CreateSimpleOperationIo(
    const IStructuredJob& structuredJob,
    const TOperationPreparer& preparer,
    const TSpec& spec,
    const TOperationOptions& options,
    bool allowSkiff)
{
    if (!HoldsAlternative<TVoidStructuredRowStream>(structuredJob.GetInputRowStreamDescription())) {
        VerifyHasElements(GetStructuredInputs(spec), "input");
    }

    auto structuredInputs = CanonizeStructuredTableList(preparer.GetAuth(),  GetStructuredInputs(spec));
    auto structuredOutputs = CanonizeStructuredTableList(preparer.GetAuth(), GetStructuredOutputs(spec));
    TUserJobFormatHints hints;
    hints.InputFormatHints_ = GetInputFormatHints(spec);
    hints.OutputFormatHints_ = GetOutputFormatHints(spec);

    auto jobSchemaInferenceResult = PrepareOperation(
        structuredJob,
        TOperationPreparationContext(
            structuredInputs,
            structuredOutputs,
            preparer.GetAuth(),
            preparer.GetClientRetryPolicy(),
            preparer.GetTransactionId()),
        &structuredInputs,
        &structuredOutputs,
        hints);

    ENodeReaderFormat nodeReaderFormat = GetNodeReaderFormat(spec, allowSkiff);

    TVector<TSmallJobFile> formatConfigList;
    TFormatBuilder formatBuilder(preparer.GetClientRetryPolicy(), preparer.GetAuth(), preparer.GetTransactionId(), options);

    // Input format
    auto [inputFormat, inputFormatConfig] = formatBuilder.CreateFormat(
        structuredJob,
        EIODirection::Input,
        structuredInputs,
        hints.InputFormatHints_,
        nodeReaderFormat,
        /* allowFormatFromTableAttribute = */ true);

    // Output format
    auto [outputFormat, outputFormatConfig] = formatBuilder.CreateFormat(
        structuredJob,
        EIODirection::Output,
        structuredOutputs,
        hints.OutputFormatHints_,
        ENodeReaderFormat::Yson,
        /* allowFormatFromTableAttribute = */ false);

    const bool inferOutputSchema = options.InferOutputSchema_.GetOrElse(TConfig::Get()->InferTableSchema);

    auto outputPaths = GetPathList(
        structuredOutputs,
        jobSchemaInferenceResult,
        inferOutputSchema);

    auto inputPaths = GetPathList(
        ApplyProtobufColumnFilters(
            structuredInputs,
            preparer,
            spec,
            options),
        /*schemaInferenceResult*/ Nothing(),
        /*inferSchema*/ false);

    return TSimpleOperationIo {
        inputPaths,
        outputPaths,

        inputFormat,
        outputFormat,

        CreateFormatConfig(inputFormatConfig, outputFormatConfig)
    };
}

template <class T>
TSimpleOperationIo CreateSimpleOperationIo(
    const IJob& job,
    const TOperationPreparer& preparer,
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

    auto inputs = CanonizeYPaths(/* retryPolicy */ nullptr, preparer.GetAuth(), spec.GetInputs());
    auto outputs = CanonizeYPaths(/* retryPolicy */ nullptr, preparer.GetAuth(), spec.GetOutputs());

    VerifyHasElements(inputs, "input");
    VerifyHasElements(outputs, "output");

    TUserJobFormatHints hints;

    auto outputSchemas = PrepareOperation(
        job,
        TOperationPreparationContext(
            inputs,
            outputs,
            preparer.GetAuth(),
            preparer.GetClientRetryPolicy(),
            preparer.GetTransactionId()),
        &inputs,
        &outputs,
        hints);

    Y_VERIFY(outputs.size() == outputSchemas.size());
    for (int i = 0; i < static_cast<int>(outputs.size()); ++i) {
        if (!outputs[i].Schema_ && !outputSchemas[i].Columns().empty()) {
            outputs[i].Schema_ = outputSchemas[i];
        }
    }

    return TSimpleOperationIo {
        inputs,
        outputs,

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
    TFileToUpload(TString fileName, TMaybe<TString> md5)
        : FileName_(std::move(fileName))
        , MD5_(std::move(md5))
    { }

    TString CalculateMD5() const override
    {
        if (MD5_) {
            return *MD5_;
        }
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
    TMaybe<TString> MD5_;
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

const TString& GetPersistentExecPathMd5()
{
    static TString md5 = MD5::File(GetPersistentExecPath());
    return md5;
}

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
        CreateStorage();
        auto cypressFileList = CanonizeYPaths(/* retryPolicy */ nullptr, OperationPreparer_.GetAuth(), spec.Files_);
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

        if (auto commandJob = dynamic_cast<const ICommandJob*>(&job)) {
            ClassName_ = TJobFactory::Get()->GetJobName(&job);
            Command_ = commandJob->GetCommand();
        } else {
            PrepareJobBinary(job, outputTableCount, jobStateSmallFile.Defined());
        }

        operationPreparer.LockFiles(&CachedFiles_);
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

private:
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
        Create(
            OperationPreparer_.GetClientRetryPolicy()->CreatePolicyForGenericRequest(),
            OperationPreparer_.GetAuth(),
            Options_.FileStorageTransactionId_,
            GetCachePath(),
            NT_MAP,
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
                return GetBackoffDuration();
            }
            return TAttemptLimitedRetryPolicy::OnRetriableError(e);
        }
    };

    int GetFileCacheReplicationFactor() const
    {
        if (IsLocalMode()) {
            return 1;
        } else {
            return TConfig::Get()->FileCacheReplicationFactor;
        }
    }

    TString UploadToRandomPath(const IItemToUpload& itemToUpload) const
    {
        TString uniquePath = AddPathPrefix(TStringBuilder() << GetFileStorage() << "/cpp_" << CreateGuidAsString());
        LOG_INFO("Uploading file to random cypress path (FileName: %s; CypressPath: %s; PreparationId: %s)",
            itemToUpload.GetDescription().c_str(),
            uniquePath.c_str(),
            OperationPreparer_.GetPreparationId().c_str());

        Create(
            OperationPreparer_.GetClientRetryPolicy()->CreatePolicyForGenericRequest(),
            OperationPreparer_.GetAuth(),
            Options_.FileStorageTransactionId_,
            uniquePath,
            NT_FILE,
            TCreateOptions()
                .IgnoreExisting(true)
                .Recursive(true)
		.Attributes(TNode()("replication_factor", GetFileCacheReplicationFactor()))
        );
        {
            TFileWriter writer(
                uniquePath,
                OperationPreparer_.GetClientRetryPolicy(),
                OperationPreparer_.GetAuth(),
                Options_.FileStorageTransactionId_);
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
            retryPolicy,
            OperationPreparer_.GetAuth(),
            TTransactionId(),
            md5Signature,
            GetCachePath(),
            TGetFileFromCacheOptions());
        if (maybePath) {
            LOG_DEBUG("File is already in cache (FileName: %s)",
                itemToUpload.GetDescription().c_str(),
                maybePath->c_str());
            return *maybePath;
        }

        TString uniquePath = AddPathPrefix(TStringBuilder() << GetFileStorage() << "/cpp_" << CreateGuidAsString());
        LOG_INFO("File not found in cache; uploading to cypress (FileName: %s; CypressPath: %s; PreparationId: %s)",
            itemToUpload.GetDescription().c_str(),
            uniquePath.c_str(),
            OperationPreparer_.GetPreparationId().c_str());

        Create(
            OperationPreparer_.GetClientRetryPolicy()->CreatePolicyForGenericRequest(),
            OperationPreparer_.GetAuth(),
            TTransactionId(),
            uniquePath,
            NT_FILE,
            TCreateOptions()
                .IgnoreExisting(true)
                .Recursive(true)
		.Attributes(TNode()("replication_factor", GetFileCacheReplicationFactor()))
        );

        {
            TFileWriter writer(
                uniquePath,
                OperationPreparer_.GetClientRetryPolicy(),
                OperationPreparer_.GetAuth(),
                TTransactionId(),
                TFileWriterOptions().ComputeMD5(true));
            itemToUpload.CreateInputStream()->ReadAll(writer);
            writer.Finish();
        }

        auto cachePath = PutFileToCache(
            retryPolicy,
            OperationPreparer_.GetAuth(),
            TTransactionId(),
            uniquePath,
            md5Signature,
            GetCachePath(),
            TPutFileToCacheOptions());

        Remove(
            OperationPreparer_.GetClientRetryPolicy()->CreatePolicyForGenericRequest(),
            OperationPreparer_.GetAuth(),
            TTransactionId(),
            uniquePath,
            TRemoveOptions().Force(true));

        LockedFileSignatures_.push_back(md5Signature);
        return cachePath;
    }

    TString UploadToCache(const IItemToUpload& itemToUpload) const
    {
        LOG_INFO("Uploading file (FileName: %s; PreparationId: %s)",
            itemToUpload.GetDescription().c_str(),
            OperationPreparer_.GetPreparationId().c_str());
        Y_DEFER {
            LOG_INFO("Complete uploading file (FileName: %s; PreparationId: %s)",
                itemToUpload.GetDescription().c_str(),
                OperationPreparer_.GetPreparationId().c_str());
        };
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
        if (!Exists(
            OperationPreparer_.GetClientRetryPolicy()->CreatePolicyForGenericRequest(),
            OperationPreparer_.GetAuth(),
            file.TransactionId_.GetOrElse(OperationPreparer_.GetTransactionId()),
            file.Path_))
        {
            ythrow yexception() << "File " << file.Path_ << " does not exist";
        }

        if (ShouldMountSandbox()) {
            auto size = Get(
                OperationPreparer_.GetClientRetryPolicy()->CreatePolicyForGenericRequest(),
                OperationPreparer_.GetAuth(),
                file.TransactionId_.GetOrElse(OperationPreparer_.GetTransactionId()),
                file.Path_ + "/@uncompressed_data_size")
                .AsInt64();

            TotalFileSize_ += RoundUpFileSize(static_cast<ui64>(size));
        }
        CypressFiles_.push_back(file);
    }

    void UploadLocalFile(const TLocalFilePath& localPath, const TAddLocalFileOptions& options, bool isApiFile = false)
    {
        TFsPath fsPath(localPath);
        fsPath.CheckExists();

        TFileStat stat;
        fsPath.Stat(stat);

        bool isExecutable = stat.Mode & (S_IXUSR | S_IXGRP | S_IXOTH);
        auto cachePath = UploadToCache(TFileToUpload(localPath, options.MD5CheckSum_));

        TRichYPath cypressPath;
        if (isApiFile) {
            cypressPath = TConfig::Get()->ApiFilePathOptions;
        }
        cypressPath.Path(cachePath).FileName(options.PathInJob_.GetOrElse(fsPath.Basename()));
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
            auto binaryLocalPath = ::Get<TJobBinaryLocalPath>(jobBinary);
            auto opts = TAddLocalFileOptions().PathInJob("cppbinary");
            if (binaryLocalPath.MD5CheckSum) {
                opts.MD5CheckSum(*binaryLocalPath.MD5CheckSum);
            }
            UploadLocalFile(binaryLocalPath.Path, opts, /* isApiFile */ true);
        } else if (HoldsAlternative<TJobBinaryCypressPath>(jobBinary)) {
            auto binaryCypressPath = ::Get<TJobBinaryCypressPath>(jobBinary);
            TRichYPath ytPath = TConfig::Get()->ApiFilePathOptions;
            ytPath.Path(binaryCypressPath.Path);
            if (binaryCypressPath.TransactionId) {
                ytPath.TransactionId(*binaryCypressPath.TransactionId);
            }
            UseFileInCypress(ytPath.FileName("cppbinary").Executable(true));
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
        auto path = TConfig::Get()->ApiFilePathOptions;
        CachedFiles_.push_back(path.Path(cachePath).FileName(smallFile.FileName));
        if (ShouldMountSandbox()) {
            TotalFileSize_ += RoundUpFileSize(smallFile.Data.size());
        }
    }

    bool IsLocalMode() const
    {
        return UseLocalModeOptimization(OperationPreparer_.GetAuth(), OperationPreparer_.GetClientRetryPolicy());
    }

    void PrepareJobBinary(const IJob& job, int outputTableCount, bool hasState)
    {
        auto jobBinary = TJobBinaryConfig();
        if (!HoldsAlternative<TJobBinaryDefault>(Spec_.GetJobBinary())) {
            jobBinary = Spec_.GetJobBinary();
        }
        TString binaryPathInsideJob;
        if (HoldsAlternative<TJobBinaryDefault>(jobBinary)) {
            if (GetInitStatus() != EInitStatus::FullInitialization) {
                ythrow yexception() << "NYT::Initialize() must be called prior to any operation";
            }

            const bool isLocalMode = IsLocalMode();
            const TMaybe<TString> md5 = !isLocalMode ? MakeMaybe(GetPersistentExecPathMd5()) : Nothing();
            jobBinary = TJobBinaryLocalPath{GetPersistentExecPath(), md5};

            if (isLocalMode) {
                binaryPathInsideJob = GetExecPath();
            }
        } else if (HoldsAlternative<TJobBinaryLocalPath>(jobBinary)) {
            const bool isLocalMode = IsLocalMode();
            if (isLocalMode) {
                binaryPathInsideJob = TFsPath(::Get<TJobBinaryLocalPath>(jobBinary).Path).RealPath();
            }
        }
        Y_ASSERT(!HoldsAlternative<TJobBinaryDefault>(jobBinary));

        // binaryPathInsideJob is only set when LocalModeOptimization option is on, so upload is not needed
        if (!binaryPathInsideJob) {
            binaryPathInsideJob = "./cppbinary";
            UploadBinary(jobBinary);
        }

        TString jobCommandPrefix = Options_.JobCommandPrefix_;
        if (!Spec_.JobCommandPrefix_.empty()) {
            jobCommandPrefix = Spec_.JobCommandPrefix_;
        }

        TString jobCommandSuffix = Options_.JobCommandSuffix_;
        if (!Spec_.JobCommandSuffix_.empty()) {
            jobCommandSuffix = Spec_.JobCommandSuffix_;
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
            hasState <<
            jobCommandSuffix;
    }

};

////////////////////////////////////////////////////////////////////

TString GetJobStderrWithRetriesAndIgnoreErrors(
    const IRequestRetryPolicyPtr& retryPolicy,
    const TAuth& auth,
    const TOperationId& operationId,
    const TJobId& jobId,
    const size_t stderrTailSize,
    const TGetJobStderrOptions& options = TGetJobStderrOptions())
{
    TString jobStderr;
    try {
        jobStderr = GetJobStderrWithRetries(
            retryPolicy,
            auth,
            operationId,
            jobId,
            options);
    } catch (const TErrorResponse& e) {
        LOG_ERROR("Cannot get job stderr OperationId: %s JobId: %s Error: %s",
                  GetGuidAsString(operationId).c_str(),
                  GetGuidAsString(jobId).c_str(),
                  e.what());
    }
    if (jobStderr.size() > stderrTailSize) {
        jobStderr = jobStderr.substr(jobStderr.size() - stderrTailSize, stderrTailSize);
    }
    return jobStderr;
}

TVector<TFailedJobInfo> GetFailedJobInfo(
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const TAuth& auth,
    const TOperationId& operationId,
    const TGetFailedJobInfoOptions& options)
{
    const auto listJobsResult = ListJobs(
        clientRetryPolicy->CreatePolicyForGenericRequest(),
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
            // There are cases when due to bad luck we cannot read stderr even if
            // list_jobs reports that stderr_size > 0.
            //
            // Such errors don't have special error code
            // so we ignore all errors and try our luck on other jobs.
            info.Stderr = GetJobStderrWithRetriesAndIgnoreErrors(
                clientRetryPolicy->CreatePolicyForGenericRequest(),
                auth,
                operationId,
                *job.Id,
                stderrTailSize);
        }
    }
    return result;
}

struct TGetJobsStderrOptions
{
    using TSelf = TGetJobsStderrOptions;

    // How many jobs to download. Which jobs will be chosen is undefined.
    FLUENT_FIELD_DEFAULT(ui64, MaxJobCount, 10);

    // How much of stderr should be downloaded.
    FLUENT_FIELD_DEFAULT(ui64, StderrTailSize, 64 * 1024);
};

static TVector<TString> GetJobsStderr(
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const TAuth& auth,
    const TOperationId& operationId,
    const TGetJobsStderrOptions& options = TGetJobsStderrOptions())
{
    const auto listJobsResult = ListJobs(
        clientRetryPolicy->CreatePolicyForGenericRequest(),
        auth,
        operationId,
        TListJobsOptions().Limit(options.MaxJobCount_).WithStderr(true));
    const auto stderrTailSize = options.StderrTailSize_;
    TVector<TString> result;
    for (const auto& job : listJobsResult.Jobs) {
        result.push_back(
            // There are cases when due to bad luck we cannot read stderr even if
            // list_jobs reports that stderr_size > 0.
            //
            // Such errors don't have special error code
            // so we ignore all errors and try our luck on other jobs.
            GetJobStderrWithRetriesAndIgnoreErrors(
                clientRetryPolicy->CreatePolicyForGenericRequest(),
                auth,
                operationId,
                *job.Id,
                stderrTailSize)
            );
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

EOperationBriefState CheckOperation(
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const TAuth& auth,
    const TOperationId& operationId)
{
    auto attributes = GetOperation(
        clientRetryPolicy->CreatePolicyForGenericRequest(),
        auth,
        operationId,
        TGetOperationOptions().AttributeFilter(TOperationAttributeFilter()
            .Add(EOperationAttribute::State)
            .Add(EOperationAttribute::Result)));
    Y_VERIFY(attributes.BriefState,
        "get_operation for operation %s has not returned \"state\" field",
        GetGuidAsString(operationId).Data());
    if (*attributes.BriefState == EOperationBriefState::Completed) {
        return EOperationBriefState::Completed;
    } else if (*attributes.BriefState == EOperationBriefState::Aborted || *attributes.BriefState == EOperationBriefState::Failed) {
        LOG_ERROR("Operation %s %s (%s)",
            GetGuidAsString(operationId).data(),
            ::ToString(*attributes.BriefState).data(),
            ToString(TOperationExecutionTimeTracker::Get()->Finish(operationId)).data());

        auto failedJobInfoList = GetFailedJobInfo(
            clientRetryPolicy,
            auth,
            operationId,
            TGetFailedJobInfoOptions());

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
    const IClientRetryPolicyPtr& clientRetryPolicy,
    const TAuth& auth,
    const TOperationId& operationId)
{
    const TDuration checkOperationStateInterval =
        UseLocalModeOptimization(auth, clientRetryPolicy)
        ? TDuration::MilliSeconds(100)
        : TDuration::Seconds(1);

    while (true) {
        auto status = CheckOperation(clientRetryPolicy, auth, operationId);
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
    TMaybe<ui16> portCount = userJobSpec.PortCount_;

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
        .DoIf(portCount.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("port_count").Value(*portCount);
        })
        .DoIf(userJobSpec.JobTimeLimit_.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("job_time_limit").Value(userJobSpec.JobTimeLimit_->MilliSeconds());
        })
        .DoIf(preparer.ShouldMountSandbox(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("tmpfs_path").Value(".");
            fluentMap.Item("tmpfs_size").Value(tmpfsSize);
            fluentMap.Item("copy_files").Value(true);
        });
}

template <typename T>
void BuildCommonOperationPart(const TOperationSpecBase<T>& baseSpec, const TOperationOptions& options, TFluentMap fluent)
{
    const TProcessState* properties = TProcessState::Get();
    const TString& pool = TConfig::Get()->Pool;

    fluent
        .Item("started_by")
        .BeginMap()
            .Item("hostname").Value(properties->FqdnHostName)
            .Item("pid").Value(properties->Pid)
            .Item("user").Value(properties->UserName)
            .Item("command").List(properties->CommandLine)
            .Item("wrapper_version").Value(properties->ClientVersion)
        .EndMap()
        .DoIf(!pool.empty(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("pool").Value(pool);
        })
        .DoIf(baseSpec.TimeLimit_.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("time_limit").Value(baseSpec.TimeLimit_->MilliSeconds());
        })
        .DoIf(options.SecureVault_.Defined(), [&] (TFluentMap fluentMap) {
            Y_ENSURE(options.SecureVault_->IsMap(),
                "SecureVault must be a map node, got " << options.SecureVault_->GetType());
            fluentMap.Item("secure_vault").Value(*options.SecureVault_);
        })
        .DoIf(baseSpec.Title_.Defined(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("title").Value(*baseSpec.Title_);
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
    if (baseSpec.WaitingJobTimeout_.Defined()) {
        (*spec)["waiting_job_timeout"] = baseSpec.WaitingJobTimeout_->MilliSeconds();
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
void CreateDebugOutputTables(const TSpec& spec, const TOperationPreparer& preparer)
{
    if (spec.StderrTablePath_.Defined()) {
        NYT::NDetail::Create(
            preparer.GetClientRetryPolicy()->CreatePolicyForGenericRequest(),
            preparer.GetAuth(),
            TTransactionId(),
            *spec.StderrTablePath_,
            NT_TABLE,
            TCreateOptions()
                .IgnoreExisting(true)
                .Recursive(true));
    }
    if (spec.CoreTablePath_.Defined()) {
        NYT::NDetail::Create(
            preparer.GetClientRetryPolicy()->CreatePolicyForGenericRequest(),
            preparer.GetAuth(),
            TTransactionId(),
            *spec.CoreTablePath_,
            NT_TABLE,
            TCreateOptions()
                .IgnoreExisting(true)
                .Recursive(true));
    }
}

void CreateOutputTable(
    const TOperationPreparer& preparer,
    const TRichYPath& path)
{
    Y_ENSURE(path.Path_, "Output table is not set");
    Create(
        preparer.GetClientRetryPolicy()->CreatePolicyForGenericRequest(),
        preparer.GetAuth(), preparer.GetTransactionId(), path.Path_, NT_TABLE,
        TCreateOptions()
            .IgnoreExisting(true)
            .Recursive(true));
}

void CreateOutputTables(
    const TOperationPreparer& preparer,
    const TVector<TRichYPath>& paths)
{
    for (auto& path : paths) {
        CreateOutputTable(preparer, path);
    }
}

void CheckInputTablesExist(
    const TOperationPreparer& preparer,
    const TVector<TRichYPath>& paths)
{
    Y_ENSURE(!paths.empty(), "Input tables are not set");
    for (auto& path : paths) {
        auto curTransactionId =  path.TransactionId_.GetOrElse(preparer.GetTransactionId());
        Y_ENSURE_EX(
            Exists(
                preparer.GetClientRetryPolicy()->CreatePolicyForGenericRequest(),
                preparer.GetAuth(),
                curTransactionId,
                path.Path_),
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

void LogYPaths(const TOperationId& opId, const TVector<TRichYPath>& paths, const char* type)
{
    for (size_t i = 0; i < paths.size(); ++i) {
        LOG_INFO("Operation %s; %s[%" PRISZT "] = %s",
            GetGuidAsString(opId).data(), type, i, paths[i].Path_.data());
    }
}

void LogYPath(const TOperationId& opId, const TRichYPath& path, const char* type)
{
    LOG_INFO("Operation %s; %s = %s",
        GetGuidAsString(opId).data(), type, path.Path_.data());
}

TString AddModeToTitleIfDebug(const TString& title) {
#ifndef NDEBUG
    return title + " (debug build)";
#else
    return title;
#endif
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TOperationId DoExecuteMap(
    TOperationPreparer& preparer,
    const TSimpleOperationIo& operationIo,
    TMapOperationSpecBase<T> spec,
    const IJob& mapper,
    const TOperationOptions& options)
{
    if (options.CreateDebugOutputTables_) {
        CreateDebugOutputTables(spec, preparer);
    }
    if (options.CreateOutputTables_) {
        CheckInputTablesExist(preparer, operationIo.Inputs);
        CreateOutputTables(preparer, operationIo.Outputs);
    }

    TJobPreparer map(
        preparer,
        spec.MapperSpec_,
        mapper,
        operationIo.Outputs.size(),
        operationIo.JobFiles,
        options);

    spec.Title_ = spec.Title_.GetOrElse(AddModeToTitleIfDebug(map.GetClassName()));

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
        .Do(std::bind(BuildCommonOperationPart<T>, spec, options, std::placeholders::_1))
    .EndMap().EndMap();

    specNode["spec"]["job_io"]["control_attributes"]["enable_row_index"] = TNode(true);
    specNode["spec"]["job_io"]["control_attributes"]["enable_range_index"] = TNode(true);
    if (!TConfig::Get()->TableWriter.Empty()) {
        specNode["spec"]["job_io"]["table_writer"] = TConfig::Get()->TableWriter;
    }

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
    LOG_DEBUG("Starting map operation (PreparationId: %s)",
        preparer.GetPreparationId().c_str());
    return DoExecuteMap(
        preparer,
        CreateSimpleOperationIo(mapper, preparer, spec, options, /* allowSkiff = */ true),
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
    LOG_DEBUG("Starting raw map operation (PreparationId: %s)",
        preparer.GetPreparationId().c_str());
    return DoExecuteMap(
        preparer,
        CreateSimpleOperationIo(mapper, preparer, spec),
        spec,
        mapper,
        options);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TOperationId DoExecuteReduce(
    TOperationPreparer& preparer,
    const TSimpleOperationIo& operationIo,
    TReduceOperationSpecBase<T> spec,
    const IJob& reducer,
    const TOperationOptions& options)
{
    if (options.CreateDebugOutputTables_) {
        CreateDebugOutputTables(spec, preparer);
    }
    if (options.CreateOutputTables_) {
        CheckInputTablesExist(preparer, operationIo.Inputs);
        CreateOutputTables(preparer, operationIo.Outputs);
    }

    TJobPreparer reduce(
        preparer,
        spec.ReducerSpec_,
        reducer,
        operationIo.Outputs.size(),
        operationIo.JobFiles,
        options);

    spec.Title_ = spec.Title_.GetOrElse(AddModeToTitleIfDebug(reduce.GetClassName()));

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
                .Item("enable_range_index").Value(true)
            .EndMap()
            .DoIf(!TConfig::Get()->TableWriter.Empty(), [&] (TFluentMap fluent) {
                fluent.Item("table_writer").Value(TConfig::Get()->TableWriter);
            })
        .EndMap()
        .Do(std::bind(BuildCommonOperationPart<T>, spec, options, std::placeholders::_1))
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
    LOG_DEBUG("Starting reduce operation (PreparationId: %s)",
        preparer.GetPreparationId().c_str());
    return DoExecuteReduce(
        preparer,
        CreateSimpleOperationIo(reducer, preparer, spec, options, /* allowSkiff = */ false),
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
    LOG_DEBUG("Starting raw reduce operation (PreparationId: %s)",
        preparer.GetPreparationId().c_str());
    return DoExecuteReduce(
        preparer,
        CreateSimpleOperationIo(reducer, preparer, spec),
        spec,
        reducer,
        options);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TOperationId DoExecuteJoinReduce(
    TOperationPreparer& preparer,
    const TSimpleOperationIo& operationIo,
    TJoinReduceOperationSpecBase<T> spec,
    const IJob& reducer,
    const TOperationOptions& options)
{
    if (options.CreateDebugOutputTables_) {
        CreateDebugOutputTables(spec, preparer);
    }
    if (options.CreateOutputTables_) {
        CheckInputTablesExist(preparer, operationIo.Inputs);
        CreateOutputTables(preparer, operationIo.Outputs);
    }

    TJobPreparer reduce(
        preparer,
        spec.ReducerSpec_,
        reducer,
        operationIo.Outputs.size(),
        operationIo.JobFiles,
        options);

    spec.Title_ = spec.Title_.GetOrElse(AddModeToTitleIfDebug(reduce.GetClassName()));

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
                .Item("enable_range_index").Value(true)
            .EndMap()
            .DoIf(!TConfig::Get()->TableWriter.Empty(), [&] (TFluentMap fluent) {
                fluent.Item("table_writer").Value(TConfig::Get()->TableWriter);
            })
        .EndMap()
        .Do(std::bind(BuildCommonOperationPart<T>, spec, options, std::placeholders::_1))
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
    LOG_DEBUG("Starting join reduce operation (PreparationId: %s)",
        preparer.GetPreparationId().c_str());
    return DoExecuteJoinReduce(
        preparer,
        CreateSimpleOperationIo(reducer, preparer, spec, options, /* allowSkiff = */ false),
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
    LOG_DEBUG("Starting raw join reduce operation (PreparationId: %s)",
        preparer.GetPreparationId().c_str());
    return DoExecuteJoinReduce(
        preparer,
        CreateSimpleOperationIo(reducer, preparer, spec),
        spec,
        reducer,
        options);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TOperationId DoExecuteMapReduce(
    TOperationPreparer& preparer,
    const TMapReduceOperationIo& operationIo,
    TMapReduceOperationSpecBase<T> spec,
    const IJob* mapper,
    const IJob* reduceCombiner,
    const IJob& reducer,
    const TOperationOptions& options)
{
    TVector<TRichYPath> allOutputs;
    allOutputs.insert(allOutputs.end(), operationIo.MapOutputs.begin(), operationIo.MapOutputs.end());
    allOutputs.insert(allOutputs.end(), operationIo.Outputs.begin(), operationIo.Outputs.end());

    if (options.CreateDebugOutputTables_) {
        CreateDebugOutputTables(spec, preparer);
    }
    if (options.CreateOutputTables_) {
        CheckInputTablesExist(preparer, operationIo.Inputs);
        CreateOutputTables(preparer, allOutputs);
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
        .DoIf(spec.ForceReduceCombiners_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("force_reduce_combiners").Value(*spec.ForceReduceCombiners_);
        })
        .Item("map_job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_row_index").Value(true)
                .Item("enable_range_index").Value(true)
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
        .Do([&] (TFluentMap) {
            spec.Title_ = spec.Title_.GetOrElse(AddModeToTitleIfDebug(title + "reducer:" + reduce.GetClassName()));
        })
        .Do(std::bind(BuildCommonOperationPart<T>, spec, options, std::placeholders::_1))
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
    LOG_DEBUG("Starting map-reduce operation (PreparationId: %s)",
        preparer.GetPreparationId().c_str());
    TMapReduceOperationSpec spec = spec_;

    TMapReduceOperationIo operationIo;
    auto structuredInputs = CanonizeStructuredTableList(preparer.GetAuth(), spec.GetStructuredInputs());
    auto structuredMapOutputs = CanonizeStructuredTableList(preparer.GetAuth(), spec.GetStructuredMapOutputs());
    auto structuredOutputs = CanonizeStructuredTableList(preparer.GetAuth(), spec.GetStructuredOutputs());

    const bool inferOutputSchema = options.InferOutputSchema_.GetOrElse(TConfig::Get()->InferTableSchema);

    TVector<TTableSchema> currentInferenceResult;

    auto fixSpec = [&] (const TFormat& format) {
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

    VerifyHasElements(structuredInputs, "inputs");

    TFormatBuilder formatBuilder(
        preparer.GetClientRetryPolicy(),
        preparer.GetAuth(),
        preparer.GetTransactionId(),
        options);

    if (mapper) {
        auto mapperOutputDescription =
            spec.GetIntermediateMapOutputDescription()
            .GetOrElse(TUnspecifiedTableStructure());
        TStructuredJobTableList mapperOutput = {
            TStructuredJobTable::Intermediate(mapperOutputDescription),
        };

        for (const auto& table : structuredMapOutputs) {
            mapperOutput.push_back(TStructuredJobTable{table.Description, table.RichYPath});
        }

        auto hints = spec.MapperFormatHints_;

        auto mapperInferenceResult = PrepareOperation<TStructuredJobTableList>(
            *mapper,
            TOperationPreparationContext(
                structuredInputs,
                mapperOutput,
                preparer.GetAuth(),
                preparer.GetClientRetryPolicy(),
                preparer.GetTransactionId()),
            &structuredInputs,
            /* outputs */ nullptr,
            hints);

        auto nodeReaderFormat = NodeReaderFormatFromHintAndGlobalConfig(spec.MapperFormatHints_);

        auto [inputFormat, inputFormatConfig] = formatBuilder.CreateFormat(
           *mapper,
           EIODirection::Input,
           structuredInputs,
           hints.InputFormatHints_,
           nodeReaderFormat,
           /* allowFormatFromTableAttribute */ true);

        auto [outputFormat, outputFormatConfig] = formatBuilder.CreateFormat(
            *mapper,
            EIODirection::Output,
            mapperOutput,
            hints.OutputFormatHints_,
            ENodeReaderFormat::Yson,
            /* allowFormatFromTableAttribute */ false);

        operationIo.MapperJobFiles = CreateFormatConfig(inputFormatConfig, outputFormatConfig);
        operationIo.MapperInputFormat = inputFormat;
        operationIo.MapperOutputFormat = outputFormat;

        Y_VERIFY(mapperInferenceResult.size() >= 1);
        currentInferenceResult = TVector<TTableSchema>{mapperInferenceResult[0]};
        // The first output as it corresponds to the intermediate data.
        TVector<TTableSchema> additionalOutputsInferenceResult(mapperInferenceResult.begin() + 1, mapperInferenceResult.end());

        operationIo.MapOutputs = GetPathList(
            structuredMapOutputs,
            additionalOutputsInferenceResult,
            inferOutputSchema);
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

        auto reduceCombinerOutputDescription = spec.GetIntermediateReduceCombinerOutputDescription()
            .GetOrElse(TUnspecifiedTableStructure());

        TStructuredJobTableList outputs = {
            TStructuredJobTable::Intermediate(reduceCombinerOutputDescription),
        };

        auto hints = spec.ReduceCombinerFormatHints_;

        if (isFirstStep) {
            currentInferenceResult = PrepareOperation<TStructuredJobTableList>(
                *reduceCombiner,
                TOperationPreparationContext(
                    inputs,
                    outputs,
                    preparer.GetAuth(),
                    preparer.GetClientRetryPolicy(),
                    preparer.GetTransactionId()),
                &inputs,
                /* outputs */ nullptr,
                hints);
        } else {
            currentInferenceResult = PrepareOperation<TStructuredJobTableList>(
                *reduceCombiner,
                TSpeculativeOperationPreparationContext(
                    currentInferenceResult,
                    inputs,
                    outputs),
                /* inputs */ nullptr,
                /* outputs */ nullptr,
                hints);
        }

        auto [inputFormat, inputFormatConfig] = formatBuilder.CreateFormat(
            *reduceCombiner,
            EIODirection::Input,
            inputs,
            hints.InputFormatHints_,
            ENodeReaderFormat::Yson,
            /* allowFormatFromTableAttribute = */ isFirstStep);

        auto [outputFormat, outputFormatConfig] = formatBuilder.CreateFormat(
            *reduceCombiner,
            EIODirection::Output,
            outputs,
            hints.OutputFormatHints_,
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

    auto hints = spec.ReducerFormatHints_;

    TVector<TTableSchema> reducerInferenceResult;
    if (isFirstStep) {
        reducerInferenceResult = PrepareOperation(
            reducer,
            TOperationPreparationContext(
                structuredInputs,
                structuredOutputs,
                preparer.GetAuth(),
                preparer.GetClientRetryPolicy(),
                preparer.GetTransactionId()),
            &structuredInputs,
            &structuredOutputs,
            hints);
    } else {
        reducerInferenceResult = PrepareOperation<TStructuredJobTableList>(
            reducer,
            TSpeculativeOperationPreparationContext(
                currentInferenceResult,
                reducerInputs,
                structuredOutputs),
            /* inputs */ nullptr,
            &structuredOutputs,
            hints);
    }

    auto [inputFormat, inputFormatConfig] = formatBuilder.CreateFormat(
        reducer,
        EIODirection::Input,
        reducerInputs,
        hints.InputFormatHints_,
        ENodeReaderFormat::Yson,
        /* allowFormatFromTableAttribute = */ isFirstStep);

    auto [outputFormat, outputFormatConfig] = formatBuilder.CreateFormat(
        reducer,
        EIODirection::Output,
        ToStructuredJobTableList(spec.GetStructuredOutputs()),
        hints.OutputFormatHints_,
        ENodeReaderFormat::Yson,
        /* allowFormatFromTableAttribute = */ false);
    operationIo.ReducerJobFiles = CreateFormatConfig(inputFormatConfig, outputFormatConfig);
    operationIo.ReducerInputFormat = inputFormat;
    operationIo.ReducerOutputFormat = outputFormat;

    if (isFirstStep) {
        fixSpec(operationIo.ReducerInputFormat);
    }

    operationIo.Inputs = GetPathList(
        ApplyProtobufColumnFilters(
            structuredInputs,
            preparer,
            spec,
            options),
        /* jobSchemaInferenceResult */ Nothing(),
        /* inferSchema */ false);

    operationIo.Outputs = GetPathList(
        structuredOutputs,
        reducerInferenceResult,
        inferOutputSchema);

    VerifyHasElements(operationIo.Outputs, "outputs");

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
    LOG_DEBUG("Starting raw map-reduce operation (PreparationId: %s)",
        preparer.GetPreparationId().c_str());
    TMapReduceOperationIo operationIo;
    operationIo.Inputs = CanonizeYPaths(/* retryPolicy */ nullptr, preparer.GetAuth(), spec.GetInputs());
    operationIo.MapOutputs = CanonizeYPaths(/* retryPolicy */ nullptr, preparer.GetAuth(), spec.GetMapOutputs());
    operationIo.Outputs = CanonizeYPaths(/* retryPolicy */ nullptr, preparer.GetAuth(), spec.GetOutputs());

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
    LOG_DEBUG("Starting sort operation (PreparationId: %s)",
        preparer.GetPreparationId().c_str());
    auto inputs = CanonizeYPaths(/* retryPolicy */ nullptr, preparer.GetAuth(), spec.Inputs_);
    auto output = CanonizeYPath(nullptr, preparer.GetAuth(), spec.Output_);

    if (options.CreateOutputTables_) {
        CheckInputTablesExist(preparer, inputs);
        CreateOutputTable(preparer, output);
    }

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("input_table_paths").List(inputs)
        .Item("output_table_path").Value(output)
        .Item("sort_by").Value(spec.SortBy_)
        .DoIf(spec.SchemaInferenceMode_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("schema_inference_mode").Value(::ToString(*spec.SchemaInferenceMode_));
        })
        .Do(std::bind(BuildCommonOperationPart<TSortOperationSpec>, spec, options, std::placeholders::_1))
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
    LOG_DEBUG("Starting merge operation (PreparationId: %s)",
        preparer.GetPreparationId().c_str());
    auto inputs = CanonizeYPaths(/* retryPolicy */ nullptr, preparer.GetAuth(), spec.Inputs_);
    auto output = CanonizeYPath(nullptr, preparer.GetAuth(), spec.Output_);

    if (options.CreateOutputTables_) {
        CheckInputTablesExist(preparer, inputs);
        CreateOutputTable(preparer, output);
    }

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("input_table_paths").List(inputs)
        .Item("output_table_path").Value(output)
        .Item("mode").Value(::ToString(spec.Mode_))
        .Item("combine_chunks").Value(spec.CombineChunks_)
        .Item("force_transform").Value(spec.ForceTransform_)
        .Item("merge_by").Value(spec.MergeBy_)
        .DoIf(spec.SchemaInferenceMode_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("schema_inference_mode").Value(::ToString(*spec.SchemaInferenceMode_));
        })
        .Do(std::bind(BuildCommonOperationPart<TMergeOperationSpec>, spec, options, std::placeholders::_1))
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
    LOG_DEBUG("Starting erase operation (PreparationId: %s)",
        preparer.GetPreparationId().c_str());
    auto tablePath = CanonizeYPath(nullptr, preparer.GetAuth(), spec.TablePath_);

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("table_path").Value(tablePath)
        .Item("combine_chunks").Value(spec.CombineChunks_)
        .DoIf(spec.SchemaInferenceMode_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("schema_inference_mode").Value(::ToString(*spec.SchemaInferenceMode_));
        })
        .Do(std::bind(BuildCommonOperationPart<TEraseOperationSpec>, spec, options, std::placeholders::_1))
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
    LOG_DEBUG("Starting remote copy operation (PreparationId: %s)",
        preparer.GetPreparationId().c_str());
    auto inputs = CanonizeYPaths(/* retryPolicy */ nullptr, preparer.GetAuth(), spec.Inputs_);
    auto output = CanonizeYPath(nullptr, preparer.GetAuth(), spec.Output_);

    if (options.CreateOutputTables_) {
        CreateOutputTable(preparer, output);
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
        .Do(std::bind(BuildCommonOperationPart<TRemoteCopyOperationSpec>, spec, options, std::placeholders::_1))
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
    LOG_DEBUG("Starting vanilla operation (PreparationId: %s)",
        preparer.GetPreparationId().c_str());

    auto addTask = [&](TFluentMap fluent, const TVanillaTask& task) {
        Y_VERIFY(task.Job_.Get());
        if (HoldsAlternative<TVoidStructuredRowStream>(task.Job_->GetOutputRowStreamDescription())) {
            Y_ENSURE_EX(task.Outputs_.empty(),
                TApiUsageError() << "Vanilla task with void IVanillaJob doesn't expect output tables");
            TJobPreparer jobPreparer(
                preparer,
                task.Spec_,
                *task.Job_,
                /* outputTableCount */ 0,
                /* smallFileList */ {},
                options);
            fluent
                .Item(task.Name_).BeginMap()
                    .Item("job_count").Value(task.JobCount_)
                    .Do(std::bind(
                        BuildUserJobFluently1,
                        std::cref(jobPreparer),
                        /* inputFormat */ Nothing(),
                        /* outputFormat */ Nothing(),
                        std::placeholders::_1))
                .EndMap();
        } else {
            auto operationIo = CreateSimpleOperationIo(
                *task.Job_,
                preparer,
                task,
                options,
                false);
            Y_ENSURE_EX(operationIo.Outputs.size() > 0,
                TApiUsageError() << "Vanilla task with IVanillaJob that has table writer expects output tables");
            if (options.CreateOutputTables_) {
                CreateOutputTables(preparer, operationIo.Outputs);
            }
            TJobPreparer jobPreparer(
                preparer,
                task.Spec_,
                *task.Job_,
                operationIo.Outputs.size(),
                operationIo.JobFiles,
                options);
            fluent
                .Item(task.Name_).BeginMap()
                    .Item("job_count").Value(task.JobCount_)
                    .Do(std::bind(
                        BuildUserJobFluently1,
                        std::cref(jobPreparer),
                        /* inputFormat */ Nothing(),
                        operationIo.OutputFormat,
                        std::placeholders::_1))
                    .Item("output_table_paths").List(operationIo.Outputs)
                    .Item("job_io").BeginMap()
                        .DoIf(!TConfig::Get()->TableWriter.Empty(), [&](TFluentMap fluent) {
                            fluent.Item("table_writer").Value(TConfig::Get()->TableWriter);
                        })
                        .Item("control_attributes").BeginMap()
                            .Item("enable_row_index").Value(TNode(true))
                            .Item("enable_range_index").Value(TNode(true))
                        .EndMap()
                    .EndMap()
                .EndMap();
        }
    };

    if (options.CreateDebugOutputTables_) {
        CreateDebugOutputTables(spec, preparer);
    }

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("tasks").DoMapFor(spec.Tasks_, addTask)
        .Do(std::bind(BuildCommonOperationPart<TVanillaOperationSpec>, spec, options, std::placeholders::_1))
    .EndMap().EndMap();

    BuildCommonUserOperationPart(spec, &specNode["spec"]);

    auto operationId = preparer.StartOperation(
        "vanilla",
        MergeSpec(std::move(specNode), options),
        /* useStartOperationRequest */ true);

    return operationId;
}

////////////////////////////////////////////////////////////////////////////////

class TOperation::TOperationImpl
    : public TThrRefBase
{
public:
    TOperationImpl(
        IClientRetryPolicyPtr clientRetryPolicy,
        TAuth auth,
        const TOperationId& operationId)
        : ClientRetryPolicy_(clientRetryPolicy)
        , Auth_(std::move(auth))
        , Id_(operationId)
    { }

    const TOperationId& GetId() const;
    TString GetWebInterfaceUrl() const;
    NThreading::TFuture<void> Watch(TYtPoller& ytPoller);

    EOperationBriefState GetBriefState();
    TMaybe<TYtError> GetError();
    TJobStatistics GetJobStatistics();
    TMaybe<TOperationBriefProgress> GetBriefProgress();
    void AbortOperation();
    void CompleteOperation();
    void SuspendOperation(const TSuspendOperationOptions& options);
    void ResumeOperation(const TResumeOperationOptions& options);
    TOperationAttributes GetAttributes(const TGetOperationOptions& options);
    void UpdateParameters(const TUpdateOperationParametersOptions& options);
    TJobAttributes GetJob(const TJobId& jobId, const TGetJobOptions& options);
    TListJobsResult ListJobs(const TListJobsOptions& options);

    void AsyncFinishOperation(TOperationAttributes operationAttributes);
    void FinishWithException(std::exception_ptr exception);
    void UpdateBriefProgress(TMaybe<TOperationBriefProgress> briefProgress);
    void AnalyzeUnrecognizedSpec(TNode unrecognizedSpec);

private:
    void UpdateAttributesAndCall(bool needJobStatistics, std::function<void(const TOperationAttributes&)> func);

    void SyncFinishOperationImpl(const TOperationAttributes&);
    static void* SyncFinishOperationProc(void* );

private:
    IClientRetryPolicyPtr ClientRetryPolicy_;
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
        auto filter = TOperationAttributeFilter()
            .Add(EOperationAttribute::State)
            .Add(EOperationAttribute::BriefProgress)
            .Add(EOperationAttribute::Result);

        if (!UnrecognizedSpecAnalyzed_) {
            filter.Add(EOperationAttribute::UnrecognizedSpec);
        }

        OperationState_ = batchRequest->GetOperation(
            OperationImpl_->GetId(),
            TGetOperationOptions().AttributeFilter(filter));
    }

    EStatus OnRequestExecuted() override
    {
        try {
            const auto& attributes = OperationState_.GetValue();
            if (!UnrecognizedSpecAnalyzed_ && !attributes.UnrecognizedSpec.Empty()) {
                OperationImpl_->AnalyzeUnrecognizedSpec(*attributes.UnrecognizedSpec);
                UnrecognizedSpecAnalyzed_ = true;
            }
            Y_VERIFY(attributes.BriefState,
                "get_operation for operation %s has not returned \"state\" field",
                GetGuidAsString(OperationImpl_->GetId()).Data());
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
    bool UnrecognizedSpecAnalyzed_ = false;
};

////////////////////////////////////////////////////////////////////////////////

const TOperationId& TOperation::TOperationImpl::GetId() const
{
    return Id_;
}

static TString GetOperationWebInterfaceUrl(TStringBuf serverName, TOperationId operationId)
{
    serverName.ChopSuffix(".yt.yandex-team.ru");
    serverName.ChopSuffix(".yt.yandex.net");
    return TStringBuilder() << "https://yt.yandex-team.ru/" << serverName <<
        "/operations/" << GetGuidAsString(operationId);
}

TString TOperation::TOperationImpl::GetWebInterfaceUrl() const
{
    return GetOperationWebInterfaceUrl(Auth_.ServerName, Id_);
}

NThreading::TFuture<void> TOperation::TOperationImpl::Watch(TYtPoller& ytPoller)
{
    auto guard = Guard(Lock_);

    if (!CompletePromise_) {
        CompletePromise_ = NThreading::NewPromise<void>();
        ytPoller.Watch(::MakeIntrusive<TOperationPollerItem>(this));
    }

    auto operationId = GetId();
    TAbortableRegistry::Get()->Add(
        operationId,
        ::MakeIntrusive<TOperationAbortable>(ClientRetryPolicy_, Auth_, operationId));
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
        Y_VERIFY(attributes.BriefState,
            "get_operation for operation %s has not returned \"state\" field",
            GetGuidAsString(Id_).Data());
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

void TOperation::TOperationImpl::AnalyzeUnrecognizedSpec(TNode unrecognizedSpec)
{
    static const TVector<TVector<TString>> knownUnrecognizedSpecFieldPaths = {
        {"mapper", "class_name"},
        {"reducer", "class_name"},
        {"reduce_combiner", "class_name"},
    };

    auto removeByPath = [] (TNode& node, auto pathBegin, auto pathEnd, auto& removeByPath) {
        if (pathBegin == pathEnd) {
            return;
        }
        if (!node.IsMap()) {
            return;
        }
        auto* child = node.AsMap().FindPtr(*pathBegin);
        if (!child) {
            return;
        }
        removeByPath(*child, std::next(pathBegin), pathEnd, removeByPath);
        if (std::next(pathBegin) == pathEnd || (child->IsMap() && child->Empty())) {
            node.AsMap().erase(*pathBegin);
        }
    };

    Y_VERIFY(unrecognizedSpec.IsMap());
    for (const auto& knownFieldPath : knownUnrecognizedSpecFieldPaths) {
        Y_VERIFY(!knownFieldPath.empty());
        removeByPath(unrecognizedSpec, knownFieldPath.cbegin(), knownFieldPath.cend(), removeByPath);
    }

    if (!unrecognizedSpec.Empty()) {
        LOG_INFO(
            "WARNING! Unrecognized spec for operation %s is not empty "
            "(fields added by the YT API library are excluded): %s",
            GetGuidAsString(Id_).Data(),
            NodeToYsonString(unrecognizedSpec).Data());
    }
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
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
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

void TOperation::TOperationImpl::AbortOperation()
{
    NYT::NDetail::AbortOperation(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, Id_);
}

void TOperation::TOperationImpl::CompleteOperation()
{
    NYT::NDetail::CompleteOperation(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, Id_);
}

void TOperation::TOperationImpl::SuspendOperation(const TSuspendOperationOptions& options)
{
    NYT::NDetail::SuspendOperation(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, Id_, options);
}

void TOperation::TOperationImpl::ResumeOperation(const TResumeOperationOptions& options)
{
    NYT::NDetail::ResumeOperation(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, Id_, options);
}

TOperationAttributes TOperation::TOperationImpl::GetAttributes(const TGetOperationOptions& options) {
    return NYT::NDetail::GetOperation(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, Id_, options);
}

void TOperation::TOperationImpl::UpdateParameters(const TUpdateOperationParametersOptions& options)
{
    return NYT::NDetail::UpdateOperationParameters(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, Id_, options);
}

TJobAttributes TOperation::TOperationImpl::GetJob(const TJobId& jobId, const TGetJobOptions& options)
{
    return NYT::NDetail::GetJob(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, Id_, jobId, options);
}

TListJobsResult TOperation::TOperationImpl::ListJobs(const TListJobsOptions& options)
{
    return NYT::NDetail::ListJobs(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, Id_, options);
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
    Y_VERIFY(attributes.BriefState,
        "get_operation for operation %s has not returned \"state\" field",
        GetGuidAsString(Id_).Data());
    Y_VERIFY(*attributes.BriefState != EOperationBriefState::InProgress);

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
                failedJobStderrInfo = NYT::NDetail::GetFailedJobInfo(ClientRetryPolicy_, Auth_, Id_, TGetFailedJobInfoOptions());
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
    , Impl_(::MakeIntrusive<TOperationImpl>(Client_->GetRetryPolicy(), Client_->GetAuth(), id))
{
}

const TOperationId& TOperation::GetId() const
{
    return Impl_->GetId();
}

TString TOperation::GetWebInterfaceUrl() const
{
    return Impl_->GetWebInterfaceUrl();
}

NThreading::TFuture<void> TOperation::Watch()
{
    return Impl_->Watch(Client_->GetYtPoller());
}

TVector<TFailedJobInfo> TOperation::GetFailedJobInfo(const TGetFailedJobInfoOptions& options)
{
    return NYT::NDetail::GetFailedJobInfo(Client_->GetRetryPolicy(), Client_->GetAuth(), GetId(), options);
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

void TOperation::SuspendOperation(const TSuspendOperationOptions& options)
{
    Impl_->SuspendOperation(options);
}

void TOperation::ResumeOperation(const TResumeOperationOptions& options)
{
    Impl_->ResumeOperation(options);
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

////////////////////////////////////////////////////////////////////////////////

TOperationPreparer::TOperationPreparer(TClientPtr client, TTransactionId transactionId)
    : Client_(std::move(client))
    , TransactionId_(transactionId)
    , FileTransaction_(MakeHolder<TPingableTransaction>(
        Client_->GetRetryPolicy(),
        Client_->GetAuth(),
        TransactionId_,
        TStartTransactionOptions()))
    , ClientRetryPolicy_(Client_->GetRetryPolicy())
    , PreparationId_(CreateGuidAsString())
{ }

const TAuth& TOperationPreparer::GetAuth() const
{
    return Client_->GetAuth();
}

TTransactionId TOperationPreparer::GetTransactionId() const
{
    return TransactionId_;
}

const TString& TOperationPreparer::GetPreparationId() const
{
    return PreparationId_;
}

const IClientRetryPolicyPtr& TOperationPreparer::GetClientRetryPolicy() const
{
    return ClientRetryPolicy_;
}

////////////////////////////////////////////////////////////////////////////////

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

    auto ysonSpec = NodeToYsonString(spec);
    auto responseInfo = RetryRequestWithPolicy(
        ClientRetryPolicy_->CreatePolicyForStartOperationRequest(),
        GetAuth(),
        header,
        ysonSpec);
    TOperationId operationId = ParseGuidFromResponse(responseInfo.Response);
    LOG_DEBUG("Operation started (OperationId: %s; PreparationId: %s)",
        GetGuidAsString(operationId).c_str(),
        GetPreparationId().c_str());

    LOG_INFO("Operation %s started (%s): %s",
        GetGuidAsString(operationId).data(),
        operationType.data(),
        GetOperationWebInterfaceUrl(GetAuth().ServerName, operationId).data());

    TOperationExecutionTimeTracker::Get()->Start(operationId);

    Client_->GetYtPoller().Watch(
        new TWaitOperationStartPollerItem(operationId, std::move(FileTransaction_)));

    return operationId;
}

void TOperationPreparer::LockFiles(TVector<TRichYPath>* paths)
{
    CheckValidity();

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
    ExecuteBatch(ClientRetryPolicy_->CreatePolicyForGenericRequest(), GetAuth(), lockRequest);

    TVector<NThreading::TFuture<TNode>> nodeIdFutures;
    nodeIdFutures.reserve(paths->size());
    TRawBatchRequest getNodeIdRequest;
    for (const auto& lockIdFuture : lockIdFutures) {
        nodeIdFutures.push_back(getNodeIdRequest.Get(
            FileTransaction_->GetId(),
            TStringBuilder() << '#' << GetGuidAsString(lockIdFuture.GetValue()) << "/@node_id",
            TGetOptions()));
    }
    ExecuteBatch(ClientRetryPolicy_->CreatePolicyForGenericRequest(), GetAuth(), getNodeIdRequest);

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
    auto retryPolicy = client->GetRetryPolicy();
    auto auth = client->GetAuth();
    auto operation = ::MakeIntrusive<TOperation>(operationId, std::move(client));
    if (options.Wait_) {
        auto finishedFuture = operation->Watch();
        TWaitProxy::Get()->WaitFuture(finishedFuture);
        finishedFuture.GetValue();
        if (TConfig::Get()->WriteStderrSuccessfulJobs) {
            auto stderrs = GetJobsStderr(retryPolicy, auth, operation->GetId());
            for (const auto& jobStderr : stderrs) {
                if (!jobStderr.empty()) {
                    Cerr << jobStderr << '\n';
                }
            }
        }
    }
    return operation;
}

////////////////////////////////////////////////////////////////////////////////

void ResetUseClientProtobuf(const char* methodName)
{
    Cerr << "WARNING! OPTION `TConfig::UseClientProtobuf' IS RESET TO `true'; "
        << "IT CAN DETERIORATE YOUR CODE PERFORMANCE!!! DON'T USE DEPRECATED METHOD `"
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

::TIntrusivePtr<IYdlReaderImpl> CreateJobYdlReader()
{
    return ::MakeIntrusive<TNodeYdlTableReader>(
        ::MakeIntrusive<TJobReader>(0),
        GetJobInputTypeHashes());
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

::TIntrusivePtr<IYdlWriterImpl> CreateJobYdlWriter(size_t outputTableCount)
{
    return ::MakeIntrusive<TNodeYdlTableWriter>(
        MakeHolder<TJobWriter>(outputTableCount),
        GetJobOutputTypeHashes());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
