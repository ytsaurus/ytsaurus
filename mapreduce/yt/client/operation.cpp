#include "operation.h"

#include "client.h"
#include "file_reader.h"
#include "operation_tracker.h"
#include "yt_poller.h"

#include <mapreduce/yt/interface/errors.h>
#include <mapreduce/yt/interface/job_statistics.h>

#include <mapreduce/yt/common/log.h>
#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/serialize.h>
#include <mapreduce/yt/common/fluent.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/wait_proxy.h>

#include <library/yson/writer.h>
#include <library/yson/json_writer.h>

#include <mapreduce/yt/http/requests.h>
#include <mapreduce/yt/http/retry_request.h>
#include <mapreduce/yt/http/error.h>

#include <mapreduce/yt/io/job_reader.h>
#include <mapreduce/yt/io/job_writer.h>
#include <mapreduce/yt/io/yamr_table_reader.h>
#include <mapreduce/yt/io/yamr_table_writer.h>
#include <mapreduce/yt/io/node_table_reader.h>
#include <mapreduce/yt/io/node_table_writer.h>
#include <mapreduce/yt/io/proto_table_reader.h>
#include <mapreduce/yt/io/proto_table_writer.h>
#include <mapreduce/yt/io/proto_helpers.h>

#include <mapreduce/yt/raw_client/raw_batch_request.h>
#include <mapreduce/yt/raw_client/raw_requests.h>

#include <util/string/printf.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/execpath.h>
#include <util/system/rwlock.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>
#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/stream/buffer.h>

#include <library/digest/md5/md5.h>

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

namespace {

ui64 RoundUpFileSize(ui64 size)
{
    constexpr ui64 roundUpTo = 4ull << 10;
    return (size + roundUpTo - 1) & ~(roundUpTo - 1);
}

bool IsLocalMode(const TAuth& auth)
{
    static yhash<TString, bool> localModeMap;
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

////////////////////////////////////////////////////////////////////////////////

class TJobPreparer
    : private TNonCopyable
{
public:
    TJobPreparer(
        const TAuth& auth,
        const TTransactionId& transactionId,
        const TString& commandLineName,
        const TUserJobSpec& spec,
        IJob* job,
        size_t outputTableCount,
        const TMultiFormatDesc& inputDesc,
        const TMultiFormatDesc& outputDesc,
        const TOperationOptions& options)
        : Auth_(auth)
        , TransactionId_(transactionId)
        , Spec_(spec)
        , InputDesc_(inputDesc)
        , OutputDesc_(outputDesc)
        , Options_(options)
    {
        CreateStorage();
        UploadFilesFromSpec();
        UploadJobState(job);
        UploadProtoConfig("proto_input", inputDesc);
        UploadProtoConfig("proto_output", outputDesc);

        BinaryPath_ = GetExecPath();
        if (TConfig::Get()->JobBinary) {
            BinaryPath_ = TConfig::Get()->JobBinary;
        }
        if (Spec_.JobBinary_) {
            BinaryPath_ = *Spec_.JobBinary_;
        }

        TString jobBinaryPath;
        if (!IsLocalMode(auth)) {
            UploadBinary();
            jobBinaryPath = "./cppbinary";
        } else {
            jobBinaryPath = BinaryPath_;
        }

        ClassName_ = TJobFactory::Get()->GetJobName(job);
        Command_ = TStringBuilder() <<
            options.JobCommandPrefix_ <<
            (TConfig::Get()->UseClientProtobuf ? "" : "YT_USE_CLIENT_PROTOBUF=0 ") <<
            jobBinaryPath << " " <<
            commandLineName << " " <<
            "\"" << ClassName_ << "\" " <<
            outputTableCount << " " <<
            HasState_ <<
            options.JobCommandSuffix_;
    }

    const yvector<TRichYPath>& GetFiles() const
    {
        return Files_;
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
    TAuth Auth_;
    TTransactionId TransactionId_;
    TUserJobSpec Spec_;
    TMultiFormatDesc InputDesc_;
    TMultiFormatDesc OutputDesc_;
    TOperationOptions Options_;

    TString BinaryPath_;
    yvector<TRichYPath> Files_;
    bool HasState_ = false;
    TString ClassName_;
    TString Command_;
    ui64 TotalFileSize_ = 0;

    static void CalculateMD5(const TString& localFileName, char* buf)
    {
        MD5::File(~localFileName, buf);
    }

    static void CalculateMD5(const TBuffer& buffer, char* buf)
    {
        MD5::Data(reinterpret_cast<const unsigned char*>(buffer.Data()), buffer.Size(), buf);
    }

    static THolder<IInputStream> CreateStream(const TString& localPath)
    {
        return new TMappedFileInput(localPath);
    }

    static THolder<IInputStream> CreateStream(const TBuffer& buffer)
    {
        return new TBufferInput(buffer);
    }

    TString GetFileStorage() const
    {
        return Options_.FileStorage_ ?
            *Options_.FileStorage_ :
            TConfig::Get()->RemoteTempFilesDirectory;
    }

    void CreateStorage() const
    {
        TString cypressFolder = TStringBuilder() << GetFileStorage() << "/hash";
        if (!Exists(Auth_, Options_.FileStorageTransactionId_, cypressFolder)) {
            NYT::NDetail::Create(Auth_, Options_.FileStorageTransactionId_, cypressFolder, NT_MAP,
                TCreateOptions()
                .IgnoreExisting(true)
                .Recursive(true));
        }
    }

    template <class TSource>
    TString UploadToCache(const TSource& source) const
    {
        constexpr size_t md5Size = 32;
        char buf[md5Size + 1];
        CalculateMD5(source, buf);

        TString twoDigits(buf + md5Size - 2, 2);

        TString cypressPath = TStringBuilder() << GetFileStorage() <<
            "/hash/" << twoDigits << "/" << buf;

        int retryCount = 256;
        for (int attempt = 0; attempt < retryCount; ++attempt) {
            TNode linkAttrs;
            if (Exists(Auth_, Options_.FileStorageTransactionId_, cypressPath + "&")) {
                try {
                    linkAttrs = Get(Auth_, Options_.FileStorageTransactionId_, cypressPath + "&/@");
                } catch (TErrorResponse& e) {
                    if (!e.IsResolveError()) {
                        throw;
                    }
                }
            }

            try {
                bool linkExists = false;
                if (linkAttrs.GetType() != TNode::UNDEFINED) {
                    if (linkAttrs["type"] == "link" &&
                        (!linkAttrs.HasKey("broken") || !linkAttrs["broken"].AsBool()))
                    {
                        linkExists = true;
                    } else {
                        NYT::NDetail::Remove(Auth_, Options_.FileStorageTransactionId_, cypressPath + "&",
                            TRemoveOptions().Recursive(true).Force(true));
                    }
                }

                if (linkExists) {
                    NYT::NDetail::Set(Auth_, Options_.FileStorageTransactionId_, cypressPath + "/@touched", "true");
                    NYT::NDetail::Set(Auth_, Options_.FileStorageTransactionId_, cypressPath + "&/@touched", "true");
                    return cypressPath;
                }

                TString uniquePath = TStringBuilder() << GetFileStorage() <<
                    "/" << twoDigits << "/cpp_" << CreateGuidAsString();

                NYT::NDetail::Create(Auth_, Options_.FileStorageTransactionId_, uniquePath, NT_FILE,
                    TCreateOptions()
                        .IgnoreExisting(true)
                        .Recursive(true)
                        .Attributes(TNode()("hash", buf)("touched", true)));

                {
                    THttpHeader header("PUT", GetWriteFileCommand());
                    header.SetToken(Auth_.Token);
                    header.AddPath(uniquePath);
                    auto streamMaker = [&source] () {
                        return CreateStream(source);
                    };
                    RetryHeavyWriteRequest(Auth_, Options_.FileStorageTransactionId_, header, streamMaker);
                }

                NYT::NDetail::Link(Auth_, Options_.FileStorageTransactionId_, uniquePath, cypressPath,
                    TLinkOptions()
                        .IgnoreExisting(true)
                        .Recursive(true)
                        .Attributes(TNode()("touched", true)));

            } catch (TErrorResponse& e) {
                if (!e.IsResolveError() || attempt + 1 == retryCount) {
                    throw;
                }
                TWaitProxy::Sleep(TDuration::Seconds(1));
                continue;
            }
            break;
        }
        return cypressPath;
    }

    void UploadFilesFromSpec()
    {
        for (const auto& file : Spec_.Files_) {
            if (!Exists(Auth_, TransactionId_, file.Path_)) {
                ythrow yexception() << "File " << file.Path_ << " does not exist";
            }

            if (ShouldMountSandbox()) {
                auto size = Get(Auth_, TransactionId_, file.Path_ + "/@uncompressed_data_size").AsInt64();

                TotalFileSize_ += RoundUpFileSize(static_cast<ui64>(size));
            }
        }

        Files_ = Spec_.Files_;

        for (const auto& localFile : Spec_.LocalFiles_) {
            TFsPath path(localFile);
            path.CheckExists();

            TFileStat stat;
            path.Stat(stat);
            bool isExecutable = stat.Mode & (S_IXUSR | S_IXGRP | S_IXOTH);

            auto cachePath = UploadToCache(localFile);

            TRichYPath cypressPath(cachePath);
            cypressPath.FileName(path.Basename());
            if (isExecutable) {
                cypressPath.Executable(true);
            }

            if (ShouldMountSandbox()) {
                TotalFileSize_ += RoundUpFileSize(stat.Size);
            }

            Files_.push_back(cypressPath);
        }
    }

    void UploadBinary()
    {
        if (ShouldMountSandbox()) {
            TFsPath path(BinaryPath_);
            TFileStat stat;
            path.Stat(stat);
            TotalFileSize_ += RoundUpFileSize(stat.Size);
        }

        auto cachePath = UploadToCache(BinaryPath_);
        Files_.push_back(TRichYPath(cachePath)
            .FileName("cppbinary")
            .Executable(true));
    }

    void UploadJobState(IJob* job)
    {
        TBufferOutput output(1 << 20);
        job->Save(output);

        if (output.Buffer().Size()) {
            auto cachePath = UploadToCache(output.Buffer());
            Files_.push_back(TRichYPath(cachePath).FileName("jobstate"));
            HasState_ = true;

            if (ShouldMountSandbox()) {
                TotalFileSize_ += RoundUpFileSize(output.Buffer().Size());
            }
        }
    }

    void UploadProtoConfig(const TString& fileName, const TMultiFormatDesc& desc) {
        if (desc.Format != TMultiFormatDesc::F_PROTO) {
            return;
        }

        TBufferOutput messageTypeList;
        for (const auto& descriptor : desc.ProtoDescriptors) {
            messageTypeList << descriptor->full_name() << Endl;
        }

        auto cachePath = UploadToCache(messageTypeList.Buffer());
        Files_.push_back(TRichYPath(cachePath).FileName(fileName));

        if (ShouldMountSandbox()) {
            TotalFileSize_ += RoundUpFileSize(messageTypeList.Buffer().Size());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

yvector<TFailedJobInfo> GetFailedJobInfo(
    const TAuth& auth,
    const TOperationId& operationId,
    const TGetFailedJobInfoOptions& options)
{
    const size_t maxJobCount = options.MaxJobCount_;
    const i64 stderrTailSize = options.StderrTailSize_;

    const auto operationPath = "//sys/operations/" + GetGuidAsString(operationId);
    const auto jobsPath = operationPath + "/jobs";

    if (!Exists(auth, TTransactionId(), jobsPath)) {
        return {};
    }

    THttpHeader header("GET", "list");
    header.AddPath(jobsPath);
    header.SetParameters(AttributeFilterToYsonString(
        TAttributeFilter()
            .AddAttribute("state")
            .AddAttribute("error")));
    auto jobList = NodeFromYsonString(RetryRequest(auth, header)).AsList();

    yvector<TFailedJobInfo> result;
    for (const auto& job : jobList) {
        if (result.size() >= maxJobCount) {
            break;
        }

        const auto& jobId = job.AsString();
        auto jobPath = jobsPath + "/" + jobId;
        auto& attributes = job.GetAttributes().AsMap();

        const auto stateIt = attributes.find("state");
        if (stateIt == attributes.end() || stateIt->second.AsString() != "failed") {
            continue;
        }
        result.push_back(TFailedJobInfo());
        auto& cur = result.back();
        cur.JobId = GetGuid(job.AsString());

        auto errorIt = attributes.find("error");
        if (errorIt != attributes.end()) {
            cur.Error = TYtError(errorIt->second);
        }

        auto stderrPath = jobPath + "/stderr";
        if (!Exists(auth, TTransactionId(), stderrPath)) {
            continue;
        }

        TRichYPath path(stderrPath);
        i64 stderrSize = Get(auth, TTransactionId(), stderrPath + "/@uncompressed_data_size").AsInt64();
        if (stderrSize > stderrTailSize) {
            path.AddRange(
                TReadRange().LowerLimit(
                    TReadLimit().Offset(stderrSize - stderrTailSize)));
        }
        IFileReaderPtr reader = new TFileReader(path, auth, TTransactionId());
        cur.Stderr = reader->ReadAll();
    }
    return result;
}

void DumpJobInfoForException(
    IOutputStream& output,
    const yvector<TFailedJobInfo>& failedJobInfoList)
{
    // Exceptions has limit to contain 65508 bytes of text, so we also limit stderr text
    constexpr size_t MAX_SIZE = 65508 / 2;

    size_t written = 0;
    for (const auto& failedJobInfo : failedJobInfoList) {
        if (written >= MAX_SIZE) {
            break;
        }
        TStringStream nextChunk;
        nextChunk << '\n';
        nextChunk << "Error: " << failedJobInfo.Error.ShortDescription() << '\n';
        if (!failedJobInfo.Stderr.empty()) {
            nextChunk << "Stderr: " << Endl;
            size_t tmpWritten = written + nextChunk.Str().size();
            if (tmpWritten >= MAX_SIZE) {
                break;
            }

            if (tmpWritten + failedJobInfo.Stderr.size() > MAX_SIZE) {
                nextChunk << failedJobInfo.Stderr.substr(failedJobInfo.Stderr.size() - (MAX_SIZE - tmpWritten));
            } else {
                nextChunk << failedJobInfo.Stderr;
            }
        }
        written += nextChunk.Str().size();
        output << nextChunk.Str();
    }
    output.Flush();
}

using TDescriptorList = yvector<const ::google::protobuf::Descriptor*>;

TMultiFormatDesc IdentityDesc(const TMultiFormatDesc& multi)
{
    const std::set<const ::google::protobuf::Descriptor*> uniqueDescrs(multi.ProtoDescriptors.begin(), multi.ProtoDescriptors.end());
    if (uniqueDescrs.size() > 1)
    {
        TApiUsageError err;
        err << __LOCATION__ << ": Different input proto descriptors";
        for (const auto& desc : multi.ProtoDescriptors) {
            err << " " << desc->full_name();
        }
        throw err;
    }
    TMultiFormatDesc result;
    result.Format = multi.Format;
    result.ProtoDescriptors.assign(uniqueDescrs.begin(), uniqueDescrs.end());
    return result;
}

//TODO: simplify to lhs == rhs after YT-6967 resolving
bool IsCompatible(const TDescriptorList& lhs, const TDescriptorList& rhs)
{
    return lhs.empty() || rhs.empty() || lhs == rhs;
}

const TMultiFormatDesc& MergeIntermediateDesc(const TMultiFormatDesc& lh, const TMultiFormatDesc& rh, const char* lhDescr, const char* rhDescr)
{
    if (rh.Format == TMultiFormatDesc::F_NONE) {
        return lh;
    } else if (lh.Format == TMultiFormatDesc::F_NONE) {
        return rh;
    } else if (lh.Format == rh.Format && IsCompatible(lh.ProtoDescriptors, rh.ProtoDescriptors)) {
        const auto& result = rh.ProtoDescriptors.empty() ? lh : rh;
        if (result.ProtoDescriptors.size() > 1) {
            ythrow TApiUsageError() << "too many proto descriptors for intermediate table";
        }
        return result;
    } else {
        ythrow TApiUsageError() << "incompatible format specifications: "
            << lhDescr << " {format=" << ui32(lh.Format) << " descrs=" << lh.ProtoDescriptors.size() << "}"
               " and "
            << rhDescr << " {format=" << ui32(rh.Format) << " descrs=" << rh.ProtoDescriptors.size() << "}"
        ;
    }
}
} // namespace

////////////////////////////////////////////////////////////////////////////////

TOperationId StartOperation(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TString& operationName,
    const TString& ysonSpec)
{
    THttpHeader header("POST", operationName);
    header.AddTransactionId(transactionId);
    header.AddMutationId();

    TOperationId operationId = ParseGuidFromResponse(
        RetryRequest(auth, header, ysonSpec, false, true));

    LOG_INFO("Operation %s started (%s): http://%s/#page=operation&mode=detail&id=%s&tab=details",
        ~GetGuidAsString(operationId), ~operationName, ~auth.ServerName, ~GetGuidAsString(operationId));

    TOperationExecutionTimeTracker::Get()->Start(operationId);

    return operationId;
}

EOperationStatus CheckOperation(
    const TAuth& auth,
    const TOperationId& operationId)
{
    auto opIdStr = GetGuidAsString(operationId);
    auto opPath = Sprintf("//sys/operations/%s", ~opIdStr);
    auto statePath = opPath + "/@state";

    if (!Exists(auth, TTransactionId(), opPath)) {
        ythrow yexception() << "Operation " << opIdStr << " does not exist";
    }

    TString state = Get(auth, TTransactionId(), statePath).AsString();

    if (state == "completed") {
        return OS_COMPLETED;

    } else if (state == "aborted" || state == "failed") {
        LOG_ERROR("Operation %s %s (%s)",
            ~opIdStr,
            ~state,
            ~ToString(TOperationExecutionTimeTracker::Get()->Finish(operationId)));

        auto errorPath = opPath + "/@result/error";
        TYtError ytError(TString("unknown operation error"));
        if (Exists(auth, TTransactionId(), errorPath)) {
            ytError = TYtError(Get(auth, TTransactionId(), errorPath));
        }

        TStringStream jobErrors;

        auto failedJobInfoList = GetFailedJobInfo(auth, operationId, TGetFailedJobInfoOptions());
        DumpJobInfoForException(jobErrors, failedJobInfoList);

        ythrow TOperationFailedError(
            state == "aborted" ?
                TOperationFailedError::Aborted :
                TOperationFailedError::Failed,
            operationId,
            ytError) << jobErrors.Str();
    }

    return OS_IN_PROGRESS;
}

void WaitForOperation(
    const TAuth& auth,
    const TOperationId& operationId)
{
    const TDuration checkOperationStateInterval =
        IsLocalMode(auth) ? TDuration::MilliSeconds(100) : TDuration::Seconds(1);

    while (true) {
        auto status = CheckOperation(auth, operationId);
        if (status == OS_COMPLETED) {
            LOG_INFO("Operation %s completed (%s)",
                ~GetGuidAsString(operationId),
                ~ToString(TOperationExecutionTimeTracker::Get()->Finish(operationId)));
            break;
        }
        TWaitProxy::Sleep(checkOperationStateInterval);
    }
}

void AbortOperation(
    const TAuth& auth,
    const TOperationId& operationId)
{
    THttpHeader header("POST", "abort_op");
    header.AddOperationId(operationId);
    header.AddMutationId();
    RetryRequest(auth, header);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

// TODO: we have inputDesc and outputDesc in TJobPreparer
void BuildUserJobFluently(
    const TJobPreparer& preparer,
    TMaybe<TNode> format,
    const TMultiFormatDesc& inputDesc,
    const TMultiFormatDesc& outputDesc,
    TFluentMap fluent)
{
    TMaybe<i64> memoryLimit = preparer.GetSpec().MemoryLimit_;

    auto tmpfsSize = preparer.GetSpec().ExtraTmpfsSize_.GetOrElse(0LL);
    if (preparer.ShouldMountSandbox()) {
        tmpfsSize += preparer.GetTotalFileSize();
        if (tmpfsSize == 0) {
            // This can be a case for example when it is local mode and we don't upload binary.
            // NOTE: YT doesn't like zero tmpfs size.
            tmpfsSize = RoundUpFileSize(1);
        }
        memoryLimit = memoryLimit.GetOrElse(512ll << 20) + tmpfsSize;
    }

    // TODO: tables as files

    fluent
    .Item("file_paths").List(preparer.GetFiles())
    .DoIf(inputDesc.Format == TMultiFormatDesc::F_YSON, [] (TFluentMap fluentMap)
    {
        fluentMap
        .Item("input_format").BeginAttributes()
            .Item("format").Value("binary")
        .EndAttributes()
        .Value("yson");
    })
    .DoIf(inputDesc.Format == TMultiFormatDesc::F_YAMR, [&] (TFluentMap fluentMap) {
        if (!format) {
            fluentMap
            .Item("input_format").BeginAttributes()
                .Item("lenval").Value(true)
                .Item("has_subkey").Value(true)
                .Item("enable_table_index").Value(true)
            .EndAttributes()
            .Value("yamr");
        } else {
            fluentMap.Item("input_format").Value(format.GetRef());
        }
    })
    .DoIf(inputDesc.Format == TMultiFormatDesc::F_PROTO, [&] (TFluentMap fluentMap)
    {
        if (TConfig::Get()->UseClientProtobuf) {
            fluentMap
            .Item("input_format").BeginAttributes()
                .Item("format").Value("binary")
            .EndAttributes()
            .Value("yson");
        } else {
            if (inputDesc.ProtoDescriptors.empty()) {
                ythrow TApiUsageError() << "messages for input_format are unknown (empty ProtoDescriptors)";
            }
            auto config = MakeProtoFormatConfig(inputDesc.ProtoDescriptors);
            fluentMap.Item("input_format").Value(config);
        }
    })
    .DoIf(outputDesc.Format == TMultiFormatDesc::F_YSON, [] (TFluentMap fluentMap)
    {
        fluentMap
        .Item("output_format").BeginAttributes()
            .Item("format").Value("binary")
        .EndAttributes()
        .Value("yson");
    })
    .DoIf(outputDesc.Format == TMultiFormatDesc::F_YAMR, [] (TFluentMap fluentMap) {
        fluentMap
        .Item("output_format").BeginAttributes()
            .Item("lenval").Value(true)
            .Item("has_subkey").Value(true)
        .EndAttributes()
        .Value("yamr");
    })
    .DoIf(outputDesc.Format == TMultiFormatDesc::F_PROTO, [&] (TFluentMap fluentMap)
    {
        if (TConfig::Get()->UseClientProtobuf) {
            fluentMap
            .Item("output_format").BeginAttributes()
                .Item("format").Value("binary")
            .EndAttributes()
            .Value("yson");
        } else {
            if (outputDesc.ProtoDescriptors.empty()) {
                ythrow TApiUsageError() << "messages for output_format are unknown (empty ProtoDescriptors)";
            }
            auto config = MakeProtoFormatConfig(outputDesc.ProtoDescriptors);
            fluentMap.Item("output_format").Value(config);
        }
    })
    .Item("command").Value(preparer.GetCommand())
    .Item("class_name").Value(preparer.GetClassName())
    .DoIf(memoryLimit.Defined(), [&] (TFluentMap fluentMap) {
        fluentMap.Item("memory_limit").Value(*memoryLimit);
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
        .DoIf(!pool.Empty(), [&] (TFluentMap fluentMap) {
            fluentMap.Item("pool").Value(pool);
        })
        .DoIf(options.SecureVault_.Defined(), [&] (TFluentMap fluentMap) {
            if (!options.SecureVault_->IsMap()) {
                ythrow yexception() << "SecureVault must be a map node";
            }
            fluentMap.Item("secure_vault").Value(*options.SecureVault_);
        });
}

template <typename TSpec>
void BuildCommonUserOperationPart(const TSpec& baseSpec, TNode* spec)
{
    if (baseSpec.MaxFailedJobCount_.Defined()) {
        (*spec)["max_failed_job_count"] = *baseSpec.MaxFailedJobCount_;
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

////////////////////////////////////////////////////////////////////////////////

TString MergeSpec(TNode& dst, const TOperationOptions& options)
{
    MergeNodes(dst["spec"], TConfig::Get()->Spec);
    if (options.Spec_) {
        MergeNodes(dst["spec"], *options.Spec_);
    }
    return NodeToYsonString(dst, YF_BINARY);
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
    if (!path.Path_) {
        ythrow yexception() << "Output table is not set";
    }
    NYT::NDetail::Create(auth, transactionId, path.Path_, NT_TABLE,
        TCreateOptions()
            .IgnoreExisting(true)
            .Recursive(true));
}

void CreateOutputTables(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const yvector<TRichYPath>& paths)
{
    if (paths.empty()) {
        ythrow yexception() << "Output tables are not set";
    }
    for (auto& path : paths) {
        CreateOutputTable(auth, transactionId, path);
    }
}

void LogJob(const TOperationId& opId, IJob* job, const char* type)
{
    if (job) {
        LOG_INFO("Operation %s; %s = %s",
            ~GetGuidAsString(opId), type, ~TJobFactory::Get()->GetJobName(job));
    }
}

TString DumpYPath(const TRichYPath& path)
{
    TStringStream stream;
    TYsonWriter writer(&stream, YF_TEXT, YT_NODE);
    Serialize(path, &writer);
    return stream.Str();
}

void LogYPaths(const TOperationId& opId, const yvector<TRichYPath>& paths, const char* type)
{
    for (size_t i = 0; i < paths.size(); ++i) {
        LOG_INFO("Operation %s; %s[%" PRISZT "] = %s",
            ~GetGuidAsString(opId), type, i, ~DumpYPath(paths[i]));
    }
}

void LogYPath(const TOperationId& opId, const TRichYPath& output, const char* type)
{
    LOG_INFO("Operation %s; %s = %s",
        ~GetGuidAsString(opId), type, ~DumpYPath(output));
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TOperationId ExecuteMap(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TMapOperationSpec& uncanonizedSpec,
    IJob* mapper,
    const TOperationOptions& options)
{
    auto spec = uncanonizedSpec;
    spec.Inputs_ = CanonizePaths(auth, spec.Inputs_);
    spec.Outputs_ = CanonizePaths(auth, spec.Outputs_);
    spec.MapperSpec_.Files_ = CanonizePaths(auth, spec.MapperSpec_.Files_);

    TMaybe<TNode> format;
    if (spec.InputDesc_.Format == TMultiFormatDesc::F_YAMR &&
        options.UseTableFormats_)
    {
        format = GetTableFormats(auth, transactionId, spec.Inputs_);
    }

    if (spec.CreateDebugOutputTables_) {
        CreateDebugOutputTables(spec, auth);
    }
    if (spec.CreateOutputTables_) {
        CreateOutputTables(auth, transactionId, spec.Outputs_);
    }

    TJobPreparer map(
        auth,
        transactionId,
        "--yt-map",
        spec.MapperSpec_,
        mapper,
        spec.Outputs_.size(),
        spec.InputDesc_,
        spec.OutputDesc_,
        options);

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("mapper").DoMap(std::bind(
            BuildUserJobFluently,
            std::cref(map),
            format,
            spec.InputDesc_,
            spec.OutputDesc_,
            std::placeholders::_1))
        .Item("input_table_paths").List(spec.Inputs_)
        .Item("output_table_paths").List(spec.Outputs_)
        .Item("job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_row_index").Value(true)
            .EndMap()
            .DoIf(!TConfig::Get()->TableWriter.Empty(), [&] (TFluentMap fluent) {
                fluent.Item("table_writer").Value(TConfig::Get()->TableWriter);
            })
        .EndMap()
        .DoIf(spec.Ordered_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("ordered").Value(spec.Ordered_.GetRef());
        })
        .Item("title").Value(map.GetClassName())
        .Do(std::bind(BuildCommonOperationPart, options, std::placeholders::_1))
    .EndMap().EndMap();

    BuildCommonUserOperationPart(spec, &specNode["spec"]);
    BuildJobCountOperationPart(spec, &specNode["spec"]);

    auto operationId = StartOperation(
        auth,
        transactionId,
        "map",
        MergeSpec(specNode, options));

    LogJob(operationId, mapper, "mapper");
    LogYPaths(operationId, spec.Inputs_, "input");
    LogYPaths(operationId, spec.Outputs_, "output");

    return operationId;
}

TOperationId ExecuteReduce(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TReduceOperationSpec& uncanonizedSpec,
    IJob* reducer,
    const TOperationOptions& options)
{
    auto spec = uncanonizedSpec;
    spec.Inputs_ = CanonizePaths(auth, spec.Inputs_);
    spec.Outputs_ = CanonizePaths(auth, spec.Outputs_);
    spec.ReducerSpec_.Files_ = CanonizePaths(auth, spec.ReducerSpec_.Files_);

    TMaybe<TNode> format;
    if (spec.InputDesc_.Format == TMultiFormatDesc::F_YAMR &&
        options.UseTableFormats_)
    {
        format = GetTableFormats(auth, transactionId, spec.Inputs_);
    }

    if (spec.CreateDebugOutputTables_) {
        CreateDebugOutputTables(spec, auth);
    }
    if (spec.CreateOutputTables_) {
        CreateOutputTables(auth, transactionId, spec.Outputs_);
    }

    TJobPreparer reduce(
        auth,
        transactionId,
        "--yt-reduce",
        spec.ReducerSpec_,
        reducer,
        spec.Outputs_.size(),
        spec.InputDesc_,
        spec.OutputDesc_,
        options);

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("reducer").DoMap(std::bind(
            BuildUserJobFluently,
            std::cref(reduce),
            format,
            spec.InputDesc_,
            spec.OutputDesc_,
            std::placeholders::_1))
        .Item("sort_by").Value(spec.SortBy_)
        .Item("reduce_by").Value(spec.ReduceBy_)
        .DoIf(spec.JoinBy_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("join_by").Value(spec.JoinBy_.GetRef());
        })
        .Item("input_table_paths").List(spec.Inputs_)
        .Item("output_table_paths").List(spec.Outputs_)
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

    auto operationId = StartOperation(
        auth,
        transactionId,
        "reduce",
        MergeSpec(specNode, options));

    LogJob(operationId, reducer, "reducer");
    LogYPaths(operationId, spec.Inputs_, "input");
    LogYPaths(operationId, spec.Outputs_, "output");

    return operationId;
}

TOperationId ExecuteJoinReduce(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TJoinReduceOperationSpec& uncanonizedSpec,
    IJob* reducer,
    const TOperationOptions& options)
{
    auto spec = uncanonizedSpec;
    spec.Inputs_ = CanonizePaths(auth, spec.Inputs_);
    spec.Outputs_ = CanonizePaths(auth, spec.Outputs_);
    spec.ReducerSpec_.Files_ = CanonizePaths(auth, spec.ReducerSpec_.Files_);

    TMaybe<TNode> format;
    if (spec.InputDesc_.Format == TMultiFormatDesc::F_YAMR &&
        options.UseTableFormats_)
    {
        format = GetTableFormats(auth, transactionId, spec.Inputs_);
    }

    if (spec.CreateDebugOutputTables_) {
        CreateDebugOutputTables(spec, auth);
    }
    if (spec.CreateOutputTables_) {
        CreateOutputTables(auth, transactionId, spec.Outputs_);
    }

    TJobPreparer reduce(
        auth,
        transactionId,
        "--yt-reduce",
        spec.ReducerSpec_,
        reducer,
        spec.Outputs_.size(),
        spec.InputDesc_,
        spec.OutputDesc_,
        options);

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("reducer").DoMap(std::bind(
            BuildUserJobFluently,
            std::cref(reduce),
            format,
            spec.InputDesc_,
            spec.OutputDesc_,
            std::placeholders::_1))
        .Item("join_by").Value(spec.JoinBy_)
        .Item("input_table_paths").List(spec.Inputs_)
        .Item("output_table_paths").List(spec.Outputs_)
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

    auto operationId = StartOperation(
        auth,
        transactionId,
        "join_reduce",
        MergeSpec(specNode, options));

    LogJob(operationId, reducer, "reducer");
    LogYPaths(operationId, spec.Inputs_, "input");
    LogYPaths(operationId, spec.Outputs_, "output");

    return operationId;
}

TOperationId ExecuteMapReduce(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TMapReduceOperationSpec& uncanonizedSpec,
    IJob* mapper,
    IJob* reduceCombiner,
    IJob* reducer,
    const TMultiFormatDesc& mapperClassOutputDesc,
    const TMultiFormatDesc& reduceCombinerClassInputDesc,
    const TMultiFormatDesc& reduceCombinerClassOutputDesc,
    const TMultiFormatDesc& reducerClassInputDesc,
    const TOperationOptions& options)
{
    auto spec = uncanonizedSpec;
    spec.Inputs_ = CanonizePaths(auth, spec.Inputs_);
    spec.Outputs_ = CanonizePaths(auth, spec.Outputs_);
    spec.MapperSpec_.Files_ = CanonizePaths(auth, spec.MapperSpec_.Files_);
    spec.ReduceCombinerSpec_.Files_ = CanonizePaths(auth, spec.ReduceCombinerSpec_.Files_);
    spec.ReducerSpec_.Files_ = CanonizePaths(auth, spec.ReducerSpec_.Files_);

    TMaybe<TNode> format;
    if (spec.InputDesc_.Format == TMultiFormatDesc::F_YAMR &&
        options.UseTableFormats_)
    {
        format = GetTableFormats(auth, transactionId, spec.Inputs_);
    }

    if (spec.CreateDebugOutputTables_) {
        CreateDebugOutputTables(spec, auth);
    }
    if (spec.CreateOutputTables_) {
        CreateOutputTables(auth, transactionId, spec.Outputs_);
    }

    TKeyColumns sortBy(spec.SortBy_);
    TKeyColumns reduceBy(spec.ReduceBy_);

    if (sortBy.Parts_.empty()) {
        sortBy = reduceBy;
    }

    if (spec.InputDesc_.Format == TMultiFormatDesc::F_YAMR && format && !mapper) {
        auto& attrs = format.Get()->Attributes();
       auto& keyColumns = attrs["key_column_names"].AsList();

        sortBy.Parts_.clear();
        reduceBy.Parts_.clear();

        for (auto& column : keyColumns) {
            sortBy.Parts_.push_back(column.AsString());
            reduceBy.Parts_.push_back(column.AsString());
        }

        if (attrs.HasKey("subkey_column_names")) {
            auto& subkeyColumns = attrs["subkey_column_names"].AsList();
            for (auto& column : subkeyColumns) {
                sortBy.Parts_.push_back(column.AsString());
            }
        }
    }

    const auto& reduceOutputDesc = spec.OutputDesc_;

    auto reduceInputDesc = MergeIntermediateDesc(reducerClassInputDesc, spec.ReduceInputHintDesc_,
        "spec from reducer CLASS input", "spec from HINT for reduce input");

    auto reduceCombinerOutputDesc = MergeIntermediateDesc(reduceCombinerClassOutputDesc, spec.ReduceCombinerOutputHintDesc_,
        "spec derived from reduce combiner CLASS output", "spec from HINT for reduce combiner output");

    auto reduceCombinerInputDesc = MergeIntermediateDesc(reduceCombinerClassInputDesc, spec.ReduceCombinerInputHintDesc_,
        "spec from reduce combiner CLASS input", "spec from HINT for reduce combiner input");

    auto mapOutputDesc = MergeIntermediateDesc(mapperClassOutputDesc, spec.MapOutputHintDesc_,
        "spec from mapper CLASS output", "spec from HINT for map output");

    const auto& mapInputDesc = spec.InputDesc_;

    const bool hasMapper = mapper != nullptr;
    const bool hasCombiner = reduceCombiner != nullptr;

    if (!hasMapper) {
        //request identity desc only for no mapper cases
        const auto& identityMapInputDesc = IdentityDesc(mapInputDesc);
        if (hasCombiner) {
            reduceCombinerInputDesc = MergeIntermediateDesc(reduceCombinerInputDesc, identityMapInputDesc,
                "spec derived from reduce combiner CLASS input", "identity spec from mapper CLASS input");
        } else {
            reduceInputDesc = MergeIntermediateDesc(reduceInputDesc, identityMapInputDesc,
                "spec derived from reduce CLASS input", "identity spec from mapper CLASS input" );
        }
    }

    TJobPreparer reduce(
        auth,
        transactionId,
        "--yt-reduce",
        spec.ReducerSpec_,
        reducer,
        spec.Outputs_.size(),
        reduceInputDesc,
        reduceOutputDesc,
        options);

    TString title;

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .DoIf(hasMapper, [&] (TFluentMap fluent) {
            TJobPreparer map(
                auth,
                transactionId,
                "--yt-map",
                spec.MapperSpec_,
                mapper,
                1,
                mapInputDesc,
                mapOutputDesc,
                options);

            fluent.Item("mapper").DoMap(std::bind(
                BuildUserJobFluently,
                std::cref(map),
                format,
                mapInputDesc,
                mapOutputDesc,
                std::placeholders::_1));

            title = "mapper:" + map.GetClassName() + " ";
        })
        .DoIf(hasCombiner, [&] (TFluentMap fluent) {
            TJobPreparer combine(
                auth,
                transactionId,
                "--yt-reduce",
                spec.ReduceCombinerSpec_,
                reduceCombiner,
                1,
                reduceCombinerInputDesc,
                reduceCombinerOutputDesc,
                options);

            fluent.Item("reduce_combiner").DoMap(std::bind(
                BuildUserJobFluently,
                std::cref(combine),
                mapper ? TMaybe<TNode>() : format,
                reduceCombinerInputDesc,
                reduceCombinerOutputDesc,
                std::placeholders::_1));
            title += "combiner:" + combine.GetClassName() + " ";
        })
        .Item("reducer").DoMap(std::bind(
            BuildUserJobFluently,
            std::cref(reduce),
            (mapper || reduceCombiner) ? TMaybe<TNode>() : format,
            reduceInputDesc,
            reduceOutputDesc,
            std::placeholders::_1))
        .Item("sort_by").Value(sortBy)
        .Item("reduce_by").Value(reduceBy)
        .Item("input_table_paths").List(spec.Inputs_)
        .Item("output_table_paths").List(spec.Outputs_)
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

    BuildCommonUserOperationPart(spec, &specNode["spec"]);
    BuildMapJobCountOperationPart(spec, &specNode["spec"]);
    BuildPartitionCountOperationPart(spec, &specNode["spec"]);

    auto operationId = StartOperation(
        auth,
        transactionId,
        "map_reduce",
        MergeSpec(specNode, options));

    LogJob(operationId, mapper, "mapper");
    LogJob(operationId, reduceCombiner, "reduce_combiner");
    LogJob(operationId, reducer, "reducer");
    LogYPaths(operationId, spec.Inputs_, "input");
    LogYPaths(operationId, spec.Outputs_, "output");

    return operationId;
}


TOperationId ExecuteSort(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TSortOperationSpec& spec,
    const TOperationOptions& options)
{
    auto inputs = CanonizePaths(auth, spec.Inputs_);
    auto output = CanonizePath(auth, spec.Output_);

    CreateOutputTable(auth, transactionId, output);

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("input_table_paths").List(inputs)
        .Item("output_table_path").Value(output)
        .Item("sort_by").Value(spec.SortBy_)
        .Do(std::bind(BuildCommonOperationPart, options, std::placeholders::_1))
    .EndMap().EndMap();

    BuildPartitionCountOperationPart(spec, &specNode["spec"]);
    BuildPartitionJobCountOperationPart(spec, &specNode["spec"]);

    auto operationId = StartOperation(
        auth,
        transactionId,
        "sort",
        MergeSpec(specNode, options));

    LogYPaths(operationId, inputs, "input");
    LogYPath(operationId, output, "output");

    return operationId;
}

TOperationId ExecuteMerge(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TMergeOperationSpec& spec,
    const TOperationOptions& options)
{
    auto inputs = CanonizePaths(auth, spec.Inputs_);
    auto output = CanonizePath(auth, spec.Output_);

    CreateOutputTable(auth, transactionId, output);

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

    auto operationId = StartOperation(
        auth,
        transactionId,
        "merge",
        MergeSpec(specNode, options));

    LogYPaths(operationId, inputs, "input");
    LogYPath(operationId, output, "output");

    return operationId;
}

TOperationId ExecuteErase(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TEraseOperationSpec& spec,
    const TOperationOptions& options)
{
    auto tablePath = CanonizePath(auth, spec.TablePath_);

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("table_path").Value(tablePath)
        .Item("combine_chunks").Value(spec.CombineChunks_)
        .Do(std::bind(BuildCommonOperationPart, options, std::placeholders::_1))
    .EndMap().EndMap();

    auto operationId = StartOperation(
        auth,
        transactionId,
        "erase",
        MergeSpec(specNode, options));

    LogYPath(operationId, tablePath, "table_path");

    return operationId;
}

////////////////////////////////////////////////////////////////////

struct TOperationAttributes
{
    EOperationStatus Status = OS_IN_PROGRESS;
    TMaybe<TYtError> Error;
    TMaybe<TJobStatistics> JobStatistics;
    TMaybe<TOperationBriefProgress> BriefProgress;
};

////////////////////////////////////////////////////////////////////////////////

TOperationAttributes ParseAttributes(const TNode& node)
{
    TOperationAttributes result;

    const auto& state = node["state"].AsString();
    if (state == "completed") {
        result.Status = OS_COMPLETED;
    } else if (state == "aborted") {
        result.Status = OS_ABORTED;
        result.Error = TYtError(node["result"]["error"]);
    } else if (state == "failed") {
        result.Status = OS_FAILED;
        result.Error = TYtError(node["result"]["error"]);
    }
    if (node.HasKey("progress")) {
        const auto& jobStatisticsNode = node["progress"]["job_statistics"];
        if (jobStatisticsNode.IsUndefined()) {
            result.JobStatistics.ConstructInPlace();
        } else {
            result.JobStatistics.ConstructInPlace(jobStatisticsNode);
        }
    }
    if (node.HasKey("brief_progress") && node["brief_progress"].HasKey("jobs")) {
        const auto& jobs = node["brief_progress"]["jobs"];
        result.BriefProgress.ConstructInPlace();
        auto load = [] (const TNode& item) {
            // Backward compatibility with old YT versions
            return item.IsInt64()
                ? item.AsInt64()
                : item["total"].AsInt64();
        };
        result.BriefProgress->Aborted = load(jobs["aborted"]);
        result.BriefProgress->Completed = load(jobs["completed"]);
        result.BriefProgress->Running = jobs["running"].AsInt64();
        result.BriefProgress->Total = jobs["total"].AsInt64();
        result.BriefProgress->Failed = jobs["failed"].AsInt64();
        result.BriefProgress->Lost = jobs["lost"].AsInt64();
        result.BriefProgress->Pending = jobs["pending"].AsInt64();
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TOperation::TOperationImpl
    : public TThrRefBase
{
public:
    TOperationImpl(const TAuth& auth, const TOperationId& operationId)
        : Auth_(auth)
        , Id_(operationId)
    { }

    const TOperationId& GetId() const;
    NThreading::TFuture<void> Watch(TYtPoller& ytPoller);

    EOperationStatus GetStatus();
    TMaybe<TYtError> GetError();
    TJobStatistics GetJobStatistics();
    TMaybe<TOperationBriefProgress> GetBriefProgress();
    void AbortOperation();

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
        : OperationAttrPath_("//sys/operations/" + GetGuidAsString(operationImpl->GetId()) + "/@")
        , OperationImpl_(operationImpl)
    { }

    virtual void PrepareRequest(TRawBatchRequest* batchRequest) override
    {
        // NOTE: we don't request progress/job_statistics here, because it is huge.
        OperationState_ = batchRequest->Get(TTransactionId(), OperationAttrPath_,
            TGetOptions().AttributeFilter(
                TAttributeFilter()
                .AddAttribute("result")
                .AddAttribute("state")
                .AddAttribute("brief_progress")));
    }

    virtual EStatus OnRequestExecuted() override
    {
        try {
            const auto& info = OperationState_.GetValue();
            auto attributes = ParseAttributes(info);
            if (attributes.Status != OS_IN_PROGRESS) {
                OperationImpl_->AsyncFinishOperation(attributes);
                return PollBreak;
            } else {
                OperationImpl_->UpdateBriefProgress(attributes.BriefProgress);
            }
        } catch (const TErrorResponse& e) {
            if (!NDetail::IsRetriable(e)) {
                OperationImpl_->FinishWithException(std::current_exception());
                return PollBreak;
            }
        }
        return PollContinue;
    }

private:
    const TYPath OperationAttrPath_;
    ::TIntrusivePtr<TOperation::TOperationImpl> OperationImpl_;
    NThreading::TFuture<TNode> OperationState_;
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
    return *CompletePromise_;
}

EOperationStatus TOperation::TOperationImpl::GetStatus()
{
    EOperationStatus result = OS_IN_PROGRESS;
    UpdateAttributesAndCall(false, [&] (const TOperationAttributes& attributes) {
        result = attributes.Status;
    });
    return result;
}

TMaybe<TYtError> TOperation::TOperationImpl::GetError()
{
    TMaybe<TYtError> result;
    UpdateAttributesAndCall(false, [&] (const TOperationAttributes& attributes) {
        result = attributes.Error;
    });
    return result;
}

TJobStatistics TOperation::TOperationImpl::GetJobStatistics()
{
    TJobStatistics result;
    UpdateAttributesAndCall(true, [&] (const TOperationAttributes& attributes) {
        result = std::move(*attributes.JobStatistics);
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
        if (Attributes_.Status != OS_IN_PROGRESS
            && (!needJobStatistics || Attributes_.JobStatistics.Defined()))
        {
            func(Attributes_);
            return;
        }
    }

    auto node = NYT::NDetail::Get(Auth_, TTransactionId(), "//sys/operations/" + GetGuidAsString(Id_) + "/@",
        TGetOptions().AttributeFilter(
            TAttributeFilter()
            .AddAttribute("result")
            .AddAttribute("progress")
            .AddAttribute("state")
            .AddAttribute("brief_progress")));
    auto attributes = ParseAttributes(node);
    func(attributes);

    if (attributes.Status != OS_IN_PROGRESS) {
        auto g = Guard(Lock_);
        Attributes_ = std::move(attributes);
    }
}

void TOperation::TOperationImpl::FinishWithException(std::exception_ptr e)
{
    CompletePromise_->SetException(e);
}

void TOperation::TOperationImpl::AbortOperation() {
    NYT::NDetail::AbortOperation(Auth_, Id_);
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
    Y_VERIFY(attributes.Status != OS_IN_PROGRESS);

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

    if (attributes.Status == OS_COMPLETED) {
        CompletePromise_->SetValue();
    } else if (attributes.Status == OS_ABORTED || attributes.Status == OS_FAILED) {
        auto error = *attributes.Error;
        LOG_ERROR("Operation %s is `%s' with error: %s",
            ~GetGuidAsString(Id_), ~::ToString(attributes.Status), ~error.FullDescription());
        TString additionalExceptionText;
        if (attributes.Status == OS_FAILED) {
            try {
                auto failedJobStderrInfo = NYT::NDetail::GetFailedJobInfo(Auth_, Id_, TGetFailedJobInfoOptions());
                TStringStream out;
                DumpJobInfoForException(out, failedJobStderrInfo);
                additionalExceptionText = out.Str();
            } catch (const NYT::TErrorResponse& e) {
                additionalExceptionText = "Cannot get job stderrs: ";
                additionalExceptionText += e.what();
            }
        }
        CompletePromise_->SetException(
            std::make_exception_ptr(
                TOperationFailedError(
                    attributes.Status == OS_FAILED ? TOperationFailedError::Failed : TOperationFailedError::Aborted,
                    Id_,
                    error) << additionalExceptionText));
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

yvector<TFailedJobInfo> TOperation::GetFailedJobInfo(const TGetFailedJobInfoOptions& options)
{
    return NYT::NDetail::GetFailedJobInfo(Client_->GetAuth(), GetId(), options);
}

EOperationStatus TOperation::GetStatus()
{
    return Impl_->GetStatus();
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
    return Impl_->AbortOperation();
}

////////////////////////////////////////////////////////////////////////////////

TOperationPtr CreateOperationAndWaitIfRequired(const TOperationId& operationId, TClientPtr client, const TOperationOptions& options)
{
    auto operation = ::MakeIntrusive<TOperation>(operationId, client);
    if (options.Wait_) {
        auto finishedFuture = operation->Watch();
        TWaitProxy::WaitFuture(finishedFuture);
        finishedFuture.GetValue();
    }
    return operation;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

::TIntrusivePtr<INodeReaderImpl> CreateJobNodeReader()
{
    return new TNodeTableReader(::MakeIntrusive<TJobReader>(0));
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
