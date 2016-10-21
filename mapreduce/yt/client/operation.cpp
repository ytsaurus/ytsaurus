#include "operation.h"

#include <mapreduce/yt/interface/errors.h>

#include <mapreduce/yt/common/log.h>
#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/serialize.h>
#include <mapreduce/yt/common/fluent.h>
#include <mapreduce/yt/common/helpers.h>

#include <library/yson/writer.h>
#include <library/yson/json_writer.h>

#include <mapreduce/yt/http/requests.h>
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
#include <mapreduce/yt/io/file_reader.h>

#include <util/string/printf.h>
#include <util/string/builder.h>
#include <util/system/execpath.h>
#include <util/system/rwlock.h>
#include <util/system/mutex.h>
#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/stream/buffer.h>

#include <library/digest/md5/md5.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

Stroka ToString(EMergeMode mode)
{
    switch (mode) {
        case MM_UNORDERED: return "unordered";
        case MM_ORDERED: return "ordered";
        case MM_SORTED: return "sorted";
        default:
            LOG_FATAL("Invalid merge mode %i", mode);
    }
}

bool IsLocalMode(const TAuth& auth)
{
    static yhash_map<Stroka, bool> localModeMap;
    static TRWMutex mutex;

    {
        TReadGuard guard(mutex);
        auto it = localModeMap.find(auth.ServerName);
        if (it != localModeMap.end()) {
            return it->second;
        }
    }

    bool isLocalMode = false;
    Stroka localModeAttr("//sys/@local_mode_fqdn");
    if (Exists(auth, TTransactionId(), localModeAttr)) {
        auto fqdn = NodeFromYsonString(Get(auth, TTransactionId(), localModeAttr)).AsString();
        isLocalMode = (fqdn == TProcessState::Get()->HostName);
    }

    {
        TWriteGuard guard(mutex);
        localModeMap[auth.ServerName] = isLocalMode;
    }

    return isLocalMode;
}

class TOperationTracker
{
public:
    void Start(const TOperationId& operationId)
    {
        with_lock(Lock_) {
            StartTimes_[operationId] = TInstant::Now();
        }
    }

    Stroka Finish(const TOperationId& operationId)
    {
        TDuration duration;
        with_lock(Lock_) {
            auto i = StartTimes_.find(operationId);
            if (i == StartTimes_.end()) {
                ythrow yexception() <<
                    "Operation " << GetGuidAsString(operationId) << " did not start";
            }
            duration = TInstant::Now() - i->second;
            StartTimes_.erase(i);
        }
        return ToString(duration);
    }

    static TOperationTracker* Get()
    {
        return Singleton<TOperationTracker>();
    }

private:
    yhash_map<TOperationId, TInstant> StartTimes_;
    TMutex Lock_;
};

////////////////////////////////////////////////////////////////////////////////

class TJobPreparer
    : private TNonCopyable
{
public:
    TJobPreparer(
        const TAuth& auth,
        const Stroka& commandLineName,
        const TUserJobSpec& spec,
        IJob* job,
        size_t outputTableCount,
        const TMultiFormatDesc& outputDesc,
        const TOperationOptions& options)
        : Auth_(auth)
        , Spec_(spec)
        , OutputDesc_(outputDesc)
        , Options_(options)
    {
        CreateStorage();
        UploadFilesFromSpec();
        UploadJobState(job);
        UploadProtoConfig();

        BinaryPath_ = GetExecPath();
        if (TConfig::Get()->JobBinary) {
            BinaryPath_ = TConfig::Get()->JobBinary;
        }
        if (Spec_.JobBinary_) {
            BinaryPath_ = *Spec_.JobBinary_;
        }

        Stroka jobBinaryPath;
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

    const Stroka& GetClassName() const
    {
        return ClassName_;
    }

    const Stroka& GetCommand() const
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
    TUserJobSpec Spec_;
    TMultiFormatDesc OutputDesc_;
    TOperationOptions Options_;

    Stroka BinaryPath_;
    yvector<TRichYPath> Files_;
    bool HasState_ = false;
    Stroka ClassName_;
    Stroka Command_;
    ui64 TotalFileSize_ = 0;

    static void CalculateMD5(const Stroka& localFileName, char* buf)
    {
        MD5::File(~localFileName, buf);
    }

    static void CalculateMD5(const TBuffer& buffer, char* buf)
    {
        MD5::Data(reinterpret_cast<const unsigned char*>(buffer.Data()), buffer.Size(), buf);
    }

    static THolder<TInputStream> CreateStream(const Stroka& localPath)
    {
        return new TMappedFileInput(localPath);
    }

    static THolder<TInputStream> CreateStream(const TBuffer& buffer)
    {
        return new TBufferInput(buffer);
    }

    Stroka GetFileStorage() const
    {
        return Options_.FileStorage_ ?
            *Options_.FileStorage_ :
            TConfig::Get()->RemoteTempFilesDirectory;
    }

    void CreateStorage() const
    {
        Stroka cypressFolder = TStringBuilder() << GetFileStorage() << "/hash";
        if (!Exists(Auth_, TTransactionId(), cypressFolder)) {
            Create(Auth_, TTransactionId(), cypressFolder, "map_node", true, true);
        }
    }

    template <class TSource>
    Stroka UploadToCache(const TSource& source) const
    {
        constexpr size_t md5Size = 32;
        char buf[md5Size + 1];
        CalculateMD5(source, buf);

        Stroka twoDigits(buf + md5Size - 2, 2);

        Stroka cypressPath = TStringBuilder() << GetFileStorage() <<
            "/hash/" << twoDigits << "/" << buf;

        int retryCount = 256;
        for (int attempt = 0; attempt < retryCount; ++attempt) {
            TNode linkAttrs;
            if (Exists(Auth_, TTransactionId(), cypressPath + "&")) {
                try {
                    linkAttrs = NodeFromYsonString(
                        Get(Auth_, TTransactionId(), cypressPath + "&/@"));
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
                        Remove(Auth_, TTransactionId(), cypressPath + "&", true, true);
                    }
                }

                if (linkExists) {
                    Set(Auth_, TTransactionId(), cypressPath + "/@touched", "\"true\"");
                    Set(Auth_, TTransactionId(), cypressPath + "&/@touched", "\"true\"");
                    return cypressPath;
                }

                Stroka uniquePath = TStringBuilder() << GetFileStorage() <<
                    "/" << twoDigits << "/cpp_" << CreateGuidAsString();

                Create(Auth_, TTransactionId(), uniquePath, "file", true, true,
                    TNode()("hash", buf)("touched", true));

                {
                    THttpHeader header("PUT", GetWriteFileCommand());
                    header.SetToken(Auth_.Token);
                    header.AddPath(uniquePath);
                    auto streamMaker = [&source] () {
                        return CreateStream(source);
                    };
                    RetryHeavyWriteRequest(Auth_, TTransactionId(), header, streamMaker);
                }

                Link(Auth_, TTransactionId(), uniquePath, cypressPath, true, true,
                    TNode()("touched", true));

            } catch (TErrorResponse& e) {
                if (!e.IsResolveError() || attempt + 1 == retryCount) {
                    throw;
                }
                Sleep(TDuration::Seconds(1));
                continue;
            }
            break;
        }
        return cypressPath;
    }

    static ui64 RoundUpFileSize(ui64 size)
    {
        constexpr ui64 roundUpTo = 4ull << 10;
        return (size + roundUpTo - 1) & ~(roundUpTo - 1);
    }

    void UploadFilesFromSpec()
    {
        for (const auto& file : Spec_.Files_) {
            if (!Exists(Auth_, TTransactionId(), file.Path_)) {
                ythrow yexception() << "File " << file.Path_ << " does not exist";
            }

            if (ShouldMountSandbox()) {
                auto size = NodeFromYsonString(
                    Get(Auth_, TTransactionId(), file.Path_ + "/@uncompressed_data_size")
                ).AsInt64();

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
                TotalFileSize_ += output.Buffer().Size();
            }
        }
    }

    void UploadProtoConfig() {
        if (OutputDesc_.Format != TMultiFormatDesc::F_PROTO) {
            return;
        }

        TBufferOutput messageTypeList;
        for (const auto& descriptor : OutputDesc_.ProtoDescriptors) {
            messageTypeList << descriptor->full_name() << Endl;
        }

        auto cachePath = UploadToCache(messageTypeList.Buffer());
        Files_.push_back(TRichYPath(cachePath).FileName("protoconfig"));
    }
};

////////////////////////////////////////////////////////////////////////////////

void DumpOperationStderrs(
    TOutputStream& stream,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const Stroka& operationPath)
{
    const size_t RESULT_LIMIT = 1 << 20;
    const size_t BLOCK_SIZE = 16 << 10;
    const i64 STDERR_LIMIT = 64 << 10;
    const size_t STDERR_COUNT_LIMIT = 20;

    auto jobsPath = operationPath + "/jobs";
    if (!Exists(auth, transactionId, jobsPath)) {
        return;
    }

    THttpHeader header("GET", "list");
    header.AddPath(jobsPath);
    header.SetParameters(AttributeFilterToYsonString(
        TAttributeFilter()
            .AddAttribute("state")
            .AddAttribute("error")
            .AddAttribute("address")));
    auto jobList = NodeFromYsonString(RetryRequest(auth, header)).AsList();

    TBuffer buffer;
    TBufferOutput output(buffer);
    buffer.Reserve(RESULT_LIMIT);

    size_t count = 0;
    for (auto& job : jobList) {
        auto jobPath = jobsPath + "/" + job.AsString();
        auto& attributes = job.Attributes();
        output << Endl;

        if (!attributes.HasKey("state") || attributes["state"].AsString() != "failed") {
            continue;
        }
        if (attributes.HasKey("address")) {
            output << "Host: " << attributes["address"].AsString() << Endl;
        }
        if (attributes.HasKey("error")) {
            output << "Error: " << NodeToYsonString(attributes["error"]) << Endl;
        }

        auto stderrPath = jobPath + "/stderr";
        if (!Exists(auth, transactionId, stderrPath)) {
            continue;
        }

        output << "Stderr: " << Endl;
        if (buffer.Size() >= RESULT_LIMIT) {
            break;
        }

        TRichYPath path(stderrPath);
        i64 stderrSize = NodeFromYsonString(
            Get(auth, transactionId, stderrPath + "/@uncompressed_data_size")).AsInt64();
        if (stderrSize > STDERR_LIMIT) {
            path.AddRange(
                TReadRange().LowerLimit(
                    TReadLimit().Offset(stderrSize - STDERR_LIMIT)));
        }
        IFileReaderPtr reader = new TFileReader(path, auth, transactionId);

        auto pos = buffer.Size();
        auto left = RESULT_LIMIT - pos;
        while (left) {
            auto blockSize = Min(left, BLOCK_SIZE);
            buffer.Resize(pos + blockSize);
            auto bytes = reader->Load(buffer.Data() + pos, blockSize);
            left -= bytes;
            if (bytes != blockSize) {
                buffer.Resize(pos + bytes);
                break;
            }
            pos += bytes;
        }

        if (left == 0 || ++count == STDERR_COUNT_LIMIT) {
            break;
        }
    }

    stream.Write(buffer.Data(), buffer.Size());
    stream << Endl;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TOperationId StartOperation(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const Stroka& operationName,
    const Stroka& ysonSpec)
{
    THttpHeader header("POST", operationName);
    header.AddTransactionId(transactionId);
    header.AddMutationId();

    TOperationId operationId = ParseGuidFromResponse(
        RetryRequest(auth, header, ysonSpec, false, true));

    LOG_INFO("Operation %s started (%s)",
        ~GetGuidAsString(operationId), ~operationName);

    TOperationTracker::Get()->Start(operationId);

    return operationId;
}

EOperationStatus CheckOperation(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TOperationId& operationId)
{
    auto opIdStr = GetGuidAsString(operationId);
    auto opPath = Sprintf("//sys/operations/%s", ~opIdStr);
    auto statePath = opPath + "/@state";

    if (!Exists(auth, transactionId, opPath)) {
        LOG_FATAL("Operation %s does not exist", ~opIdStr);
    }

    Stroka state = NodeFromYsonString(
        Get(auth, transactionId, statePath)).AsString();

    if (state == "completed") {
        return OS_COMPLETED;

    } else if (state == "aborted" || state == "failed") {
        LOG_ERROR("Operation %s %s (%s)",
            ~opIdStr,
            ~state,
            ~TOperationTracker::Get()->Finish(operationId));

        auto errorPath = opPath + "/@result/error";
        Stroka error;
        if (Exists(auth, transactionId, errorPath)) {
            error = Get(auth, transactionId, errorPath);
        }

        TStringStream jobErrors;
        DumpOperationStderrs(jobErrors, auth, transactionId, opPath);
        error += jobErrors.Str();
        Cerr << error << Endl;

        ythrow TOperationFailedError(
            state == "aborted" ?
                TOperationFailedError::Aborted :
                TOperationFailedError::Failed,
            operationId,
            error);
    }

    return OS_RUNNING;
}

void WaitForOperation(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TOperationId& operationId)
{
    const TDuration checkOperationStateInterval =
        IsLocalMode(auth) ? TDuration::MilliSeconds(100) : TDuration::Seconds(1);

    while (true) {
        auto status = CheckOperation(auth, transactionId, operationId);
        if (status == OS_COMPLETED) {
            LOG_INFO("Operation %s completed (%s)",
                ~GetGuidAsString(operationId),
                ~TOperationTracker::Get()->Finish(operationId));
            break;
        }
        Sleep(checkOperationStateInterval);
    }
}

void AbortOperation(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TOperationId& operationId)
{
    THttpHeader header("POST", "abort_op");
    header.AddTransactionId(transactionId);
    header.AddOperationId(operationId);
    header.AddMutationId();
    RetryRequest(auth, header);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

void BuildUserJobFluently(
    const TJobPreparer& preparer,
    TMaybe<TNode> format,
    const TMultiFormatDesc& inputDesc,
    const TMultiFormatDesc& outputDesc,
    TFluentMap fluent)
{
    TMaybe<i64> memoryLimit = preparer.GetSpec().MemoryLimit_;

    i64 tmpfsSize = 0;
    if (preparer.ShouldMountSandbox()) {
        tmpfsSize = preparer.GetTotalFileSize();
        memoryLimit = memoryLimit.GetOrElse(512ll << 20) + tmpfsSize;
    }

    // TODO: tables as files

    fluent
    .Item("file_paths").List(preparer.GetFiles())
    .DoIf(inputDesc.Format == TMultiFormatDesc::F_YSON, [] (TFluentMap fluent)
    {
        fluent
        .Item("input_format").BeginAttributes()
            .Item("format").Value("binary")
        .EndAttributes()
        .Value("yson");
    })
    .DoIf(inputDesc.Format == TMultiFormatDesc::F_YAMR, [&] (TFluentMap fluent) {
        if (!format) {
            fluent
            .Item("input_format").BeginAttributes()
                .Item("lenval").Value(true)
                .Item("has_subkey").Value(true)
                .Item("enable_table_index").Value(true)
            .EndAttributes()
            .Value("yamr");
        } else {
            fluent.Item("input_format").Value(format.GetRef());
        }
    })
    .DoIf(inputDesc.Format == TMultiFormatDesc::F_PROTO, [&] (TFluentMap fluent)
    {
        if (TConfig::Get()->UseClientProtobuf) {
            fluent
            .Item("input_format").BeginAttributes()
                .Item("format").Value("binary")
            .EndAttributes()
            .Value("yson");
        } else {
            auto config = MakeProtoFormatConfig(inputDesc.ProtoDescriptors);
            fluent.Item("input_format").Value(config);
        }
    })
    .DoIf(outputDesc.Format == TMultiFormatDesc::F_YSON, [] (TFluentMap fluent)
    {
        fluent
        .Item("output_format").BeginAttributes()
            .Item("format").Value("binary")
        .EndAttributes()
        .Value("yson");
    })
    .DoIf(outputDesc.Format == TMultiFormatDesc::F_YAMR, [] (TFluentMap fluent) {
        fluent
        .Item("output_format").BeginAttributes()
            .Item("lenval").Value(true)
            .Item("has_subkey").Value(true)
        .EndAttributes()
        .Value("yamr");
    })
    .DoIf(outputDesc.Format == TMultiFormatDesc::F_PROTO, [&] (TFluentMap fluent)
    {
        if (TConfig::Get()->UseClientProtobuf) {
            fluent
            .Item("output_format").BeginAttributes()
                .Item("format").Value("binary")
            .EndAttributes()
            .Value("yson");
        } else {
            auto config = MakeProtoFormatConfig(outputDesc.ProtoDescriptors);
            fluent.Item("output_format").Value(config);
        }
    })
    .Item("command").Value(preparer.GetCommand())
    .Item("class_name").Value(preparer.GetClassName())
    .DoIf(memoryLimit.Defined(), [&] (TFluentMap fluent) {
        fluent.Item("memory_limit").Value(*memoryLimit);
    })
    .DoIf(preparer.ShouldMountSandbox(), [&] (TFluentMap fluent) {
        fluent.Item("tmpfs_path").Value(".");
        fluent.Item("tmpfs_size").Value(tmpfsSize);
        fluent.Item("copy_files").Value(true);
    });
}

void BuildCommonOperationPart(const TOperationOptions& options, TFluentMap fluent)
{
    const TProcessState* properties = TProcessState::Get();
    const Stroka& pool = TConfig::Get()->Pool;

    fluent
        .Item("started_by")
        .BeginMap()
            .Item("hostname").Value(properties->HostName)
            .Item("pid").Value(properties->Pid)
            .Item("user").Value(properties->UserName)
            .Item("command").List(properties->CommandLine)
            .Item("wrapper_version").Value(properties->ClientVersion)
        .EndMap()
        .DoIf(!pool.Empty(), [&] (TFluentMap fluent) {
            fluent.Item("pool").Value(pool);
        })
        .DoIf(options.SecureVault_.Defined(), [&] (TFluentMap fluent) {
            if (!options.SecureVault_->IsMap()) {
                ythrow yexception() << "SecureVault must be a map node";
            }
            fluent.Item("secure_vault").Value(*options.SecureVault_);
        });
}

////////////////////////////////////////////////////////////////////////////////

Stroka MergeSpec(TNode& dst, const TOperationOptions& options)
{
    MergeNodes(dst["spec"], TConfig::Get()->Spec);
    if (options.Spec_) {
        MergeNodes(dst["spec"], *options.Spec_);
    }
    return NodeToYsonString(dst);
}

void CreateOutputTable(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TRichYPath& path)
{
    if (!path.Path_) {
        ythrow yexception() << "Output table is not set";
    }
    Create(auth, transactionId, path.Path_, "table", true, true);
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

Stroka DumpYPath(const TRichYPath& path)
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
    const TMapOperationSpec& spec,
    IJob* mapper,
    const TOperationOptions& options)
{
    auto inputs = CanonizePaths(auth, spec.Inputs_);
    auto outputs = CanonizePaths(auth, spec.Outputs_);

    TMaybe<TNode> format;
    if (spec.InputDesc_.Format == TMultiFormatDesc::F_YAMR &&
        options.UseTableFormats_)
    {
        format = GetTableFormats(auth, transactionId, inputs);
    }

    CreateOutputTables(auth, transactionId, outputs);

    TJobPreparer map(
        auth,
        "--yt-map",
        spec.MapperSpec_,
        mapper,
        outputs.size(),
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
        .Item("input_table_paths").List(inputs)
        .Item("output_table_paths").List(outputs)
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
        .Do(std::bind(BuildCommonOperationPart, options, std::placeholders::_1))
    .EndMap().EndMap();

    auto operationId = StartOperation(
        auth,
        transactionId,
        "map",
        MergeSpec(specNode, options));

    LogJob(operationId, mapper, "mapper");
    LogYPaths(operationId, inputs, "input");
    LogYPaths(operationId, outputs, "output");

    if (options.Wait_) {
        WaitForOperation(auth, transactionId, operationId);
    }
    return operationId;
}

TOperationId ExecuteReduce(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TReduceOperationSpec& spec,
    IJob* reducer,
    const TOperationOptions& options)
{
    auto inputs = CanonizePaths(auth, spec.Inputs_);
    auto outputs = CanonizePaths(auth, spec.Outputs_);

    TMaybe<TNode> format;
    if (spec.InputDesc_.Format == TMultiFormatDesc::F_YAMR &&
        options.UseTableFormats_)
    {
        format = GetTableFormats(auth, transactionId, inputs);
    }

    CreateOutputTables(auth, transactionId, outputs);

    TJobPreparer reduce(
        auth,
        "--yt-reduce",
        spec.ReducerSpec_,
        reducer,
        outputs.size(),
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
        .Item("input_table_paths").List(inputs)
        .Item("output_table_paths").List(outputs)
        .Item("job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_key_switch").Value(true)
                .Item("enable_row_index").Value(true)
            .EndMap()
            .DoIf(!TConfig::Get()->TableWriter.Empty(), [&] (TFluentMap fluent) {
                fluent.Item("table_writer").Value(TConfig::Get()->TableWriter);
            })
        .EndMap()
        .Do(std::bind(BuildCommonOperationPart, options, std::placeholders::_1))
    .EndMap().EndMap();

    auto operationId = StartOperation(
        auth,
        transactionId,
        "reduce",
        MergeSpec(specNode, options));

    LogJob(operationId, reducer, "reducer");
    LogYPaths(operationId, inputs, "input");
    LogYPaths(operationId, outputs, "output");

    if (options.Wait_) {
        WaitForOperation(auth, transactionId, operationId);
    }
    return operationId;
}

TOperationId ExecuteJoinReduce(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TJoinReduceOperationSpec& spec,
    IJob* reducer,
    const TOperationOptions& options)
{
    auto inputs = CanonizePaths(auth, spec.Inputs_);
    auto outputs = CanonizePaths(auth, spec.Outputs_);

    TMaybe<TNode> format;
    if (spec.InputDesc_.Format == TMultiFormatDesc::F_YAMR &&
        options.UseTableFormats_)
    {
        format = GetTableFormats(auth, transactionId, inputs);
    }

    CreateOutputTables(auth, transactionId, outputs);

    TJobPreparer reduce(
        auth,
        "--yt-reduce",
        spec.ReducerSpec_,
        reducer,
        outputs.size(),
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
        .Item("input_table_paths").List(inputs)
        .Item("output_table_paths").List(outputs)
        .Item("job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_key_switch").Value(true)
                .Item("enable_row_index").Value(true)
            .EndMap()
            .DoIf(!TConfig::Get()->TableWriter.Empty(), [&] (TFluentMap fluent) {
                fluent.Item("table_writer").Value(TConfig::Get()->TableWriter);
            })
        .EndMap()
        .Do(std::bind(BuildCommonOperationPart, options, std::placeholders::_1))
    .EndMap().EndMap();

    auto operationId = StartOperation(
        auth,
        transactionId,
        "join_reduce",
        MergeSpec(specNode, options));

    LogJob(operationId, reducer, "reducer");
    LogYPaths(operationId, inputs, "input");
    LogYPaths(operationId, outputs, "output");

    if (options.Wait_) {
        WaitForOperation(auth, transactionId, operationId);
    }
    return operationId;
}

TOperationId ExecuteMapReduce(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TMapReduceOperationSpec& spec,
    IJob* mapper,
    IJob* reduceCombiner,
    IJob* reducer,
    const TMultiFormatDesc& outputMapperDesc,
    const TMultiFormatDesc& inputReduceCombinerDesc,
    const TMultiFormatDesc& outputReduceCombinerDesc,
    const TMultiFormatDesc& inputReducerDesc,
    const TOperationOptions& options)
{
    auto inputs = CanonizePaths(auth, spec.Inputs_);
    auto outputs = CanonizePaths(auth, spec.Outputs_);

    TMaybe<TNode> format;
    if (spec.InputDesc_.Format == TMultiFormatDesc::F_YAMR &&
        options.UseTableFormats_)
    {
        format = GetTableFormats(auth, transactionId, inputs);
    }

    CreateOutputTables(auth, transactionId, outputs);

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

    TJobPreparer reduce(
        auth,
        "--yt-reduce",
        spec.ReducerSpec_,
        reducer,
        outputs.size(),
        spec.OutputDesc_,
        options);

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .DoIf(mapper, [&] (TFluentMap fluent) {
            TJobPreparer map(
                auth,
                "--yt-map",
                spec.MapperSpec_,
                mapper,
                1,
                outputMapperDesc,
                options);

            fluent.Item("mapper").DoMap(std::bind(
                BuildUserJobFluently,
                std::cref(map),
                format,
                spec.InputDesc_,
                outputMapperDesc,
                std::placeholders::_1));
        })
        .DoIf(reduceCombiner, [&] (TFluentMap fluent) {
            TJobPreparer combine(
                auth,
                "--yt-reduce",
                spec.ReduceCombinerSpec_,
                reduceCombiner,
                1,
                outputReduceCombinerDesc,
                options);

            fluent.Item("reduce_combiner").DoMap(std::bind(
                BuildUserJobFluently,
                std::cref(combine),
                mapper ? TMaybe<TNode>() : format,
                inputReduceCombinerDesc,
                outputReduceCombinerDesc,
                std::placeholders::_1));
        })
        .Item("reducer").DoMap(std::bind(
            BuildUserJobFluently,
            std::cref(reduce),
            (mapper || reduceCombiner) ? TMaybe<TNode>() : format,
            inputReducerDesc,
            spec.OutputDesc_,
            std::placeholders::_1))
        .Item("sort_by").Value(sortBy)
        .Item("reduce_by").Value(reduceBy)
        .Item("input_table_paths").List(inputs)
        .Item("output_table_paths").List(outputs)
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
        .Do(std::bind(BuildCommonOperationPart, options, std::placeholders::_1))
    .EndMap().EndMap();

    auto operationId = StartOperation(
        auth,
        transactionId,
        "map_reduce",
        MergeSpec(specNode, options));

    LogJob(operationId, mapper, "mapper");
    LogJob(operationId, reduceCombiner, "reduce_combiner");
    LogJob(operationId, reducer, "reducer");
    LogYPaths(operationId, inputs, "input");
    LogYPaths(operationId, outputs, "output");

    if (options.Wait_) {
        WaitForOperation(auth, transactionId, operationId);
    }
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

    auto operationId = StartOperation(
        auth,
        transactionId,
        "sort",
        MergeSpec(specNode, options));

    LogYPaths(operationId, inputs, "input");
    LogYPath(operationId, output, "output");

    if (options.Wait_) {
        WaitForOperation(auth, transactionId, operationId);
    }
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
        .Item("mode").Value(ToString(spec.Mode_))
        .Item("combine_chunks").Value(spec.CombineChunks_)
        .Item("force_transform").Value(spec.ForceTransform_)
        .Item("merge_by").Value(spec.MergeBy_)
        .Do(std::bind(BuildCommonOperationPart, options, std::placeholders::_1))
    .EndMap().EndMap();

    auto operationId = StartOperation(
        auth,
        transactionId,
        "merge",
        MergeSpec(specNode, options));

    LogYPaths(operationId, inputs, "input");
    LogYPath(operationId, output, "output");

    if (options.Wait_) {
        WaitForOperation(auth, transactionId, operationId);
    }
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

    if (options.Wait_) {
        WaitForOperation(auth, transactionId, operationId);
    }
    return operationId;
}

////////////////////////////////////////////////////////////////////////////////

TIntrusivePtr<INodeReaderImpl> CreateJobNodeReader()
{
    return new TNodeTableReader(MakeHolder<TJobReader>(0));
}

TIntrusivePtr<IYaMRReaderImpl> CreateJobYaMRReader()
{
    return new TYaMRTableReader(MakeHolder<TJobReader>(0));
}

TIntrusivePtr<IProtoReaderImpl> CreateJobProtoReader()
{
    if (TConfig::Get()->UseClientProtobuf) {
        return new TProtoTableReader(MakeHolder<TJobReader>(0));
    } else {
        return new TLenvalProtoTableReader(MakeHolder<TJobReader>(0));
    }
}

TIntrusivePtr<INodeWriterImpl> CreateJobNodeWriter(size_t outputTableCount)
{
    return new TNodeTableWriter(MakeHolder<TJobWriter>(outputTableCount));
}

TIntrusivePtr<IYaMRWriterImpl> CreateJobYaMRWriter(size_t outputTableCount)
{
    return new TYaMRTableWriter(MakeHolder<TJobWriter>(outputTableCount));
}

TIntrusivePtr<IProtoWriterImpl> CreateJobProtoWriter(size_t outputTableCount)
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
