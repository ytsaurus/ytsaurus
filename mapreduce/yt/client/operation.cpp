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
        const TOperationOptions& options)
        : Auth_(auth)
        , Spec_(spec)
    {
        UploadFilesFromSpec(spec);
        UploadJobState(job);

        Stroka binaryPath;
        if (!IsLocalMode(auth)) {
            UploadBinary();
            binaryPath = "./cppbinary";
        } else {
            binaryPath = GetExecPath();
        }

        ClassName_ = TJobFactory::Get()->GetJobName(job);
        Command_ = TStringBuilder() <<
            options.JobCommandPrefix_ <<
            binaryPath << " " <<
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

private:
    TAuth Auth_;
    TUserJobSpec Spec_;
    yvector<TRichYPath> Files_;
    bool HasState_ = false;
    Stroka ClassName_;
    Stroka Command_;

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

    template <class TSource>
    Stroka UploadToCache(const TSource& source)
    {
        char buf[33];
        CalculateMD5(source, buf);

        Stroka cypressPath = TStringBuilder() <<
            TConfig::Get()->RemoteTempFilesDirectory << "/hash/" << buf;

        if (Exists(Auth_, TTransactionId(), cypressPath)) {
            Set(Auth_, TTransactionId(), cypressPath + "/@touched", "\"true\"");
            try {
                Set(Auth_, TTransactionId(), cypressPath + "&/@touched", "\"true\"");
            } catch (TErrorResponse& e) {
                if (!e.IsResolveError()) {
                    throw;
                }
            }
            return cypressPath;
        }

        Stroka uniquePath = TStringBuilder() <<
            TConfig::Get()->RemoteTempFilesDirectory << "/cpp_" << CreateGuidAsString();

        Create(Auth_, TTransactionId(), uniquePath, "file", true, true);
        {
            THttpHeader header("PUT", GetWriteFileCommand());
            header.SetToken(Auth_.Token);
            header.AddPath(uniquePath);
            header.SetChunkedEncoding();
            auto streamMaker = [&source] () {
                return CreateStream(source);
            };
            RetryHeavyWriteRequest(Auth_, TTransactionId(), header, streamMaker);
        }
        Set(Auth_, TTransactionId(), uniquePath + "/@hash",
            TStringBuilder() << "\"" << buf << "\"");
        {
            THttpHeader header("POST", "link");
            header.AddParam("target_path", uniquePath);
            header.AddParam("link_path", cypressPath);
            header.AddMutationId();
            header.AddParam("recursive", true);
            header.AddParam("ignore_existing", true);
            RetryRequest(Auth_, header);
        }

        return cypressPath;
    }

    void UploadFilesFromSpec(const TUserJobSpec& spec)
    {
        Files_ = spec.Files_;

        for (const auto& localFile : spec.LocalFiles_) {
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
            Files_.push_back(cypressPath);
        }
    }

    void UploadBinary()
    {
        auto cachePath = UploadToCache(GetExecPath());
        Files_.push_back(TRichYPath(cachePath).FileName("cppbinary").Executable(true));
    }

    void UploadJobState(IJob* job)
    {
        TBufferOutput output(1 << 20);
        job->Save(output);
        if (output.Buffer().Size()) {
            auto cachePath = UploadToCache(output.Buffer());
            Files_.push_back(TRichYPath(cachePath).FileName("jobstate"));
            HasState_ = true;
        }
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
    // TODO: tables as files
    fluent
    .Item("file_paths").List(preparer.GetFiles())
    .DoIf(inputDesc.Format == TMultiFormatDesc::F_YSON
        || inputDesc.Format == TMultiFormatDesc::F_PROTO, [] (TFluentMap fluent)
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
    .DoIf(outputDesc.Format == TMultiFormatDesc::F_YSON
        || outputDesc.Format == TMultiFormatDesc::F_PROTO, [] (TFluentMap fluent)
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
    .Item("command").Value(preparer.GetCommand())
    .Item("class_name").Value(preparer.GetClassName())
    .DoIf(preparer.GetSpec().MemoryLimit_.Defined(), [&] (TFluentMap fluent) {
        fluent.Item("memory_limit").Value(*preparer.GetSpec().MemoryLimit_);
    });
}

void BuildCommonOperationPart(TFluentMap fluent)
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
        });
}

void BuildPathPrefix(TFluentList fluent, const TRichYPath& path)
{
    fluent.Item().Value(AddPathPrefix(path));
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
    Create(auth, transactionId, AddPathPrefix(path).Path_, "table");
}

void CreateOutputTables(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const yvector<TRichYPath>& paths)
{
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
    TMaybe<TNode> format;
    if (spec.InputDesc_.Format == TMultiFormatDesc::F_YAMR &&
        options.UseTableFormats_)
    {
        format = GetTableFormats(auth, transactionId, spec.Inputs_);
    }

    CreateOutputTables(auth, transactionId, spec.Outputs_);

    TJobPreparer map(
        auth,
        "--yt-map",
        spec.MapperSpec_,
        mapper,
        spec.Outputs_.size(),
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
        .Item("input_table_paths").DoListFor(spec.Inputs_, BuildPathPrefix)
        .Item("output_table_paths").DoListFor(spec.Outputs_, BuildPathPrefix)
        .Item("job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_row_index").Value(true)
            .EndMap()
        .EndMap()
        .DoIf(spec.Ordered_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("ordered").Value(spec.Ordered_.GetRef());
        })
        .Do(BuildCommonOperationPart)
    .EndMap().EndMap();

    auto operationId = StartOperation(
        auth,
        transactionId,
        "map",
        MergeSpec(specNode, options));

    LogJob(operationId, mapper, "mapper");
    LogYPaths(operationId, spec.Inputs_, "input");
    LogYPaths(operationId, spec.Outputs_, "output");

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
    TMaybe<TNode> format;
    if (spec.InputDesc_.Format == TMultiFormatDesc::F_YAMR &&
        options.UseTableFormats_)
    {
        format = GetTableFormats(auth, transactionId, spec.Inputs_);
    }

    CreateOutputTables(auth, transactionId, spec.Outputs_);

    TJobPreparer reduce(
        auth,
        "--yt-reduce",
        spec.ReducerSpec_,
        reducer,
        spec.Outputs_.size(),
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
        .Item("input_table_paths").DoListFor(spec.Inputs_, BuildPathPrefix)
        .Item("output_table_paths").DoListFor(spec.Outputs_, BuildPathPrefix)
        .Item("job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_key_switch").Value(true)
                .Item("enable_row_index").Value(true)
            .EndMap()
        .EndMap()
        .Do(BuildCommonOperationPart)
    .EndMap().EndMap();

    auto operationId = StartOperation(
        auth,
        transactionId,
        "reduce",
        MergeSpec(specNode, options));

    LogJob(operationId, reducer, "reducer");
    LogYPaths(operationId, spec.Inputs_, "input");
    LogYPaths(operationId, spec.Outputs_, "output");

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
    TMaybe<TNode> format;
    if (spec.InputDesc_.Format == TMultiFormatDesc::F_YAMR &&
        options.UseTableFormats_)
    {
        format = GetTableFormats(auth, transactionId, spec.Inputs_);
    }

    CreateOutputTables(auth, transactionId, spec.Outputs_);

    TJobPreparer reduce(
        auth,
        "--yt-reduce",
        spec.ReducerSpec_,
        reducer,
        spec.Outputs_.size(),
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
        .Item("input_table_paths").DoListFor(spec.Inputs_, BuildPathPrefix)
        .Item("output_table_paths").DoListFor(spec.Outputs_, BuildPathPrefix)
        .Item("job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_key_switch").Value(true)
                .Item("enable_row_index").Value(true)
            .EndMap()
        .EndMap()
        .Do(BuildCommonOperationPart)
    .EndMap().EndMap();

    auto operationId = StartOperation(
        auth,
        transactionId,
        "join_reduce",
        MergeSpec(specNode, options));

    LogJob(operationId, reducer, "reducer");
    LogYPaths(operationId, spec.Inputs_, "input");
    LogYPaths(operationId, spec.Outputs_, "output");

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
    TMaybe<TNode> format;
    if (spec.InputDesc_.Format == TMultiFormatDesc::F_YAMR &&
        options.UseTableFormats_)
    {
        format = GetTableFormats(auth, transactionId, spec.Inputs_);
    }

    CreateOutputTables(auth, transactionId, spec.Outputs_);

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
        spec.Outputs_.size(),
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
        .Item("input_table_paths").DoListFor(spec.Inputs_, BuildPathPrefix)
        .Item("output_table_paths").DoListFor(spec.Outputs_, BuildPathPrefix)
        .Item("map_job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_row_index").Value(true)
            .EndMap()
        .EndMap()
        .Item("sort_job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_key_switch").Value(true)
            .EndMap()
        .EndMap()
        .Item("reduce_job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_key_switch").Value(true)
            .EndMap()
        .EndMap()
        .Do(BuildCommonOperationPart)
    .EndMap().EndMap();

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
    CreateOutputTable(auth, transactionId, spec.Output_);

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("input_table_paths").DoListFor(spec.Inputs_, BuildPathPrefix)
        .Item("output_table_path").Value(AddPathPrefix(spec.Output_))
        .Item("sort_by").Value(spec.SortBy_)
        .Do(BuildCommonOperationPart)
    .EndMap().EndMap();

    auto operationId = StartOperation(
        auth,
        transactionId,
        "sort",
        MergeSpec(specNode, options));

    LogYPaths(operationId, spec.Inputs_, "input");
    LogYPath(operationId, spec.Output_, "output");

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
    CreateOutputTable(auth, transactionId, spec.Output_);

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("input_table_paths").DoListFor(spec.Inputs_, BuildPathPrefix)
        .Item("output_table_path").Value(AddPathPrefix(spec.Output_))
        .Item("mode").Value(ToString(spec.Mode_))
        .Item("combine_chunks").Value(spec.CombineChunks_)
        .Item("force_transform").Value(spec.ForceTransform_)
        .Item("merge_by").Value(spec.MergeBy_)
        .Do(BuildCommonOperationPart)
    .EndMap().EndMap();

    auto operationId = StartOperation(
        auth,
        transactionId,
        "merge",
        MergeSpec(specNode, options));

    LogYPaths(operationId, spec.Inputs_, "input");
    LogYPath(operationId, spec.Output_, "output");

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
    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("table_path").Value(AddPathPrefix(spec.TablePath_))
        .Item("combine_chunks").Value(spec.CombineChunks_)
        .Do(BuildCommonOperationPart)
    .EndMap().EndMap();

    auto operationId = StartOperation(
        auth,
        transactionId,
        "erase",
        MergeSpec(specNode, options));

    LogYPath(operationId, spec.TablePath_, "table_path");

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
    return new TProtoTableReader(MakeHolder<TJobReader>(0));
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
    return new TProtoTableWriter(MakeHolder<TJobWriter>(outputTableCount));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
