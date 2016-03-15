#include "operation.h"

#include <mapreduce/yt/interface/errors.h>

#include <mapreduce/yt/common/log.h>
#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/serialize.h>
#include <mapreduce/yt/common/fluent.h>
#include <mapreduce/yt/common/helpers.h>

#include <mapreduce/yt/yson/writer.h>
#include <mapreduce/yt/yson/json_writer.h>

#include <mapreduce/yt/http/requests.h>

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
#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/stream/buffer.h>

#include <library/digest/md5/md5.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

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
        UploadBinary();
        UploadJobState(job);

        ClassName_ = TJobFactory::Get()->GetJobName(job);
        Command_ = TStringBuilder() <<
            options.JobCommandPrefix_ <<
            "./cppbinary " <<
            commandLineName << " " <<
            "\"" << ClassName_ << "\" " <<
            outputTableCount << " " <<
            HasState_ <<
            options.JobCommandSuffix_;
    }

    struct TFile
    {
        Stroka SandboxName;
        Stroka CypressPath;
        bool Executable;
    };

    const yvector<TFile>& GetFiles() const
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
    yvector<TFile> Files_;
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
        static const Stroka YT_WRAPPER_FILE_CACHE = "//tmp/yt_wrapper/file_storage/";

        char buf[33];
        CalculateMD5(source, buf);

        Stroka cypressPath = TStringBuilder() <<
            YT_WRAPPER_FILE_CACHE << "hash/" << buf;

        if (Exists(Auth_, TTransactionId(), cypressPath)) {
            return cypressPath;
        }

        Stroka uniquePath = TStringBuilder() <<
            YT_WRAPPER_FILE_CACHE << "cpp_" << CreateGuidAsString();

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
        for (const auto& file : spec.Files_) {
            Files_.push_back(TFile{"", file, false});
        }
        for (const auto& localFile : spec.LocalFiles_) {
            TFsPath path(localFile);
            path.CheckExists();
            auto cachePath = UploadToCache(localFile);

            TFileStat stat;
            path.Stat(stat);
            bool isExecutable = stat.Mode & (S_IXUSR | S_IXGRP | S_IXOTH);

            Files_.push_back(TFile{path.Basename(), cachePath, isExecutable});
        }
    }

    void UploadBinary()
    {
        auto cachePath = UploadToCache(GetExecPath());
        Files_.push_back(TFile{"cppbinary", cachePath, true});
    }

    void UploadJobState(IJob* job)
    {
        TBufferOutput output(1 << 20);
        job->Save(output);
        if (output.Buffer().Size()) {
            Stroka cachePath = UploadToCache(output.Buffer());
            Files_.push_back(TFile{"jobstate", cachePath, false});
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

TOperationId StartOperation(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const Stroka& operationName,
    const Stroka& ysonSpec,
    bool wait)
{
    THttpHeader header("POST", operationName);
    header.AddTransactionId(transactionId);
    header.AddMutationId();

    TOperationId operationId = ParseGuidFromResponse(RetryRequest(auth, header, ysonSpec));
    LOG_INFO("Operation %s started", ~GetGuidAsString(operationId));

    if (wait) {
        WaitForOperation(auth, transactionId, operationId);
    }
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
        LOG_ERROR("Operation %s %s", ~opIdStr, ~state);

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
    const TDuration checkOperationStateInterval = TDuration::Seconds(1);

    while (true) {
        auto status = CheckOperation(auth, transactionId, operationId);
        if (status == OS_COMPLETED) {
            LOG_INFO("Operation %s completed", ~GetGuidAsString(operationId));
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

void BuildUserJobFluently(
    const TJobPreparer& preparer,
    TMaybe<TNode> format,
    const TMultiFormatDesc& inputDesc,
    const TMultiFormatDesc& outputDesc,
    TFluentMap fluent)
{
    // TODO: tables as files
    fluent
    .Item("file_paths").DoListFor(preparer.GetFiles(),
        [&] (TFluentList fluent, const TJobPreparer::TFile& file) {
            fluent.Item()
                .BeginAttributes()
                    .DoIf(!file.SandboxName.Empty(), [&] (TFluentAttributes fluent) {
                        fluent.Item("file_name").Value(file.SandboxName);
                    })
                    .DoIf(file.Executable, [&] (TFluentAttributes fluent) {
                        fluent.Item("executable").Value(true);
                    })
                .EndAttributes()
            .Value(file.CypressPath);
        })
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

    return StartOperation(
        auth,
        transactionId,
        "map",
        MergeSpec(specNode, options),
        options.Wait_);
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

    TKeyColumns sortBy(spec.SortBy_);
    TKeyColumns reduceBy(spec.ReduceBy_);

    if (spec.InputDesc_.Format == TMultiFormatDesc::F_YAMR) {
        sortBy = TKeyColumns("key", "subkey");
        reduceBy = TKeyColumns("key");
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
        .Item("reducer").DoMap(std::bind(
            BuildUserJobFluently,
            std::cref(reduce),
            format,
            spec.InputDesc_,
            spec.OutputDesc_,
            std::placeholders::_1))
        .Item("sort_by").Value(sortBy)
        .Item("reduce_by").Value(reduceBy)
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

    return StartOperation(
        auth,
        transactionId,
        "reduce",
        MergeSpec(specNode, options),
        options.Wait_);
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

    TKeyColumns joinBy(spec.JoinBy_);

    if (spec.InputDesc_.Format == TMultiFormatDesc::F_YAMR) {
        joinBy = TKeyColumns("key");
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
        .Item("reducer").DoMap(std::bind(
            BuildUserJobFluently,
            std::cref(reduce),
            format,
            spec.InputDesc_,
            spec.OutputDesc_,
            std::placeholders::_1))
        .Item("join_by").Value(joinBy)
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

    return StartOperation(
        auth,
        transactionId,
        "join_reduce",
        MergeSpec(specNode, options),
        options.Wait_);
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

    if (spec.InputDesc_.Format == TMultiFormatDesc::F_YAMR) {
        if (!mapper && format) {
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
        } else {
            sortBy = TKeyColumns("key", "subkey");
            reduceBy = TKeyColumns("key");
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

    return StartOperation(
        auth,
        transactionId,
        "map_reduce",
        MergeSpec(specNode, options),
        options.Wait_);
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

    return StartOperation(
        auth,
        transactionId,
        "sort",
        MergeSpec(specNode, options),
        options.Wait_);
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

    return StartOperation(
        auth,
        transactionId,
        "merge",
        MergeSpec(specNode, options),
        options.Wait_);
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

    return StartOperation(
        auth,
        transactionId,
        "erase",
        MergeSpec(specNode, options),
        options.Wait_);
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
