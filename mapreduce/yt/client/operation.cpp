#include "operation.h"

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

class TFileUploader
    : private TNonCopyable
{
public:
    TFileUploader(const TAuth& auth)
        : Auth_(auth)
    { }

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

    void UploadFiles(const TUserJobSpec& spec, IJob* job)
    {
        UploadFilesFromSpec(spec);
        UploadBinary();
        UploadJobState(job);
    }

    bool HasState() const
    {
        return HasState_;
    }

private:
    TAuth Auth_;
    yvector<TFile> Files_;
    bool HasState_ = false;

    static void CalculateMD5(const Stroka& localFileName, char* buf)
    {
        MD5::File(~localFileName, buf);
    }

    static void CalculateMD5(const TBuffer& buffer, char* buf)
    {
        MD5::Data(reinterpret_cast<const unsigned char*>(buffer.Data()), buffer.Size(), buf);
    }

    static void Transfer(const Stroka& localPath, TOutputStream* output)
    {
        TMappedFileInput fileInput(localPath);
        TransferData(&fileInput, output);
    }

    static void Transfer(const TBuffer& buffer, TOutputStream* output)
    {
        TBufferInput bufferInput(buffer);
        TransferData(&bufferInput, output);
    }

    template <class TSource>
    Stroka UploadToCache(const TSource& source)
    {
        static const Stroka YT_WRAPPER_FILE_CACHE = "//tmp/yt_wrapper/file_storage/";

        char buf[33];
        CalculateMD5(source, buf);
        Stroka cypressPath(YT_WRAPPER_FILE_CACHE);
        cypressPath += buf;

        if (Exists(Auth_, TTransactionId(), cypressPath)) {
            return cypressPath;
        }

        {
            THttpHeader header("POST", "create");
            header.AddPath(cypressPath);
            header.AddParam("type", "file");
            header.AddParam("recursive", true);
            header.AddParam("ignore_existing", true);
            header.AddMutationId();
            RetryRequest(Auth_, header);
        }

        Stroka proxyName = GetProxyForHeavyRequest(Auth_);

        THttpHeader header("PUT", GetWriteFileCommand());
        header.SetToken(Auth_.Token);
        header.AddPath(cypressPath);
        header.SetChunkedEncoding();

        THttpRequest request(proxyName);
        request.Connect();
        TOutputStream* output = request.StartRequest(header);

        Transfer(source, output);

        request.FinishRequest();
        request.GetResponse();

        return cypressPath;
    }

    void UploadFilesFromSpec(const TUserJobSpec& spec)
    {
        for (const auto& localFile : spec.LocalFiles_) {
            auto cachePath = UploadToCache(localFile);
            Files_.push_back(TFile{TFsPath(localFile).Basename(), cachePath, false});
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
        if (output.GetBuffer().Size()) {
            Stroka cachePath = UploadToCache(output.GetBuffer());
            Files_.push_back(TFile{"jobstate", cachePath, false});
            HasState_ = true;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

void DumpOperationStderrs(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const Stroka& operationPath)
{
    const size_t RESULT_LIMIT = 16 << 20;
    const size_t BLOCK_SIZE = 64 << 10;
    const i64 STDERR_LIMIT = 1 << 20;
    const size_t STDERR_COUNT_LIMIT = 5;

    auto jobsPath = operationPath + "/jobs";
    if (!Exists(auth, transactionId, jobsPath)) {
        return;
    }

    THttpHeader header("GET", "list");
    header.AddPath(jobsPath);
    header.SetParameters(AttributeFilterToJsonString(
        TAttributeFilter().AddAttribute("error").AddAttribute("address")));
    auto jobList = NodeFromYsonString(RetryRequest(auth, header)).AsList();

    TBuffer buffer;
    TBufferOutput output(buffer);
    buffer.Reserve(RESULT_LIMIT);

    size_t count = 0;
    for (auto& job : jobList) {
        auto jobPath = jobsPath + "/" + job.AsString();
        auto& attributes = job.Attributes();
        output << Endl;
        output << "Host: " << attributes["address"].AsString() << Endl;
        output << "Error: " << NodeToYsonString(attributes["error"]) << Endl;

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

    Cerr.Write(buffer.Data(), buffer.Size());
    Cerr << Endl;
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

    TOperationId operationId = ParseGuid(RetryRequest(auth, header, ysonSpec));
    LOG_INFO("Operation %s started", ~GetGuidAsString(operationId));

    if (wait) {
        WaitForOperation(auth, transactionId, operationId);
    }
    return operationId;
}

void WaitForOperation(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TOperationId& operationId)
{
    const TDuration checkOperationStateInterval = TDuration::Seconds(1);

    Stroka opIdStr = GetGuidAsString(operationId);
    Stroka opPath = Sprintf("//sys/operations/%s", ~opIdStr);
    Stroka statePath = opPath + "/@state";

    while (true) {
       if (!Exists(auth, transactionId, opPath)) {
            LOG_FATAL("Operation %s does not exist", ~opIdStr);
        }

        Stroka state = NodeFromYsonString(
            Get(auth, transactionId, statePath)).AsString();

        if (state == "completed") {
            LOG_INFO("Operation %s completed", ~opIdStr);
            break;

        } else if (state == "aborted") {
            LOG_FATAL("Operation %s aborted", ~opIdStr);

        } else if (state == "failed") {
            LOG_ERROR("Operation %s failed", ~opIdStr);
            Stroka errorPath = opPath + "/@result/error";
            Stroka error = Get(auth, transactionId, errorPath);
            Cerr << error << Endl;
            DumpOperationStderrs(auth, transactionId, opPath);
            LOG_FATAL("");
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
    const Stroka& command,
    const yvector<TFileUploader::TFile>& files,
    TMaybe<TNode> format,
    const TMultiFormatDesc& inputDesc,
    const TMultiFormatDesc& outputDesc,
    TFluentMap fluent)
{
    fluent
    .Item("file_paths").DoListFor(files,
        [&] (TFluentList fluent, const TFileUploader::TFile& file) {
            fluent.Item()
                .BeginAttributes()
                    .Item("file_name").Value(file.SandboxName)
                    .Item("executable").Value(file.Executable)
                .EndAttributes()
            .Value(file.CypressPath);
        })
    .DoIf(inputDesc.Format == TMultiFormatDesc::F_YSON, [] (TFluentMap fluent) {
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
    .DoIf(outputDesc.Format == TMultiFormatDesc::F_YSON, [] (TFluentMap fluent) {
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
    .Item("command").Value(command);
}

void BuildCommonOperationPart(TFluentMap fluent)
{
    const TProcessProperties* properties = TProcessProperties::Get();
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

////////////////////////////////////////////////////////////////////////////////

TOperationId ExecuteMap(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TMapOperationSpec& spec,
    IJob* mapper,
    const TOperationOptions& options)
{
    TFileUploader uploader(auth);
    uploader.UploadFiles(spec.MapperSpec_, mapper);

    TMaybe<TNode> format;
    if (spec.InputDesc_.Format == TMultiFormatDesc::F_YAMR &&
        options.UseTableFormats_)
    {
        format = GetTableFormats(auth, transactionId, spec.Inputs_);
    }

    Stroka command = Sprintf("./cppbinary --yt-map \"%s\" %" PRISZT " %d",
        ~TJobFactory::Get()->GetJobName(mapper),
        spec.Outputs_.size(),
        uploader.HasState());

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("mapper").DoMap(std::bind(
            BuildUserJobFluently,
            command,
            uploader.GetFiles(),
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

    TKeyColumns sortBy(spec.SortBy_);
    TKeyColumns reduceBy(spec.ReduceBy_);

    if (spec.InputDesc_.Format == TMultiFormatDesc::F_YAMR) {
        sortBy = TKeyColumns("key", "subkey");
        reduceBy = TKeyColumns("key");
    }

    TFileUploader uploader(auth);
    uploader.UploadFiles(spec.ReducerSpec_, reducer);

    Stroka command = Sprintf("./cppbinary --yt-reduce \"%s\" %" PRISZT " %d",
        ~TJobFactory::Get()->GetJobName(reducer),
        spec.Outputs_.size(),
        uploader.HasState());

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("reducer").DoMap(std::bind(
            BuildUserJobFluently,
            command,
            uploader.GetFiles(),
            format,
            spec.InputDesc_,
            spec.OutputDesc_,
            std::placeholders::_1))
        .Item("sort_by").Value(sortBy)
        .Item("reduce_by").Value(reduceBy)
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

TOperationId ExecuteMapReduce(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TMapReduceOperationSpec& spec,
    IJob* mapper,
    IJob* reducer,
    const TMultiFormatDesc& outputMapperDesc,
    const TMultiFormatDesc& inputReducerDesc,
    const TOperationOptions& options)
{
    TMaybe<TNode> format;
    if (spec.InputDesc_.Format == TMultiFormatDesc::F_YAMR &&
        options.UseTableFormats_)
    {
        format = GetTableFormats(auth, transactionId, spec.Inputs_);
    }

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

    TFileUploader mapUploader(auth);
    if (mapper) {
        mapUploader.UploadFiles(spec.MapperSpec_, mapper);
    }

    TFileUploader reduceUploader(auth);
    reduceUploader.UploadFiles(spec.ReducerSpec_, reducer);

    Stroka mapCommand;
    if (mapper) {
        mapCommand = Sprintf("./cppbinary --yt-map \"%s\" 1 %d",
            ~TJobFactory::Get()->GetJobName(mapper),
            mapUploader.HasState());
    }

    Stroka reduceCommand = Sprintf("./cppbinary --yt-reduce \"%s\" %" PRISZT " %d",
        ~TJobFactory::Get()->GetJobName(reducer),
        spec.Outputs_.size(),
        reduceUploader.HasState());

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .DoIf(mapper, [&] (TFluentMap fluent) {
            fluent.Item("mapper").DoMap(std::bind(
                BuildUserJobFluently,
                mapCommand,
                mapUploader.GetFiles(),
                format,
                spec.InputDesc_,
                outputMapperDesc,
                std::placeholders::_1));
        })
        .Item("reducer").DoMap(std::bind(
            BuildUserJobFluently,
            reduceCommand,
            reduceUploader.GetFiles(),
            mapper ? TMaybe<TNode>() : format,
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
