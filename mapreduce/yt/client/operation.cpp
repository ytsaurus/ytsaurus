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

#include <util/string/printf.h>
#include <util/lambda/bind.h>
#include <util/system/execpath.h>
#include <util/folder/path.h>
#include <util/stream/file.h>

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
    TFileUploader(const Stroka& serverName)
        : ServerName_(serverName)
        , HasState_(false)
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
    const Stroka& ServerName_;
    yvector<TFile> Files_;
    bool HasState_;

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

        if (Exists(ServerName_, cypressPath)) {
            return cypressPath;
        }

        {
            THttpHeader header("POST", "create");
            header.AddPath(cypressPath);
            header.AddParam("type", "file");
            header.AddMutationId();
            RetryRequest(ServerName_, header);
        }

        Stroka proxyName = GetProxyForHeavyRequest(ServerName_);

        THttpHeader header("PUT", "upload");
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
            Stroka cachePath = UploadToCache(localFile);
            TFile file;
            file.SandboxName = TFsPath(localFile).Basename();
            file.CypressPath = cachePath;
            file.Executable = false;
            Files_.push_back(file);
        }
    }

    void UploadBinary()
    {
        Stroka cachePath = UploadToCache(GetExecPath());
        TFile file;
        file.SandboxName = "cppbinary";
        file.CypressPath = cachePath;
        file.Executable = true;
        Files_.push_back(file);
    }

    void UploadJobState(IJob* job)
    {
        TBufferOutput output(1 << 20);
        job->Save(output);
        if (output.GetBuffer().Size()) {
            Stroka cachePath = UploadToCache(output.GetBuffer());
            TFile file;
            file.SandboxName = "jobstate";
            file.CypressPath = cachePath;
            file.Executable = false;
            Files_.push_back(file);
            HasState_ = true;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

void BuildUserJobFluently(
    const Stroka& command,
    const yvector<TFileUploader::TFile>& files,
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
    .DoIf(inputDesc.Format == TMultiFormatDesc::F_YAMR, [] (TFluentMap fluent) {
        fluent
        .Item("input_format").BeginAttributes()
            .Item("lenval").Value(true)
            .Item("has_subkey").Value(true)
            .Item("enable_table_index").Value(true)
        .EndAttributes()
        .Value("yamr");
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

void BuildStartedBy(TFluentMap fluent)
{
    const TProcessProperties* properties = TProcessProperties::Get();
    fluent
        .Item("hostname").Value(properties->HostName)
        .Item("pid").Value(properties->Pid)
        .Item("user").Value(properties->UserName)
        .Item("command").List(properties->CommandLine)
        .Item("wrapper_version").Value(properties->ClientVersion);
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
    const Stroka& serverName,
    const TTransactionId& transactionId,
    const TMapOperationSpec& spec,
    IJob* mapper,
    const TOperationOptions& options)
{
    TFileUploader uploader(serverName);
    uploader.UploadFiles(spec.MapperSpec_, mapper);

    Stroka command = Sprintf("./cppbinary --yt-map \"%s\" %" PRISZT " %d",
        ~TJobFactory::Get()->GetJobName(mapper),
        spec.Outputs_.size(),
        uploader.HasState());

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("mapper").DoMap(BindFirst(
            BuildUserJobFluently,
            command,
            uploader.GetFiles(),
            spec.InputDesc_,
            spec.OutputDesc_))
        .Item("input_table_paths").DoListFor(spec.Inputs_, BuildPathPrefix)
        .Item("output_table_paths").DoListFor(spec.Outputs_, BuildPathPrefix)
        .Do(BuildCommonOperationPart)
    .EndMap().EndMap();

    return StartOperation(
        serverName,
        transactionId,
        "map",
        MergeSpec(specNode, options),
        options.Wait_);
}

TOperationId ExecuteReduce(
    const Stroka& serverName,
    const TTransactionId& transactionId,
    const TReduceOperationSpec& spec,
    IJob* reducer,
    const TOperationOptions& options)
{
    TFileUploader uploader(serverName);
    uploader.UploadFiles(spec.ReducerSpec_, reducer);

    Stroka command = Sprintf("./cppbinary --yt-reduce \"%s\" %" PRISZT " %d",
        ~TJobFactory::Get()->GetJobName(reducer),
        spec.Outputs_.size(),
        uploader.HasState());

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("reducer").DoMap(BindFirst(
            BuildUserJobFluently,
            command,
            uploader.GetFiles(),
            spec.InputDesc_,
            spec.OutputDesc_))
        .Item("reduce_by").Value(spec.ReduceBy_)
        .Item("input_table_paths").DoListFor(spec.Inputs_, BuildPathPrefix)
        .Item("output_table_paths").DoListFor(spec.Outputs_, BuildPathPrefix)
        .Item("job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_key_switch").Value(true)
            .EndMap()
        .EndMap()
        .Do(BuildCommonOperationPart)
    .EndMap().EndMap();

    return StartOperation(
        serverName,
        transactionId,
        "reduce",
        MergeSpec(specNode, options),
        options.Wait_);
}

TOperationId ExecuteMapReduce(
    const Stroka& serverName,
    const TTransactionId& transactionId,
    const TMapReduceOperationSpec& spec,
    IJob* mapper,
    IJob* reducer,
    const TMultiFormatDesc& outputMapperDesc,
    const TMultiFormatDesc& inputReducerDesc,
    const TOperationOptions& options)
{
    TFileUploader mapUploader(serverName);
    mapUploader.UploadFiles(spec.MapperSpec_, mapper);

    TFileUploader reduceUploader(serverName);
    reduceUploader.UploadFiles(spec.ReducerSpec_, reducer);

    Stroka mapCommand = Sprintf("./cppbinary --yt-map \"%s\" 1 %d",
        ~TJobFactory::Get()->GetJobName(mapper),
        mapUploader.HasState());

    Stroka reduceCommand = Sprintf("./cppbinary --yt-reduce \"%s\" %" PRISZT " %d",
        ~TJobFactory::Get()->GetJobName(reducer),
        spec.Outputs_.size(),
        reduceUploader.HasState());

    TNode specNode = BuildYsonNodeFluently()
    .BeginMap().Item("spec").BeginMap()
        .Item("mapper").DoMap(BindFirst(
            BuildUserJobFluently,
            mapCommand,
            mapUploader.GetFiles(),
            spec.InputDesc_,
            outputMapperDesc))
        .Item("reducer").DoMap(BindFirst(
            BuildUserJobFluently,
            reduceCommand,
            reduceUploader.GetFiles(),
            inputReducerDesc,
            spec.OutputDesc_))
        .Item("sort_by").Value(spec.SortBy_)
        .Item("reduce_by").Value(spec.ReduceBy_)
        .Item("input_table_paths").DoListFor(spec.Inputs_, BuildPathPrefix)
        .Item("output_table_paths").DoListFor(spec.Outputs_, BuildPathPrefix)
        .Item("reduce_job_io").BeginMap()
            .Item("control_attributes").BeginMap()
                .Item("enable_key_switch").Value(true)
            .EndMap()
        .EndMap()
        .Do(BuildCommonOperationPart)
    .EndMap().EndMap();

    return StartOperation(
        serverName,
        transactionId,
        "map_reduce",
        MergeSpec(specNode, options),
        options.Wait_);
}

TOperationId ExecuteSort(
    const Stroka& serverName,
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
        serverName,
        transactionId,
        "sort",
        MergeSpec(specNode, options),
        options.Wait_);
}

TOperationId ExecuteMerge(
    const Stroka& serverName,
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
        serverName,
        transactionId,
        "merge",
        MergeSpec(specNode, options),
        options.Wait_);
}

TOperationId ExecuteErase(
    const Stroka& serverName,
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
        serverName,
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
