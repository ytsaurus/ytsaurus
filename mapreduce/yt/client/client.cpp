#include "operation.h"

#include <mapreduce/yt/interface/client.h>

#include <mapreduce/yt/common/log.h>
#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/fluent.h>

#include <mapreduce/yt/http/http.h>
#include <mapreduce/yt/http/requests.h>

#include <mapreduce/yt/io/client_reader.h>
#include <mapreduce/yt/io/client_writer.h>
#include <mapreduce/yt/io/job_reader.h>
#include <mapreduce/yt/io/job_writer.h>
#include <mapreduce/yt/io/yamr_table_reader.h>
#include <mapreduce/yt/io/yamr_table_writer.h>
#include <mapreduce/yt/io/node_table_reader.h>
#include <mapreduce/yt/io/node_table_writer.h>
#include <mapreduce/yt/io/proto_table_reader.h>
#include <mapreduce/yt/io/proto_table_writer.h>
#include <mapreduce/yt/io/file_reader.h>
#include <mapreduce/yt/io/file_writer.h>

#include <mapreduce/yt/yson/json_writer.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

Stroka ToString(ELockMode mode)
{
    switch (mode) {
        case LM_EXCLUSIVE: return "exclusive";
        case LM_SHARED: return "shared";
        case LM_SNAPSHOT: return "snapshot";
        default:
            LOG_FATAL("Invalid lock mode %i", static_cast<int>(mode));
    }
}

Stroka ToString(ENodeType type)
{
    switch (type) {
        case NT_STRING: return "string_node";
        case NT_INT64: return "integer_node";
        case NT_UINT64: return "";
        case NT_DOUBLE: return "double_node";
        case NT_BOOLEAN: return "";
        case NT_MAP: return "map_node";
        case NT_LIST: return "list_node";
        case NT_FILE: return "file";
        case NT_TABLE: return "table";
        default:
            LOG_FATAL("Invalid node type %i", static_cast<int>(type));
    }
}

////////////////////////////////////////////////////////////////////////////////

ITransactionPtr CreateTransactionObject(
    const Stroka& serverName,
    const TTransactionId& parentId,
    const TStartTransactionOptions& options);

////////////////////////////////////////////////////////////////////////////////

class TClientBase
    : virtual public IClientBase
{
public:
    explicit TClientBase(
        const Stroka& serverName,
        const TTransactionId& transactionId = TTransactionId())
        : ServerName_(serverName)
        , TransactionId_(transactionId)
    { }

    virtual ITransactionPtr StartTransaction(
        const TStartTransactionOptions& options) override
    {
        return CreateTransactionObject(ServerName_, TransactionId_, options);
    }

    // cypress

    virtual TNodeId Create(
        const TYPath& path,
        ENodeType type,
        const TCreateOptions& options) override
    {
        THttpHeader header("POST", "create");
        header.AddPath(AddPathPrefix(path));
        header.AddParam("type", ToString(type));
        header.AddTransactionId(TransactionId_);
        header.AddMutationId();

        header.AddParam("recursive", options.Recursive_);
        header.AddParam("ignore_existing", options.IgnoreExisting_);
        if (options.Attributes_) {
            header.SetParameters(AttributesToJsonString(*options.Attributes_));
        }

        return ParseGuid(RetryRequest(ServerName_, header));
    }

    virtual void Remove(
        const TYPath& path,
        const TRemoveOptions& options) override
    {
        THttpHeader header("POST", "remove");
        header.AddPath(AddPathPrefix(path));
        header.AddTransactionId(TransactionId_);
        header.AddMutationId();

        header.AddParam("recursive", options.Recursive_);
        header.AddParam("force", options.Force_);
        RetryRequest(ServerName_, header);
    }

    virtual bool Exists(
        const TYPath& path) override
    {
        THttpHeader header("GET", "exists");
        header.AddPath(AddPathPrefix(path));
        header.AddTransactionId(TransactionId_);
        return ParseBool(RetryRequest(ServerName_, header));
    }

    virtual TNode Get(
        const TYPath& path,
        const TGetOptions& options) override
    {
        THttpHeader header("GET", "get");
        header.AddPath(AddPathPrefix(path));
        header.AddTransactionId(TransactionId_);

        if (options.AttributeFilter_) {
            header.SetParameters(AttributeFilterToJsonString(*options.AttributeFilter_));
        }
        if (options.MaxSize_) {
            header.AddParam("max_size", *options.MaxSize_);
        }
        header.AddParam("ignore_opaque", options.IgnoreOpaque_);
        return NodeFromYsonString(RetryRequest(ServerName_, header));
    }

    virtual void Set(
        const TYPath& path,
        const TNode& value) override
    {
        THttpHeader header("PUT", "set");
        header.AddPath(AddPathPrefix(path));
        header.AddTransactionId(TransactionId_);
        header.AddMutationId();
        RetryRequest(ServerName_, header, NodeToYsonString(value));
    }

    virtual TNode::TList List(
        const TYPath& path,
        const TListOptions& options) override
    {
        THttpHeader header("GET", "list");
        header.AddPath(AddPathPrefix(path));
        header.AddTransactionId(TransactionId_);

        if (options.AttributeFilter_) {
            header.SetParameters(AttributeFilterToJsonString(*options.AttributeFilter_));
        }
        if (options.MaxSize_) {
            header.AddParam("max_size", *options.MaxSize_);
        }
        return NodeFromYsonString(RetryRequest(ServerName_, header)).AsList();
    }

    virtual TNodeId Copy(
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TCopyOptions& options) override
    {
        THttpHeader header("POST", "copy");
        header.AddParam("source_path", AddPathPrefix(sourcePath));
        header.AddParam("destination_path", AddPathPrefix(destinationPath));
        header.AddTransactionId(TransactionId_);
        header.AddMutationId();

        header.AddParam("recursive", options.Recursive_);
        header.AddParam("preserve_account", options.PreserveAccount_);
        return ParseGuid(RetryRequest(ServerName_, header));
    }

    virtual TNodeId Move(
        const TYPath& sourcePath,
        const TYPath& destinationPath,
        const TMoveOptions& options) override
    {
        THttpHeader header("POST", "move");
        header.AddParam("source_path", AddPathPrefix(sourcePath));
        header.AddParam("destination_path", AddPathPrefix(destinationPath));
        header.AddTransactionId(TransactionId_);
        header.AddMutationId();

        header.AddParam("recursive", options.Recursive_);
        header.AddParam("preserve_account", options.PreserveAccount_);
        return ParseGuid(RetryRequest(ServerName_, header));
    }

    virtual TNodeId Link(
        const TYPath& targetPath,
        const TYPath& linkPath,
        const TLinkOptions& options) override
    {
        THttpHeader header("POST", "link");
        header.AddParam("target_path", AddPathPrefix(targetPath));
        header.AddParam("link_path", AddPathPrefix(linkPath));
        header.AddTransactionId(TransactionId_);
        header.AddMutationId();

        header.AddParam("recursive", options.Recursive_);
        header.AddParam("ignore_existing", options.IgnoreExisting_);
        if (options.Attributes_) {
            header.SetParameters(AttributesToJsonString(*options.Attributes_));
        }
        return ParseGuid(RetryRequest(ServerName_, header));
    }

    // io

    virtual IFileReaderPtr CreateFileReader(const TRichYPath& path) override
    {
        return new TFileReader(path, ServerName_, TransactionId_);
    }

    virtual IFileWriterPtr CreateFileWriter(const TRichYPath& path) override
    {
        return new TFileWriter(path, ServerName_, TransactionId_);
    }

    // operations

    virtual TOperationId DoMap(
        const TMapOperationSpec& spec,
        IJob* mapper,
        const TOperationOptions& options)
    {
        return ExecuteMap(
            ServerName_,
            TransactionId_,
            spec,
            mapper,
            options);
    }

    virtual TOperationId DoReduce(
        const TReduceOperationSpec& spec,
        IJob* reducer,
        const TOperationOptions& options)
    {
        return ExecuteReduce(
            ServerName_,
            TransactionId_,
            spec,
            reducer,
            options);
    }

    virtual TOperationId DoMapReduce(
        const TMapReduceOperationSpec& spec,
        IJob* mapper,
        IJob* reducer,
        const TMultiFormatDesc& outputMapperDesc,
        const TMultiFormatDesc& inputReducerDesc,
        const TOperationOptions& options)
    {
        return ExecuteMapReduce(
            ServerName_,
            TransactionId_,
            spec,
            mapper,
            reducer,
            outputMapperDesc,
            inputReducerDesc,
            options);
    }

    virtual TOperationId Sort(
        const TSortOperationSpec& spec,
        const TOperationOptions& options)
    {
        return ExecuteSort(
            ServerName_,
            TransactionId_,
            spec,
            options);
    }

    virtual TOperationId Merge(
        const TMergeOperationSpec& spec,
        const TOperationOptions& options)
    {
        return ExecuteMerge(
            ServerName_,
            TransactionId_,
            spec,
            options);
    }

    virtual TOperationId Erase(
        const TEraseOperationSpec& spec,
        const TOperationOptions& options)
    {
        return ExecuteErase(
            ServerName_,
            TransactionId_,
            spec,
            options);
    }

    virtual void AbortOperation(const TOperationId& operationId)
    {
        NYT::AbortOperation(ServerName_, operationId);
    }

    virtual void WaitForOperation(const TOperationId& operationId)
    {
        NYT::WaitForOperation(ServerName_, operationId);
    }

protected:
    Stroka ServerName_;
    TTransactionId TransactionId_;

private:
    THolder<TClientReader> CreateClientReader(const TRichYPath& path, EDataStreamFormat format)
    {
        return MakeHolder<TClientReader>(path, ServerName_, TransactionId_, format);
    }

    virtual TIntrusivePtr<INodeReaderImpl> CreateNodeReader(const TRichYPath& path) override
    {
        return new TNodeTableReader(CreateClientReader(path, DSF_YSON_BINARY));
    }

    virtual TIntrusivePtr<IYaMRReaderImpl> CreateYaMRReader(const TRichYPath& path) override
    {
        return new TYaMRTableReader(
            MakeHolder<TClientReader>(path, ServerName_, TransactionId_, DSF_YAMR_LENVAL));
    }

    virtual TIntrusivePtr<IProtoReaderImpl> CreateProtoReader(const TRichYPath& path) override
    {
        return new TProtoTableReader(
            MakeHolder<TClientReader>(path, ServerName_, TransactionId_, DSF_YSON_BINARY)); // later: DSF_PROTO
    }

    virtual TIntrusivePtr<INodeWriterImpl> CreateNodeWriter(const TRichYPath& path) override
    {
        return new TNodeTableWriter(
            MakeHolder<TClientWriter>(path, ServerName_, TransactionId_, DSF_YSON_BINARY));
    }

    virtual TIntrusivePtr<IYaMRWriterImpl> CreateYaMRWriter(const TRichYPath& path) override
    {
        return new TYaMRTableWriter(
            MakeHolder<TClientWriter>(path, ServerName_, TransactionId_, DSF_YAMR_LENVAL));
    }

    virtual TIntrusivePtr<IProtoWriterImpl> CreateProtoWriter(const TRichYPath& path) override
    {
        return new TProtoTableWriter(
            MakeHolder<TClientWriter>(path, ServerName_, TransactionId_, DSF_YSON_BINARY));
    }

};

class TTransaction
    : public ITransaction
    , public TClientBase
{
public:
    TTransaction(
        const Stroka& serverName,
        const TTransactionId& parentId,
        const TStartTransactionOptions& options)
        : TClientBase(serverName)
        , PingableTx_(
            serverName,
            parentId,
            options.Timeout_,
            options.PingAncestors_,
            options.Attributes_)
    {
        TransactionId_ = PingableTx_.GetId();
    }

    virtual const TTransactionId& GetId() const override
    {
        return TransactionId_;
    }

    virtual TLockId Lock(
        const TYPath& path,
        ELockMode mode,
        const TLockOptions& options) override
    {
        THttpHeader header("POST", "lock");
        header.AddPath(path);
        header.AddParam("mode", ToString(mode));
        header.AddTransactionId(TransactionId_);
        header.AddMutationId();

        header.AddParam("waitable", options.Waitable_);
        return ParseGuid(RetryRequest(ServerName_, header));
    }

    virtual void Commit() override
    {
        PingableTx_.Commit();
    }

    virtual void Abort() override
    {
        PingableTx_.Abort();
    }

private:
    TPingableTransaction PingableTx_;
};

ITransactionPtr CreateTransactionObject(
    const Stroka& serverName,
    const TTransactionId& parentId,
    const TStartTransactionOptions& options)
{
    return new TTransaction(serverName, parentId, options);
}

////////////////////////////////////////////////////////////////////////////////

class TClient
    : public IClient
    , public TClientBase
{
public:
    TClient(const Stroka& serverName, const TTransactionId& globalId)
        : TClientBase(serverName, globalId)
    { }

    virtual void InsertRows(
        const TYPath& path,
        const TNode::TList& rows) override
    {
        ModifyRows("insert_rows", path, rows);
    }

    virtual void DeleteRows(
        const TYPath& path,
        const TNode::TList& keys) override
    {
        ModifyRows("delete_rows", path, keys);
    }

    virtual TNode::TList LookupRows(
        const TYPath& path,
        const TNode::TList& keys,
        const TLookupRowsOptions& options) override
    {
        UNUSED(options);
        THttpHeader header("PUT", "lookup_rows");
        header.AddPath(path);
        header.SetDataStreamFormat(DSF_YSON_BINARY);

        header.SetParameters(BuildJsonStringFluently().BeginMap()
            .DoIf(options.Timeout_.Defined(), [&] (TFluentMap fluent) {
                fluent.Item("timeout").Value(options.Timeout_->MilliSeconds());
            })
            .Item("keep_missing_rows").Value(options.KeepMissingRows_)
        .EndMap());

        auto body = NodeListToYsonString(keys);
        auto response = RetryRequest(ServerName_, header, body, true);
        return NodeFromYsonString(response, YT_LIST_FRAGMENT).AsList();
    }

    virtual TNode::TList SelectRows(
        const Stroka& query,
        const TSelectRowsOptions& options) override
    {
        THttpHeader header("GET", "select_rows");
        header.SetDataStreamFormat(DSF_YSON_BINARY);

        header.SetParameters(BuildJsonStringFluently().BeginMap()
            .Item("query").Value(query)
            .DoIf(options.Timeout_.Defined(), [&] (TFluentMap fluent) {
                fluent.Item("timeout").Value(options.Timeout_->MilliSeconds());
            })
            .DoIf(options.InputRowLimit_.Defined(), [&] (TFluentMap fluent) {
                fluent.Item("input_row_limit").Value(*options.InputRowLimit_);
            })
            .DoIf(options.OutputRowLimit_.Defined(), [&] (TFluentMap fluent) {
                fluent.Item("output_row_limit").Value(*options.OutputRowLimit_);
            })
            .Item("range_expansion_limit").Value(options.RangeExpansionLimit_)
            .Item("fail_on_incomplete_result").Value(options.FailOnIncompleteResult_)
            .Item("verbose_logging").Value(options.VerboseLogging_)
            .Item("enable_code_cache").Value(options.EnableCodeCache_)
        .EndMap());

        auto response = RetryRequest(ServerName_, header, "", true);
        return NodeFromYsonString(response, YT_LIST_FRAGMENT).AsList();
    }

private:
    void ModifyRows(
        const Stroka& command,
        const TYPath& path,
        const TNode::TList& rows)
    {
        THttpHeader header("PUT", command);
        header.AddPath(path);
        header.SetDataStreamFormat(DSF_YSON_BINARY);

        auto body = NodeListToYsonString(rows);
        RetryRequest(ServerName_, header, body, true);
    }
};

IClientPtr CreateClient(const Stroka& serverName)
{
    auto globalTxId = GetGuid(TConfig::Get()->GlobalTxId);
    return new TClient(serverName, globalTxId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
