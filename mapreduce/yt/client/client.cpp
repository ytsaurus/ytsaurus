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
        case NT_INT64: return "int64_node";
        case NT_UINT64: return "uint64_node";
        case NT_DOUBLE: return "double_node";
        case NT_BOOLEAN: return "boolean_node";
        case NT_MAP: return "map_node";
        case NT_LIST: return "list_node";
        case NT_FILE: return "file";
        case NT_TABLE: return "table";
        case NT_DOCUMENT: return "document";
        default:
            LOG_FATAL("Invalid node type %i", static_cast<int>(type));
    }
}

////////////////////////////////////////////////////////////////////////////////

ITransactionPtr CreateTransactionObject(
    const TAuth& auth,
    const TTransactionId& transactionId,
    bool isOwning,
    const TStartTransactionOptions& options = TStartTransactionOptions());

////////////////////////////////////////////////////////////////////////////////

class TClientBase
    : virtual public IClientBase
{
public:
    explicit TClientBase(
        const TAuth& auth,
        const TTransactionId& transactionId = TTransactionId())
        : Auth_(auth)
        , TransactionId_(transactionId)
    { }

    virtual ITransactionPtr StartTransaction(
        const TStartTransactionOptions& options) override
    {
        return CreateTransactionObject(Auth_, TransactionId_, true, options);
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
            header.SetParameters(AttributesToYsonString(*options.Attributes_));
        }

        return ParseGuidFromResponse(RetryRequest(Auth_, header));
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
        RetryRequest(Auth_, header);
    }

    virtual bool Exists(
        const TYPath& path) override
    {
        THttpHeader header("GET", "exists");
        header.AddPath(AddPathPrefix(path));
        header.AddTransactionId(TransactionId_);
        return ParseBoolFromResponse(RetryRequest(Auth_, header));
    }

    virtual TNode Get(
        const TYPath& path,
        const TGetOptions& options) override
    {
        THttpHeader header("GET", "get");
        header.AddPath(AddPathPrefix(path));
        header.AddTransactionId(TransactionId_);

        if (options.AttributeFilter_) {
            header.SetParameters(AttributeFilterToYsonString(*options.AttributeFilter_));
        }
        if (options.MaxSize_) {
            header.AddParam("max_size", *options.MaxSize_);
        }
        header.AddParam("ignore_opaque", options.IgnoreOpaque_);
        return NodeFromYsonString(RetryRequest(Auth_, header));
    }

    virtual void Set(
        const TYPath& path,
        const TNode& value) override
    {
        THttpHeader header("PUT", "set");
        header.AddPath(AddPathPrefix(path));
        header.AddTransactionId(TransactionId_);
        header.AddMutationId();
        RetryRequest(Auth_, header, NodeToYsonString(value));
    }

    virtual TNode::TList List(
        const TYPath& path,
        const TListOptions& options) override
    {
        THttpHeader header("GET", "list");

        // FIXME: ugly but quick empty path special case
        if (path.empty() && TConfig::Get()->Prefix == "//") {
            header.AddPath("/");
        } else {
            header.AddPath(AddPathPrefix(path));
        }
        header.AddTransactionId(TransactionId_);

        if (options.AttributeFilter_) {
            header.SetParameters(AttributeFilterToYsonString(*options.AttributeFilter_));
        }
        if (options.MaxSize_) {
            header.AddParam("max_size", *options.MaxSize_);
        }
        return NodeFromYsonString(RetryRequest(Auth_, header)).AsList();
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
        return ParseGuidFromResponse(RetryRequest(Auth_, header));
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
        return ParseGuidFromResponse(RetryRequest(Auth_, header));
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
            header.SetParameters(AttributesToYsonString(*options.Attributes_));
        }
        return ParseGuidFromResponse(RetryRequest(Auth_, header));
    }

    // io

    virtual IFileReaderPtr CreateFileReader(
        const TRichYPath& path,
        const TFileReaderOptions& options) override
    {
        return new TFileReader(AddPathPrefix(path), Auth_, TransactionId_, options);
    }

    virtual IFileWriterPtr CreateFileWriter(
        const TRichYPath& path,
        const TFileWriterOptions& options) override
    {
        return new TFileWriter(AddPathPrefix(path), Auth_, TransactionId_, options);
    }

    // operations

    virtual TOperationId DoMap(
        const TMapOperationSpec& spec,
        IJob* mapper,
        const TOperationOptions& options) override
    {
        return ExecuteMap(
            Auth_,
            TransactionId_,
            spec,
            mapper,
            options);
    }

    virtual TOperationId DoReduce(
        const TReduceOperationSpec& spec,
        IJob* reducer,
        const TOperationOptions& options) override
    {
        return ExecuteReduce(
            Auth_,
            TransactionId_,
            spec,
            reducer,
            options);
    }

    virtual TOperationId DoJoinReduce(
        const TJoinReduceOperationSpec& spec,
        IJob* reducer,
        const TOperationOptions& options) override
    {
        return ExecuteJoinReduce(
            Auth_,
            TransactionId_,
            spec,
            reducer,
            options);
    }

    virtual TOperationId DoMapReduce(
        const TMapReduceOperationSpec& spec,
        IJob* mapper,
        IJob* reduceCombiner,
        IJob* reducer,
        const TMultiFormatDesc& outputMapperDesc,
        const TMultiFormatDesc& inputReduceCombinerDesc,
        const TMultiFormatDesc& outputReduceCombinerDesc,
        const TMultiFormatDesc& inputReducerDesc,
        const TOperationOptions& options) override
    {
        return ExecuteMapReduce(
            Auth_,
            TransactionId_,
            spec,
            mapper,
            reduceCombiner,
            reducer,
            outputMapperDesc,
            inputReduceCombinerDesc,
            outputReduceCombinerDesc,
            inputReducerDesc,
            options);
    }

    virtual TOperationId Sort(
        const TSortOperationSpec& spec,
        const TOperationOptions& options) override
    {
        return ExecuteSort(
            Auth_,
            TransactionId_,
            spec,
            options);
    }

    virtual TOperationId Merge(
        const TMergeOperationSpec& spec,
        const TOperationOptions& options) override
    {
        return ExecuteMerge(
            Auth_,
            TransactionId_,
            spec,
            options);
    }

    virtual TOperationId Erase(
        const TEraseOperationSpec& spec,
        const TOperationOptions& options) override
    {
        return ExecuteErase(
            Auth_,
            TransactionId_,
            spec,
            options);
    }

    virtual EOperationStatus CheckOperation(const TOperationId& operationId) override
    {
        return NYT::CheckOperation(Auth_, TransactionId_, operationId);
    }

    virtual void AbortOperation(const TOperationId& operationId) override
    {
        NYT::AbortOperation(Auth_, TransactionId_, operationId);
    }

    virtual void WaitForOperation(const TOperationId& operationId) override
    {
        NYT::WaitForOperation(Auth_, TransactionId_, operationId);
    }

protected:
    TAuth Auth_;
    TTransactionId TransactionId_;

private:
    THolder<TClientReader> CreateClientReader(
        const TRichYPath& path, EDataStreamFormat format, const TTableReaderOptions& options)
    {
        return MakeHolder<TClientReader>(
            AddPathPrefix(path), Auth_, TransactionId_, format, options);
    }

    THolder<TClientWriter> CreateClientWriter(
        const TRichYPath& path, EDataStreamFormat format, const TTableWriterOptions& options)
    {
        return MakeHolder<TClientWriter>(
            AddPathPrefix(path), Auth_, TransactionId_, format, options);
    }

    virtual TIntrusivePtr<INodeReaderImpl> CreateNodeReader(
        const TRichYPath& path, const TTableReaderOptions& options) override
    {
        return new TNodeTableReader(CreateClientReader(path, DSF_YSON_BINARY, options));
    }

    virtual TIntrusivePtr<IYaMRReaderImpl> CreateYaMRReader(
        const TRichYPath& path, const TTableReaderOptions& options) override
    {
        return new TYaMRTableReader(CreateClientReader(path, DSF_YAMR_LENVAL, options));
    }

    virtual TIntrusivePtr<IProtoReaderImpl> CreateProtoReader(
        const TRichYPath& path, const TTableReaderOptions& options) override
    {
        return new TProtoTableReader(CreateClientReader(path, DSF_YSON_BINARY, options));
        // later: DSF_PROTO
    }

    virtual TIntrusivePtr<INodeWriterImpl> CreateNodeWriter(
        const TRichYPath& path, const TTableWriterOptions& options) override
    {
        return new TNodeTableWriter(CreateClientWriter(path, DSF_YSON_BINARY, options));
    }

    virtual TIntrusivePtr<IYaMRWriterImpl> CreateYaMRWriter(
        const TRichYPath& path, const TTableWriterOptions& options) override
    {
        return new TYaMRTableWriter(CreateClientWriter(path, DSF_YAMR_LENVAL, options));
    }

    virtual TIntrusivePtr<IProtoWriterImpl> CreateProtoWriter(
        const TRichYPath& path, const TTableWriterOptions& options) override
    {
        return new TProtoTableWriter(CreateClientWriter(path, DSF_YSON_BINARY, options));
        // later: DSF_PROTO
    }
};

class TTransaction
    : public ITransaction
    , public TClientBase
{
public:
    TTransaction(
        const TAuth& auth,
        const TTransactionId& transactionId,
        bool isOwning,
        const TStartTransactionOptions& options)
        : TClientBase(auth)
    {
        if (isOwning) {
            PingableTx_ = new TPingableTransaction(
                auth,
                transactionId, // parent id
                options.Timeout_,
                options.PingAncestors_,
                options.Attributes_);
            TransactionId_ = PingableTx_->GetId();
        } else {
            TransactionId_ = transactionId;
        }
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
        header.AddPath(AddPathPrefix(path));
        header.AddParam("mode", ToString(mode));
        header.AddTransactionId(TransactionId_);
        header.AddMutationId();

        header.AddParam("waitable", options.Waitable_);
        return ParseGuidFromResponse(RetryRequest(Auth_, header));
    }

    virtual void Commit() override
    {
        if (PingableTx_) {
            PingableTx_->Commit();
        } else {
            CommitTransaction(Auth_, TransactionId_);
        }
    }

    virtual void Abort() override
    {
        if (PingableTx_) {
            PingableTx_->Abort();
        } else {
            AbortTransaction(Auth_, TransactionId_);
        }
    }

private:
    THolder<TPingableTransaction> PingableTx_;
};

ITransactionPtr CreateTransactionObject(
    const TAuth& auth,
    const TTransactionId& transactionId,
    bool isOwning,
    const TStartTransactionOptions& options)
{
    return new TTransaction(auth, transactionId, isOwning, options);
}

////////////////////////////////////////////////////////////////////////////////

class TClient
    : public IClient
    , public TClientBase
{
public:
    TClient(
        const TAuth& auth,
        const TTransactionId& globalId)
        : TClientBase(auth, globalId)
    { }

    virtual ITransactionPtr AttachTransaction(
        const TTransactionId& transactionId) override
    {
        return CreateTransactionObject(Auth_, transactionId, false);
    }

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

        header.SetParameters(BuildYsonStringFluently().BeginMap()
            .DoIf(options.Timeout_.Defined(), [&] (TFluentMap fluent) {
                fluent.Item("timeout").Value(options.Timeout_->MilliSeconds());
            })
            .Item("keep_missing_rows").Value(options.KeepMissingRows_)
        .EndMap());

        auto body = NodeListToYsonString(keys);
        auto response = RetryRequest(Auth_, header, body, true);
        return NodeFromYsonString(response, YT_LIST_FRAGMENT).AsList();
    }

    virtual TNode::TList SelectRows(
        const Stroka& query,
        const TSelectRowsOptions& options) override
    {
        THttpHeader header("GET", "select_rows");
        header.SetDataStreamFormat(DSF_YSON_BINARY);

        header.SetParameters(BuildYsonStringFluently().BeginMap()
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

        auto response = RetryRequest(Auth_, header, "", true);
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
        RetryRequest(Auth_, header, body, true);
    }
};

IClientPtr CreateClient(
    const Stroka& serverName,
    const TCreateClientOptions& options)
{
    auto globalTxId = GetGuid(TConfig::Get()->GlobalTxId);

    TAuth auth;
    auth.ServerName = serverName;
    auth.Token = TConfig::Get()->Token;
    if (!options.Token_.empty()) {
        TConfig::ValidateToken(options.Token_);
        auth.Token = options.Token_;
    }

    return new TClient(auth, globalTxId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
