#include "client.h"

#include "batch_request_impl.h"
#include "client_reader.h"
#include "client_writer.h"
#include "file_reader.h"
#include "file_writer.h"
#include "format_hints.h"
#include "lock.h"
#include "operation.h"
#include "retryful_writer.h"
#include "skiff.h"
#include "yt_poller.h"

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/interface/fluent.h>

#include <mapreduce/yt/interface/logging/log.h>

#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/finally_guard.h>

#include <mapreduce/yt/http/http.h>
#include <mapreduce/yt/http/requests.h>
#include <mapreduce/yt/http/retry_request.h>

#include <mapreduce/yt/raw_client/raw_requests.h>

#include <mapreduce/yt/io/yamr_table_reader.h>
#include <mapreduce/yt/io/yamr_table_writer.h>
#include <mapreduce/yt/io/node_table_reader.h>
#include <mapreduce/yt/io/node_table_writer.h>
#include <mapreduce/yt/io/proto_table_reader.h>
#include <mapreduce/yt/io/proto_table_writer.h>
#include <mapreduce/yt/io/proto_helpers.h>
#include <mapreduce/yt/io/skiff_table_reader.h>

#include <mapreduce/yt/raw_client/rpc_parameters_serialization.h>

#include <library/json/json_reader.h>

#include <util/generic/algorithm.h>
#include <util/string/type.h>
#include <util/system/env.h>

#include <exception>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

const size_t TClientBase::BUFFER_SIZE = 64 << 20;

////////////////////////////////////////////////////////////////////////////////

TClientBase::TClientBase(
    const TAuth& auth,
    const TTransactionId& transactionId)
    : Auth_(auth)
    , TransactionId_(transactionId)
{ }

ITransactionPtr TClientBase::StartTransaction(
    const TStartTransactionOptions& options)
{
    return MakeIntrusive<TTransaction>(GetParentClient(), Auth_, TransactionId_, true, options);
}

TNodeId TClientBase::Create(
    const TYPath& path,
    ENodeType type,
    const TCreateOptions& options)
{
    THttpHeader header("POST", "create");
    header.AddMutationId();
    header.MergeParameters(NDetail::SerializeParamsForCreate(TransactionId_, path, type, options));
    return ParseGuidFromResponse(RetryRequest(Auth_, header));
}

void TClientBase::Remove(
    const TYPath& path,
    const TRemoveOptions& options)
{
    THttpHeader header("POST", "remove");
    header.AddMutationId();
    header.MergeParameters(NDetail::SerializeParamsForRemove(TransactionId_, path, options));
    RetryRequest(Auth_, header);
}

bool TClientBase::Exists(const TYPath& path)
{
    return NYT::NDetail::Exists(Auth_, TransactionId_, path);
}

TNode TClientBase::Get(
    const TYPath& path,
    const TGetOptions& options)
{
    return NDetail::Get(Auth_, TransactionId_, path, options);
}

void TClientBase::Set(
    const TYPath& path,
    const TNode& value,
    const TSetOptions& options)
{
    NDetail::Set(Auth_, TransactionId_, path, value, options);
}

TNode::TListType TClientBase::List(
    const TYPath& path,
    const TListOptions& options)
{
    return NDetail::List(Auth_, TransactionId_, path, options);
}

TNodeId TClientBase::Copy(
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options)
{
    THttpHeader header("POST", "copy");
    header.AddMutationId();
    header.MergeParameters(NDetail::SerializeParamsForCopy(TransactionId_, sourcePath, destinationPath, options));
    return ParseGuidFromResponse(RetryRequest(Auth_, header));
}

TNodeId TClientBase::Move(
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options)
{
    THttpHeader header("POST", "move");
    header.AddMutationId();
    header.MergeParameters(NDetail::SerializeParamsForMove(TransactionId_, sourcePath, destinationPath, options));
    return ParseGuidFromResponse(RetryRequest(Auth_, header));
}

TNodeId TClientBase::Link(
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options)
{
    return NYT::NDetail::Link(Auth_, TransactionId_, targetPath, linkPath, options);
}

void TClientBase::Concatenate(
    const TVector<TYPath>& sourcePaths,
    const TYPath& destinationPath,
    const TConcatenateOptions& options)
{
    THttpHeader header("POST", "concatenate");
    header.AddTransactionId(TransactionId_);
    header.AddMutationId();

    TRichYPath path(AddPathPrefix(destinationPath));
    path.Append(options.Append_);
    header.MergeParameters(BuildYsonNodeFluently().BeginMap()
        .Item("source_paths").DoListFor(sourcePaths,
            [] (TFluentList fluent, const TYPath& thePath) {
                fluent.Item().Value(AddPathPrefix(thePath));
            })
        .Item("destination_path").Value(path)
    .EndMap());

    RetryRequest(Auth_, header);
}

TRichYPath TClientBase::CanonizeYPath(const TRichYPath& path)
{
    return CanonizePath(Auth_, path);
}

TVector<TTableColumnarStatistics> TClientBase::GetTableColumnarStatistics(const TVector<TRichYPath>& paths)
{
    THttpHeader header("GET", "get_table_columnar_statistics");
    header.MergeParameters(NDetail::SerializeParamsForGetTableColumnarStatistics(TransactionId_, paths));
    auto response = NodeFromYsonString(RetryRequest(Auth_, header));
    TVector<TTableColumnarStatistics> result;
    Deserialize(result, response);
    return result;
}

IFileReaderPtr TClientBase::CreateBlobTableReader(
    const TYPath& path,
    const TKey& key,
    const TBlobTableReaderOptions& options)
{
    return new TBlobTableReader(
        path,
        key,
        Auth_,
        TransactionId_,
        options);
}

IFileReaderPtr TClientBase::CreateFileReader(
    const TRichYPath& path,
    const TFileReaderOptions& options)
{
    return new TFileReader(
        CanonizePath(Auth_, path),
        Auth_,
        TransactionId_,
        options);
}

IFileWriterPtr TClientBase::CreateFileWriter(
    const TRichYPath& path,
    const TFileWriterOptions& options)
{
    auto realPath = CanonizePath(Auth_, path);
    if (!NYT::NDetail::Exists(Auth_, TransactionId_, realPath.Path_)) {
        NYT::NDetail::Create(Auth_, TransactionId_, realPath.Path_, NT_FILE,
            TCreateOptions().IgnoreExisting(true));
    }
    return new TFileWriter(realPath, Auth_, TransactionId_, options);
}

TTableWriterPtr<::google::protobuf::Message> TClientBase::CreateTableWriter(
    const TRichYPath& path, const ::google::protobuf::Descriptor& descriptor, const TTableWriterOptions& options)
{
    const Message* prototype = google::protobuf::MessageFactory::generated_factory()->GetPrototype(&descriptor);
    return new TTableWriter<::google::protobuf::Message>(CreateProtoWriter(path, options, prototype));
}

TRawTableReaderPtr TClientBase::CreateRawReader(
    const TRichYPath& path,
    const TFormat& format,
    const TTableReaderOptions& options)
{
    return CreateClientReader(path, format, options).Get();
}

TRawTableWriterPtr TClientBase::CreateRawWriter(
    const TRichYPath& path,
    const TFormat& format,
    const TTableWriterOptions& options)
{
    return ::MakeIntrusive<TRetryfulWriter>(
        Auth_,
        TransactionId_,
        GetWriteTableCommand(),
        format,
        CanonizePath(Auth_, path),
        BUFFER_SIZE,
        options).Get();
}

IOperationPtr TClientBase::DoMap(
    const TMapOperationSpec& spec,
    IJob* mapper,
    const TOperationOptions& options)
{
    auto operationId = ExecuteMap(
        Auth_,
        TransactionId_,
        spec,
        mapper,
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClient(), options);
}

IOperationPtr TClientBase::RawMap(
    const TRawMapOperationSpec& spec,
    ::TIntrusivePtr<IRawJob> mapper,
    const TOperationOptions& options)
{
    auto operationId = ExecuteRawMap(
        Auth_,
        TransactionId_,
        spec,
        mapper.Get(),
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClient(), options);
}

IOperationPtr TClientBase::DoReduce(
    const TReduceOperationSpec& spec,
    IJob* reducer,
    const TOperationOptions& options)
{
    auto operationId = ExecuteReduce(
        Auth_,
        TransactionId_,
        spec,
        reducer,
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClient(), options);
}

IOperationPtr TClientBase::RawReduce(
    const TRawReduceOperationSpec& spec,
    ::TIntrusivePtr<IRawJob> reducer,
    const TOperationOptions& options)
{
    auto operationId = ExecuteRawReduce(
        Auth_,
        TransactionId_,
        spec,
        reducer.Get(),
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClient(), options);
}

IOperationPtr TClientBase::DoJoinReduce(
    const TJoinReduceOperationSpec& spec,
    IJob* reducer,
    const TOperationOptions& options)
{
    auto operationId = ExecuteJoinReduce(
        Auth_,
        TransactionId_,
        spec,
        reducer,
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClient(), options);
}

IOperationPtr TClientBase::RawJoinReduce(
    const TRawJoinReduceOperationSpec& spec,
    ::TIntrusivePtr<IRawJob> reducer,
    const TOperationOptions& options)
{
    auto operationId = ExecuteRawJoinReduce(
        Auth_,
        TransactionId_,
        spec,
        reducer.Get(),
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClient(), options);
}

IOperationPtr TClientBase::DoMapReduce(
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
    auto operationId = ExecuteMapReduce(
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
    return CreateOperationAndWaitIfRequired(operationId, GetParentClient(), options);
}

IOperationPtr TClientBase::RawMapReduce(
    const TRawMapReduceOperationSpec& spec,
    ::TIntrusivePtr<IRawJob> mapper,
    ::TIntrusivePtr<IRawJob> reduceCombiner,
    ::TIntrusivePtr<IRawJob> reducer,
    const TOperationOptions& options)
{
    auto operationId = ExecuteRawMapReduce(
        Auth_,
        TransactionId_,
        spec,
        mapper.Get(),
        reduceCombiner.Get(),
        reducer.Get(),
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClient(), options);
}

IOperationPtr TClientBase::Sort(
    const TSortOperationSpec& spec,
    const TOperationOptions& options)
{
    auto operationId = ExecuteSort(
        Auth_,
        TransactionId_,
        spec,
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClient(), options);
}

IOperationPtr TClientBase::Merge(
    const TMergeOperationSpec& spec,
    const TOperationOptions& options)
{
    auto operationId = ExecuteMerge(
        Auth_,
        TransactionId_,
        spec,
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClient(), options);
}

IOperationPtr TClientBase::Erase(
    const TEraseOperationSpec& spec,
    const TOperationOptions& options)
{
    auto operationId = ExecuteErase(
        Auth_,
        TransactionId_,
        spec,
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClient(), options);
}

IOperationPtr TClientBase::RunVanilla(
    const TVanillaOperationSpec& spec,
    const TOperationOptions& options)
{
    auto operationId = ExecuteVanilla(
        Auth_,
        TransactionId_,
        spec,
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClient(), options);
}

IOperationPtr TClientBase::AttachOperation(const TOperationId& operationId)
{
    auto operation = ::MakeIntrusive<TOperation>(operationId, GetParentClient());
    operation->GetBriefState(); // check that operation exists
    return operation;
}

EOperationBriefState TClientBase::CheckOperation(const TOperationId& operationId)
{
    return NYT::NDetail::CheckOperation(Auth_, operationId);
}

void TClientBase::AbortOperation(const TOperationId& operationId)
{
    NYT::NDetail::AbortOperation(Auth_, operationId);
}

void TClientBase::CompleteOperation(const TOperationId& operationId)
{
    NYT::NDetail::CompleteOperation(Auth_, operationId);
}

void TClientBase::WaitForOperation(const TOperationId& operationId)
{
    NYT::NDetail::WaitForOperation(Auth_, operationId);
}

TOperationAttributes TClientBase::GetOperation(
    const TOperationId& operationId,
    const TGetOperationOptions& options)
{
    return NYT::NDetail::GetOperation(Auth_, operationId, options);
}

TListOperationsResult TClientBase::ListOperations(
    const TListOperationsOptions& options)
{
    return NYT::NDetail::ListOperations(Auth_, options);
}

void TClientBase::UpdateOperationParameters(
    const TOperationId& operationId,
    const TNode& newParameters)
{
    return NYT::NDetail::UpdateOperationParameters(Auth_, operationId, newParameters);
}

TListJobsResult TClientBase::ListJobs(
    const TOperationId& operationId,
    const TListJobsOptions& options)
{
    return NYT::NDetail::ListJobs(Auth_, operationId, options);
}

void TClientBase::AlterTable(
    const TYPath& path,
    const TAlterTableOptions& options)
{
    THttpHeader header("POST", "alter_table");
    header.AddMutationId();
    header.MergeParameters(SerializeParamsForAlterTable(TransactionId_, path, options));
    RetryRequest(Auth_, header);
}

::TIntrusivePtr<TClientReader> TClientBase::CreateClientReader(
    const TRichYPath& path,
    const TFormat& format,
    const TTableReaderOptions& options,
    bool useFormatFromTableAttributes)
{
    return ::MakeIntrusive<TClientReader>(
        CanonizePath(Auth_, path),
        Auth_,
        TransactionId_,
        format,
        options,
        useFormatFromTableAttributes);
}

THolder<TClientWriter> TClientBase::CreateClientWriter(
    const TRichYPath& path,
    const TFormat& format,
    const TTableWriterOptions& options)
{
    auto realPath = CanonizePath(Auth_, path);
    if (!NYT::NDetail::Exists(Auth_, TransactionId_, realPath.Path_)) {
        NYT::NDetail::Create(Auth_, TransactionId_, realPath.Path_, NT_TABLE,
            TCreateOptions().IgnoreExisting(true));
    }
    return MakeHolder<TClientWriter>(
        realPath, Auth_, TransactionId_, format, options);
}

::TIntrusivePtr<INodeReaderImpl> TClientBase::CreateNodeReader(
    const TRichYPath& path, const TTableReaderOptions& options)
{
    auto format = TFormat::YsonBinary();
    ApplyFormatHints<TNode>(&format, options.FormatHints_);

    // Skiff is disabled here because of large header problem (see https://st.yandex-team.ru/YT-6926).
    // Revert this code to r3614168 when it is fixed.
    return new TNodeTableReader(
        CreateClientReader(path, format, options),
        options.SizeLimit_);
}

::TIntrusivePtr<IYaMRReaderImpl> TClientBase::CreateYaMRReader(
    const TRichYPath& path, const TTableReaderOptions& options)
{
    return new TYaMRTableReader(
        CreateClientReader(path, TFormat::YaMRLenval(), options, /* useFormatFromTableAttributes = */ true));
}

::TIntrusivePtr<IProtoReaderImpl> TClientBase::CreateProtoReader(
    const TRichYPath& path,
    const TTableReaderOptions& options,
    const Message* prototype)
{
    TVector<const ::google::protobuf::Descriptor*> descriptors;
    descriptors.push_back(prototype->GetDescriptor());

    if (TConfig::Get()->UseClientProtobuf) {
        return new TProtoTableReader(
            CreateClientReader(path, TFormat::YsonBinary(), options),
            std::move(descriptors));
    } else {
        auto format = TFormat::Protobuf({prototype->GetDescriptor()});
        return new TLenvalProtoTableReader(
            CreateClientReader(path, format, options),
            std::move(descriptors));
    }
}

::TIntrusivePtr<INodeWriterImpl> TClientBase::CreateNodeWriter(
    const TRichYPath& path, const TTableWriterOptions& options)
{
    return new TNodeTableWriter(
        CreateClientWriter(path, TFormat::YsonBinary(), options));
}

::TIntrusivePtr<IYaMRWriterImpl> TClientBase::CreateYaMRWriter(
    const TRichYPath& path, const TTableWriterOptions& options)
{
    return new TYaMRTableWriter(
        CreateClientWriter(path, TFormat::YaMRLenval(), options));
}

::TIntrusivePtr<IProtoWriterImpl> TClientBase::CreateProtoWriter(
    const TRichYPath& path,
    const TTableWriterOptions& options,
    const Message* prototype)
{
    TVector<const ::google::protobuf::Descriptor*> descriptors;
    descriptors.push_back(prototype->GetDescriptor());

    if (TConfig::Get()->UseClientProtobuf) {
        return new TProtoTableWriter(
            CreateClientWriter(path, TFormat::YsonBinary(), options),
            std::move(descriptors));
    } else {
        auto format = TFormat::Protobuf({prototype->GetDescriptor()});
        return new TLenvalProtoTableWriter(
            CreateClientWriter(path, format, options),
            std::move(descriptors));
    }
}

TBatchRequestPtr TClientBase::CreateBatchRequest()
{
    return MakeIntrusive<TBatchRequest>(TransactionId_, GetParentClient());
}

const TAuth& TClientBase::GetAuth() const
{
    return Auth_;
}

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(
    TClientPtr parentClient,
    const TAuth& auth,
    const TTransactionId& transactionId,
    bool isOwning,
    const TStartTransactionOptions& options)
    : TClientBase(auth, transactionId)
    , PingableTx_(isOwning ?
        new TPingableTransaction(
            auth,
            transactionId, // parent id
            options.Timeout_,
            options.PingAncestors_,
            options.AutoPingable_,
            options.Title_,
            options.Attributes_)
        : nullptr)
    , ParentClient_(parentClient)
{
    TransactionId_ = isOwning ? PingableTx_->GetId() : transactionId;
}

const TTransactionId& TTransaction::GetId() const
{
    return TransactionId_;
}

ILockPtr TTransaction::Lock(
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options)
{
    auto lockId = NYT::NDetail::Lock(Auth_, TransactionId_, path, mode, options);
    if (options.Waitable_) {
        return ::MakeIntrusive<TLock>(lockId, GetParentClient());
    } else {
        return ::MakeIntrusive<TLock>(lockId);
    }
}

void TTransaction::Commit()
{
    if (PingableTx_) {
        PingableTx_->Commit();
    } else {
        CommitTransaction(Auth_, TransactionId_);
    }
}

void TTransaction::Abort()
{
    if (PingableTx_) {
        PingableTx_->Abort();
    } else {
        AbortTransaction(Auth_, TransactionId_);
    }
}

void TTransaction::Ping()
{
    TPingRetryPolicy retryPolicy(TConfig::Get()->RetryCount);
    PingTx(Auth_, TransactionId_, &retryPolicy);
}

TClientPtr TTransaction::GetParentClient()
{
    return ParentClient_;
}

////////////////////////////////////////////////////////////////////////////////

TClient::TClient(
    const TAuth& auth,
    const TTransactionId& globalId)
    : TClientBase(auth, globalId)
{ }

TClient::~TClient() = default;

ITransactionPtr TClient::AttachTransaction(
    const TTransactionId& transactionId)
{
    return MakeIntrusive<TTransaction>(this, Auth_, transactionId, false, TStartTransactionOptions());
}

void TClient::MountTable(
    const TYPath& path,
    const TMountTableOptions& options)
{
    THttpHeader header("POST", "mount_table");
    SetTabletParams(header, path, options);
    if (options.CellId_) {
        header.AddParameter("cell_id", GetGuidAsString(*options.CellId_));
    }
    header.AddParameter("freeze", options.Freeze_);
    RetryRequest(Auth_, header);
}

void TClient::UnmountTable(
    const TYPath& path,
    const TUnmountTableOptions& options)
{
    THttpHeader header("POST", "unmount_table");
    SetTabletParams(header, path, options);
    header.AddParameter("force", options.Force_);
    RetryRequest(Auth_, header);
}

void TClient::RemountTable(
    const TYPath& path,
    const TRemountTableOptions& options)
{
    THttpHeader header("POST", "remount_table");
    SetTabletParams(header, path, options);
    RetryRequest(Auth_, header);
}

void TClient::FreezeTable(
    const TYPath& path,
    const TFreezeTableOptions& options)
{
    THttpHeader header("POST", "freeze_table");
    SetTabletParams(header, path, options);
    RetryRequest(Auth_, header);
}

void TClient::UnfreezeTable(
    const TYPath& path,
    const TUnfreezeTableOptions& options)
{
    THttpHeader header("POST", "unfreeze_table");
    SetTabletParams(header, path, options);
    RetryRequest(Auth_, header);
}

void TClient::ReshardTable(
    const TYPath& path,
    const TVector<TKey>& keys,
    const TReshardTableOptions& options)
{
    THttpHeader header("POST", "reshard_table");
    SetTabletParams(header, path, options);
    header.AddParameter("pivot_keys", BuildYsonNodeFluently().List(keys));
    RetryRequest(Auth_, header);
}

void TClient::ReshardTable(
    const TYPath& path,
    i64 tabletCount,
    const TReshardTableOptions& options)
{
    THttpHeader header("POST", "reshard_table");
    SetTabletParams(header, path, options);
    header.AddParameter("tablet_count", tabletCount);
    RetryRequest(Auth_, header);
}

void TClient::InsertRows(
    const TYPath& path,
    const TNode::TListType& rows,
    const TInsertRowsOptions& options)
{
    THttpHeader header("PUT", "insert_rows");
    header.SetInputFormat(TFormat::YsonBinary());
    header.MergeParameters(NDetail::SerializeParametersForInsertRows(path, options));

    auto body = NodeListToYsonString(rows);
    RetryRequest(Auth_, header, TStringBuf(body), true);
}

void TClient::DeleteRows(
    const TYPath& path,
    const TNode::TListType& keys,
    const TDeleteRowsOptions& options)
{
    THttpHeader header("PUT", "delete_rows");
    header.SetInputFormat(TFormat::YsonBinary());
    header.MergeParameters(NDetail::SerializeParametersForDeleteRows(path, options));

    auto body = NodeListToYsonString(keys);
    RetryRequest(Auth_, header, TStringBuf(body), true);
}

void TClient::TrimRows(
    const TYPath& path,
    i64 tabletIndex,
    i64 rowCount,
    const TTrimRowsOptions& options)
{
    THttpHeader header("POST", "trim_rows");
    header.AddParameter("trimmed_row_count", rowCount);
    header.AddParameter("tablet_index", tabletIndex);
    header.MergeParameters(NDetail::SerializeParametersForTrimRows(path, options));
    RetryRequest(Auth_, header, Nothing(), true);
}

TNode::TListType TClient::LookupRows(
    const TYPath& path,
    const TNode::TListType& keys,
    const TLookupRowsOptions& options)
{
    Y_UNUSED(options);
    THttpHeader header("PUT", "lookup_rows");
    header.AddPath(AddPathPrefix(path));
    header.SetInputFormat(TFormat::YsonBinary());
    header.SetOutputFormat(TFormat::YsonBinary());

    header.MergeParameters(BuildYsonNodeFluently().BeginMap()
        .DoIf(options.Timeout_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("timeout").Value(static_cast<i64>(options.Timeout_->MilliSeconds()));
        })
        .Item("keep_missing_rows").Value(options.KeepMissingRows_)
        .DoIf(options.Columns_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("column_names").Value(*options.Columns_);
        })
    .EndMap());

    auto body = NodeListToYsonString(keys);
    auto response = RetryRequest(Auth_, header, TStringBuf(body), true);
    return NodeFromYsonString(response, YT_LIST_FRAGMENT).AsList();
}

TNode::TListType TClient::SelectRows(
    const TString& query,
    const TSelectRowsOptions& options)
{
    THttpHeader header("GET", "select_rows");
    header.SetInputFormat(TFormat::YsonBinary());
    header.SetOutputFormat(TFormat::YsonBinary());

    header.MergeParameters(BuildYsonNodeFluently().BeginMap()
        .Item("query").Value(query)
        .DoIf(options.Timeout_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("timeout").Value(static_cast<i64>(options.Timeout_->MilliSeconds()));
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

    auto response = RetryRequest(Auth_, header, Nothing(), true);
    return NodeFromYsonString(response, YT_LIST_FRAGMENT).AsList();
}

void TClient::EnableTableReplica(const TReplicaId& replicaid)
{
    THttpHeader header("POST", "enable_table_replica");
    header.AddParameter("replica_id", GetGuidAsString(replicaid));
    RetryRequest(Auth_, header);
}

void TClient::DisableTableReplica(const TReplicaId& replicaid)
{
    THttpHeader header("POST", "disable_table_replica");
    header.AddParameter("replica_id", GetGuidAsString(replicaid));
    RetryRequest(Auth_, header);
}

void TClient::AlterTableReplica(const TReplicaId& replicaId, const TAlterTableReplicaOptions& options)
{
    THttpHeader header("POST", "alter_table_replica");
    header.AddMutationId();
    header.MergeParameters(NDetail::SerializeParamsForAlterTableReplica(replicaId, options));
    RetryRequest(Auth_, header);
}

ui64 TClient::GenerateTimestamp()
{
    THttpHeader header("GET", "generate_timestamp");
    auto response = RetryRequest(Auth_, header, Nothing(), true);
    return NodeFromYsonString(response).AsUint64();
}

TAuthorizationInfo TClient::WhoAmI()
{
    THttpHeader header("GET", "auth/whoami", /* isApi = */ false);
    auto response = RetryRequest(Auth_, header);
    TAuthorizationInfo result;

    NJson::TJsonValue jsonValue;
    bool ok = NJson::ReadJsonTree(response, &jsonValue, /* throwOnError = */ true);
    Y_VERIFY(ok);
    result.Login = jsonValue["login"].GetString();
    result.Realm = jsonValue["realm"].GetString();
    return result;
}

TYtPoller& TClient::GetYtPoller()
{
    auto g = Guard(YtPollerLock_);
    if (!YtPoller_) {
        // We don't use current client and create new client because YtPoller_ might use
        // this client during current client shutdown.
        // That might lead to incrementing of current client refcount and double delete of current client object.
        YtPoller_ = MakeHolder<TYtPoller>(Auth_);
    }
    return *YtPoller_;
}

TClientPtr TClient::GetParentClient()
{
    return this;
}

template <class TOptions>
void TClient::SetTabletParams(
    THttpHeader& header,
    const TYPath& path,
    const TOptions& options)
{
    header.AddPath(AddPathPrefix(path));
    if (options.FirstTabletIndex_) {
        header.AddParameter("first_tablet_index", *options.FirstTabletIndex_);
    }
    if (options.LastTabletIndex_) {
        header.AddParameter("last_tablet_index", *options.LastTabletIndex_);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    const TString& serverName,
    const TCreateClientOptions& options)
{
    auto globalTxId = GetGuid(TConfig::Get()->GlobalTxId);

    TAuth auth;
    auth.ServerName = serverName;
    if (serverName.find('.') == TString::npos &&
        serverName.find(':') == TString::npos)
    {
        auth.ServerName += ".yt.yandex.net";
    }

    auth.Token = TConfig::Get()->Token;
    if (options.Token_) {
        auth.Token = options.Token_;
    } else if (options.TokenPath_) {
        auth.Token = TConfig::LoadTokenFromFile(options.TokenPath_);
    }
    TConfig::ValidateToken(auth.Token);

    return new NDetail::TClient(auth, globalTxId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
