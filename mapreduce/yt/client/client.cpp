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

#include <mapreduce/yt/client/retry_transaction.h>

#include <mapreduce/yt/interface/client.h>
#include <mapreduce/yt/interface/fluent.h>

#include <mapreduce/yt/interface/logging/log.h>

#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/common/config.h>
#include <mapreduce/yt/common/retry_lib.h>

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
#include <mapreduce/yt/io/ydl_table_reader.h>
#include <mapreduce/yt/io/ydl_table_writer.h>
#include <mapreduce/yt/io/proto_helpers.h>
#include <mapreduce/yt/io/skiff_table_reader.h>

#include <mapreduce/yt/raw_client/rpc_parameters_serialization.h>

#include <mapreduce/yt/library/table_schema/protobuf.h>

#include <library/cpp/json/json_reader.h>

#include <util/generic/algorithm.h>
#include <util/string/type.h>
#include <util/system/env.h>

#include <exception>

using namespace NYT::NDetail::NRawClient;

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

TClientBase::TClientBase(
    const TAuth& auth,
    const TTransactionId& transactionId,
    IClientRetryPolicyPtr retryPolicy)
    : Auth_(auth)
    , TransactionId_(transactionId)
    , ClientRetryPolicy_(std::move(retryPolicy))
{ }

ITransactionPtr TClientBase::StartTransaction(
    const TStartTransactionOptions& options)
{
    return MakeIntrusive<TTransaction>(GetParentClientImpl(), Auth_, TransactionId_, options);
}

TNodeId TClientBase::Create(
    const TYPath& path,
    ENodeType type,
    const TCreateOptions& options)
{
    return NRawClient::Create(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, TransactionId_, path, type, options);
}

void TClientBase::Remove(
    const TYPath& path,
    const TRemoveOptions& options)
{
    return NRawClient::Remove(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, TransactionId_, path, options);
}

bool TClientBase::Exists(
    const TYPath& path,
    const TExistsOptions& options)
{
    return NRawClient::Exists(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, TransactionId_, path, options);
}

TNode TClientBase::Get(
    const TYPath& path,
    const TGetOptions& options)
{
    return NRawClient::Get(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, TransactionId_, path, options);
}

void TClientBase::Set(
    const TYPath& path,
    const TNode& value,
    const TSetOptions& options)
{
    NRawClient::Set(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, TransactionId_, path, value, options);
}

void TClientBase::MultisetAttributes(
    const TYPath& path, const TNode::TMapType& value, const TMultisetAttributesOptions& options)
{
    NRawClient::MultisetAttributes(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, TransactionId_, path, value, options);
}


TNode::TListType TClientBase::List(
    const TYPath& path,
    const TListOptions& options)
{
    return NRawClient::List(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, TransactionId_, path, options);
}

TNodeId TClientBase::Copy(
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options)
{
    return NRawClient::Copy(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, TransactionId_, sourcePath, destinationPath, options);
}

TNodeId TClientBase::Move(
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options)
{
    return NRawClient::Move(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, TransactionId_, sourcePath, destinationPath, options);
}

TNodeId TClientBase::Link(
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options)
{
    return NRawClient::Link(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, TransactionId_, targetPath, linkPath, options);
}

void TClientBase::Concatenate(
    const TVector<TRichYPath>& sourcePaths,
    const TRichYPath& destinationPath,
    const TConcatenateOptions& options)
{
    std::function<void(ITransactionPtr)> lambda = [&sourcePaths, &destinationPath, &options, this](ITransactionPtr transaction) {
        if (!options.Append_ && !sourcePaths.empty() && !transaction->Exists(destinationPath.Path_)) {
            auto typeNode = transaction->Get(CanonizeYPath(sourcePaths.front()).Path_ + "/@type");
            auto type = FromString<ENodeType>(typeNode.AsString());
            transaction->Create(destinationPath.Path_, type, TCreateOptions().IgnoreExisting(true));
        }
        NRawClient::Concatenate(this->Auth_, transaction->GetId(), sourcePaths, destinationPath, options);
    };
    RetryTransactionWithPolicy(this, lambda, ClientRetryPolicy_->CreatePolicyForGenericRequest());
}

TRichYPath TClientBase::CanonizeYPath(const TRichYPath& path)
{
    return NRawClient::CanonizeYPath(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, path);
}

TVector<TTableColumnarStatistics> TClientBase::GetTableColumnarStatistics(
    const TVector<TRichYPath>& paths,
    const TGetTableColumnarStatisticsOptions& options)
{
    return NRawClient::GetTableColumnarStatistics(
        ClientRetryPolicy_->CreatePolicyForGenericRequest(),
        Auth_,
        TransactionId_,
        paths,
        options);
}

TMaybe<TYPath> TClientBase::GetFileFromCache(
    const TString& md5Signature,
    const TYPath& cachePath,
    const TGetFileFromCacheOptions& options)
{
    return NRawClient::GetFileFromCache(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, TransactionId_, md5Signature, cachePath, options);
}

TYPath TClientBase::PutFileToCache(
    const TYPath& filePath,
    const TString& md5Signature,
    const TYPath& cachePath,
    const TPutFileToCacheOptions& options)
{
    return NRawClient::PutFileToCache(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, TransactionId_, filePath, md5Signature, cachePath, options);
}

IFileReaderPtr TClientBase::CreateBlobTableReader(
    const TYPath& path,
    const TKey& key,
    const TBlobTableReaderOptions& options)
{
    return new TBlobTableReader(
        path,
        key,
        ClientRetryPolicy_,
        Auth_,
        TransactionId_,
        options);
}

IFileReaderPtr TClientBase::CreateFileReader(
    const TRichYPath& path,
    const TFileReaderOptions& options)
{
    return new TFileReader(
        CanonizeYPath(path),
        ClientRetryPolicy_,
        Auth_,
        TransactionId_,
        options);
}

IFileWriterPtr TClientBase::CreateFileWriter(
    const TRichYPath& path,
    const TFileWriterOptions& options)
{
    auto realPath = CanonizeYPath(path);
    if (!NRawClient::Exists(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, TransactionId_, realPath.Path_)) {
        NRawClient::Create(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, TransactionId_, realPath.Path_, NT_FILE,
            TCreateOptions().IgnoreExisting(true));
    }
    return new TFileWriter(realPath, ClientRetryPolicy_, Auth_, TransactionId_, options);
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
        ClientRetryPolicy_,
        Auth_,
        TransactionId_,
        GetWriteTableCommand(),
        format,
        CanonizeYPath(path),
        options).Get();
}

IOperationPtr TClientBase::DoMap(
    const TMapOperationSpec& spec,
    const IStructuredJob& mapper,
    const TOperationOptions& options)
{
    TOperationPreparer preparer(GetParentClientImpl(), TransactionId_);
    auto operationId = ExecuteMap(
        preparer,
        spec,
        mapper,
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClientImpl(), options);
}

IOperationPtr TClientBase::RawMap(
    const TRawMapOperationSpec& spec,
    ::TIntrusivePtr<IRawJob> mapper,
    const TOperationOptions& options)
{
    TOperationPreparer preparer(GetParentClientImpl(), TransactionId_);
    auto operationId = ExecuteRawMap(
        preparer,
        spec,
        *mapper,
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClientImpl(), options);
}

IOperationPtr TClientBase::DoReduce(
    const TReduceOperationSpec& spec,
    const IStructuredJob& reducer,
    const TOperationOptions& options)
{
    TOperationPreparer preparer(GetParentClientImpl(), TransactionId_);
    auto operationId = ExecuteReduce(
        preparer,
        spec,
        reducer,
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClientImpl(), options);
}

IOperationPtr TClientBase::RawReduce(
    const TRawReduceOperationSpec& spec,
    ::TIntrusivePtr<IRawJob> reducer,
    const TOperationOptions& options)
{
    TOperationPreparer preparer(GetParentClientImpl(), TransactionId_);
    auto operationId = ExecuteRawReduce(
        preparer,
        spec,
        *reducer,
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClientImpl(), options);
}

IOperationPtr TClientBase::DoJoinReduce(
    const TJoinReduceOperationSpec& spec,
    const IStructuredJob& reducer,
    const TOperationOptions& options)
{
    TOperationPreparer preparer(GetParentClientImpl(), TransactionId_);
    auto operationId = ExecuteJoinReduce(
        preparer,
        spec,
        reducer,
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClientImpl(), options);
}

IOperationPtr TClientBase::RawJoinReduce(
    const TRawJoinReduceOperationSpec& spec,
    ::TIntrusivePtr<IRawJob> reducer,
    const TOperationOptions& options)
{
    TOperationPreparer preparer(GetParentClientImpl(), TransactionId_);
    auto operationId = ExecuteRawJoinReduce(
        preparer,
        spec,
        *reducer,
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClientImpl(), options);
}

IOperationPtr TClientBase::DoMapReduce(
    const TMapReduceOperationSpec& spec,
    const IStructuredJob* mapper,
    const IStructuredJob* reduceCombiner,
    const IStructuredJob& reducer,
    const TOperationOptions& options)
{
    TOperationPreparer preparer(GetParentClientImpl(), TransactionId_);
    auto operationId = ExecuteMapReduce(
        preparer,
        spec,
        mapper,
        reduceCombiner,
        reducer,
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClientImpl(), options);
}

IOperationPtr TClientBase::RawMapReduce(
    const TRawMapReduceOperationSpec& spec,
    ::TIntrusivePtr<IRawJob> mapper,
    ::TIntrusivePtr<IRawJob> reduceCombiner,
    ::TIntrusivePtr<IRawJob> reducer,
    const TOperationOptions& options)
{
    TOperationPreparer preparer(GetParentClientImpl(), TransactionId_);
    auto operationId = ExecuteRawMapReduce(
        preparer,
        spec,
        mapper.Get(),
        reduceCombiner.Get(),
        *reducer,
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClientImpl(), options);
}

IOperationPtr TClientBase::Sort(
    const TSortOperationSpec& spec,
    const TOperationOptions& options)
{
    TOperationPreparer preparer(GetParentClientImpl(), TransactionId_);
    auto operationId = ExecuteSort(
        preparer,
        spec,
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClientImpl(), options);
}

IOperationPtr TClientBase::Merge(
    const TMergeOperationSpec& spec,
    const TOperationOptions& options)
{
    TOperationPreparer preparer(GetParentClientImpl(), TransactionId_);
    auto operationId = ExecuteMerge(
        preparer,
        spec,
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClientImpl(), options);
}

IOperationPtr TClientBase::Erase(
    const TEraseOperationSpec& spec,
    const TOperationOptions& options)
{
    TOperationPreparer preparer(GetParentClientImpl(), TransactionId_);
    auto operationId = ExecuteErase(
        preparer,
        spec,
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClientImpl(), options);
}

IOperationPtr TClientBase::RemoteCopy(
    const TRemoteCopyOperationSpec& spec,
    const TOperationOptions& options)
{
    TOperationPreparer preparer(GetParentClientImpl(), TransactionId_);
    auto operationId = ExecuteRemoteCopy(
        preparer,
        spec,
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClientImpl(), options);
}

IOperationPtr TClientBase::RunVanilla(
    const TVanillaOperationSpec& spec,
    const TOperationOptions& options)
{
    TOperationPreparer preparer(GetParentClientImpl(), TransactionId_);
    auto operationId = ExecuteVanilla(
        preparer,
        spec,
        options);
    return CreateOperationAndWaitIfRequired(operationId, GetParentClientImpl(), options);
}

IOperationPtr TClientBase::AttachOperation(const TOperationId& operationId)
{
    auto operation = ::MakeIntrusive<TOperation>(operationId, GetParentClientImpl());
    operation->GetBriefState(); // check that operation exists
    return operation;
}

EOperationBriefState TClientBase::CheckOperation(const TOperationId& operationId)
{
    return NYT::NDetail::CheckOperation(ClientRetryPolicy_, Auth_, operationId);
}

void TClientBase::AbortOperation(const TOperationId& operationId)
{
    NRawClient::AbortOperation(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, operationId);
}

void TClientBase::CompleteOperation(const TOperationId& operationId)
{
    NRawClient::CompleteOperation(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, operationId);
}

void TClientBase::WaitForOperation(const TOperationId& operationId)
{
    NYT::NDetail::WaitForOperation(ClientRetryPolicy_, Auth_, operationId);
}

void TClientBase::AlterTable(
    const TYPath& path,
    const TAlterTableOptions& options)
{
    NRawClient::AlterTable(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, TransactionId_, path, options);
}

::TIntrusivePtr<TClientReader> TClientBase::CreateClientReader(
    const TRichYPath& path,
    const TFormat& format,
    const TTableReaderOptions& options,
    bool useFormatFromTableAttributes)
{
    return ::MakeIntrusive<TClientReader>(
        CanonizeYPath(path),
        ClientRetryPolicy_,
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
    auto realPath = CanonizeYPath(path);
    if (!NRawClient::Exists(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, TransactionId_, realPath.Path_)) {
        NRawClient::Create(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, TransactionId_, realPath.Path_, NT_TABLE,
            TCreateOptions().IgnoreExisting(true));
    }
    return MakeHolder<TClientWriter>(
        realPath,
        ClientRetryPolicy_,
        Auth_,
        TransactionId_,
        format,
        options
    );
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

::TIntrusivePtr<IYdlReaderImpl> TClientBase::CreateYdlReader(
    const TRichYPath& path,
    const TTableReaderOptions& options,
    NTi::TTypePtr type)
{
    return MakeIntrusive<TNodeYdlTableReader>(
        CreateClientReader(path, TFormat::YsonBinary(), options),
        TVector({type->GetHash()}));
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
        auto format = TFormat::Protobuf({prototype->GetDescriptor()}, TConfig::Get()->ProtobufFormatWithDescriptors);
        return new TLenvalProtoTableReader(
            CreateClientReader(path, format, options),
            std::move(descriptors));
    }
}

::TIntrusivePtr<INodeWriterImpl> TClientBase::CreateNodeWriter(
    const TRichYPath& path, const TTableWriterOptions& options)
{
    auto format = TFormat::YsonBinary();
    ApplyFormatHints<TNode>(&format, options.FormatHints_);

    return new TNodeTableWriter(
        CreateClientWriter(path, format, options));
}

::TIntrusivePtr<IYaMRWriterImpl> TClientBase::CreateYaMRWriter(
    const TRichYPath& path, const TTableWriterOptions& options)
{
    auto format = TFormat::YaMRLenval();
    ApplyFormatHints<TYaMRRow>(&format, options.FormatHints_);

    return new TYaMRTableWriter(
        CreateClientWriter(path, format, options));
}

::TIntrusivePtr<IYdlWriterImpl> TClientBase::CreateYdlWriter(
    const TRichYPath& path,
    const TTableWriterOptions& options,
    NTi::TTypePtr type)
{
    auto pathWithSchema = path;
    if (!path.Schema_) {
        pathWithSchema.Schema(CreateTableSchema(type));
    }

    auto format = TFormat::YsonBinary();
    ApplyFormatHints<TNode>(&format, options.FormatHints_);
    return MakeIntrusive<TNodeYdlTableWriter>(
        CreateClientWriter(pathWithSchema, format, options),
        TVector({type->GetHash()}));
}

::TIntrusivePtr<IProtoWriterImpl> TClientBase::CreateProtoWriter(
    const TRichYPath& path,
    const TTableWriterOptions& options,
    const Message* prototype)
{
    TVector<const ::google::protobuf::Descriptor*> descriptors;
    descriptors.push_back(prototype->GetDescriptor());

    auto pathWithSchema = path;
    if (options.InferSchema_.GetOrElse(TConfig::Get()->InferTableSchema) && !path.Schema_) {
        pathWithSchema.Schema(CreateTableSchema(*prototype->GetDescriptor()));
    }

    if (TConfig::Get()->UseClientProtobuf) {
        auto format = TFormat::YsonBinary();
        ApplyFormatHints<TNode>(&format, options.FormatHints_);
        return new TProtoTableWriter(
            CreateClientWriter(pathWithSchema, format, options),
            std::move(descriptors));
    } else {
        auto format = TFormat::Protobuf({prototype->GetDescriptor()}, TConfig::Get()->ProtobufFormatWithDescriptors);
        ApplyFormatHints<::google::protobuf::Message>(&format, options.FormatHints_);
        return new TLenvalProtoTableWriter(
            CreateClientWriter(pathWithSchema, format, options),
            std::move(descriptors));
    }
}

TBatchRequestPtr TClientBase::CreateBatchRequest()
{
    return MakeIntrusive<TBatchRequest>(TransactionId_, GetParentClientImpl());
}

IClientPtr TClientBase::GetParentClient()
{
    return GetParentClientImpl();
}

const TAuth& TClientBase::GetAuth() const
{
    return Auth_;
}

const IClientRetryPolicyPtr& TClientBase::GetRetryPolicy() const
{
    return ClientRetryPolicy_;
}

////////////////////////////////////////////////////////////////////////////////

TTransaction::TTransaction(
    TClientPtr parentClient,
    const TAuth& auth,
    const TTransactionId& parentTransactionId,
    const TStartTransactionOptions& options)
    : TClientBase(auth, parentTransactionId, parentClient->GetRetryPolicy())
    , PingableTx_(
        MakeHolder<TPingableTransaction>(
            parentClient->GetRetryPolicy(),
            auth,
            parentTransactionId,
            options))
    , ParentClient_(parentClient)
{
    TransactionId_ = PingableTx_->GetId();
}

TTransaction::TTransaction(
    TClientPtr parentClient,
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TAttachTransactionOptions& options)
    : TClientBase(auth, transactionId, parentClient->GetRetryPolicy())
    , PingableTx_(
        new TPingableTransaction(
            parentClient->GetRetryPolicy(),
            auth,
            transactionId,
            options))
    , ParentClient_(parentClient)
{ }

const TTransactionId& TTransaction::GetId() const
{
    return TransactionId_;
}

ILockPtr TTransaction::Lock(
    const TYPath& path,
    ELockMode mode,
    const TLockOptions& options)
{
    auto lockId = NRawClient::Lock(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, TransactionId_, path, mode, options);
    return ::MakeIntrusive<TLock>(lockId, GetParentClientImpl(), options.Waitable_);
}

void TTransaction::Unlock(
    const TYPath& path,
    const TUnlockOptions& options)
{
    NRawClient::Unlock(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, TransactionId_, path, options);
}

void TTransaction::Commit()
{
    PingableTx_->Commit();
}

void TTransaction::Abort()
{
    PingableTx_->Abort();
}

void TTransaction::Ping()
{
    PingTx(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, TransactionId_);
}

void TTransaction::Detach()
{
    PingableTx_->Detach();
}

TClientPtr TTransaction::GetParentClientImpl()
{
    return ParentClient_;
}

////////////////////////////////////////////////////////////////////////////////

TClient::TClient(
    const TAuth& auth,
    const TTransactionId& globalId,
    IClientRetryPolicyPtr retryPolicy)
    : TClientBase(auth, globalId, retryPolicy)
{ }

TClient::~TClient() = default;

ITransactionPtr TClient::AttachTransaction(
    const TTransactionId& transactionId,
    const TAttachTransactionOptions& options)
{
    return MakeIntrusive<TTransaction>(this, Auth_, transactionId, options);
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
    RetryRequestWithPolicy(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, header);
}

void TClient::UnmountTable(
    const TYPath& path,
    const TUnmountTableOptions& options)
{
    THttpHeader header("POST", "unmount_table");
    SetTabletParams(header, path, options);
    header.AddParameter("force", options.Force_);
    RetryRequestWithPolicy(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, header);
}

void TClient::RemountTable(
    const TYPath& path,
    const TRemountTableOptions& options)
{
    THttpHeader header("POST", "remount_table");
    SetTabletParams(header, path, options);
    RetryRequestWithPolicy(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, header);
}

void TClient::FreezeTable(
    const TYPath& path,
    const TFreezeTableOptions& options)
{
    NRawClient::FreezeTable(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, path, options);
}

void TClient::UnfreezeTable(
    const TYPath& path,
    const TUnfreezeTableOptions& options)
{
    NRawClient::UnfreezeTable(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, path, options);
}

void TClient::ReshardTable(
    const TYPath& path,
    const TVector<TKey>& keys,
    const TReshardTableOptions& options)
{
    THttpHeader header("POST", "reshard_table");
    SetTabletParams(header, path, options);
    header.AddParameter("pivot_keys", BuildYsonNodeFluently().List(keys));
    RetryRequestWithPolicy(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, header);
}

void TClient::ReshardTable(
    const TYPath& path,
    i64 tabletCount,
    const TReshardTableOptions& options)
{
    THttpHeader header("POST", "reshard_table");
    SetTabletParams(header, path, options);
    header.AddParameter("tablet_count", tabletCount);
    RetryRequestWithPolicy(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, header);
}

void TClient::InsertRows(
    const TYPath& path,
    const TNode::TListType& rows,
    const TInsertRowsOptions& options)
{
    THttpHeader header("PUT", "insert_rows");
    header.SetInputFormat(TFormat::YsonBinary());
    // TODO: use corresponding raw request
    header.MergeParameters(SerializeParametersForInsertRows(path, options));

    auto body = NodeListToYsonString(rows);
    TRequestConfig config;
    config.IsHeavy = true;
    auto requestResult = RetryRequestWithPolicy(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, header, body, config);
}

void TClient::DeleteRows(
    const TYPath& path,
    const TNode::TListType& keys,
    const TDeleteRowsOptions& options)
{
    return NRawClient::DeleteRows(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, path, keys, options);
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
    // TODO: use corresponding raw request
    header.MergeParameters(NRawClient::SerializeParametersForTrimRows(path, options));

    TRequestConfig config;
    config.IsHeavy = true;
    RetryRequestWithPolicy(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, header, {}, config);
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
        .DoIf(options.Versioned_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("versioned").Value(*options.Versioned_);
        })
        .DoIf(options.Columns_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("column_names").Value(*options.Columns_);
        })
    .EndMap());

    auto body = NodeListToYsonString(keys);
    TRequestConfig config;
    config.IsHeavy = true;
    auto result = RetryRequestWithPolicy(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, header, body, config);
    return NodeFromYsonString(result.Response, YT_LIST_FRAGMENT).AsList();
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

    TRequestConfig config;
    config.IsHeavy = true;
    auto result = RetryRequestWithPolicy(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, header, {}, config);
    return NodeFromYsonString(result.Response, YT_LIST_FRAGMENT).AsList();
}

void TClient::AlterTableReplica(const TReplicaId& replicaId, const TAlterTableReplicaOptions& options)
{
    NRawClient::AlterTableReplica(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, replicaId, options);
}

ui64 TClient::GenerateTimestamp()
{
    THttpHeader header("GET", "generate_timestamp");
    TRequestConfig config;
    config.IsHeavy = true;
    auto requestResult = RetryRequestWithPolicy(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, header, {}, config);
    return NodeFromYsonString(requestResult.Response).AsUint64();
}

TAuthorizationInfo TClient::WhoAmI()
{
    THttpHeader header("GET", "auth/whoami", /* isApi = */ false);
    auto requestResult = RetryRequestWithPolicy(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, header);
    TAuthorizationInfo result;

    NJson::TJsonValue jsonValue;
    bool ok = NJson::ReadJsonTree(requestResult.Response, &jsonValue, /* throwOnError = */ true);
    Y_VERIFY(ok);
    result.Login = jsonValue["login"].GetString();
    result.Realm = jsonValue["realm"].GetString();
    return result;
}

TOperationAttributes TClient::GetOperation(
    const TOperationId& operationId,
    const TGetOperationOptions& options)
{
    return NRawClient::GetOperation(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, operationId, options);
}

TListOperationsResult TClient::ListOperations(
    const TListOperationsOptions& options)
{
    return NRawClient::ListOperations(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, options);
}

void TClient::UpdateOperationParameters(
    const TOperationId& operationId,
    const TUpdateOperationParametersOptions& options)
{
    return NRawClient::UpdateOperationParameters(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, operationId, options);
}

TJobAttributes TClient::GetJob(
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobOptions& options)
{
    return NRawClient::GetJob(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, operationId, jobId, options);
}

TListJobsResult TClient::ListJobs(
    const TOperationId& operationId,
    const TListJobsOptions& options)
{
    return NRawClient::ListJobs(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, operationId, options);
}

IFileReaderPtr TClient::GetJobInput(
    const TJobId& jobId,
    const TGetJobInputOptions& options)
{
    return NRawClient::GetJobInput(Auth_, jobId, options);
}

IFileReaderPtr TClient::GetJobFailContext(
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobFailContextOptions& options)
{
    return NRawClient::GetJobFailContext(Auth_, operationId, jobId, options);
}

IFileReaderPtr TClient::GetJobStderr(
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobStderrOptions& options)
{
    return NRawClient::GetJobStderr(Auth_, operationId, jobId, options);
}

TNode::TListType TClient::SkyShareTable(
    const std::vector<TYPath>& tablePaths,
    const TSkyShareTableOptions& options)
{
    return NRawClient::SkyShareTable(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, tablePaths, options);
}

TCheckPermissionResponse TClient::CheckPermission(
    const TString& user,
    EPermission permission,
    const TYPath& path,
    const TCheckPermissionOptions& options)
{
    return NRawClient::CheckPermission(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, user, permission, path, options);
}

TVector<TTabletInfo> TClient::GetTabletInfos(
    const TYPath& path,
    const TVector<int>& tabletIndexes,
    const TGetTabletInfosOptions& options)
{
    return NRawClient::GetTabletInfos(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, path, tabletIndexes, options);
}


void TClient::SuspendOperation(
    const TOperationId& operationId,
    const TSuspendOperationOptions& options)
{
    NRawClient::SuspendOperation(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, operationId, options);
}

void TClient::ResumeOperation(
    const TOperationId& operationId,
    const TResumeOperationOptions& options)
{
    NRawClient::ResumeOperation(ClientRetryPolicy_->CreatePolicyForGenericRequest(), Auth_, operationId, options);
}

TYtPoller& TClient::GetYtPoller()
{
    auto g = Guard(YtPollerLock_);
    if (!YtPoller_) {
        // We don't use current client and create new client because YtPoller_ might use
        // this client during current client shutdown.
        // That might lead to incrementing of current client refcount and double delete of current client object.
        YtPoller_ = MakeHolder<TYtPoller>(Auth_, ClientRetryPolicy_);
    }
    return *YtPoller_;
}

TClientPtr TClient::GetParentClientImpl()
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

    auto retryConfigProvider = options.RetryConfigProvider_;
    if (!retryConfigProvider) {
        retryConfigProvider = CreateDefaultRetryConfigProvider();
    }
    return new NDetail::TClient(auth, globalTxId, CreateDefaultClientRetryPolicy(retryConfigProvider));
}

IClientPtr CreateClientFromEnv(const TCreateClientOptions& options)
{
    auto serverName = GetEnv("YT_PROXY");
    if (!serverName) {
        ythrow yexception() << "YT_PROXY is not set";
    }

    return CreateClient(serverName, options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
