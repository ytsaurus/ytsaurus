#pragma once

#include <mapreduce/yt/interface/client.h>

#include <library/cpp/testing/gmock_in_unittest/gmock.h>

namespace NYT {
namespace NTesting {
    class TClientMock : public IClient {
    public:
        TClientMock();
        ~TClientMock();

        MOCK_METHOD3(Create, TNodeId(const TYPath&, ENodeType, const TCreateOptions&));
        MOCK_METHOD2(Remove, void(const TYPath&, const TRemoveOptions&));
        MOCK_METHOD1(Exists, bool(const TYPath&));
        MOCK_METHOD2(Get, TNode(const TYPath&, const TGetOptions&));
        MOCK_METHOD3(Set, void(const TYPath&, const TNode&, const TSetOptions&));
        MOCK_METHOD3(MultisetAttributes, void(const TYPath&, const TNode::TMapType&, const TMultisetAttributesOptions&));
        MOCK_METHOD2(List, TNode::TListType(const TYPath&, const TListOptions&));
        MOCK_METHOD3(Copy, TNodeId(const TYPath&, const TYPath&, const TCopyOptions&));
        MOCK_METHOD3(Move, TNodeId(const TYPath&, const TYPath&, const TMoveOptions&));
        MOCK_METHOD3(Link, TNodeId(const TYPath&, const TYPath&, const TLinkOptions&));
        MOCK_METHOD3(Concatenate, void(const TVector<TRichYPath>&, const TRichYPath&, const TConcatenateOptions&));
        MOCK_METHOD3(Concatenate, void(const TVector<TYPath>&, const TYPath&, const TConcatenateOptions&));
        MOCK_METHOD1(CanonizeYPath, TRichYPath(const TRichYPath&));
        MOCK_METHOD2(GetTableColumnarStatistics, TVector<TTableColumnarStatistics>(const TVector<TRichYPath>&, const TGetTableColumnarStatisticsOptions&));
        MOCK_METHOD0(CreateBatchRequest, TBatchRequestPtr());
        MOCK_METHOD3(GetTabletInfos, TVector<TTabletInfo>(const TYPath&, const TVector<int>&, const TGetTabletInfosOptions&));

        MOCK_METHOD2(CreateFileReader, IFileReaderPtr(const TRichYPath&, const TFileReaderOptions&));
        MOCK_METHOD2(CreateFileWriter, IFileWriterPtr(const TRichYPath&, const TFileWriterOptions&));
        MOCK_METHOD3(CreateBlobTableReader, IFileReaderPtr(const TYPath&, const TKey&, const TBlobTableReaderOptions&));
        MOCK_METHOD3(CreateTableWriter, TTableWriterPtr<::google::protobuf::Message>(const TRichYPath& path, const ::google::protobuf::Descriptor& descriptor, const TTableWriterOptions& options));
        MOCK_METHOD3(CreateRawReader, TRawTableReaderPtr(const TRichYPath& path, const TFormat& format, const TTableReaderOptions& options));
        MOCK_METHOD3(CreateRawWriter, TRawTableWriterPtr(const TRichYPath& path, const TFormat& format, const TTableWriterOptions& options));
        MOCK_METHOD2(CreateNodeReader, ::TIntrusivePtr<INodeReaderImpl>(const TRichYPath&, const TTableReaderOptions&));
        MOCK_METHOD2(CreateYaMRReader, ::TIntrusivePtr<IYaMRReaderImpl>(const TRichYPath&, const TTableReaderOptions&));
        MOCK_METHOD3(CreateProtoReader, ::TIntrusivePtr<IProtoReaderImpl>(const TRichYPath&, const TTableReaderOptions&, const ::google::protobuf::Message*));
        MOCK_METHOD2(CreateNodeWriter, ::TIntrusivePtr<INodeWriterImpl>(const TRichYPath&, const TTableWriterOptions&));
        MOCK_METHOD2(CreateYaMRWriter, ::TIntrusivePtr<IYaMRWriterImpl>(const TRichYPath&, const TTableWriterOptions&));
        MOCK_METHOD3(CreateProtoWriter, ::TIntrusivePtr<IProtoWriterImpl>(const TRichYPath&, const TTableWriterOptions&, const ::google::protobuf::Message*));

        MOCK_METHOD2(Sort, IOperationPtr(const TSortOperationSpec&, const TOperationOptions&));
        MOCK_METHOD2(Merge, IOperationPtr(const TMergeOperationSpec&, const TOperationOptions&));
        MOCK_METHOD2(Erase, IOperationPtr(const TEraseOperationSpec&, const TOperationOptions&));
        MOCK_METHOD2(RemoteCopy, IOperationPtr(const TRemoteCopyOperationSpec&, const TOperationOptions&));
        MOCK_METHOD1(AbortOperation, void(const TOperationId&));
        MOCK_METHOD1(CompleteOperation, void(const TOperationId&));
        MOCK_METHOD2(SuspendOperation, void(const TOperationId&, const TSuspendOperationOptions&));
        MOCK_METHOD2(ResumeOperation, void(const TOperationId&, const TResumeOperationOptions&));
        MOCK_METHOD1(WaitForOperation, void(const TOperationId&));
        MOCK_METHOD1(CheckOperation, EOperationBriefState(const TOperationId&));
        MOCK_METHOD3(DoMap, IOperationPtr(const TMapOperationSpec&, const IStructuredJob&, const TOperationOptions&));
        MOCK_METHOD3(RawMap, IOperationPtr(const TRawMapOperationSpec&, ::TIntrusivePtr<IRawJob>, const TOperationOptions&));
        MOCK_METHOD3(DoReduce, IOperationPtr(const TReduceOperationSpec&, const IStructuredJob&, const TOperationOptions&));
        MOCK_METHOD3(RawReduce, IOperationPtr(const TRawReduceOperationSpec&, ::TIntrusivePtr<IRawJob>, const TOperationOptions&));
        MOCK_METHOD3(DoJoinReduce, IOperationPtr(const TJoinReduceOperationSpec&, const IStructuredJob&, const TOperationOptions&));
        MOCK_METHOD3(RawJoinReduce, IOperationPtr(const TRawJoinReduceOperationSpec&, ::TIntrusivePtr<IRawJob>, const TOperationOptions&));
        MOCK_METHOD5(DoMapReduce, IOperationPtr(const TMapReduceOperationSpec&, const IStructuredJob*, const IStructuredJob*, const IStructuredJob&, const TOperationOptions&));
        MOCK_METHOD5(RawMapReduce, IOperationPtr(const TRawMapReduceOperationSpec&, ::TIntrusivePtr<IRawJob>, ::TIntrusivePtr<IRawJob>, ::TIntrusivePtr<IRawJob>, const TOperationOptions&));
        MOCK_METHOD2(RunVanilla, IOperationPtr(const TVanillaOperationSpec&, const TOperationOptions&));
        MOCK_METHOD1(AttachOperation, IOperationPtr(const TOperationId&));

        MOCK_METHOD1(StartTransaction, ITransactionPtr(const TStartTransactionOptions&));
        MOCK_METHOD2(AlterTable, void(const TYPath&, const TAlterTableOptions&));

        MOCK_METHOD2(AttachTransaction, ITransactionPtr(const TTransactionId&, const TAttachTransactionOptions&));
        MOCK_METHOD2(MountTable, void(const TYPath&, const TMountTableOptions&));
        MOCK_METHOD2(UnmountTable, void(const TYPath&, const TUnmountTableOptions&));
        MOCK_METHOD2(RemountTable, void(const TYPath&, const TRemountTableOptions&));
        MOCK_METHOD2(FreezeTable, void(const TYPath&, const TFreezeTableOptions&));
        MOCK_METHOD2(UnfreezeTable, void(const TYPath&, const TUnfreezeTableOptions&));
        MOCK_METHOD3(ReshardTable, void(const TYPath&, const TVector<TKey>&, const TReshardTableOptions&));
        MOCK_METHOD3(ReshardTable, void(const TYPath&, i64, const TReshardTableOptions&));
        MOCK_METHOD3(InsertRows, void(const TYPath&, const TNode::TListType&, const TInsertRowsOptions&));
        MOCK_METHOD3(DeleteRows, void(const TYPath&, const TNode::TListType&, const TDeleteRowsOptions&));
        MOCK_METHOD4(TrimRows, void(const TYPath&, i64, i64, const TTrimRowsOptions&));
        MOCK_METHOD3(LookupRows, TNode::TListType(const TYPath&, const TNode::TListType&, const TLookupRowsOptions&));
        MOCK_METHOD2(SelectRows, TNode::TListType(const TString&, const TSelectRowsOptions&));
        MOCK_METHOD0(GenerateTimestamp, ui64());
        MOCK_METHOD1(EnableTableReplica, void(const TReplicaId& replicaid));
        MOCK_METHOD1(DisableTableReplica, void(const TReplicaId& replicaid));
        MOCK_METHOD2(AlterTableReplica, void(const TReplicaId& replicaid, const TAlterTableReplicaOptions&));

        MOCK_METHOD0(WhoAmI, TAuthorizationInfo());

        MOCK_METHOD2(GetOperation, TOperationAttributes(const TOperationId&, const TGetOperationOptions&));
        MOCK_METHOD1(ListOperations, TListOperationsResult(const TListOperationsOptions&));
        MOCK_METHOD2(UpdateOperationParameters, void(const TOperationId&, const TUpdateOperationParametersOptions&));
        MOCK_METHOD3(GetJob, TJobAttributes(const TOperationId&, const TJobId&, const TGetJobOptions&));
        MOCK_METHOD2(ListJobs, TListJobsResult(const TOperationId&, const TListJobsOptions&));
        MOCK_METHOD2(GetJobInput, IFileReaderPtr(const TJobId&, const TGetJobInputOptions&));
        MOCK_METHOD3(GetJobFailContext, IFileReaderPtr(const TOperationId&, const TJobId&, const TGetJobFailContextOptions&));
        MOCK_METHOD3(GetJobStderr, IFileReaderPtr(const TOperationId&, const TJobId&, const TGetJobStderrOptions&));

        MOCK_METHOD2(SkyShareTable, TNode::TListType(const std::vector<TYPath>&, const TSkyShareTableOptions&));
        MOCK_METHOD3(GetFileFromCache, TMaybe<TYPath>(const TString& md5Signature, const TYPath& cachePath, const TGetFileFromCacheOptions&));
        MOCK_METHOD4(PutFileToCache, TYPath(const TYPath&, const TString& md5Signature, const TYPath& cachePath, const TPutFileToCacheOptions&));
        MOCK_METHOD4(CheckPermission, TCheckPermissionResponse(const TString&, EPermission, const TYPath&, const TCheckPermissionOptions&));

        MOCK_METHOD0(GetParentClient, IClientPtr());

        MOCK_METHOD4(CreateTable, TNodeId(const TYPath&, const ::google::protobuf::Descriptor&, const TKeyColumns&, const TCreateOptions&));
    };

    class TTransactionMock : public ITransaction {
    public:
        TTransactionMock();
        ~TTransactionMock();

        MOCK_METHOD3(Create, TNodeId(const TYPath&, ENodeType, const TCreateOptions&));
        MOCK_METHOD2(Remove, void(const TYPath&, const TRemoveOptions&));
        MOCK_METHOD1(Exists, bool(const TYPath&));
        MOCK_METHOD2(Get, TNode(const TYPath&, const TGetOptions&));
        MOCK_METHOD3(Set, void(const TYPath&, const TNode&, const TSetOptions&));
        MOCK_METHOD2(List, TNode::TListType(const TYPath&, const TListOptions&));
        MOCK_METHOD3(Copy, TNodeId(const TYPath&, const TYPath&, const TCopyOptions&));
        MOCK_METHOD3(Move, TNodeId(const TYPath&, const TYPath&, const TMoveOptions&));
        MOCK_METHOD3(Link, TNodeId(const TYPath&, const TYPath&, const TLinkOptions&));
        MOCK_METHOD3(Concatenate, void(const TVector<TYPath>&, const TYPath&, const TConcatenateOptions&));
        MOCK_METHOD1(CanonizeYPath, TRichYPath(const TRichYPath&));
        MOCK_METHOD2(GetTableColumnarStatistics, TVector<TTableColumnarStatistics>(const TVector<TRichYPath>&, const TGetTableColumnarStatisticsOptions&));
        MOCK_METHOD0(CreateBatchRequest, TBatchRequestPtr());

        MOCK_METHOD2(CreateFileReader, IFileReaderPtr(const TRichYPath&, const TFileReaderOptions&));
        MOCK_METHOD2(CreateFileWriter, IFileWriterPtr(const TRichYPath&, const TFileWriterOptions&));
        MOCK_METHOD3(CreateTableWriter, TTableWriterPtr<::google::protobuf::Message>(const TRichYPath& path, const ::google::protobuf::Descriptor& descriptor, const TTableWriterOptions& options));
        MOCK_METHOD3(CreateRawReader, TRawTableReaderPtr(const TRichYPath& path, const TFormat& format, const TTableReaderOptions& options));
        MOCK_METHOD3(CreateRawWriter, TRawTableWriterPtr(const TRichYPath& path, const TFormat& format, const TTableWriterOptions& options));
        MOCK_METHOD3(CreateBlobTableReader, IFileReaderPtr(const TYPath&, const TKey&, const TBlobTableReaderOptions&));
        MOCK_METHOD2(CreateNodeReader, ::TIntrusivePtr<INodeReaderImpl>(const TRichYPath&, const TTableReaderOptions&));
        MOCK_METHOD2(CreateYaMRReader, ::TIntrusivePtr<IYaMRReaderImpl>(const TRichYPath&, const TTableReaderOptions&));
        MOCK_METHOD3(CreateProtoReader, ::TIntrusivePtr<IProtoReaderImpl>(const TRichYPath&, const TTableReaderOptions&, const ::google::protobuf::Message*));
        MOCK_METHOD2(CreateNodeWriter, ::TIntrusivePtr<INodeWriterImpl>(const TRichYPath&, const TTableWriterOptions&));
        MOCK_METHOD2(CreateYaMRWriter, ::TIntrusivePtr<IYaMRWriterImpl>(const TRichYPath&, const TTableWriterOptions&));
        MOCK_METHOD3(CreateProtoWriter, ::TIntrusivePtr<IProtoWriterImpl>(const TRichYPath&, const TTableWriterOptions&, const ::google::protobuf::Message*));

        MOCK_METHOD2(Sort, IOperationPtr(const TSortOperationSpec&, const TOperationOptions&));
        MOCK_METHOD2(Merge, IOperationPtr(const TMergeOperationSpec&, const TOperationOptions&));
        MOCK_METHOD2(Erase, IOperationPtr(const TEraseOperationSpec&, const TOperationOptions&));
        MOCK_METHOD1(AbortOperation, void(const TOperationId&));
        MOCK_METHOD1(CompleteOperation, void(const TOperationId&));
        MOCK_METHOD1(WaitForOperation, void(const TOperationId&));
        MOCK_METHOD1(CheckOperation, EOperationBriefState(const TOperationId&));
        MOCK_METHOD3(DoMap, IOperationPtr(const TMapOperationSpec&, const IStructuredJob&, const TOperationOptions&));
        MOCK_METHOD3(RawMap, IOperationPtr(const TRawMapOperationSpec&, ::TIntrusivePtr<IRawJob>, const TOperationOptions&));
        MOCK_METHOD3(DoReduce, IOperationPtr(const TReduceOperationSpec&, const IStructuredJob&, const TOperationOptions&));
        MOCK_METHOD3(RawReduce, IOperationPtr(const TRawReduceOperationSpec&, ::TIntrusivePtr<IRawJob>, const TOperationOptions&));
        MOCK_METHOD3(DoJoinReduce, IOperationPtr(const TJoinReduceOperationSpec&, const IStructuredJob&, const TOperationOptions&));
        MOCK_METHOD3(RawJoinReduce, IOperationPtr(const TRawJoinReduceOperationSpec&, ::TIntrusivePtr<IRawJob>, const TOperationOptions&));
        MOCK_METHOD5(DoMapReduce, IOperationPtr(const TMapReduceOperationSpec&, const IStructuredJob*, const IStructuredJob*, const IStructuredJob&, const TOperationOptions&));
        MOCK_METHOD5(RawMapReduce, IOperationPtr(const TRawMapReduceOperationSpec&, ::TIntrusivePtr<IRawJob>, ::TIntrusivePtr<IRawJob>, ::TIntrusivePtr<IRawJob>, const TOperationOptions&));
        MOCK_METHOD2(RunVanilla, IOperationPtr(const TVanillaOperationSpec&, const TOperationOptions&));
        MOCK_METHOD1(AttachOperation, IOperationPtr(const TOperationId&));

        MOCK_METHOD1(StartTransaction, ITransactionPtr(const TStartTransactionOptions&));
        MOCK_METHOD2(AlterTable, void(const TYPath&, const TAlterTableOptions&));

        MOCK_CONST_METHOD0(GetId, const TTransactionId&());

        MOCK_METHOD3(Lock, ILockPtr(const TYPath& path, ELockMode mode, const TLockOptions& options));
        MOCK_METHOD2(Unlock, void(const TYPath& path, const TUnlockOptions& options));

        MOCK_METHOD0(Commit, void());
        MOCK_METHOD0(Abort, void());
        MOCK_METHOD0(Ping, void());

        MOCK_METHOD0(GetParentClient, IClientPtr());
    };

    class TLockMock : public ILock {
    public:
        MOCK_CONST_METHOD0(GetId, const TLockId&());
        MOCK_CONST_METHOD0(GetAcquiredFuture, const NThreading::TFuture<void>&());
    };

    class TOperationMock : public IOperation {
        MOCK_CONST_METHOD0(GetId, const TOperationId&());
    };

} // namespace NTesting
} // namespace NYT
