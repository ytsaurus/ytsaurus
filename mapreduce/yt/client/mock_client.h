#pragma once

#include <mapreduce/yt/interface/client.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TMockClient : public IClient {
public:
    TNodeId Create(const TYPath&, ENodeType, const TCreateOptions&) override final;

    void Remove(const TYPath&, const TRemoveOptions&) override final;

    bool Exists(const TYPath&) override final;

    TNode Get(const TYPath&, const TGetOptions&) override final;

    void Set(const TYPath&, const TNode&);

    TNode::TList List(const TYPath&, const TListOptions&);

    TNodeId Copy(const TYPath&, const TYPath&, const TCopyOptions&);

    TNodeId Move(const TYPath&, const TYPath&, const TMoveOptions&);

    TNodeId Link(const TYPath&, const TYPath&, const TLinkOptions&);

    void Concatenate(const yvector<TYPath>&, const TYPath&, const TConcatenateOptions&);

    IFileReaderPtr CreateFileReader(const TRichYPath&, const TFileReaderOptions&);

    IFileWriterPtr CreateFileWriter(const TRichYPath&, const TFileWriterOptions&);

    TIntrusivePtr<INodeReaderImpl> CreateNodeReader(const TRichYPath&, const TTableReaderOptions&);

    TIntrusivePtr<IYaMRReaderImpl> CreateYaMRReader(const TRichYPath&, const TTableReaderOptions&);

    TIntrusivePtr<IProtoReaderImpl> CreateProtoReader(const TRichYPath&, const TTableReaderOptions&, const ::google::protobuf::Message*);

    TIntrusivePtr<INodeWriterImpl> CreateNodeWriter(const TRichYPath&, const TTableWriterOptions&);

    TIntrusivePtr<IYaMRWriterImpl> CreateYaMRWriter(const TRichYPath&, const TTableWriterOptions&);

    TIntrusivePtr<IProtoWriterImpl> CreateProtoWriter(const TRichYPath&, const TTableWriterOptions&, const ::google::protobuf::Message*);

    TOperationId Sort(const TSortOperationSpec&, const TOperationOptions&);

    TOperationId Merge(const TMergeOperationSpec&, const TOperationOptions&);

    TOperationId Erase(const TEraseOperationSpec&, const TOperationOptions&);

    void AbortOperation(const TOperationId&);

    void WaitForOperation(const TOperationId&);

    EOperationStatus CheckOperation(const TOperationId&);

    TOperationId DoMap(const TMapOperationSpec&, IJob*, const TOperationOptions&);

    TOperationId DoReduce(const TReduceOperationSpec&, IJob*, const TOperationOptions&);

    TOperationId DoJoinReduce(const TJoinReduceOperationSpec&, IJob*, const TOperationOptions&);

    TOperationId DoMapReduce(const TMapReduceOperationSpec&, IJob*, IJob*, IJob*, const TMultiFormatDesc&, const TMultiFormatDesc&, const TMultiFormatDesc&, const TMultiFormatDesc&, const TOperationOptions&);

    ITransactionPtr StartTransaction(const TStartTransactionOptions&);

    ITransactionPtr AttachTransaction(const TTransactionId&);

    void AlterTable(const TYPath&, const TAlterTableOptions&);

    void MountTable(const TYPath&, const TMountTableOptions&);

    void UnmountTable(const TYPath&, const TUnmountTableOptions&);

    void RemountTable(const TYPath&, const TRemountTableOptions&);

    void InsertRows(const TYPath&, const TNode::TList&);

    void DeleteRows(const TYPath&, const TNode::TList&);

    TNode::TList LookupRows(const TYPath&, const TNode::TList&, const TLookupRowsOptions&);

    TNode::TList SelectRows(const Stroka&, const TSelectRowsOptions&);
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
