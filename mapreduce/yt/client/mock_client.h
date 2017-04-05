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

    void Set(const TYPath&, const TNode&) override;

    TNode::TList List(const TYPath&, const TListOptions&) override;

    TNodeId Copy(const TYPath&, const TYPath&, const TCopyOptions&) override;

    TNodeId Move(const TYPath&, const TYPath&, const TMoveOptions&) override;

    TNodeId Link(const TYPath&, const TYPath&, const TLinkOptions&) override;

    void Concatenate(const yvector<TYPath>&, const TYPath&, const TConcatenateOptions&) override;

    IFileReaderPtr CreateFileReader(const TRichYPath&, const TFileReaderOptions&) override;

    IFileWriterPtr CreateFileWriter(const TRichYPath&, const TFileWriterOptions&) override;

    TRawTableReaderPtr CreateRawReader(const TRichYPath& path, EDataStreamFormat format, const TTableReaderOptions& options, const Stroka& formatConfig) override;

    TRawTableWriterPtr CreateRawWriter(const TRichYPath& path, EDataStreamFormat format, const TTableWriterOptions& options, const Stroka& formatConfig) override;

    ::TIntrusivePtr<INodeReaderImpl> CreateNodeReader(const TRichYPath&, const TTableReaderOptions&) override;

    ::TIntrusivePtr<IYaMRReaderImpl> CreateYaMRReader(const TRichYPath&, const TTableReaderOptions&) override;

    ::TIntrusivePtr<IProtoReaderImpl> CreateProtoReader(const TRichYPath&, const TTableReaderOptions&, const ::google::protobuf::Message*) override;

    ::TIntrusivePtr<INodeWriterImpl> CreateNodeWriter(const TRichYPath&, const TTableWriterOptions&) override;

    ::TIntrusivePtr<IYaMRWriterImpl> CreateYaMRWriter(const TRichYPath&, const TTableWriterOptions&) override;

    ::TIntrusivePtr<IProtoWriterImpl> CreateProtoWriter(const TRichYPath&, const TTableWriterOptions&, const ::google::protobuf::Message*) override;

    TOperationId Sort(const TSortOperationSpec&, const TOperationOptions&) override;

    TOperationId Merge(const TMergeOperationSpec&, const TOperationOptions&) override;

    TOperationId Erase(const TEraseOperationSpec&, const TOperationOptions&) override;

    void AbortOperation(const TOperationId&) override;

    void WaitForOperation(const TOperationId&) override;

    EOperationStatus CheckOperation(const TOperationId&) override;

    TOperationId DoMap(const TMapOperationSpec&, IJob*, const TOperationOptions&) override;

    TOperationId DoReduce(const TReduceOperationSpec&, IJob*, const TOperationOptions&) override;

    TOperationId DoJoinReduce(const TJoinReduceOperationSpec&, IJob*, const TOperationOptions&) override;

    TOperationId DoMapReduce(const TMapReduceOperationSpec&, IJob*, IJob*, IJob*, const TMultiFormatDesc&, const TMultiFormatDesc&, const TMultiFormatDesc&, const TMultiFormatDesc&, const TOperationOptions&) override;

    ITransactionPtr StartTransaction(const TStartTransactionOptions&) override;

    ITransactionPtr AttachTransaction(const TTransactionId&) override;

    void AlterTable(const TYPath&, const TAlterTableOptions&) override;

    void MountTable(const TYPath&, const TMountTableOptions&) override;

    void UnmountTable(const TYPath&, const TUnmountTableOptions&) override;

    void RemountTable(const TYPath&, const TRemountTableOptions&) override;

    void ReshardTable(const TYPath&, const yvector<TKey>&, const TReshardTableOptions&) override;

    void ReshardTable(const TYPath&, i32, const TReshardTableOptions&) override;

    void InsertRows(const TYPath&, const TNode::TList&) override;

    void DeleteRows(const TYPath&, const TNode::TList&) override;

    TNode::TList LookupRows(const TYPath&, const TNode::TList&, const TLookupRowsOptions&) override;

    TNode::TList SelectRows(const Stroka&, const TSelectRowsOptions&) override;

    ui64 GenerateTimestamp() override;
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
