#include "mock_client.h"

#include <util/generic/guid.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {
    TNodeId GetDefaultGuid() {
        TGUID guid;
        CreateGuid(&guid);
        return guid;
    }
} // namespace


TNodeId TMockClient::Create(const TYPath&, ENodeType, const TCreateOptions&) {
    return GetDefaultGuid();
}

void TMockClient::Remove(const TYPath&, const TRemoveOptions&) {
}

bool TMockClient::Exists(const TYPath&) {
    return true;
}

TNode TMockClient::Get(const TYPath&, const TGetOptions&) {
    return TNode();
}

void TMockClient::Set(const TYPath&, const TNode&) {
}

TNode::TList TMockClient::List(const TYPath&, const TListOptions&) {
    return TNode::TList();
}

TNodeId TMockClient::Copy(const TYPath&, const TYPath&, const TCopyOptions&) {
    return GetDefaultGuid();
}
TNodeId TMockClient::Move(const TYPath&, const TYPath&, const TMoveOptions&) {
    return GetDefaultGuid();
}
TNodeId TMockClient::Link(const TYPath&, const TYPath&, const TLinkOptions&) {
    return GetDefaultGuid();
}
void TMockClient::Concatenate(const yvector<TYPath>&, const TYPath&, const TConcatenateOptions&) {
}

IFileReaderPtr TMockClient::CreateFileReader(const TRichYPath&, const TFileReaderOptions&) {
    ythrow yexception() << "This operation is not supported by TMockClient";
}

IFileWriterPtr TMockClient::CreateFileWriter(const TRichYPath&, const TFileWriterOptions&) {
    ythrow yexception() << "This operation is not supported by TMockClient";
}

TIntrusivePtr<INodeReaderImpl> TMockClient::CreateNodeReader(const TRichYPath&, const TTableReaderOptions&) {
    ythrow yexception() << "This operation is not supported by TMockClient";
}

TIntrusivePtr<IYaMRReaderImpl> TMockClient::CreateYaMRReader(const TRichYPath&, const TTableReaderOptions&) {
    ythrow yexception() << "This operation is not supported by TMockClient";
}

TIntrusivePtr<IProtoReaderImpl> TMockClient::CreateProtoReader(const TRichYPath&, const TTableReaderOptions&) {
    ythrow yexception() << "This operation is not supported by TMockClient";
}

TIntrusivePtr<INodeWriterImpl> TMockClient::CreateNodeWriter(const TRichYPath&, const TTableWriterOptions&) {
    ythrow yexception() << "This operation is not supported by TMockClient";
}

TIntrusivePtr<IYaMRWriterImpl> TMockClient::CreateYaMRWriter(const TRichYPath&, const TTableWriterOptions&) {
    ythrow yexception() << "This operation is not supported by TMockClient";
}

TIntrusivePtr<IProtoWriterImpl> TMockClient::CreateProtoWriter(const TRichYPath&, const TTableWriterOptions&) {
    ythrow yexception() << "This operation is not supported by TMockClient";
}

TOperationId TMockClient::Sort(const TSortOperationSpec&, const TOperationOptions&) {
    return GetDefaultGuid();
}

TOperationId TMockClient::Merge(const TMergeOperationSpec&, const TOperationOptions&) {
    return GetDefaultGuid();
}

TOperationId TMockClient::Erase(const TEraseOperationSpec&, const TOperationOptions&) {
    return GetDefaultGuid();
}

void TMockClient::AbortOperation(const TOperationId&) {
}

void TMockClient::WaitForOperation(const TOperationId&) {
}

EOperationStatus TMockClient::CheckOperation(const TOperationId&) {
    return OS_COMPLETED;
}


TOperationId TMockClient::DoMap(const TMapOperationSpec&, IJob*, const TOperationOptions&) {
    return GetDefaultGuid();
}

TOperationId TMockClient::DoReduce(const TReduceOperationSpec&, IJob*, const TOperationOptions&) {
    return GetDefaultGuid();
}

TOperationId TMockClient::DoJoinReduce(const TJoinReduceOperationSpec&, IJob*, const TOperationOptions&) {
    return GetDefaultGuid();
}

TOperationId TMockClient::DoMapReduce(const TMapReduceOperationSpec&, IJob*, IJob*, IJob*, const TMultiFormatDesc&, const TMultiFormatDesc&, const TMultiFormatDesc&, const TMultiFormatDesc&, const TOperationOptions&) {
    return GetDefaultGuid();
}

ITransactionPtr TMockClient::StartTransaction(const TStartTransactionOptions&) {
    ythrow yexception() << "This operation is not supported by TMockClient";
}

ITransactionPtr TMockClient::AttachTransaction(const TTransactionId&) {
    ythrow yexception() << "This operation is not supported by TMockClient";
}

void TMockClient::InsertRows(const TYPath&, const TNode::TList&) {
}

void TMockClient::DeleteRows(const TYPath&, const TNode::TList&) {
}

TNode::TList TMockClient::LookupRows(const TYPath&, const TNode::TList&, const TLookupRowsOptions&) {
    return TNode::TList();
}

TNode::TList TMockClient::SelectRows(const Stroka&, const TSelectRowsOptions&) {
    return TNode::TList();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
