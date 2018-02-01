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

    class TMockTransaction
        : public ITransaction
        , public TMockClient
    {
    public:
        TMockTransaction()
            : TransactionId_(GetDefaultGuid())
        {
        }

        const TTransactionId& GetId() const override
        {
            return TransactionId_;
        }

        ILockPtr Lock(
            const TYPath&,
            ELockMode,
            const TLockOptions&) override
        {
            ythrow yexception() << "not implemented";
        }

        void AlterTable(
            const TYPath&,
            const TAlterTableOptions&) override
        { }

        void Commit() override {}
        void Abort() override {}

    private:
        TTransactionId TransactionId_;
    };

    class TMockFileReader
        : public IFileReader
    {
    protected:
        size_t DoRead(void*, size_t) override {
            return 0;
        }
    };

    class TMockFileWriter
        : public IFileWriter
    {
    public:
        ~TMockFileWriter() override {}

    protected:
        void DoWrite(const void*, size_t) override {}
        void DoFinish() override {}
    };

    class TMockRawTableReader
        : public TRawTableReader
    {
    public:
        virtual bool Retry(const TMaybe<ui32>&, const TMaybe<ui64>&) override {
            return false;
        }

        virtual bool HasRangeIndices() const override {
            return false;
        }

    protected:
        size_t DoRead(void*, size_t) override {
            return 0;
        }
    };

    class TMockRawTableWriter
        : public TRawTableWriter
    {
    public:
        ~TMockRawTableWriter() override {}
        void NotifyRowEnd() override {}

    protected:
        void DoWrite(const void*, size_t) override {}
        void DoFinish() override {}
    };

    class TMockNodeTableReader
        : public INodeReaderImpl
    {
    public:
        ~TMockNodeTableReader() override {}

        const TNode& GetRow() const override {
            return Default;
        }
        void MoveRow(TNode* result) override {
            *result = Default;
        }
        bool IsValid() const override {
            return false;
        }
        void Next() override {}
        ui32 GetTableIndex() const override {
            return 0;
        }
        ui64 GetRowIndex() const override {
            return 0;
        }
        void NextKey() override {}
    private:
        TNode Default;
    };


    class TMockNodeTableWriter
        : public INodeWriterImpl
    {
    public:
        ~TMockNodeTableWriter() override {}

        void AddRow(const TNode&, size_t) override {}
        size_t GetStreamCount() const override {
            return 0;
        }
        IOutputStream* GetStream(size_t) const override {
            return nullptr;
        }
    };

    class TMockProtoTableReader
        : public IProtoReaderImpl
    {
    public:
        ~TMockProtoTableReader() override {}

        void ReadRow(Message*) override {}
        bool IsValid() const override {
            return false;
        }
        void Next() override {}
        ui32 GetTableIndex() const override {
            return 0;
        }
        ui64 GetRowIndex() const override {
            return 0;
        }
        void NextKey() override {}
    };

    class TMockProtoTableWriter
        : public IProtoWriterImpl
    {
    public:
        ~TMockProtoTableWriter() override {}

        void AddRow(const Message&, size_t) override {}
        size_t GetStreamCount() const override {
            return 0;
        }
        IOutputStream* GetStream(size_t) const override {
            return nullptr;
        }
    };

    class TMockYaMRTableReader
        : public IYaMRReaderImpl
    {
    public:
        ~TMockYaMRTableReader() override {}

        const TYaMRRow& GetRow() const override {
            return Default;
        }
        void MoveRow(TYaMRRow* row) {
            *row = Default;
        }
        bool IsValid() const override {
            return false;
        }
        void Next() override {}
        ui32 GetTableIndex() const override {
            return 0;
        }
        ui64 GetRowIndex() const override {
            return 0;
        }
        void NextKey() override {}
    private:
        TYaMRRow Default;
    };


    class TMockYaMRTableWriter
        : public IYaMRWriterImpl
    {
    public:
        ~TMockYaMRTableWriter() override {}

        void AddRow(const TYaMRRow&, size_t) override {}
        size_t GetStreamCount() const override {
            return 0;
        }
        IOutputStream* GetStream(size_t) const override {
            return nullptr;
        }
    };

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

void TMockClient::Set(const TYPath&, const TNode&, const TSetOptions&) {
}

TNode::TListType TMockClient::List(const TYPath&, const TListOptions&) {
    return TNode::TListType();
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
void TMockClient::Concatenate(const TVector<TYPath>&, const TYPath&, const TConcatenateOptions&) {
}

TRichYPath TMockClient::CanonizeYPath(const TRichYPath&) {
    ythrow yexception() << "not implemented";
}

IFileReaderPtr TMockClient::CreateFileReader(const TRichYPath&, const TFileReaderOptions&) {
    return new TMockFileReader();
}

IFileWriterPtr TMockClient::CreateFileWriter(const TRichYPath&, const TFileWriterOptions&) {
    return new TMockFileWriter();
}

IFileReaderPtr TMockClient::CreateBlobTableReader(const TYPath&, const TKey&, const TBlobTableReaderOptions&) {
    ythrow yexception() << "not implemented";
}

TTableWriterPtr<::google::protobuf::Message> TMockClient::CreateTableWriter(const TRichYPath&, const ::google::protobuf::Descriptor&, const TTableWriterOptions&) {
    return new TTableWriter<::google::protobuf::Message>(new TMockProtoTableWriter());
}

TRawTableReaderPtr TMockClient::CreateRawReader(const TRichYPath&, const TFormat&, const TTableReaderOptions&) {
    return new TMockRawTableReader();
}

TRawTableWriterPtr TMockClient::CreateRawWriter(const TRichYPath&, const TFormat&, const TTableWriterOptions&) {
    return new TMockRawTableWriter();
}

::TIntrusivePtr<INodeReaderImpl> TMockClient::CreateNodeReader(const TRichYPath&, const TTableReaderOptions&) {
    return new TMockNodeTableReader();
}

::TIntrusivePtr<IYaMRReaderImpl> TMockClient::CreateYaMRReader(const TRichYPath&, const TTableReaderOptions&) {
    return new TMockYaMRTableReader();
}

::TIntrusivePtr<IProtoReaderImpl> TMockClient::CreateProtoReader(const TRichYPath&, const TTableReaderOptions&, const ::google::protobuf::Message*) {
    return new TMockProtoTableReader();
}

::TIntrusivePtr<INodeWriterImpl> TMockClient::CreateNodeWriter(const TRichYPath&, const TTableWriterOptions&) {
    return new TMockNodeTableWriter();
}

::TIntrusivePtr<IYaMRWriterImpl> TMockClient::CreateYaMRWriter(const TRichYPath&, const TTableWriterOptions&) {
    return new TMockYaMRTableWriter();
}

::TIntrusivePtr<IProtoWriterImpl> TMockClient::CreateProtoWriter(const TRichYPath&, const TTableWriterOptions&, const ::google::protobuf::Message*) {
    return new TMockProtoTableWriter();
}

IOperationPtr TMockClient::Sort(const TSortOperationSpec&, const TOperationOptions&) {
    return nullptr;
}

IOperationPtr TMockClient::Merge(const TMergeOperationSpec&, const TOperationOptions&) {
    return nullptr;
}

IOperationPtr TMockClient::Erase(const TEraseOperationSpec&, const TOperationOptions&) {
    return nullptr;
}

void TMockClient::AbortOperation(const TOperationId&) {
}

void TMockClient::CompleteOperation(const TOperationId&) {
}

void TMockClient::WaitForOperation(const TOperationId&) {
}

EOperationStatus TMockClient::CheckOperation(const TOperationId&) {
    return OS_COMPLETED;
}


IOperationPtr TMockClient::DoMap(const TMapOperationSpec&, IJob*, const TOperationOptions&) {
    return nullptr;
}

IOperationPtr TMockClient::RawMap(const TRawMapOperationSpec&, ::TIntrusivePtr<IRawJob>, const TOperationOptions&) {
    return nullptr;
}

IOperationPtr TMockClient::DoReduce(const TReduceOperationSpec&, IJob*, const TOperationOptions&) {
    return nullptr;
}

IOperationPtr TMockClient::RawReduce(const TRawReduceOperationSpec&, ::TIntrusivePtr<IRawJob>, const TOperationOptions&) {
    return nullptr;
}

IOperationPtr TMockClient::DoJoinReduce(const TJoinReduceOperationSpec&, IJob*, const TOperationOptions&) {
    return nullptr;
}

IOperationPtr TMockClient::RawJoinReduce(const TRawJoinReduceOperationSpec&, ::TIntrusivePtr<IRawJob>, const TOperationOptions&) {
    return nullptr;
}

IOperationPtr TMockClient::DoMapReduce(const TMapReduceOperationSpec&, IJob*, IJob*, IJob*, const TMultiFormatDesc&, const TMultiFormatDesc&, const TMultiFormatDesc&, const TMultiFormatDesc&, const TOperationOptions&) {
    return nullptr;
}

IOperationPtr TMockClient::RawMapReduce(const TRawMapReduceOperationSpec&, ::TIntrusivePtr<IRawJob>, ::TIntrusivePtr<IRawJob>, ::TIntrusivePtr<IRawJob>, const TOperationOptions&) {
    return nullptr;
}

ITransactionPtr TMockClient::StartTransaction(const TStartTransactionOptions&) {
    return new TMockTransaction();
}

ITransactionPtr TMockClient::AttachTransaction(const TTransactionId&) {
    return new TMockTransaction();
}

void TMockClient::AlterTable(const TYPath&, const TAlterTableOptions&) {
}

void TMockClient::MountTable(const TYPath&, const TMountTableOptions&) {
}

void TMockClient::UnmountTable(const TYPath&, const TUnmountTableOptions&) {
}

void TMockClient::RemountTable(const TYPath&, const TRemountTableOptions&) {
}

void TMockClient::FreezeTable(const TYPath&, const TFreezeTableOptions&) {
}

void TMockClient::UnfreezeTable(const TYPath&, const TUnfreezeTableOptions&) {
}

void TMockClient::ReshardTable(const TYPath&, const TVector<TKey>&, const TReshardTableOptions&) {
}

void TMockClient::ReshardTable(const TYPath&, i32, const TReshardTableOptions&) {
}

void TMockClient::InsertRows(const TYPath&, const TNode::TListType&, const TInsertRowsOptions&) {
}

void TMockClient::DeleteRows(const TYPath&, const TNode::TListType&, const TDeleteRowsOptions&) {
}

TNode::TListType TMockClient::LookupRows(const TYPath&, const TNode::TListType&, const TLookupRowsOptions&) {
    return TNode::TListType();
}

TNode::TListType TMockClient::SelectRows(const TString&, const TSelectRowsOptions&) {
    return TNode::TListType();
}

void TMockClient::EnableTableReplica(const TReplicaId&) {
}

void TMockClient::DisableTableReplica(const TReplicaId&) {
}

ui64 TMockClient::GenerateTimestamp() {
    return 0;
}

void TMockClient::AlterTableReplica(
    const TReplicaId& /*replicaId*/,
    const TAlterTableReplicaOptions& /*alterTableReplicaOptions*/)
{
    ythrow yexception() << "not implemented";
}

TBatchRequestPtr TMockClient::CreateBatchRequest() {
    return nullptr;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
