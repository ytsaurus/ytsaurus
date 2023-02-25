#include "blob_table.h"

#include <yt/cpp/mapreduce/interface/serialize.h>

#include <util/generic/hash_set.h>

namespace NYtBlobTable {

using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

TTableSchema CreateYtSchema(const TBlobTableSchema& blobSchema)
{
    TTableSchema result;
    result.Columns(blobSchema.BlobIdColumns_);
    result.AddColumn(blobSchema.PartIndexColumnName_, VT_INT64);
    result.AddColumn(blobSchema.DataColumnName_, VT_STRING);
    return result;
}

static bool IsSortedSchema(const TNode& tableSchema, const TBlobTableSchema& blobTableSchema)
{
    if (!tableSchema.IsList()) {
        ythrow yexception() << "Corrupted schema unexpected type: " << tableSchema.GetType();
    }
    const auto blobIdColumnCount = blobTableSchema.BlobIdColumns_.size();
    if (blobIdColumnCount > tableSchema.AsList().size() + 1) {
        return false;
    }
    for (size_t i = 0; i != blobIdColumnCount; ++i) {
        if (tableSchema[i]["sort_order"] != "ascending") {
            return false;
        }
        if (tableSchema[i]["name"] != blobTableSchema.BlobIdColumns_[i].Name()) {
            return false;
        }
    }
    if (tableSchema[blobIdColumnCount]["sort_order"] != "ascending") {
        return false;
    }
    if (tableSchema[blobIdColumnCount]["name"] != blobTableSchema.PartIndexColumnName_) {
        return false;
    }
    return true;
}

static TTableSchema CreateSortedSchema(const TTableSchema& tableSchema, const TBlobTableSchema& blobTableSchema)
{
    TTableSchema result = tableSchema;
    result.UniqueKeys(true);
    result.Columns({});

    THashSet<TString> keyColumns;
    for (auto columnSchema : blobTableSchema.BlobIdColumns_) {
        result.AddColumn(columnSchema.SortOrder(SO_ASCENDING));
        keyColumns.emplace(columnSchema.Name());
    }

    keyColumns.emplace(blobTableSchema.PartIndexColumnName_);
    result.AddColumn(blobTableSchema.PartIndexColumnName_, VT_ANY, SO_ASCENDING);

    for (const auto& columnSchema : tableSchema.Columns()) {
        if (keyColumns.contains(columnSchema.Name())) {
            continue;
        }
        result.AddColumn(columnSchema);
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TBlobTableWriter
    : public IFileWriter
{
public:
    TBlobTableWriter(TTableWriterPtr<TNode> writer, TNode node, const TBlobTableSchema& schema)
        : Writer_(writer)
        , Node_(node)
        , DataColumnName_(schema.DataColumnName_)
        , PartIndexColumnName_(schema.PartIndexColumnName_)
        , PartSize_(schema.PartSize_)
    { }

    ~TBlobTableWriter()
    {
        try {
            if (!Finished_) {
                Writer_->Finish();
            }
        } catch (...) {
        }
    }

private:
    void DoWrite(const void* buf, size_t size) override
    {
        Y_VERIFY(!Finished_, "Writer is finished");
        while (size > 0) {
            Y_VERIFY(Data_.size() < PartSize_);
            size_t curSize = Min(size, PartSize_ - Data_.size());

            size_t oldSize = Data_.size();
            Data_.ReserveAndResize(Data_.size() + curSize);
            char* dest = Data_.Detach() + oldSize;
            memcpy(dest, buf, curSize);

            if (Data_.size() == PartSize_) {
                SendData();
            }

            buf = static_cast<const char*>(buf) + curSize;
            size -= curSize;
        }
    }

    void DoFinish() override
    {
        Y_VERIFY(!Finished_);
        Finished_ = true;
        SendData();
        Writer_->Finish();
    }

    void SendData()
    {
        Node_[PartIndexColumnName_] = TNode(PartIndex_++);

        auto& node = Node_[DataColumnName_];
        node = TNode(Data_);

        Writer_->AddRow(Node_);

        // We release reference counter for Data_, so we don't need to detach string for other operations.
        node = TNode();

        Data_.clear();
    }

private:
    TTableWriterPtr<TNode> Writer_;
    TNode Node_;
    TString Data_;
    const TString DataColumnName_;
    const TString PartIndexColumnName_;
    const size_t PartSize_;
    i64 PartIndex_ = 0;
    bool Finished_ = false;
};

////////////////////////////////////////////////////////////////////////////////

TBlobTable::TBlobTable(
    NYT::IClientBasePtr client,
    const NYT::TYPath& path,
    const TBlobTableSchema& blobTableSchema,
    const TBlobTableOpenOptions& openOptions)
    : Client_(client)
    , Path_(path)
    , Schema_(blobTableSchema)
{
    auto tx = Client_->StartTransaction();
    bool tableExists = tx->Exists(Path_);
    if (!tableExists) {
        if (!openOptions.Create_) {
            ythrow yexception() << "Table " << Path_ << " does not exist";
        } else {
            auto createOptions = openOptions.CreateOptions_;
            if (!createOptions.Attributes_) {
                createOptions.Attributes_.ConstructInPlace();
            }
            auto& attributes = createOptions.Attributes_.GetRef();
            attributes["schema"] = CreateYtSchema(Schema_).ToNode();
            Client_->Create(Path_, NT_TABLE, createOptions);
        }
        tx->Commit();
    } else {
        auto schema = Client_->Get(Path_ + "/@schema");
        tx->Abort();
    }
}

NYT::TYPath TBlobTable::GetPath() const
{
    return Path_;
}

bool TBlobTable::IsSorted() const
{
    auto schema = Client_->Get(Path_ + "/@schema");
    return IsSortedSchema(schema, Schema_);
}

NYT::IOperationPtr TBlobTable::Sort()
{
    auto op = SortAsync();
    auto doneFuture = op->Watch();
    doneFuture.Wait();
    doneFuture.GetValue();
    return op;
}

void TBlobTable::ResetSorting()
{
    TTableSchema tableSchema;
    auto schema = Client_->Get(Path_ + "/@schema");
    Deserialize(tableSchema, schema);

    tableSchema.UniqueKeys(false);
    for (auto& columnSchema : tableSchema.MutableColumns()) {
        columnSchema.ResetSortOrder();
    }

    Client_->AlterTable(Path_, TAlterTableOptions().Schema(tableSchema));
}

NYT::IOperationPtr TBlobTable::SortAsync()
{
    TSortColumns keyColumns;
    for (const auto& columnSchema : Schema_.BlobIdColumns_) {
        keyColumns.Parts_.push_back(columnSchema.Name());
    }
    keyColumns.Parts_.push_back(Schema_.PartIndexColumnName_);

    TTableSchema tableSchema;
    auto schema = Client_->Get(Path_ + "/@schema");
    Deserialize(tableSchema, schema);

    auto sortedSchema = CreateSortedSchema(tableSchema, Schema_);

    return Client_->Sort(TSortOperationSpec()
        .SortBy(keyColumns)
        .AddInput(Path_)
        .Output(TRichYPath(Path_).Schema(sortedSchema)),
        TOperationOptions().Wait(false));
}

IFileWriterPtr TBlobTable::AppendBlob(const TKey& key)
{
    if (key.Parts_.size() != Schema_.BlobIdColumns_.size()) {
        ythrow yexception() << "Wrong key size, expected: " << Schema_.BlobIdColumns_.size() << " actual: " << key.Parts_.size();
    }

    TNode keyNode = TNode::CreateMap();
    for (size_t i = 0; i != key.Parts_.size(); ++i) {
        keyNode[Schema_.BlobIdColumns_[i].Name()] = key.Parts_[i];
    }

    auto writer = Client_->CreateTableWriter<TNode>(TRichYPath(Path_).Append(true));
    return MakeIntrusive<TBlobTableWriter>(writer, keyNode, Schema_);
}

IFileReaderPtr TBlobTable::ReadBlob(const TKey& key)
{
    if (key.Parts_.size() != Schema_.BlobIdColumns_.size()) {
        ythrow yexception() << "Wrong key size, expected: " << Schema_.BlobIdColumns_.size() << " actual: " << key.Parts_.size();
    }

    return Client_->CreateBlobTableReader(
        Path_,
        key,
        TBlobTableReaderOptions()
            .PartSize(Schema_.PartSize_)
            .DataColumnName(Schema_.DataColumnName_)
            .PartIndexColumnName(Schema_.PartIndexColumnName_));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYtBlobTable
