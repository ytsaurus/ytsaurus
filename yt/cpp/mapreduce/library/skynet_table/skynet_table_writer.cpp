#include "skynet_table_writer.h"
#include <yt/cpp/mapreduce/interface/finish_or_die.h>

namespace NYtSkynetTable {

using namespace NYT;

////////////////////////////////////////////////////////////////////////////////

ISkynetTableWriter::~ISkynetTableWriter() = default;

////////////////////////////////////////////////////////////////////////////////

template <typename TWriterPtr>
class TSkynetTableFileWriter
    : public IFileWriter
{
public:
    TSkynetTableFileWriter(TString fileName, TWriterPtr writer, size_t tableIndex)
        : TableIndex_(tableIndex)
        , Writer_(std::move(writer))
    {
        Row_.SetFileName(std::move(fileName));
        Row_.SetPartIndex(0);
        Row_.MutableData(); // Set data
    }

    ~TSkynetTableFileWriter()
    {
        NYT::NDetail::FinishOrDie(this, "TSkynetTableFileWriter");
    }

private:
    void DoWrite(const void* voidData, size_t len) override
    {
        Y_ENSURE(IsValid_, "Trying to Write after Finish");

        const auto* data = static_cast<const char*>(voidData);
        while (len > 0) {
            auto toSend = Min(PartSize_ - Row_.GetData().size(), len);
            Row_.MutableData()->append(data, toSend);
            if (Row_.GetData().size() == PartSize_) {
                Send();
            }
            data += toSend;
            len -= toSend;
        }
    }

    void DoFlush() override
    {
        // Do nothing
        //
        // N.B. Flush does not call Send.
        // We must call Send with 4MB (full PartSize) of data for all chunks maybe except the last one.
    }

    void DoFinish() override
    {
        if (IsValid_) {
            IsValid_ = false;
            if (!Row_.GetData().empty() || Row_.GetPartIndex() == 0) {
                Send();
            }
        }
    }

    void Send()
    {
        Writer_->AddRow(Row_, TableIndex_);
        Row_.MutableData()->clear();
        Row_.SetPartIndex(Row_.GetPartIndex() + 1);
    }

private:
    static constexpr size_t PartSize_ = 4 * 1024 * 1024;

    const size_t TableIndex_;
    const TWriterPtr Writer_;

    TSkynetTableRow Row_;
    bool IsValid_ = true;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TWriterPtr>
class TSkynetTableWriter
    : public ISkynetTableWriter
{
public:
    TSkynetTableWriter(TWriterPtr writer, size_t tableIndex)
        : Writer_(std::move(writer))
        , TableIndex_(tableIndex)
    { }

    ~TSkynetTableWriter()
    {
        NYT::NDetail::FinishOrDie(this, "TSkynetTableFileWriter");
    }

    NYT::IFileWriterPtr AppendFile(TString fileName) override
    {
        return MakeIntrusive<TSkynetTableFileWriter<TWriterPtr>>(fileName, Writer_, TableIndex_);
    }

    void Finish() override
    {
        Writer_->Finish();
    }

private:
    const TWriterPtr Writer_;
    const size_t TableIndex_;
};

////////////////////////////////////////////////////////////////////////////////

class TClientSkynetTableWriter
    : public TSkynetTableWriter<TTableWriterPtr<TSkynetTableRow>>
{
private:
    using TBase = TSkynetTableWriter<TTableWriterPtr<TSkynetTableRow>>;

public:
    TClientSkynetTableWriter(TTableWriterPtr<TSkynetTableRow> writer, ITransactionPtr transaction)
        : TBase(std::move(writer), 0)
        , Transaction_(std::move(transaction))
    { }

    ~TClientSkynetTableWriter()
    {
        if (!IsValid_) {
            return;
        }
        if (std::uncaught_exception()) {
            try {
                Transaction_->Abort();
            } catch (...) {
            }
        } else {
            NYT::NDetail::FinishOrDie(this, "TClientSkynetTableWriter");
        }
    }

    void Finish() override
    {
        if (IsValid_) {
            IsValid_ = false;
            TBase::Finish();
            Transaction_->Commit();
        }
    }

private:
    ITransactionPtr Transaction_;
    bool IsValid_ = true;
};

////////////////////////////////////////////////////////////////////////////////

NYT::TNodeId CreateSkynetTable(IClientBasePtr client, const TYPath& path, const TCreateOptions& options)
{
    TCreateOptions optionsCopy = options;
    TTableSchema schema;
    schema.AddColumn(TColumnSchema().Group("meta").Name("filename").Type(VT_STRING).SortOrder(SO_ASCENDING));
    schema.AddColumn(TColumnSchema().Group("meta").Name("part_index").Type(VT_INT64).SortOrder(SO_ASCENDING));
    schema.AddColumn(TColumnSchema().Group("data").Name("data").Type(VT_STRING));
    schema.AddColumn(TColumnSchema().Group("meta").Name("data_size").Type(VT_INT64));
    schema.AddColumn(TColumnSchema().Group("meta").Name("sha1").Type(VT_STRING));
    schema.AddColumn(TColumnSchema().Group("meta").Name("md5").Type(VT_STRING));

    auto& attributes = optionsCopy.Attributes_;
    if (!attributes) {
        attributes.ConstructInPlace();
    }
    const auto& updateAttribute = [&] (TStringBuf key, TNode value) {
        Y_ENSURE((*attributes)[key].IsUndefined(), "Attribute " << key << " must not be set in TCreateOptions");
        (*attributes)[key] = value;
    };

    updateAttribute("enable_skynet_sharing", true);
    updateAttribute("optimize_for", "scan");
    updateAttribute("schema", schema.ToNode());
    return client->Create(path, NT_TABLE, optionsCopy);
}

ISkynetTableWriterPtr CreateSkynetWriter(const NYT::IClientBasePtr& client, const NYT::TYPath& path)
{
    auto transaction = client->StartTransaction();
    CreateSkynetTable(transaction, path, TCreateOptions().IgnoreExisting(true));
    auto writer = transaction->CreateTableWriter<TSkynetTableRow>(path,
        TTableWriterOptions()
        .SingleHttpRequest(true));
    return MakeIntrusive<TClientSkynetTableWriter>(writer, transaction);
}

ISkynetTableWriterPtr CreateSkynetWriter(NYT::TTableWriter<TSkynetTableRow>* writer, size_t tableIndex)
{
    using TWriterPtr = TTableWriterPtr<TSkynetTableRow>;
    return MakeIntrusive<TSkynetTableWriter<TWriterPtr>>(writer, tableIndex);
}

ISkynetTableWriterPtr CreateSkynetWriter(TTableWriter<::google::protobuf::Message>* writer, size_t tableIndex)
{
    using TWriterPtr = TTableWriterPtr<::google::protobuf::Message>;
    return MakeIntrusive<TSkynetTableWriter<TWriterPtr>>(writer, tableIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYtSkynetTable
