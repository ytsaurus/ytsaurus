#include "yamr_table_reader.h"

#include "proxy_input.h"

#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/raw_client/raw_requests.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TMaybe<TNode> GetTableFormat(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TRichYPath& path)
{
    auto formatPath = path.Path_ + "/@_format";
    if (!NDetail::Exists(auth, transactionId, formatPath)) {
        return TMaybe<TNode>();
    }
    TMaybe<TNode> format = NDetail::Get(auth, transactionId, formatPath);
    if (format.Get()->AsString() != "yamred_dsv") {
        return TMaybe<TNode>();
    }
    auto& formatAttrs = format.Get()->Attributes();
    if (!formatAttrs.HasKey("key_column_names")) {
        ythrow yexception() <<
            "Table '" << path.Path_ << "': attribute 'key_column_names' is missing";
    }
    formatAttrs["has_subkey"] = "true";
    formatAttrs["lenval"] = "true";
    return format;
}

TMaybe<TNode> GetTableFormats(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const yvector<TRichYPath>& inputs)
{
    TMaybe<TNode> result;

    bool start = true;
    for (auto& table : inputs) {
        TMaybe<TNode> format = GetTableFormat(auth, transactionId, table);

        if (start) {
            result = format;
            start = false;
            continue;
        }

        if (result.Defined() != format.Defined()) {
            ythrow yexception() << "Different formats of input tables";
        }

        if (!result.Defined()) {
            continue;
        }

        auto& resultAttrs = result.Get()->Attributes();
        auto& formatAttrs = format.Get()->Attributes();

        if (resultAttrs["key_column_names"] != formatAttrs["key_column_names"]) {
            ythrow yexception() << "Different formats of input tables";
        }

        bool hasSubkeyColumns = resultAttrs.HasKey("subkey_column_names");
        if (hasSubkeyColumns != formatAttrs.HasKey("subkey_column_names")) {
            ythrow yexception() << "Different formats of input tables";
        }

        if (hasSubkeyColumns &&
            resultAttrs["subkey_column_names"] != formatAttrs["subkey_column_names"])
        {
            ythrow yexception() << "Different formats of input tables";
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TYaMRTableReader::TYaMRTableReader(::TIntrusivePtr<TProxyInput> input)
    : TLenvalTableReader(std::move(input))
{
    TLenvalTableReader::Next();
}

TYaMRTableReader::~TYaMRTableReader()
{ }

const TYaMRRow& TYaMRTableReader::GetRow() const
{
    CheckValidity();
    if (!RowTaken_) {
        const_cast<TYaMRTableReader*>(this)->ReadRow();
    }
    return Row_;
}

bool TYaMRTableReader::IsValid() const
{
    return Valid_;
}

void TYaMRTableReader::Next()
{
    TLenvalTableReader::Next();
}

void TYaMRTableReader::NextKey()
{
    TLenvalTableReader::NextKey();
}

ui32 TYaMRTableReader::GetTableIndex() const
{
    return TLenvalTableReader::GetTableIndex();
}

ui64 TYaMRTableReader::GetRowIndex() const
{
    return TLenvalTableReader::GetRowIndex();
}

void TYaMRTableReader::ReadField(TString* result, i32 length)
{
    result->resize(length);
    size_t count = Load(result->begin(), length);
    if (count != static_cast<size_t>(length)) {
        ythrow yexception() <<
            "Premature end of YaMR stream";
    }
}

void TYaMRTableReader::ReadRow()
{
    i32 value = static_cast<i32>(Length_);
    ReadField(&Key_, value);
    Row_.Key = Key_;

    ReadInteger(&value);
    ReadField(&SubKey_, value);
    Row_.SubKey = SubKey_;

    ReadInteger(&value);
    ReadField(&Value_, value);
    Row_.Value = Value_;

    RowTaken_ = true;
}

void TYaMRTableReader::SkipRow()
{
    i32 value = static_cast<i32>(Length_);
    Input_->Skip(value);

    ReadInteger(&value);
    Input_->Skip(value);

    ReadInteger(&value);
    Input_->Skip(value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
