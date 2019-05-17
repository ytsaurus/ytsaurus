#include "yamr_table_reader.h"

#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/raw_client/raw_requests.h>

////////////////////////////////////////////////////////////////////

static void CheckedSkip(IInputStream* input, size_t byteCount)
{
    size_t skipped = input->Skip(byteCount);
    Y_ENSURE(skipped == byteCount, "Premature end of YaMR stream");
}

////////////////////////////////////////////////////////////////////

namespace NYT {

using namespace NYT::NDetail::NRawClient;

////////////////////////////////////////////////////////////////////////////////

TMaybe<TNode> GetCommonTableFormat(
    const TVector<TMaybe<TNode>>& formats)
{
    TMaybe<TNode> result;
    bool start = true;
    for (auto& format : formats) {
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

        auto& resultAttrs = result.Get()->GetAttributes();
        auto& formatAttrs = format.Get()->GetAttributes();

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

TMaybe<TNode> GetTableFormat(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TRichYPath& path)
{
    auto formatPath = path.Path_ + "/@_format";
    if (!Exists(auth, transactionId, formatPath)) {
        return TMaybe<TNode>();
    }
    TMaybe<TNode> format = Get(auth, transactionId, formatPath);
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
    const TVector<TRichYPath>& inputs)
{
    TVector<TMaybe<TNode>> formats;
    for (auto& table : inputs) {
        formats.push_back(GetTableFormat(auth, transactionId, table));
    }

    return GetCommonTableFormat(formats);
}

////////////////////////////////////////////////////////////////////////////////

TYaMRTableReader::TYaMRTableReader(::TIntrusivePtr<TRawTableReader> input)
    : TLenvalTableReader(std::move(input))
{ }

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

ui32 TYaMRTableReader::GetRangeIndex() const
{
    return TLenvalTableReader::GetRangeIndex();
}

ui64 TYaMRTableReader::GetRowIndex() const
{
    return TLenvalTableReader::GetRowIndex();
}

TMaybe<size_t> TYaMRTableReader::GetReadByteCount() const
{
    return TLenvalTableReader::GetReadByteCount();
}

void TYaMRTableReader::ReadField(TString* result, i32 length)
{
    result->resize(length);
    size_t count = Input_.Load(result->begin(), length);
    Y_ENSURE(count == static_cast<size_t>(length), "Premature end of YaMR stream");
}

void TYaMRTableReader::ReadRow()
{
    while (true) {
        try {
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

            // We successfully parsed one more row from the stream,
            // so reset retry count to their initial value.
            Input_.ResetRetries();

            break;
        } catch (const yexception& ) {
            if (!TLenvalTableReader::Retry()) {
                throw;
            }
        }
    }
}

void TYaMRTableReader::SkipRow()
{
    while (true) {
        try {
            i32 value = static_cast<i32>(Length_);
            CheckedSkip(&Input_, value);

            ReadInteger(&value);
            CheckedSkip(&Input_, value);

            ReadInteger(&value);
            CheckedSkip(&Input_, value);
            break;
        } catch (const yexception& ) {
            if (!TLenvalTableReader::Retry()) {
                throw;
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
