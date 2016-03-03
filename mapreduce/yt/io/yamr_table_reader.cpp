#include "yamr_table_reader.h"

#include "proxy_input.h"

#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/http/requests.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TRetryException
    : public yexception
{ };

const i32 CONTROL_ATTR_TABLE_INDEX = -1;
const i32 CONTROL_ATTR_KEY_SWITCH = -2;
const i32 CONTROL_ATTR_ROW_INDEX = -4;

////////////////////////////////////////////////////////////////////////////////

TMaybe<TNode> GetTableFormat(
    const TAuth& auth,
    const TTransactionId& transactionId,
    const TRichYPath& path)
{
    auto formatPath = path.Path_ + "/@_format";
    if (!Exists(auth, transactionId, formatPath)) {
        return TMaybe<TNode>();
    }
    TMaybe<TNode> format = NodeFromYsonString(Get(auth, transactionId, formatPath));
    if (format.Get()->AsString() != "yamred_dsv") {
        ythrow yexception() <<
            "'yamred_dsv format expected, got " << format.Get()->AsString();
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
        TMaybe<TNode> format = GetTableFormat(auth, transactionId, AddPathPrefix(table));

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

TYaMRTableReader::TYaMRTableReader(THolder<TProxyInput> input)
    : Input_(std::move(input))
{
    Next();
}

TYaMRTableReader::~TYaMRTableReader()
{ }

void TYaMRTableReader::CheckValidity() const
{
    if (!IsValid()) {
        ythrow yexception() << "Iterator is not valid";
    }
}

size_t TYaMRTableReader::Load(void *buf, size_t len)
{
    size_t count = 0;
    bool hasError = false;
    yexception ex;

    try {
        count = Input_->Load(buf, len);
    } catch (yexception& e) {
        hasError = true;
        ex = e;
    }

    if (hasError) {
        if (Input_->OnStreamError(ex)) {
            throw TRetryException();
        } else {
            ythrow ex;
        }
    }

    return count;
}

template <class T>
bool TYaMRTableReader::ReadInteger(T* result, bool acceptEndOfStream)
{
    size_t count = Load(result, sizeof(T));
    if (acceptEndOfStream && count == 0) {
        Finished_ = true;
        Valid_ = false;
        return false;
    }
    if (count != sizeof(T)) {
        ythrow yexception() << "Premature end of YaMR stream";
    }
    return true;
}

void TYaMRTableReader::ReadField(Stroka* result, i32 length)
{
    result->resize(length);
    size_t count = Load(result->begin(), length);
    if (count != static_cast<size_t>(length)) {
        ythrow yexception() << "Premature end of YaMR stream";
    }
}

const TYaMRRow& TYaMRTableReader::GetRow() const
{
    CheckValidity();
    return Row_;
}

bool TYaMRTableReader::IsValid() const
{
    return Valid_;
}

void TYaMRTableReader::Next()
{
    CheckValidity();

    if (RowIndex_) {
        ++*RowIndex_;
    }

    while (true) {
        try {
            i32 value = 0;
            if (!ReadInteger(&value, true)) {
                return;
            }

            while (value < 0) {
                switch (value) {
                    case CONTROL_ATTR_TABLE_INDEX:
                        ReadInteger(&value);
                        TableIndex_ = static_cast<ui32>(value);
                        ReadInteger(&value);
                        break;

                    case CONTROL_ATTR_KEY_SWITCH:
                        Valid_ = false;
                        return;

                    case CONTROL_ATTR_ROW_INDEX: {
                        ui64 rowIndex = 0;
                        ReadInteger(&rowIndex);
                        RowIndex_ = rowIndex;
                        ReadInteger(&value);
                        break;
                    }
                    default:
                        ythrow yexception() <<
                            Sprintf("Invalid control integer %d in YaMR stream", value);
                }
            }

            ReadField(&Row_.Key, value);
            ReadInteger(&value);
            ReadField(&Row_.SubKey, value);
            ReadInteger(&value);
            ReadField(&Row_.Value, value);

        } catch (TRetryException& e) {
            continue;
        }
        break;
    }

    Input_->OnRowFetched();
}

void TYaMRTableReader::NextKey()
{
    while (Valid_) {
        Next();
    }

    if (Finished_) {
        return;
    }

    Valid_ = true;
}

ui32 TYaMRTableReader::GetTableIndex() const
{
    CheckValidity();
    return TableIndex_;
}

ui64 TYaMRTableReader::GetRowIndex() const
{
    CheckValidity();
    return RowIndex_.GetOrElse(0UL);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
