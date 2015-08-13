#include "yamr_table_reader.h"

#include "proxy_input.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TRetryException
    : public yexception
{ };

const i32 CONTROL_ATTR_TABLE_INDEX = -1;
const i32 CONTROL_ATTR_KEY_SWITCH = -2;
const i32 CONTROL_ATTR_ROW_INDEX = -4;

////////////////////////////////////////////////////////////////////////////////

TYaMRTableReader::TYaMRTableReader(THolder<TProxyInput> input)
    : Input_(MoveArg(input))
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
    Next();
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
