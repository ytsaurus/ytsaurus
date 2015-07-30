#include "yamr_table_reader.h"

#include "proxy_input.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TRetryException
    : public yexception
{ };

////////////////////////////////////////////////////////////////////////////////

TYaMRTableReader::TYaMRTableReader(THolder<TProxyInput> input)
    : Input_(MoveArg(input))
    , Valid_(true)
    , Finished_(false)
    , TableIndex_(0)
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

bool TYaMRTableReader::ReadInteger(i32* result, bool acceptEndOfStream)
{
    size_t count = Load(result, sizeof(i32));
    if (acceptEndOfStream && count == 0) {
        Finished_ = true;
        Valid_ = false;
        return false;
    }
    if (count != sizeof(i32)) {
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

    while (true) {
        try {
            i32 value = 0;
            if (!ReadInteger(&value, true)) {
                return;
            }

            while (value < 0) {
                switch (value) {
                    case -2:
                        Valid_ = false;
                        return;

                    case -1:
                        ReadInteger(&value);
                        TableIndex_ = static_cast<size_t>(value);
                        ReadInteger(&value);
                        break;

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

size_t TYaMRTableReader::GetTableIndex() const
{
    CheckValidity();
    return TableIndex_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
