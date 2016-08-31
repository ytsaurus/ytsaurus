#include "lenval_table_reader.h"

#include "proxy_input.h"

#include <mapreduce/yt/common/helpers.h>
#include <mapreduce/yt/http/requests.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TRetryException
    : public yexception
{ };

const i32 CONTROL_ATTR_TABLE_INDEX = -1;
const i32 CONTROL_ATTR_KEY_SWITCH  = -2;
const i32 CONTROL_ATTR_RANGE_INDEX = -3;
const i32 CONTROL_ATTR_ROW_INDEX   = -4;

////////////////////////////////////////////////////////////////////////////////

TLenvalTableReader::TLenvalTableReader(THolder<TProxyInput> input)
    : Input_(std::move(input))
{ }

TLenvalTableReader::~TLenvalTableReader()
{ }

void TLenvalTableReader::CheckValidity() const
{
    if (!IsValid()) {
        ythrow yexception() << "Iterator is not valid";
    }
}

size_t TLenvalTableReader::Load(void *buf, size_t len)
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
        if (Input_->OnStreamError(ex, !RowIndex_.Defined(),
            RangeIndex_.GetOrElse(0ul), RowIndex_.GetOrElse(0ull)))
        {
            RowIndex_.Clear();
            RangeIndex_.Clear();
            throw TRetryException();
        } else {
            ythrow ex;
        }
    }

    return count;
}

bool TLenvalTableReader::IsValid() const
{
    return Valid_;
}

void TLenvalTableReader::Next()
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

            TMaybe<ui64> rowIndex;
            TMaybe<ui32> rangeIndex;

            while (value < 0) {
                switch (value) {
                    case CONTROL_ATTR_KEY_SWITCH:
                        if (!AtStart_) {
                            Valid_ = false;
                            return;
                        }
                        break;

                    case CONTROL_ATTR_TABLE_INDEX: {
                        ui32 tmp = 0;
                        ReadInteger(&tmp);
                        TableIndex_ = tmp;
                        ReadInteger(&value);
                        break;
                    }
                    case CONTROL_ATTR_ROW_INDEX: {
                        ui64 tmp = 0;
                        ReadInteger(&tmp);
                        rowIndex = tmp;
                        ReadInteger(&value);
                        break;
                    }
                    case CONTROL_ATTR_RANGE_INDEX: {
                        ui32 tmp = 0;
                        ReadInteger(&tmp);
                        rangeIndex = tmp;
                        ReadInteger(&value);
                        break;
                    }
                    default:
                        ythrow yexception() <<
                            Sprintf("Invalid control integer %d in YaMR stream", value);
                }
            }

            if (rowIndex) {
                if (Input_->HasRangeIndices()) {
                    if (rangeIndex) {
                        RowIndex_ = rowIndex;
                        RangeIndex_ = rangeIndex;
                    }
                } else {
                    RowIndex_ = rowIndex;
                }
            }

            Length_ = static_cast<ui32>(value);
            OnRowStart();

        } catch (TRetryException& e) {
            continue;
        }
        break;
    }
}

void TLenvalTableReader::NextKey()
{
    while (Valid_) {
        Next();
    }

    if (Finished_) {
        return;
    }

    Valid_ = true;

    if (RowIndex_) {
        --*RowIndex_;
    }
}

ui32 TLenvalTableReader::GetTableIndex() const
{
    CheckValidity();
    return TableIndex_;
}

ui64 TLenvalTableReader::GetRowIndex() const
{
    CheckValidity();
    return RowIndex_.GetOrElse(0UL);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
