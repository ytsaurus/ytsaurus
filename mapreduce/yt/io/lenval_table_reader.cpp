#include "lenval_table_reader.h"

#include <mapreduce/yt/common/helpers.h>

#include <mapreduce/yt/interface/logging/log.h>

#include <util/string/printf.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

const i32 CONTROL_ATTR_TABLE_INDEX = -1;
const i32 CONTROL_ATTR_KEY_SWITCH  = -2;
const i32 CONTROL_ATTR_RANGE_INDEX = -3;
const i32 CONTROL_ATTR_ROW_INDEX   = -4;

////////////////////////////////////////////////////////////////////////////////

TLenvalTableReader::TLenvalTableReader(::TIntrusivePtr<TRawTableReader> input)
    : Input_(std::move(input))
{
    TLenvalTableReader::Next();
}

TLenvalTableReader::~TLenvalTableReader()
{ }

void TLenvalTableReader::CheckValidity() const
{
    if (!IsValid()) {
        ythrow yexception() << "Iterator is not valid";
    }
}

bool TLenvalTableReader::IsValid() const
{
    return Valid_;
}

void TLenvalTableReader::Next()
{
    if (!RowTaken_) {
        SkipRow();
    }

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
                    case CONTROL_ATTR_KEY_SWITCH:
                        if (!AtStart_) {
                            Valid_ = false;
                            return;
                        } else {
                            ReadInteger(&value);
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
                        RowIndex_ = tmp;
                        ReadInteger(&value);
                        break;
                    }
                    case CONTROL_ATTR_RANGE_INDEX: {
                        ui32 tmp = 0;
                        ReadInteger(&tmp);
                        RangeIndex_ = tmp;
                        ReadInteger(&value);
                        break;
                    }
                    default:
                        ythrow yexception() <<
                            Sprintf("Invalid control integer %d in YaMR stream", value);
                }
            }

            Length_ = static_cast<ui32>(value);
            RowTaken_ = false;
            AtStart_ = false;
        } catch (const yexception& e) {
            if (!PrepareRetry()) {
                throw;
            }
            continue;
        }
        break;
    }
}

bool TLenvalTableReader::Retry()
{
    if (PrepareRetry()) {
        RowTaken_ = true;
        Next();
        return true;
    }
    return false;
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

    RowTaken_ = true;
}

ui32 TLenvalTableReader::GetTableIndex() const
{
    CheckValidity();
    return TableIndex_;
}

ui32 TLenvalTableReader::GetRangeIndex() const
{
    CheckValidity();
    return RangeIndex_.GetOrElse(0);
}

ui64 TLenvalTableReader::GetRowIndex() const
{
    CheckValidity();
    return RowIndex_.GetOrElse(0UL);
}

TMaybe<size_t> TLenvalTableReader::GetReadByteCount() const
{
    return Input_.GetReadByteCount();
}

bool TLenvalTableReader::PrepareRetry()
{
    if (Input_.Retry(RangeIndex_, RowIndex_)) {
        RowIndex_.Clear();
        RangeIndex_.Clear();
        return true;
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
