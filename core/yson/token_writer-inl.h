#pragma once

#ifndef TOKEN_WRITER_INL_H_
#error "Direct inclusion of this file is not allowed, include token_writer.h"
// For the sake of sane code completion.
#include "token_writer.h"
#endif

#include "detail.h"

#include <util/generic/typetraits.h>
#include <util/string/escape.h>

#include <yt/core/misc/varint.h>

#include <cctype>

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

void TUncheckedYsonTokenWriter::Flush()
{
    if (Y_LIKELY(RemainingBytes_ > 0)) {
        Stream_->Undo(RemainingBytes_);
        RemainingBytes_ = 0;
    }
}

void TUncheckedYsonTokenWriter::Refill()
{
    YT_ASSERT(RemainingBytes_ == 0);
    RemainingBytes_ = Stream_->Next(&Position_);
}

void TUncheckedYsonTokenWriter::Advance(size_t size)
{
    YT_ASSERT(size <= RemainingBytes_);
    Position_ += size;
    RemainingBytes_ -= size;
}

void TUncheckedYsonTokenWriter::DoWrite(const void* data, size_t size)
{
    if (RemainingBytes_ < size) {
        Flush();
        Stream_->Write(data, size);
        Refill();
        return;
    }
    memcpy(Position_, data, size);
    Advance(size);
}

template <typename T>
void TUncheckedYsonTokenWriter::WriteSimple(T value)
{
    DoWrite(&value, sizeof(value));
}

template <typename T>
void TUncheckedYsonTokenWriter::WriteVarInt(T value)
{
    if (Y_UNLIKELY(RemainingBytes_ < MaxVarInt64Size)) {
        Flush();
        if constexpr (std::is_same_v<T, ui64>) {
            WriteVarUint64(Stream_, value);
        } else if constexpr (std::is_same_v<T, i64>) {
            WriteVarInt64(Stream_, value);
        } else if constexpr (std::is_same_v<T, i32>) {
            WriteVarInt32(Stream_, value);
        } else {
            static_assert(TDependentFalse<T>::value);
        }
        Refill();
        return;
    }
    int written;
    if constexpr (std::is_same_v<T, ui64>) {
        written = WriteVarUint64(Position_, value);
    } else if constexpr (std::is_same_v<T, i64>) {
        written = WriteVarInt64(Position_, value);
    } else if constexpr (std::is_same_v<T, i32>) {
        written = WriteVarInt32(Position_, value);
    } else {
        static_assert(TDependentFalse<T>::value);
    }
    Advance(written);
}

void TUncheckedYsonTokenWriter::WriteBinaryBoolean(bool value)
{
    WriteSimple(value ? NDetail::TrueMarker : NDetail::FalseMarker);
}

void TUncheckedYsonTokenWriter::WriteBinaryInt64(i64 value)
{
    WriteSimple(NDetail::Int64Marker);
    WriteVarInt<i64>(value);
}

void TUncheckedYsonTokenWriter::WriteBinaryUint64(ui64 value)
{
    WriteSimple(NDetail::Uint64Marker);
    WriteVarInt<ui64>(value);
}

void TUncheckedYsonTokenWriter::WriteBinaryDouble(double value)
{
    WriteSimple(NDetail::DoubleMarker);
    WriteSimple(value);
}

void TUncheckedYsonTokenWriter::WriteBinaryString(TStringBuf value)
{
    WriteSimple(NDetail::StringMarker);
    WriteVarInt<i32>(value.length());
    DoWrite(value.begin(), value.size());
}

void TUncheckedYsonTokenWriter::WriteEntity()
{
    WriteSimple(NDetail::EntitySymbol);
}

void TUncheckedYsonTokenWriter::WriteBeginMap()
{
    WriteSimple(NDetail::BeginMapSymbol);
}

void TUncheckedYsonTokenWriter::WriteEndMap()
{
    WriteSimple(NDetail::EndMapSymbol);
}

void TUncheckedYsonTokenWriter::WriteBeginAttributes()
{
    WriteSimple(NDetail::BeginAttributesSymbol);
}

void TUncheckedYsonTokenWriter::WriteEndAttributes()
{
    WriteSimple(NDetail::EndAttributesSymbol);
}

void TUncheckedYsonTokenWriter::WriteBeginList()
{
    WriteSimple(NDetail::BeginListSymbol);
}

void TUncheckedYsonTokenWriter::WriteEndList()
{
    WriteSimple(NDetail::EndListSymbol);
}

void TUncheckedYsonTokenWriter::WriteItemSeparator()
{
    WriteSimple(NDetail::ItemSeparatorSymbol);
}

void TUncheckedYsonTokenWriter::WriteKeyValueSeparator()
{
    WriteSimple(NDetail::KeyValueSeparatorSymbol);
}

void TUncheckedYsonTokenWriter::WriteSpace(char value)
{
    YT_ASSERT(std::isspace(value));
    WriteSimple(value);
}

void TUncheckedYsonTokenWriter::Finish()
{
    Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
