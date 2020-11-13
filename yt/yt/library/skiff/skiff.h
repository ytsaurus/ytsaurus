#pragma once

#include "public.h"

#include <yt/core/misc/zerocopy_output_writer.h>

#include <util/generic/buffer.h>

#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NYT::NSkiff {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
constexpr T EndOfSequenceTag()
{
    static_assert(std::is_integral<T>::value && std::is_unsigned<T>::value, "T must be unsigned integer");
    return T(-1);
}

////////////////////////////////////////////////////////////////////////////////

struct TInt128
{
    ui64 Low = 0;
    i64 High = 0;
};

bool operator==(TInt128 lhs, TInt128 rhs);
bool operator!=(TInt128 lhs, TInt128 rhs);

////////////////////////////////////////////////////////////////////////////////

class TUncheckedSkiffParser
{
public:
    explicit TUncheckedSkiffParser(IZeroCopyInput* stream);
    TUncheckedSkiffParser(const TSkiffSchemaPtr& schema, IZeroCopyInput* stream);

    i8 ParseInt8();
    i16 ParseInt16();
    i32 ParseInt32();
    i64 ParseInt64();

    ui8 ParseUint8();
    ui16 ParseUint16();
    ui32 ParseUint32();
    ui64 ParseUint64();

    TInt128 ParseInt128();

    double ParseDouble();

    bool ParseBoolean();

    TStringBuf ParseString32();

    TStringBuf ParseYson32();

    ui8 ParseVariant8Tag();
    ui16 ParseVariant16Tag();

    bool HasMoreData();

    void ValidateFinished();

    ui64 GetReadBytesCount();

private:
    const void* GetData(size_t size);
    const void* GetDataViaBuffer(size_t size);

    size_t RemainingBytes() const;
    void Advance(size_t size);
    void RefillBuffer();

    template <typename T>
    T ParseSimple();

private:
    IZeroCopyInput* const Underlying_;

    TBuffer Buffer_;
    ui64 ReadBytesCount_ = 0;
    char* Position_ = nullptr;
    char* End_ = nullptr;
    bool Exhausted_ = false;
};

////////////////////////////////////////////////////////////////////////////////

class TCheckedSkiffParser
{
public:
    TCheckedSkiffParser(const TSkiffSchemaPtr& schema, IZeroCopyInput* stream);
    ~TCheckedSkiffParser();

    i8 ParseInt8();
    i16 ParseInt16();
    i32 ParseInt32();
    i64 ParseInt64();

    ui8 ParseUint8();
    ui16 ParseUint16();
    ui32 ParseUint32();
    ui64 ParseUint64();

    TInt128 ParseInt128();

    double ParseDouble();

    bool ParseBoolean();

    TStringBuf ParseString32();

    TStringBuf ParseYson32();

    ui8 ParseVariant8Tag();
    ui16 ParseVariant16Tag();

    bool HasMoreData();

    void ValidateFinished();

    ui64 GetReadBytesCount();

private:
    TUncheckedSkiffParser Parser_;
    std::unique_ptr<TSkiffValidator> Validator_;
};

////////////////////////////////////////////////////////////////////

class TUncheckedSkiffWriter
{
public:
    explicit TUncheckedSkiffWriter(IZeroCopyOutput* underlying);
    TUncheckedSkiffWriter(const TSkiffSchemaPtr& schema, IZeroCopyOutput* underlying);

    ~TUncheckedSkiffWriter();

    void WriteDouble(double value);
    void WriteBoolean(bool value);

    void WriteInt8(i8 value);
    void WriteInt16(i16 value);
    void WriteInt32(i32 value);
    void WriteInt64(i64 value);

    void WriteUint8(ui8 value);
    void WriteUint16(ui16 value);
    void WriteUint32(ui32 value);
    void WriteUint64(ui64 value);

    void WriteInt128(TInt128 value);

    void WriteString32(TStringBuf value);

    void WriteYson32(TStringBuf value);

    void WriteVariant8Tag(ui8 tag);
    void WriteVariant16Tag(ui16 tag);

    void Flush();
    void Finish();

private:

    template <typename T>
    void WriteSimple(T data);

private:
    TZeroCopyOutputStreamWriter Underlying_;
};

////////////////////////////////////////////////////////////////////////////////

class TCheckedSkiffWriter
{
public:
    TCheckedSkiffWriter(const TSkiffSchemaPtr& schema, IZeroCopyOutput* underlying);

    ~TCheckedSkiffWriter();

    void WriteInt8(i8 value);
    void WriteInt16(i16 value);
    void WriteInt32(i32 value);
    void WriteInt64(i64 value);

    void WriteUint8(ui8 value);
    void WriteUint16(ui16 value);
    void WriteUint32(ui32 value);
    void WriteUint64(ui64 value);

    void WriteDouble(double value);
    void WriteBoolean(bool value);

    void WriteInt128(TInt128 value);

    void WriteString32(TStringBuf value);

    void WriteYson32(TStringBuf value);

    void WriteVariant8Tag(ui8 tag);
    void WriteVariant16Tag(ui16 tag);

    void Flush();
    void Finish();

private:
    TUncheckedSkiffWriter Writer_;
    std::unique_ptr<TSkiffValidator> Validator_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSkiff
