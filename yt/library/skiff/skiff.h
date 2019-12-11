#pragma once

#include "public.h"

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

class TUncheckedSkiffParser
{
public:
    explicit TUncheckedSkiffParser(IZeroCopyInput* stream);
    TUncheckedSkiffParser(const TSkiffSchemaPtr& schema, IZeroCopyInput* stream);

    i64 ParseInt64();
    ui64 ParseUint64();
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

    i64 ParseInt64();
    ui64 ParseUint64();
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
    : private IOutputStream
{
public:
    explicit TUncheckedSkiffWriter(IOutputStream* underlying);
    TUncheckedSkiffWriter(const TSkiffSchemaPtr& schema, IOutputStream* underlying);

    ~TUncheckedSkiffWriter();

    void WriteDouble(double value);
    void WriteBoolean(bool value);

    void WriteInt64(i64 value);
    void WriteUint64(ui64 value);

    void WriteString32(TStringBuf value);

    void WriteYson32(TStringBuf value);

    void WriteVariant8Tag(ui8 tag);
    void WriteVariant16Tag(ui16 tag);

    using IOutputStream::Flush;
    void Finish();

private:
    void DoWrite(const void* data, size_t size) override final;
    void DoFlush() override final;

    template <typename T>
    void WriteSimple(T data);

private:
    IOutputStream* const Underlying_;
    TBuffer Buffer_;
    size_t RemainingBytes_;
    char* Position_;
};

////////////////////////////////////////////////////////////////////////////////

class TCheckedSkiffWriter
{
public:
    TCheckedSkiffWriter(const TSkiffSchemaPtr& schema, IOutputStream* underlying);

    ~TCheckedSkiffWriter();

    void WriteInt64(i64 value);
    void WriteUint64(ui64 value);

    void WriteDouble(double value);
    void WriteBoolean(bool value);

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
