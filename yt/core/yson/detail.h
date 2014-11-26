#pragma once

#include "public.h"
#include "zigzag.h"

#include <core/misc/error.h>
#include <core/misc/property.h>

#include <core/concurrency/coroutine.h>

#include <util/string/escape.h>

namespace NYT {
namespace NYson {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {
/*! \internal */
////////////////////////////////////////////////////////////////////////////////

//! Indicates the beginning of a list.
const char BeginListSymbol = '[';
//! Indicates the end of a list.
const char EndListSymbol = ']';

//! Indicates the beginning of a map.
const char BeginMapSymbol = '{';
//! Indicates the end of a map.
const char EndMapSymbol = '}';

//! Indicates the beginning of an attribute map.
const char BeginAttributesSymbol = '<';
//! Indicates the end of an attribute map.
const char EndAttributesSymbol = '>';

//! Separates items in lists.
const char ListItemSeparatorSymbol = ';';
//! Separates items in maps, attributes.
const char KeyedItemSeparatorSymbol = ';';
//! Separates keys from values in maps.
const char KeyValueSeparatorSymbol = '=';

//! Indicates an entity.
const char EntitySymbol = '#';

//! Indicates end of stream.
const char EndSymbol = '\0';

//! Marks the beginning of a binary string literal.
const char StringMarker = '\x01';
//! Marks the beginning of a binary i64 literal.
const char IntegerMarker = '\x02';
//! Marks the beginning of a binary double literal.
const char DoubleMarker = '\x03';

template <bool EnableLinePositionInfo>
class TPositionInfo;

template <>
class TPositionInfo<true>
{
private:
    int Offset;
    int Line;
    int Column;

public:
    TPositionInfo() 
        : Offset(0)
        , Line(1)
        , Column(1)
    { }

    void OnRangeConsumed(const char* begin, const char* end)
    {
        Offset += end - begin; 
        for (auto current = begin; current != end; ++current) {
            ++Column;
            if (*current == '\n') { //TODO: memchr
                ++Line;
                Column = 1;
            }
        }
    }

    Stroka GetPositionInfo() const
    {
        TStringStream stream;
        stream << "Offset: " << Offset;
        stream << ", Line: " << Line;
        stream << ", Column: " << Column;
        return stream.Str();
    } 
};

template <>
class TPositionInfo<false>
{
private:
    int Offset;

public:
    TPositionInfo() 
        : Offset(0)
    { }

    void OnRangeConsumed(const char* begin, const char* end)
    {
        Offset += end - begin;
    }

    Stroka GetPositionInfo() const
    {
        TStringStream stream;
        stream << "Offset: " << Offset;
        return stream.Str();
    } 
};

template <class TBlockStream, class TPositionBase>
class TCharStream 
    : public TBlockStream
    , public TPositionBase
{
public:
    TCharStream(const TBlockStream& blockStream) 
        : TBlockStream(blockStream)
    { }

    bool IsEmpty() const
    {
        return TBlockStream::Begin() == TBlockStream::End();
    }

    template <bool AllowFinish>
    void Refresh()
    {
        while (IsEmpty() && !TBlockStream::IsFinished()) {
            TBlockStream::RefreshBlock();
        }
        if (IsEmpty() && TBlockStream::IsFinished() && !AllowFinish) {
            THROW_ERROR_EXCEPTION("Premature end of stream (%s)",
                ~TPositionBase::GetPositionInfo());
        }
    }

    void Refresh()
    {
        return Refresh<false>();
    }
        
    template <bool AllowFinish>
    char GetChar()
    {
        Refresh<AllowFinish>();
        return !IsEmpty() ? *TBlockStream::Begin() : '\0';
    }

    char GetChar()
    {
        return GetChar<false>();
    }

    void Advance(size_t amount)
    {
        TPositionBase::OnRangeConsumed(TBlockStream::Begin(), TBlockStream::Begin() + amount);
        TBlockStream::Advance(amount);
    }
    
    size_t Length() const
    {
        return TBlockStream::End() - TBlockStream::Begin();
    }
};

template <class TBaseStream>
class TCodedStream
    : public TBaseStream
{
private:
    static const int MaxVarintBytes = 10;
    static const int MaxVarint32Bytes = 5;

    const ui8* BeginByte() const
    {
        return reinterpret_cast<const ui8*>(TBaseStream::Begin());
    }

    const ui8* EndByte() const
    {
        return reinterpret_cast<const ui8*>(TBaseStream::End());
    }

    // Following functions is an adaptation Protobuf code from coded_stream.cc
    bool ReadVarint32FromArray(ui32* value)
    {
        // Fast path:  We have enough bytes left in the buffer to guarantee that
        // this read won't cross the end, so we can skip the checks.
        const ui8* ptr = BeginByte();
        ui32 b;
        ui32 result;

        b = *(ptr++); result  = (b & 0x7F)      ;if (!(b & 0x80)) goto done;
        b = *(ptr++); result |= (b & 0x7F) <<  7; if (!(b & 0x80)) goto done;
        b = *(ptr++); result |= (b & 0x7F) << 14; if (!(b & 0x80)) goto done;
        b = *(ptr++); result |= (b & 0x7F) << 21; if (!(b & 0x80)) goto done;
        b = *(ptr++); result |=  b         << 28; if (!(b & 0x80)) goto done;

        // If the input is larger than 32 bits, we still need to read it all
        // and discard the high-order bits.
        
        for (int i = 0; i < MaxVarintBytes - MaxVarint32Bytes; i++) {
            b = *(ptr++); if (!(b & 0x80)) goto done;
        }
        
        // We have overrun the maximum size of a Varint (10 bytes).  Assume
        // the data is corrupt.
        return false;

    done:
        TBaseStream::Advance(ptr - BeginByte());
        *value = result;
        return true;
    }

    bool ReadVarint32Fallback(ui32* value) 
    {
        if (BeginByte() + MaxVarint32Bytes <= EndByte() ||
            // Optimization:  If the Varint ends at exactly the end of the buffer,
            // we can detect that and still use the fast path.
            BeginByte() < EndByte() && !(EndByte()[-1] & 0x80)) 
        {
            return ReadVarint32FromArray(value);
        } else { 
            // Really slow case: we will incur the cost of an extra function call here,
            // but moving this out of line reduces the size of this function, which
            // improves the common case. In micro benchmarks, this is worth about 10-15%
            return ReadVarint32Slow(value);
        }
    }

    bool ReadVarint32Slow(ui32* value)
    {
        ui64 result;
        // Directly invoke ReadVarint64Fallback, since we already tried to optimize
        // for one-byte Varints.
        if (ReadVarint64Fallback(&result)) {
            *value = static_cast<ui32>(result);
            return true;
        } else {
            return false;
        }      
    }

    bool ReadVarint64Slow(ui64* value)
    {
        // Slow path:  This read might cross the end of the buffer, so we
        // need to check and refresh the buffer if and when it does.

        ui64 result = 0;
        int count = 0;
        ui32 b;

        do {
            if (count == MaxVarintBytes) {
                return false;
            }
            while (BeginByte() == EndByte()) { 
                TBaseStream::Refresh();
            }
            b = *BeginByte();
            result |= static_cast<ui64>(b & 0x7F) << (7 * count);
            TBaseStream::Advance(1);
            ++count;
        } while (b & 0x80);

        *value = result;
        return true;
    }

    bool ReadVarint64Fallback(ui64* value)
    {
        if (BeginByte() + MaxVarintBytes <= EndByte() ||
            // Optimization:  If the Varint ends at exactly the end of the buffer,
            // we can detect that and still use the fast path.
            BeginByte() < EndByte() && !(EndByte()[-1] & 0x80))
        {
            // Fast path:  We have enough bytes left in the buffer to guarantee that
            // this read won't cross the end, so we can skip the checks.

            const ui8* ptr = BeginByte();
            ui32 b;

            // Splitting into 32-bit pieces gives better performance on 32-bit
            // processors.
            ui32 part0 = 0, part1 = 0, part2 = 0;

            b = *(ptr++); part0  = (b & 0x7F)      ; if (!(b & 0x80)) goto done;
            b = *(ptr++); part0 |= (b & 0x7F) <<  7; if (!(b & 0x80)) goto done;
            b = *(ptr++); part0 |= (b & 0x7F) << 14; if (!(b & 0x80)) goto done;
            b = *(ptr++); part0 |= (b & 0x7F) << 21; if (!(b & 0x80)) goto done;
            b = *(ptr++); part1  = (b & 0x7F)      ; if (!(b & 0x80)) goto done;
            b = *(ptr++); part1 |= (b & 0x7F) <<  7; if (!(b & 0x80)) goto done;
            b = *(ptr++); part1 |= (b & 0x7F) << 14; if (!(b & 0x80)) goto done;
            b = *(ptr++); part1 |= (b & 0x7F) << 21; if (!(b & 0x80)) goto done;
            b = *(ptr++); part2  = (b & 0x7F)      ; if (!(b & 0x80)) goto done;
            b = *(ptr++); part2 |= (b & 0x7F) <<  7; if (!(b & 0x80)) goto done;

            // We have overrun the maximum size of a Varint (10 bytes).  The data
            // must be corrupt.
            return nullptr;

        done:
            TBaseStream::Advance(ptr - BeginByte());
            *value = (static_cast<ui64>(part0)      ) |
                        (static_cast<ui64>(part1) << 28) |
                        (static_cast<ui64>(part2) << 56);
            return true;
        } else {
            return ReadVarint64Slow(value);
        }
    }

public:
    TCodedStream(const TBaseStream& baseStream)
        : TBaseStream(baseStream)
    { }

    bool ReadVarint64(ui64* value)
    {
        if (BeginByte() < EndByte() && *BeginByte() < 0x80) {
            *value = *BeginByte();
            TBaseStream::Advance(1);
            return true;
        } else {
            return ReadVarint64Fallback(value);
        }
    }

    bool ReadVarint32(ui32* value)
    {
        if (BeginByte() < EndByte() && *BeginByte() < 0x80) {
            *value = *BeginByte();
            TBaseStream::Advance(1);
            return true;
        } else {
            return ReadVarint32Fallback(value);
        }
    }
};

template <class TBlockStream, bool EnableLinePositionInfo>
class TLexerBase
    : public TCodedStream<TCharStream<TBlockStream, TPositionInfo<EnableLinePositionInfo> > >
{
private:
    typedef TCodedStream<TCharStream<TBlockStream, TPositionInfo<EnableLinePositionInfo> > > TBaseStream;
    std::vector<char> Buffer_;
    TNullable<i64> MemoryLimit_;

    void CheckMemoryLimit()
    {
        if (MemoryLimit_ && Buffer_.capacity() > *MemoryLimit_) {
            THROW_ERROR_EXCEPTION(
                "Memory limit exceeded while parsing YSON stream: allocated %" PRId64 ", limit %" PRId64,
                Buffer_.capacity(),
                *MemoryLimit_);
        }
    }

public:
    TLexerBase(const TBlockStream& blockStream, TNullable<i64> memoryLimit) 
        : TBaseStream(blockStream)
        , MemoryLimit_(memoryLimit)
    { }
    
protected:
    /// Lexer routines 

    // Returns true if double, false if integer
    template <bool AllowFinish>
    bool ReadNumeric(TStringBuf* value)
    {
        Buffer_.clear();
        bool isDouble = false;
        while (true) {
            char ch = TBaseStream::template GetChar<AllowFinish>();
            if (isdigit(ch) || ch == '+' || ch == '-') { // Seems like it can't be '+' or '-'
                Buffer_.push_back(ch);
            } else if (ch == '.' || ch == 'e' || ch == 'E') {
                Buffer_.push_back(ch);
                isDouble = true;
            } else if (isalpha(ch)) {
                THROW_ERROR_EXCEPTION("Unexpected character in numeric (Char: %s) (%s)",
                    ~Stroka(ch).Quote(),
                    ~TBaseStream::GetPositionInfo());
            } else {
                break;
            }
            CheckMemoryLimit();
            TBaseStream::Advance(1);
        }

        *value = TStringBuf(Buffer_.data(), Buffer_.size());
        return isDouble;
    }

    void ReadQuotedString(TStringBuf* value)
    {
        Buffer_.clear();
        while (true) {
            if (TBaseStream::IsEmpty()) {
                TBaseStream::Refresh();
            }
            char ch = *TBaseStream::Begin();
            TBaseStream::Advance(1);
            if (ch != '"') {
                Buffer_.push_back(ch);
            } else {
                // We must count the number of '\' at the end of StringValue
                // to check if it's not \"
                int slashCount = 0;
                int length = Buffer_.size();
                while (slashCount < length && Buffer_[length - 1 - slashCount] == '\\') {
                    ++slashCount;
                }
                if (slashCount % 2 == 0) {
                    break;
                } else {
                    Buffer_.push_back(ch);
                }
            }            
            CheckMemoryLimit();
        }

        auto unquotedValue = UnescapeC(Buffer_.data(), Buffer_.size());
        Buffer_.clear();
        Buffer_.insert(Buffer_.end(), unquotedValue.data(), unquotedValue.data() + unquotedValue.size());
        CheckMemoryLimit();
        *value = TStringBuf(Buffer_.data(), Buffer_.size());
    }

    template <bool AllowFinish>
    void ReadUnquotedString(TStringBuf* value)
    {
        Buffer_.clear();
        while (true) {
            char ch = TBaseStream::template GetChar<AllowFinish>();
            if (isalpha(ch) || isdigit(ch) ||
                ch == '_' || ch == '-' || ch == '%' || ch == '.')
            {
                Buffer_.push_back(ch);
            } else {
                break;
            }
            CheckMemoryLimit();
            TBaseStream::Advance(1);
        }
        *value = TStringBuf(Buffer_.data(), Buffer_.size());
    }

    void ReadUnquotedString(TStringBuf* value)
    {
        return ReadUnquotedString<false>(value);
    }

    void ReadBinaryString(TStringBuf* value)
    {
        ui32 ulength = 0;
        if (TBaseStream::ReadVarint32(&ulength)) { 
            i32 length = ZigZagDecode32(ulength);
            if (length < 0) {
                THROW_ERROR_EXCEPTION("Error reading binary string: String cannot have negative length (Length: %" PRId64 ") (%s)",
                        length,
                        ~TBaseStream::GetPositionInfo());
            }

            if (TBaseStream::Begin() + length <= TBaseStream::End()) { 
                *value = TStringBuf(TBaseStream::Begin(), length);
                TBaseStream::Advance(length);
            } else { // reading in Buffer
                size_t needToRead = length;
                Buffer_.clear();
                while (needToRead) {
                    if (TBaseStream::IsEmpty()) {
                        TBaseStream::Refresh();
                        continue;
                    }
                    size_t readingBytes = needToRead < TBaseStream::Length() ? needToRead : TBaseStream::Length(); // TODO: min               
                    Buffer_.insert(Buffer_.end(), TBaseStream::Begin(), TBaseStream::Begin() + readingBytes);
                    CheckMemoryLimit();
                    needToRead -= readingBytes;
                    TBaseStream::Advance(readingBytes);
                }
                *value = TStringBuf(Buffer_.data(), Buffer_.size());
            }
        } else {
            THROW_ERROR_EXCEPTION("Error while parsing varint (%s)", 
                ~TBaseStream::GetPositionInfo());
        }
    }
    
    void ReadBinaryInteger(i64* result)
    {
        ui64 value;
        if (TBaseStream::ReadVarint64(&value)) { 
            *result = ZigZagDecode64(value);
        } else {
            THROW_ERROR_EXCEPTION("Error while parsing varint (%s)", 
                ~TBaseStream::GetPositionInfo());
        }
    }
    
    void ReadBinaryDouble(double* value)
    {
        size_t needToRead = sizeof(double);

        while (needToRead != 0) {
            if (TBaseStream::IsEmpty()) {
                TBaseStream::Refresh();
                continue;
            }

            size_t chunkSize = std::min(needToRead, TBaseStream::Length());
            if (chunkSize == 0) {
                THROW_ERROR_EXCEPTION("Error while parsing binary double (%s)", 
                    ~TBaseStream::GetPositionInfo());
            }
            std::copy(TBaseStream::Begin(), TBaseStream::Begin() + chunkSize, reinterpret_cast<char*>(value) + (sizeof(double) - needToRead));
            needToRead -= chunkSize;
            TBaseStream::Advance(chunkSize);
        }
    }
    
    /// Helpers
    void SkipCharToken(char symbol)
    {
        char ch = SkipSpaceAndGetChar();
        if (ch != symbol) {
            THROW_ERROR_EXCEPTION("Expected %s but character %s found (%s)",
                ~Stroka(symbol).Quote(),
                ~Stroka(ch).Quote(),
                ~TBaseStream::GetPositionInfo());
        }
        
        TBaseStream::Advance(1);
    }

    static bool IsSpaceFast(char ch)
    {
        static const ui8 lookupTable[] = 
        {
            0,0,0,0,0,0,0,0, 0,1,1,1,1,1,0,0,
            0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
            1,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
                         
            0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
                         
            0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
                         
            0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,
            0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0
        };
        return lookupTable[static_cast<ui8>(ch)];
    }

    template <bool AllowFinish>
    char SkipSpaceAndGetChar()
    {
        if (!TBaseStream::IsEmpty()) {
            char ch = *TBaseStream::Begin();
            if (!IsSpaceFast(ch)) {
                return ch;
            }
        }
        return SkipSpaceAndGetCharFallback<AllowFinish>();
    }

    char SkipSpaceAndGetChar()
    {
        return SkipSpaceAndGetChar<false>();
    }

    template <bool AllowFinish>
    char SkipSpaceAndGetCharFallback()
    {
        while (true) {
            if (TBaseStream::IsEmpty()) {
                if (TBaseStream::IsFinished()) {
                    return '\0';
                }
                TBaseStream::template Refresh<AllowFinish>();
                continue;
            }
            if (!IsSpaceFast(*TBaseStream::Begin())) {
                break;
            }
            TBaseStream::Advance(1);
        }
        return TBaseStream::template GetChar<AllowFinish>();
    }    
};
////////////////////////////////////////////////////////////////////////////////
/*! \endinternal */
} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TStringReader
{
private:
    const char* BeginPtr;
    const char* EndPtr;

public:
    TStringReader()
        : BeginPtr(0)
        , EndPtr(0)
    { }

    TStringReader(const char* begin, const char* end)
        : BeginPtr(begin)
        , EndPtr(end)
    { }

    const char* Begin() const
    {
        return BeginPtr;
    }

    const char* End() const
    {
        return EndPtr;
    }

    void RefreshBlock()
    {
        YUNREACHABLE();
    }

    void Advance(size_t amount)
    {
        BeginPtr += amount;
    }
    
    bool IsFinished() const
    {
        return true;
    }

    void SetBuffer(const char* begin, const char* end)
    {
        BeginPtr = begin;
        EndPtr = end;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TParserCoroutine>
class TBlockReader
{
private:
    TParserCoroutine& Coroutine;

    const char* BeginPtr;
    const char* EndPtr;
    bool FinishFlag;

public:
    TBlockReader(
        TParserCoroutine& coroutine,
        const char* begin,
        const char* end,
        bool finish)
        : Coroutine(coroutine)
        , BeginPtr(begin)
        , EndPtr(end)
        , FinishFlag(finish)
    { }

    const char* Begin() const
    {
        return BeginPtr;
    }

    const char* End() const
    {
        return EndPtr;
    }

    void RefreshBlock()
    {
        std::tie(BeginPtr, EndPtr, FinishFlag) = Coroutine.Yield(0);
    }

    void Advance(size_t amount)
    {
        BeginPtr += amount;
    }
    
    bool IsFinished() const
    {
        return FinishFlag;
    }
};


////////////////////////////////////////////////////////////////////////////////

} // namespace NYson
} // namespace NYT
