#pragma once

#include <algorithm>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

//! A dead-simple string formatter.
/*!
 *  This formatter is intended to be as simple as possible and async signal safe.
 *  This is the reason we do not use printf(): it does not meet signal-safety
 *  requirements.
 */
template <size_t N>
class TRawFormatter
{
public:
    TRawFormatter()
        : Begin(Buffer)
        , Cursor(Buffer)
        , End(Buffer + N)
    {
        for (int i = 0; i < N; ++i) {
            Buffer[i] = 0;
        }
    }

    TRawFormatter(char* buffer, int length)
        : Begin(buffer)
        , Cursor(buffer)
        , End(buffer + length)
    { }

    //! Returns an underlying cursor.
    char *GetCursor()
    {
        return Cursor;
    }

    //! Returns an  pointer to the underlying buffer.
    const char *GetData() const
    {
        return Begin;
    }

    //! Returns the number of bytes written in the buffer.
    int GetBytesWritten() const
    {
        return Cursor - Begin;
    }

    //! Returns the number of bytes available in the buffer.
    int GetBytesRemaining() const
    {
        return End - Cursor;
    }

    //! Advances the internal cursor (assuming the data is already present).
    void Advance(int offset)
    {
        Cursor += offset;

        if (Cursor + offset > End) {
            Cursor = End;
        }
    }

    //! Appends the string and updates the internal cursor.
    void AppendString(const char* string)
    {
        while (*string != '\0' && Cursor < End) {
            *Cursor++ = *string++;
        }
    }

    //! Formats |number| in base |radix| and updates the internal cursor.
    void AppendNumber(uintptr_t number, int radix = 10, int width = 0)
    {
        int i = 0;
        while (Cursor + i < End) {
            const int modulus = number % radix;
            number /= radix;
            Cursor[i] = (modulus < 10 ? '0' + modulus : 'a' + modulus - 10);
            ++i;
            if (number == 0) {
                break;
            }
        }

        // Reverse the bytes written.
        std::reverse(Cursor, Cursor + i);

        if (i < width) {
            auto delta = width - i;
            std::copy(Cursor, Cursor + i, Cursor + delta);
            std::fill(Cursor, Cursor + delta, ' ');
            Cursor += width;
        } else {
            Cursor += i;
        }
    }

    //! Formats |number| as hexadecimal number and updates the internal cursor.
    //! Padding will be added in front if needed.
    void AppendNumberAsHexWithPadding(uintptr_t number, int width)
    {
        char* begin = Cursor;
        AppendString("0x");
        AppendNumber(number, 16);
        // Move to right and add padding in front if needed.
        if (Cursor < begin + width) {
            auto delta = begin + width - Cursor;
            std::copy(begin, Cursor, begin + delta);
            std::fill(begin, begin + delta, ' ');
            Cursor = begin + width;
        }
    }

    //! Resets the underlying cursor.
    void Reset()
    {
        Cursor = Begin;
    }

private:
    char Buffer[N];
    char* const Begin;
    char* const End;
    char* Cursor;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace anonymous
} // namespace NYT

