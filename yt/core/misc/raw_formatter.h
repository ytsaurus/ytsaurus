#pragma once

#include <algorithm>
#include <array>

namespace NYT {

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
        : Begin(Buffer.data())
        , Cursor(Buffer.data())
        , End(Buffer.data() + N)
    { }

    TRawFormatter(char* buffer, int length)
        : Begin(buffer)
        , Cursor(buffer)
        , End(buffer + length)
    { }

    //! Returns an underlying cursor.
    char* GetCursor()
    {
        return Cursor;
    }

    //! Returns an pointer to the underlying buffer.
    const char* GetData() const
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

        if (Cursor > End) {
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

    //! Appends a single character and updates the internal cursor.
    void AppendChar(char ch)
    {
        if (Cursor < End) {
            *Cursor++ = ch;
        }
    }

    //! Formats |number| in base |radix| and updates the internal cursor.
    void AppendNumber(uintptr_t number, int radix = 10, int width = 0)
    {
        int digits = 0;

        if (radix == 16) {
            // Optimize output of hex numbers.

            uintptr_t reverse = 0;
            int length = 0;
            do {
                reverse <<= 4;
                reverse |= number & 0xf;
                number >>= 4;
                ++length;
            } while (number > 0);

            for (int index = 0; index < length && Cursor + digits < End; ++index) {
                unsigned int modulus = reverse & 0xf;
                Cursor[digits] = (modulus < 10 ? '0' + modulus : 'a' + modulus - 10);
                ++digits;
                reverse >>= 4;
            }
        } else {
            while (Cursor + digits < End) {
                const int modulus = number % radix;
                number /= radix;
                Cursor[digits] = (modulus < 10 ? '0' + modulus : 'a' + modulus - 10);
                ++digits;
                if (number == 0) {
                    break;
                }
            }

            // Reverse the bytes written.
            std::reverse(Cursor, Cursor + digits);
        }

        if (digits < width) {
            auto delta = width - digits;
            std::copy(Cursor, Cursor + digits, Cursor + delta);
            std::fill(Cursor, Cursor + delta, ' ');
            Cursor += width;
        } else {
            Cursor += digits;
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
    std::array<char, N> Buffer;
    char* const Begin;
    char* Cursor;
    char* const End;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

