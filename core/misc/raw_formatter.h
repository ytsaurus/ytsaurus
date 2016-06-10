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
        : Begin_(Buffer_.data())
        , Cursor_(Buffer_.data())
        , End_(Buffer_.data() + N)
    { }

    TRawFormatter(char* buffer, int length)
        : Begin_(buffer)
        , Cursor_(buffer)
        , End_(buffer + length)
    { }

    //! Returns an underlying cursor.
    char* GetCursor()
    {
        return Cursor_;
    }

    //! Returns an pointer to the underlying buffer.
    const char* GetData() const
    {
        return Begin_;
    }

    //! Returns the number of bytes written in the buffer.
    int GetBytesWritten() const
    {
        return Cursor_ - Begin_;
    }

    //! Returns the number of bytes available in the buffer.
    int GetBytesRemaining() const
    {
        return End_ - Cursor_;
    }

    //! Advances the internal cursor (assuming the data is already present).
    void Advance(int offset)
    {
        Cursor_ += offset;

        if (Cursor_ > End_) {
            Cursor_ = End_;
        }
    }

    //! Appends the string and updates the internal cursor.
    void AppendString(const char* string)
    {
        while (*string != '\0' && Cursor_ < End_) {
            *Cursor_++ = *string++;
        }
    }

    //! Appends a single character and updates the internal cursor.
    void AppendChar(char ch)
    {
        if (Cursor_ < End_) {
            *Cursor_++ = ch;
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

            for (int index = 0; index < length && Cursor_ + digits < End_; ++index) {
                unsigned int modulus = reverse & 0xf;
                Cursor_[digits] = (modulus < 10 ? '0' + modulus : 'a' + modulus - 10);
                ++digits;
                reverse >>= 4;
            }
        } else {
            while (Cursor_ + digits < End_) {
                const int modulus = number % radix;
                number /= radix;
                Cursor_[digits] = (modulus < 10 ? '0' + modulus : 'a' + modulus - 10);
                ++digits;
                if (number == 0) {
                    break;
                }
            }

            // Reverse the bytes written.
            std::reverse(Cursor_, Cursor_ + digits);
        }

        if (digits < width) {
            auto delta = width - digits;
            std::copy(Cursor_, Cursor_ + digits, Cursor_ + delta);
            std::fill(Cursor_, Cursor_ + delta, ' ');
            Cursor_ += width;
        } else {
            Cursor_ += digits;
        }
    }

    //! Formats |number| as hexadecimal number and updates the internal cursor.
    //! Padding will be added in front if needed.
    void AppendNumberAsHexWithPadding(uintptr_t number, int width)
    {
        char* begin = Cursor_;
        AppendString("0x");
        AppendNumber(number, 16);
        // Move to right and add padding in front if needed.
        if (Cursor_ < begin + width) {
            auto delta = begin + width - Cursor_;
            std::copy(begin, Cursor_, begin + delta);
            std::fill(begin, begin + delta, ' ');
            Cursor_ = begin + width;
        }
    }

    //! Resets the underlying cursor.
    void Reset()
    {
        Cursor_ = Begin_;
    }

private:
    std::array<char, N> Buffer_;
    char* const Begin_;
    char* Cursor_;
    char* const End_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

