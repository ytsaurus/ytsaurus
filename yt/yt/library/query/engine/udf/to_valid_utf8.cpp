#include <yt/yt/library/query/misc/udf_cpp_abi.h>

#include <util/charset/utf8.h>

using namespace NYT::NQueryClient::NUdf;

inline unsigned char* AsUnsigned(char* pointer)
{
    return reinterpret_cast<unsigned char*>(pointer);
}

void WriteRuneWithMaybeReallocation(
    TExpressionContext* context,
    wchar32 rune,
    char** begin,
    char** end,
    char** cursor)
{
    size_t runeLength = 0;
    auto result = SafeWriteUTF8Char(rune, runeLength, AsUnsigned(*cursor), AsUnsigned(*end));

    if (result == RECODE_EOOUTPUT) {
        i64 previousSize = (*end - *begin);
        auto desiredSize = std::max(previousSize * 2, previousSize + static_cast<i64>(sizeof(wchar32)));

        auto* newBegin = AllocateBytes(context, desiredSize);
        auto* newEnd = *end + desiredSize;
        auto* newCursor = newBegin + (*cursor - *begin);
        memcpy(newBegin, *begin, *cursor - *begin);

        *begin = newBegin;
        *cursor = newCursor;
        *end = newEnd;

        WriteUTF8Char(rune, runeLength, AsUnsigned(*cursor));
    }

    *cursor += runeLength;
}

extern "C" void to_valid_utf8(
    TExpressionContext* context,
    char** result,
    int* resultLength,
    char* input,
    int inputLength)
{
    auto* const inputEnd = input + inputLength;
    bool inInvalidSpan = false;

    auto* bufferBegin = AllocateBytes(context, inputLength);
    auto* bufferEnd = bufferBegin + inputLength;
    auto* bufferCursor = bufferBegin;

    while (input < inputEnd) {
        wchar32 rune;
        size_t runeLength;

        auto recodeResult = SafeReadUTF8Char(rune, runeLength, AsUnsigned(input), AsUnsigned(inputEnd));

        if (recodeResult == RECODE_OK) {
            input += runeLength;
            inInvalidSpan = false;
            WriteRuneWithMaybeReallocation(
                context,
                rune,
                &bufferBegin,
                &bufferEnd,
                &bufferCursor);
        } else if (!inInvalidSpan) {
            input++;
            inInvalidSpan = true;
            WriteRuneWithMaybeReallocation(
                context,
                BROKEN_RUNE,
                &bufferBegin,
                &bufferEnd,
                &bufferCursor);
        } else {
            input++;
        }
    }

    *result = bufferBegin;
    *resultLength = (bufferCursor - bufferBegin);
}

extern "C" int8_t is_valid_utf8(
    TExpressionContext* /*context*/,
    char* input,
    int inputLength)
{
    return IsUtf(input, inputLength);
}
