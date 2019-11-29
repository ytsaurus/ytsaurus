#pragma once

#include "public.h"

#include "detail.h"

#include "syntax_checker.h"

#include <yt/core/misc/variant.h>

#include <yt/core/ytree/convert.h>

#include <util/generic/strbuf.h>
#include <util/stream/zerocopy.h>

#include <stack>


namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

// TYsonItem represents single meaningful yson item.
// We don't use std::variant for performance reasons.
class TYsonItem
{
public:
    Y_FORCE_INLINE TYsonItem(const TYsonItem& other);

    static Y_FORCE_INLINE TYsonItem Simple(EYsonItemType type);
    static Y_FORCE_INLINE TYsonItem Boolean(bool data);
    static Y_FORCE_INLINE TYsonItem Int64(i64 data);
    static Y_FORCE_INLINE TYsonItem Uint64(ui64 data);
    static Y_FORCE_INLINE TYsonItem Double(double data);
    static Y_FORCE_INLINE TYsonItem String(TStringBuf data);
    Y_FORCE_INLINE EYsonItemType GetType() const;
    Y_FORCE_INLINE bool UncheckedAsBoolean() const;
    Y_FORCE_INLINE i64 UncheckedAsInt64() const;
    Y_FORCE_INLINE ui64 UncheckedAsUint64() const;
    Y_FORCE_INLINE double UncheckedAsDouble() const;
    Y_FORCE_INLINE TStringBuf UncheckedAsString() const;
    Y_FORCE_INLINE bool IsEndOfStream() const;

private:
    TYsonItem() = default;

private:
    #pragma pack(push, 1)
    struct TSmallStringBuf
    {
        const char* Ptr;
        ui32 Size;
    };
    union TData
    {
        bool Boolean;
        i64 Int64;
        ui64 Uint64;
        double Double;
        TSmallStringBuf String;
    };
    TData Data_;
    EYsonItemType Type_;
    #pragma pack(pop)
};
// NB TYsonItem is 16 bytes long so it fits in a processor register.
static_assert(sizeof(TYsonItem) <= 16);

bool operator==(TYsonItem lhs, TYsonItem rhs);

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

class TZeroCopyInputStreamReader
{
public:
    TZeroCopyInputStreamReader(IZeroCopyInput* reader);

    Y_FORCE_INLINE void RefreshBlock();
    Y_FORCE_INLINE const char* Begin() const;
    Y_FORCE_INLINE const char* Current() const;
    Y_FORCE_INLINE const char* End() const;
    Y_FORCE_INLINE void Advance(size_t bytes);
    Y_FORCE_INLINE bool IsFinished() const;
    ui64 GetTotalReadSize() const;

private:
    IZeroCopyInput* Reader_;
    const char* Begin_ = nullptr;
    const char* End_ = nullptr;
    const char* Current_ = nullptr;
    ui64 TotalReadBlocksSize_ = 0;
    bool Finished_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TYsonPullParser
{
public:
    TYsonPullParser(IZeroCopyInput* input, EYsonType ysonType);

    TYsonItem Next();

    Y_FORCE_INLINE size_t GetNestingLevel() const;
    Y_FORCE_INLINE bool IsOnValueBoundary(size_t nestingLevel) const;

    ui64 GetTotalReadSize() const;

    // Return error attributes about yson context that is being parsed.
    std::vector<TErrorAttribute> GetErrorAttributes() const;

private:
    Y_FORCE_INLINE TYsonItem NextImpl();

private:
    using TLexer = NDetail::TLexerBase<NDetail::TReaderWithContext<NDetail::TZeroCopyInputStreamReader, 64>, false>;

    NDetail::TZeroCopyInputStreamReader StreamReader_;
    TLexer Lexer_;
    NDetail::TYsonSyntaxChecker SyntaxChecker_;
};

////////////////////////////////////////////////////////////////////////////////

// Cursor based iteration using pull parser.
// To check if yson stream is exhausted check if current item is of type EYsonItemType::EndOfStream.
class TYsonPullParserCursor
{
public:
    // This constructor extracts next element from parser immediately.
    Y_FORCE_INLINE TYsonPullParserCursor(TYsonPullParser* parser);

    Y_FORCE_INLINE TYsonPullParserCursor(TYsonItem current, TYsonPullParser* parser);

    Y_FORCE_INLINE const TYsonItem& GetCurrent() const;
    Y_FORCE_INLINE const TYsonItem* operator->() const;
    Y_FORCE_INLINE const TYsonItem& operator*() const;

    Y_FORCE_INLINE void Next();

    // Return error attributes about current yson context.
    std::vector<TErrorAttribute> GetErrorAttributes() const;

    // If cursor is positioned over simple value  (i.e. just integer) cursor is moved one element further.
    // If cursor is positioned over start of list/map cursor will be moved to the first item after
    // current list/map.
    // If cursor is positioned over start of attributes all attributes will be skipped and then value which
    // owns these attributes will be skipped as well.
    void SkipComplexValue();

    // Transfer complex value is similar to SkipComplexValue
    // except that it feeds passed consumer with skipped value.
    void TransferComplexValue(IYsonConsumer* consumer);
    void TransferComplexValue(TCheckedInDebugYsonTokenWriter* writer);

    // |Parse...| methods call |function(this)| for each item of corresponding composite object
    // and expect |function| to consume it (e.g. call |cursor->Next()|).
    // For map and attributes cursor will point to the key and |function| must consume both key and value.
    template <typename TFunction>
    void ParseMap(TFunction function);
    template <typename TFunction>
    void ParseList(TFunction function);
    template <typename TFunction>
    void ParseAttributes(TFunction function);

    // Transfer or skip attributes (if cursor is not positioned over attributes, throws an error).
    void SkipAttributes();
    void TransferAttributes(IYsonConsumer* consumer);
    void TransferAttributes(TCheckedInDebugYsonTokenWriter* writer);

private:
    TYsonItem Current_;
    TYsonPullParser* Parser_;

private:
    void SkipComplexValueOrAttributes();
};

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void EnsureYsonToken(
    TStringBuf description,
    const TYsonPullParserCursor& cursor,
    EYsonItemType expected);

void ThrowUnexpectedYsonTokenException(
    TStringBuf description,
    const TYsonPullParserCursor& cursor,
    const std::vector<EYsonItemType>& expected);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

#define PULL_PARSER_INL_H_
#include "pull_parser-inl.h"
#undef PULL_PARSER_INL_H_
