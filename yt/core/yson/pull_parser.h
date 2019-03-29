#pragma once

#include "public.h"

#include "detail.h"

#include <yt/core/misc/variant.h>

#include <yt/core/ytree/convert.h>

#include <util/generic/strbuf.h>
#include <util/stream/zerocopy.h>

#include <stack>


namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EYsonItemType, ui8,
    (EndOfStream)
    (BeginMap)
    (EndMap)
    (BeginAttributes)
    (EndAttributes)
    (BeginList)
    (EndList)
    (EntityValue)
    (BooleanValue)
    (Int64Value)
    (Uint64Value)
    (DoubleValue)
    (StringValue)
);

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

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EYsonState, ui8,
    (Terminated)
    (ExpectValue)
    (ExpectAttributelessValue)

    // top level list fragment
    (InsideListFragmentExpectAttributelessValue)
    (InsideListFragmentExpectValue)
    (InsideListFragmentExpectSeparator)

    // top level map fragment
    (InsideMapFragmentExpectKey)
    (InsideMapFragmentExpectEquality)
    (InsideMapFragmentExpectAttributelessValue)
    (InsideMapFragmentExpectValue)
    (InsideMapFragmentExpectSeparator)

    (InsideMapExpectKey)
    (InsideMapExpectEquality)
    (InsideMapExpectAttributelessValue)
    (InsideMapExpectValue)
    (InsideMapExpectSeparator)

    (InsideAttributeMapExpectKey)
    (InsideAttributeMapExpectEquality)
    (InsideAttributeMapExpectAttributelessValue)
    (InsideAttributeMapExpectValue)
    (InsideAttributeMapExpectSeparator)

    (InsideListExpectAttributelessValue)
    (InsideListExpectValue)
    (InsideListExpectSeparator)
);

////////////////////////////////////////////////////////////////////////////////

class TYsonSyntaxChecker
{
public:
    explicit TYsonSyntaxChecker(EYsonType ysonType);

    Y_FORCE_INLINE void OnSimpleNonstring(EYsonItemType itemType);
    Y_FORCE_INLINE void OnString();
    Y_FORCE_INLINE void OnFinish();
    Y_FORCE_INLINE void OnEquality();
    Y_FORCE_INLINE void OnSeparator();
    Y_FORCE_INLINE void OnBeginList();
    Y_FORCE_INLINE void OnEndList();
    Y_FORCE_INLINE void OnBeginMap();
    Y_FORCE_INLINE void OnEndMap();
    Y_FORCE_INLINE void OnAttributesBegin();

    Y_FORCE_INLINE void OnAttributesEnd();

    Y_FORCE_INLINE size_t GetNestingLevel() const;
    Y_FORCE_INLINE bool IsOnValueBoundary(size_t nestingLevel) const;

private:
    template <bool isString>
    Y_FORCE_INLINE void OnSimple(EYsonItemType itemType);
    Y_FORCE_INLINE void IncrementNestingLevel();
    Y_FORCE_INLINE void DecrementNestingLevel();

    static TStringBuf StateExpectationString(EYsonState state);
    void ThrowUnexpectedToken(TStringBuf token);

private:
    SmallVector<EYsonState, 16> StateStack_;
    // We don't use stack size, we compute depth level precisely to be compatible with old yson parser.
    ui32 NestingLevel_ = 0;

    static constexpr ui32 NestingLevelLimit = 64;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

class TYsonPullParser
{
public:
    TYsonPullParser(IZeroCopyInput* input, EYsonType ysonType);

    Y_FORCE_INLINE TYsonItem Next();

    Y_FORCE_INLINE size_t GetNestingLevel() const;
    Y_FORCE_INLINE bool IsOnValueBoundary(size_t nestingLevel) const;

    ui64 GetTotalReadSize() const;

private:
    Y_FORCE_INLINE TYsonItem NextImpl();

private:
    using TLexer = NDetail::TLexerBase<NDetail::TReaderWithContext<NDetail::TZeroCopyInputStreamReader, 64>, false>;

    NDetail::TZeroCopyInputStreamReader StreamReader_;
    TLexer Lexer_;
    NDetail::TYsonSyntaxChecker SyntaxChecker_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson

#define PULL_PARSER_INL_H_
#include "pull_parser-inl.h"
#undef PULL_PARSER_INL_H_
