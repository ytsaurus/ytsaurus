#include "ypath_resolver.h"

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/yson/consumer.h>
#include <yt/core/yson/parser.h>

#include <yt/core/misc/error.h>

namespace NYT {
namespace NYTree {

using NYPath::ETokenType;
using NYPath::TTokenizer;

using NYson::IYsonConsumer;
using NYson::EYsonType;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EExpectedItem,
    (Initial)
    (BeginMap)
    (Key)
    (BeginAttribute)
    (ListItem)
    (Value)
);

class TCompleteParsingException
{ };

template <typename T>
class TCompleteParsingWithResultException
    : public TCompleteParsingException
{
public:
    template <typename U>
    TCompleteParsingWithResultException(U&& val)
        : Result(val)
    { }

    T Result;
};

class TYPathResolver
    : public IYsonConsumer
{
public:
    explicit TYPathResolver(const TYPath& path)
        : Tokenizer_(path)
    {
        NextToken();
    }

    virtual void OnStringScalar(const TStringBuf& value) override
    {
        OnStringValue(value);
    }

    virtual void OnInt64Scalar(i64 value) override
    {
        OnValue(value);
    }

    virtual void OnUint64Scalar(ui64 value) override
    {
        OnValue(value);
    }

    virtual void OnDoubleScalar(double value) override
    {
        OnValue(value);
    }

    virtual void OnBooleanScalar(bool value) override
    {
        OnValue(value);
    }

    virtual void OnEntity() override
    { }

    virtual void OnBeginList() override
    {
        if (CurrentDepth_++ == PathDepth_) {
            switch (Expected_) {
                case EExpectedItem::BeginMap:
                    Expected_ = EExpectedItem::ListItem;
                    CurrentListIndex_ = 0;
                    try {
                        ListIndex_ = FromString<int>(Literal_);
                    } catch (const TBadCastException&) {
                        throw TCompleteParsingException{};
                    }
                    ++PathDepth_;
                    break;

                case EExpectedItem::ListItem:
                case EExpectedItem::Key:
                    break;

                default:
                    throw TCompleteParsingException{};
            }
        }
    }

    virtual void OnListItem() override
    {
        if (CurrentDepth_ == PathDepth_) {
            if (Expected_ != EExpectedItem::ListItem) {
                throw TCompleteParsingException{};
            }
            if (CurrentListIndex_ == ListIndex_) {
                NextToken();
            } else if (CurrentListIndex_ > ListIndex_) {
                throw TCompleteParsingException{};
            }
            ++CurrentListIndex_;
        }
    }

    virtual void OnEndList() override
    {
        OnDecDepth();
    }

    virtual void OnBeginMap() override
    {
        if (CurrentDepth_++ == PathDepth_) {
            switch (Expected_) {
                case EExpectedItem::BeginMap:
                    ++PathDepth_;
                    Expected_ = EExpectedItem::Key;
                    break;

                case EExpectedItem::ListItem:
                case EExpectedItem::Key:
                    break;

                default:
                    throw TCompleteParsingException{};
            }
        }
    }

    virtual void OnKeyedItem(const TStringBuf& key) override
    {
        if (CurrentDepth_ == PathDepth_) {
            if (Expected_ != EExpectedItem::Key) {
                throw TCompleteParsingException{};
            }
            if (Literal_ == key) {
                NextToken();
            }
        }
    }

    virtual void OnEndMap() override
    {
        OnDecDepth();
    }

    virtual void OnBeginAttributes() override
    {
        if (CurrentDepth_++ == PathDepth_) {
            switch (Expected_) {
                case EExpectedItem::BeginAttribute:
                    ++PathDepth_;
                    Expected_ = EExpectedItem::Key;
                    break;

                case EExpectedItem::BeginMap:
                    break;

                default:
                    throw TCompleteParsingException{};
            }
        }
    }

    virtual void OnEndAttributes() override
    {
        OnDecDepth();
    }

    virtual void OnRaw(const TStringBuf& yson, EYsonType type) override
    { }

private:
    TTokenizer Tokenizer_;

    EExpectedItem Expected_ = EExpectedItem::Initial;
    TString Literal_;
    int ListIndex_ = 0;
    int CurrentListIndex_ = 0;
    int CurrentDepth_ = 0;
    int PathDepth_ = 0;

    void OnDecDepth()
    {
        if (--CurrentDepth_ < PathDepth_) {
            throw TCompleteParsingException{};
        }
    }

    template <typename T>
    void OnValue(const T& t)
    {
        if (Expected_ == EExpectedItem::Value) {
            throw TCompleteParsingWithResultException<T>(t);
        }
    }

    void OnStringValue(const TStringBuf& t)
    {
        if (Expected_ == EExpectedItem::Value) {
            throw TCompleteParsingWithResultException<TString>(t);
        }
    }

    void NextToken()
    {
        Expected_ = EExpectedItem::Initial;
        while (true) {
            ETokenType type = Tokenizer_.Advance();
            switch (type) {
                case ETokenType::EndOfStream:
                    if (Expected_ != EExpectedItem::Initial) {
                        THROW_ERROR_EXCEPTION(
                            "Unexpected YPath ending while parsing %Qv",
                            Tokenizer_.GetPrefix());
                    }
                    Expected_ = EExpectedItem::Value;
                    return;

                case ETokenType::Slash:
                    if (Expected_ != EExpectedItem::Initial) {
                        UnexpectedToken();
                    }
                    Expected_ = EExpectedItem::BeginMap;
                    break;

                case ETokenType::At:
                    if (Expected_ != EExpectedItem::BeginMap) {
                        UnexpectedToken();
                    }
                    Expected_ = EExpectedItem::BeginAttribute;
                    break;

                case ETokenType::Literal:
                    if (Expected_ != EExpectedItem::BeginMap && Expected_ != EExpectedItem::BeginAttribute) {
                        UnexpectedToken();
                    }
                    Literal_ = Tokenizer_.GetLiteralValue();
                    return;

                default:
                    UnexpectedToken();
            }
        }
    }

    void UnexpectedToken()
    {
        THROW_ERROR_EXCEPTION(
            "Unexpected YPath token %Qv while parsing %Qv",
            Tokenizer_.GetToken(),
            Tokenizer_.GetInput());
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TNullable<T> TryGetValue(const TStringBuf& yson, const TYPath& ypath)
{
    try {
        TYPathResolver resolver(ypath);
        ParseYsonStringBuffer(
            yson,
            EYsonType::Node,
            &resolver);
    } catch (const TCompleteParsingWithResultException<T>& value) {
        return value.Result;
    } catch (const TCompleteParsingException&) {
    }
    return {};
}

////////////////////////////////////////////////////////////////////////////////

TNullable<i64> TryGetInt64(const TStringBuf& yson, const TYPath& ypath)
{
    return TryGetValue<i64>(yson, ypath);
}

TNullable<ui64> TryGetUint64(const TStringBuf& yson, const TYPath& ypath)
{
    return TryGetValue<ui64>(yson, ypath);
}

TNullable<bool> TryGetBoolean(const TStringBuf& yson, const TYPath& ypath)
{
    return TryGetValue<bool>(yson, ypath);
}

TNullable<double> TryGetDouble(const TStringBuf& yson, const TYPath& ypath)
{
    return TryGetValue<double>(yson, ypath);
}

TNullable<TString> TryGetString(const TStringBuf& yson, const TYPath& ypath)
{
    return TryGetValue<TString>(yson, ypath);
}

} // namespace NYTree
} // namespace NYT
