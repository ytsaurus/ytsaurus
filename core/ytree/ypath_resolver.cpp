#include "ypath_resolver.h"

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/yson/forwarding_consumer.h>
#include <yt/core/yson/parser.h>
#include <yt/core/yson/writer.h>

#include <yt/core/misc/error.h>

namespace NYT {
namespace NYTree {

using NYPath::ETokenType;
using NYPath::TTokenizer;

using NYson::IYsonConsumer;
using NYson::TForwardingYsonConsumer;
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
    : public TForwardingYsonConsumer
{
public:
    TYPathResolver(const TYPath& path, bool isAny)
        : Tokenizer_(path)
        , IsAny_(isAny)
    {
        if (isAny) {
            ResultWriter_ = std::make_unique<NYson::TBufferedBinaryYsonWriter>(&ResultStream_);
        }

        NextToken();
    }

    virtual void OnMyStringScalar(const TStringBuf& value) override
    {
        OnStringValue(value);
    }

    virtual void OnMyInt64Scalar(i64 value) override
    {
        OnValue(value);
    }

    virtual void OnMyUint64Scalar(ui64 value) override
    {
        OnValue(value);
    }

    virtual void OnMyDoubleScalar(double value) override
    {
        OnValue(value);
    }

    virtual void OnMyBooleanScalar(bool value) override
    {
        OnValue(value);
    }

    virtual void OnMyEntity() override
    { }

    virtual void OnMyBeginList() override
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

    virtual void OnMyListItem() override
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

    virtual void OnMyEndList() override
    {
        OnDecDepth();
    }

    virtual void OnMyBeginMap() override
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

    virtual void OnMyKeyedItem(const TStringBuf& key) override
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

    virtual void OnMyEndMap() override
    {
        OnDecDepth();
    }

    virtual void OnMyBeginAttributes() override
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

    virtual void OnMyEndAttributes() override
    {
        OnDecDepth();
    }

    virtual void OnMyRaw(const TStringBuf& yson, EYsonType type) override
    { }

private:
    TTokenizer Tokenizer_;
    const bool IsAny_ = false;

    EExpectedItem Expected_ = EExpectedItem::Initial;
    TString Literal_;
    int ListIndex_ = 0;
    int CurrentListIndex_ = 0;
    int CurrentDepth_ = 0;
    int PathDepth_ = 0;

    TStringStream ResultStream_;
    std::unique_ptr<NYson::IFlushableYsonConsumer> ResultWriter_;

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

    void OnForwardingFinished()
    { }

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

                    if (IsAny_) {
                        Forward(ResultWriter_.get(), [this] {
                            ResultWriter_->Flush();
                            throw TCompleteParsingWithResultException<TString>(ResultStream_.Str());
                        });
                    } else {
                        Expected_ = EExpectedItem::Value;
                    }
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
TNullable<T> TryGetValue(const TStringBuf& yson, const TYPath& ypath, bool isAny = false)
{
    try {
        TYPathResolver resolver(ypath, isAny);
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

TNullable<TString> TryGetAny(const TStringBuf& yson, const TYPath& ypath)
{
    return TryGetValue<TString>(yson, ypath, true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
