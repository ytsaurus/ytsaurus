#include "ypath_resolver.h"

#include <yt/core/ypath/tokenizer.h>

#include <yt/core/yson/forwarding_consumer.h>
#include <yt/core/yson/parser.h>
#include <yt/core/yson/writer.h>

#include <yt/core/misc/error.h>
#include <yt/core/misc/variant.h>

namespace NYT::NYTree {

using NYPath::ETokenType;
using NYPath::TTokenizer;

using NYson::IYsonConsumer;
using NYson::TForwardingYsonConsumer;
using NYson::EYsonType;
using NYson::TStatelessYsonParser;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EExpectedItem,
    (Initial)
    (BeginMap)
    (Key)
    (BeginAttribute)
    (ListItem)
    (Value)
);

struct TNullLiteralValue
{ };

class TYPathResolver
    : public TForwardingYsonConsumer
{
public:
    TYPathResolver(const TYPath& path, bool isAny)
        : Tokenizer_(path)
        , IsAny_(isAny)
        , Parser_(this)
    {
        if (isAny) {
            ResultWriter_ = std::make_unique<NYson::TBufferedBinaryYsonWriter>(&ResultStream_);
        }

        NextToken();
    }

    virtual void OnMyStringScalar(TStringBuf value) override
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
                        Parser_.Stop();
                        return;
                    }
                    ++PathDepth_;
                    break;

                case EExpectedItem::ListItem:
                case EExpectedItem::Key:
                    break;

                default:
                    Parser_.Stop();
                    break;
            }
        }
    }

    virtual void OnMyListItem() override
    {
        if (CurrentDepth_ == PathDepth_) {
            if (Expected_ != EExpectedItem::ListItem) {
                Parser_.Stop();
                return;
            }
            if (CurrentListIndex_ == ListIndex_) {
                NextToken();
            } else if (CurrentListIndex_ > ListIndex_) {
                Parser_.Stop();
                return;
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
                    Parser_.Stop();
                    break;
            }
        }
    }

    virtual void OnMyKeyedItem(TStringBuf key) override
    {
        if (CurrentDepth_ == PathDepth_) {
            if (Expected_ != EExpectedItem::Key) {
                Parser_.Stop();
                return;
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
                    Parser_.Stop();
                    break;
            }
        }
    }

    virtual void OnMyEndAttributes() override
    {
        OnDecDepth();
    }

    virtual void OnMyRaw(TStringBuf yson, EYsonType type) override
    { }

    void Parse(TStringBuf input)
    {
        Parser_.Parse(input);
    }

    typedef TVariant<TNullLiteralValue, bool, i64, ui64, double, TString> TResult;

    DEFINE_BYREF_RO_PROPERTY(TResult, Result, TVariantTypeTag<TNullLiteralValue>());

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

    TStatelessYsonParser Parser_;

    void OnDecDepth()
    {
        if (--CurrentDepth_ < PathDepth_) {
            Parser_.Stop();
        }
    }

    template <typename T>
    void OnValue(const T& t)
    {
        if (Expected_ == EExpectedItem::Value) {
            Result_ = t;
            Parser_.Stop();
        }
    }

    void OnStringValue(TStringBuf t)
    {
        if (Expected_ == EExpectedItem::Value) {
            Result_ = TString(t);
            Parser_.Stop();
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
                            Result_ = ResultStream_.Str();
                            Parser_.Stop();
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
std::optional<T> TryGetValue(TStringBuf yson, const TYPath& ypath, bool isAny = false)
{
    TYPathResolver resolver(ypath, isAny);
    resolver.Parse(yson);

    if (const auto* value = resolver.Result().TryAs<T>()) {
        return *value;
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

std::optional<i64> TryGetInt64(TStringBuf yson, const TYPath& ypath)
{
    return TryGetValue<i64>(yson, ypath);
}

std::optional<ui64> TryGetUint64(TStringBuf yson, const TYPath& ypath)
{
    return TryGetValue<ui64>(yson, ypath);
}

std::optional<bool> TryGetBoolean(TStringBuf yson, const TYPath& ypath)
{
    return TryGetValue<bool>(yson, ypath);
}

std::optional<double> TryGetDouble(TStringBuf yson, const TYPath& ypath)
{
    return TryGetValue<double>(yson, ypath);
}

std::optional<TString> TryGetString(TStringBuf yson, const TYPath& ypath)
{
    return TryGetValue<TString>(yson, ypath);
}

std::optional<TString> TryGetAny(TStringBuf yson, const TYPath& ypath)
{
    return TryGetValue<TString>(yson, ypath, true);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
