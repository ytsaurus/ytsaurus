#include "attribute_consumer.h"
#include "writer.h"

namespace NYT::NYson {

////////////////////////////////////////////////////////////////////////////////

TAttributeFragmentConsumer::TAttributeFragmentConsumer(IAsyncYsonConsumer* underlyingConsumer)
    : UnderlyingConsumer_(underlyingConsumer)
{ }

TAttributeFragmentConsumer::~TAttributeFragmentConsumer()
{
    End();
}

void TAttributeFragmentConsumer::OnRaw(TFuture<TYsonString> asyncStr)
{
    Begin();
    UnderlyingConsumer_->OnRaw(std::move(asyncStr));
}

void TAttributeFragmentConsumer::OnRaw(TStringBuf yson, EYsonType type)
{
    if (!yson.empty()) {
        Begin();
        UnderlyingConsumer_->OnRaw(yson, type);
    }
}

// Calling Begin() on other events is redundant.

void TAttributeFragmentConsumer::OnEndAttributes()
{
    UnderlyingConsumer_->OnEndAttributes();
}

void TAttributeFragmentConsumer::OnBeginAttributes()
{
    UnderlyingConsumer_->OnBeginAttributes();
}

void TAttributeFragmentConsumer::OnEndMap()
{
    UnderlyingConsumer_->OnEndMap();
}

void TAttributeFragmentConsumer::OnKeyedItem(TStringBuf key)
{
    Begin();
    UnderlyingConsumer_->OnKeyedItem(key);
}

void TAttributeFragmentConsumer::OnBeginMap()
{
    UnderlyingConsumer_->OnBeginMap();
}

void TAttributeFragmentConsumer::OnEndList()
{
    UnderlyingConsumer_->OnEndList();
}

void TAttributeFragmentConsumer::OnListItem()
{
    UnderlyingConsumer_->OnListItem();
}

void TAttributeFragmentConsumer::OnBeginList()
{
    UnderlyingConsumer_->OnBeginList();
}

void TAttributeFragmentConsumer::OnEntity()
{
    UnderlyingConsumer_->OnEntity();
}

void TAttributeFragmentConsumer::OnBooleanScalar(bool value)
{
    UnderlyingConsumer_->OnBooleanScalar(value);
}

void TAttributeFragmentConsumer::OnDoubleScalar(double value)
{
    UnderlyingConsumer_->OnDoubleScalar(value);
}

void TAttributeFragmentConsumer::OnUint64Scalar(ui64 value)
{
    UnderlyingConsumer_->OnUint64Scalar(value);
}

void TAttributeFragmentConsumer::OnInt64Scalar(i64 value)
{
    UnderlyingConsumer_->OnInt64Scalar(value);
}

void TAttributeFragmentConsumer::OnStringScalar(TStringBuf value)
{
    UnderlyingConsumer_->OnStringScalar(value);
}

void TAttributeFragmentConsumer::Begin()
{
    if (!HasAttributes_) {
        UnderlyingConsumer_->OnBeginAttributes();
        HasAttributes_ = true;
    }
}

void TAttributeFragmentConsumer::End()
{
    if (HasAttributes_) {
        UnderlyingConsumer_->OnEndAttributes();
    }
}

////////////////////////////////////////////////////////////////////////////////

TAttributeValueConsumer::TAttributeValueConsumer(
    IAsyncYsonConsumer* underlyingConsumer,
    TStringBuf key)
    : UnderlyingConsumer_(underlyingConsumer)
    , Key_(key)
{ }

void TAttributeValueConsumer::OnStringScalar(TStringBuf value)
{
    ProduceKeyIfNeeded();
    UnderlyingConsumer_->OnStringScalar(value);
}

void TAttributeValueConsumer::OnInt64Scalar(i64 value)
{
    ProduceKeyIfNeeded();
    UnderlyingConsumer_->OnInt64Scalar(value);
}

void TAttributeValueConsumer::OnUint64Scalar(ui64 value)
{
    ProduceKeyIfNeeded();
    UnderlyingConsumer_->OnUint64Scalar(value);
}

void TAttributeValueConsumer::OnDoubleScalar(double value)
{
    ProduceKeyIfNeeded();
    UnderlyingConsumer_->OnDoubleScalar(value);
}

void TAttributeValueConsumer::OnBooleanScalar(bool value)
{
    ProduceKeyIfNeeded();
    UnderlyingConsumer_->OnBooleanScalar(value);
}

void TAttributeValueConsumer::OnEntity()
{
    ProduceKeyIfNeeded();
    UnderlyingConsumer_->OnEntity();
}

void TAttributeValueConsumer::OnBeginList()
{
    ProduceKeyIfNeeded();
    UnderlyingConsumer_->OnBeginList();
}

void TAttributeValueConsumer::OnListItem()
{
    ProduceKeyIfNeeded();
    UnderlyingConsumer_->OnListItem();
}

void TAttributeValueConsumer::OnEndList()
{
    ProduceKeyIfNeeded();
    UnderlyingConsumer_->OnEndList();
}

void TAttributeValueConsumer::OnBeginMap()
{
    ProduceKeyIfNeeded();
    UnderlyingConsumer_->OnBeginMap();
}

void TAttributeValueConsumer::OnKeyedItem(TStringBuf key)
{
    ProduceKeyIfNeeded();
    UnderlyingConsumer_->OnKeyedItem(key);
}

void TAttributeValueConsumer::OnEndMap()
{
    ProduceKeyIfNeeded();
    UnderlyingConsumer_->OnEndMap();
}

void TAttributeValueConsumer::OnBeginAttributes()
{
    ProduceKeyIfNeeded();
    UnderlyingConsumer_->OnBeginAttributes();
}

void TAttributeValueConsumer::OnEndAttributes()
{
    ProduceKeyIfNeeded();
    UnderlyingConsumer_->OnEndAttributes();
}

void TAttributeValueConsumer::OnRaw(TStringBuf yson, EYsonType type)
{
    ProduceKeyIfNeeded();
    UnderlyingConsumer_->OnRaw(yson, type);
}

void TAttributeValueConsumer::OnRaw(TFuture<TYsonString> asyncStr)
{
    if (Empty_) {
        auto key = Key_;
        UnderlyingConsumer_->OnRaw(asyncStr.Apply(BIND([key] (const TYsonString& str) {
            if (str) {
                YCHECK(str.GetType() == EYsonType::Node);
                TStringStream stream;
                TBufferedBinaryYsonWriter writer(&stream, EYsonType::MapFragment);
                writer.OnKeyedItem(key);
                writer.OnRaw(str);
                writer.Flush();
                return TYsonString(stream.Str(), EYsonType::MapFragment);
            } else {
                return TYsonString(TString(), EYsonType::MapFragment);
            }
        })));
    } else {
        UnderlyingConsumer_->OnRaw(std::move(asyncStr));
    }
}

void TAttributeValueConsumer::ProduceKeyIfNeeded()
{
    if (Empty_) {
        UnderlyingConsumer_->OnKeyedItem(Key_);
        Empty_ = false;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYson
