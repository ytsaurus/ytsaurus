#include "stdafx.h"
#include "attribute_fragment_consumer.h"

namespace NYT {
namespace NYson {

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

void TAttributeFragmentConsumer::OnRaw(const TStringBuf& yson, EYsonType type)
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

void TAttributeFragmentConsumer::OnKeyedItem(const TStringBuf& key)
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

void TAttributeFragmentConsumer::OnStringScalar(const TStringBuf& value)
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

} // namespace NYson
} // namespace NYT
