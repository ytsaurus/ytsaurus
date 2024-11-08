#include "patch_unwrapping_consumer.h"

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

TPatchUnwrappingConsumer::TPatchUnwrappingConsumer(NYson::IYsonConsumer* underlying)
    : Underlying_(underlying)
{ }

void TPatchUnwrappingConsumer::OnMyBeginAttributes()
{
    Forward(Builder_.GetConsumer(), /*onFinished*/ nullptr, NYson::EYsonType::MapFragment);
}

void TPatchUnwrappingConsumer::OnMyEndAttributes()
{
    AttributesFragment_ = Builder_.Flush();
}

void TPatchUnwrappingConsumer::OnMyBeginMap()
{ }

void TPatchUnwrappingConsumer::OnMyEndMap()
{ }

void TPatchUnwrappingConsumer::OnMyKeyedItem(TStringBuf key)
{
    Underlying_->OnKeyedItem(key);
    if (AttributesFragment_) {
        Underlying_->OnBeginAttributes();
        Underlying_->OnRaw(AttributesFragment_->AsStringBuf(), NYson::EYsonType::MapFragment);
        Underlying_->OnEndAttributes();
    }
    Forward(Underlying_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
