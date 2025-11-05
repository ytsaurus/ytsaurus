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

TPatchUnwrappingConsumer::TState TPatchUnwrappingConsumer::GetState()
{
    return TState{
        .BaseState = TBase::GetState(),
        .BuilderCheckpoint = Builder_.CreateCheckpoint(),
        .AttributesFragmentHasValue = AttributesFragment_.has_value()};
}

void TPatchUnwrappingConsumer::SetState(const TState& state)
{
    TBase::SetState(state.BaseState);
    Builder_.RestoreCheckpoint(state.BuilderCheckpoint);
    AttributesFragment_ = state.AttributesFragmentHasValue
        ? std::optional<NYson::TYsonString>(Builder_.GetYsonString())
        : std::nullopt;
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
