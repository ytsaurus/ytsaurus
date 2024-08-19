#include "yson_builder.h"

#include <yt/yt/core/yson/writer.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

NYson::IYsonConsumer* IYsonBuilder::operator->()
{
    return GetConsumer();
}

////////////////////////////////////////////////////////////////////////////////

TYsonStringBuilder::TYsonStringBuilder(NYson::EYsonFormat format, NYson::EYsonType type, bool enableRaw)
    : Output_(ValueString_)
    , Writer_(CreateYsonWriter(&Output_, format, type, enableRaw))
{ }

NYson::IYsonConsumer* TYsonStringBuilder::GetConsumer()
{
    return Writer_.get();
}

IYsonBuilder::TCheckpoint TYsonStringBuilder::CreateCheckpoint()
{
    Writer_->Flush();
    return IYsonBuilder::TCheckpoint(std::ssize(ValueString_));
}

void TYsonStringBuilder::RestoreCheckpoint(IYsonBuilder::TCheckpoint checkpoint)
{
    Writer_->Flush();
    int checkpointSize = checkpoint.Underlying();
    YT_VERIFY(checkpointSize >= 0 && checkpointSize <= std::ssize(ValueString_));
    ValueString_.resize(static_cast<size_t>(checkpointSize));
}

NYson::TYsonString TYsonStringBuilder::Flush()
{
    Writer_->Flush();
    auto result = NYson::TYsonString(ValueString_);
    ValueString_.clear();
    return result;
}

bool TYsonStringBuilder::IsEmpty()
{
    Writer_->Flush();
    return ValueString_.Empty();
}

////////////////////////////////////////////////////////////////////////////////

TYsonBuilder::TYsonBuilder(
    EYsonBuilderForwardingPolicy policy,
    IYsonBuilder* underlying,
    NYson::IYsonConsumer* consumer)
    : Policy_(policy)
    , Underlying_(underlying)
    , Consumer_(consumer)
{ }

NYson::IYsonConsumer* TYsonBuilder::GetConsumer()
{
    return Consumer_;
}

IYsonBuilder::TCheckpoint TYsonBuilder::CreateCheckpoint()
{
    switch (Policy_) {
        case EYsonBuilderForwardingPolicy::Forward:
            return Underlying_->CreateCheckpoint();
        case EYsonBuilderForwardingPolicy::Crash:
            return IYsonBuilder::TCheckpoint{};
        case EYsonBuilderForwardingPolicy::Ignore:
            return IYsonBuilder::TCheckpoint{};
    }
}

void TYsonBuilder::RestoreCheckpoint(TCheckpoint checkpoint)
{
    switch (Policy_) {
        case EYsonBuilderForwardingPolicy::Forward:
            Underlying_->RestoreCheckpoint(checkpoint);
            break;
        case EYsonBuilderForwardingPolicy::Crash:
            YT_ABORT();
        case EYsonBuilderForwardingPolicy::Ignore:
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
