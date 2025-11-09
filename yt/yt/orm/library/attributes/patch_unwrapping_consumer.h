#pragma once


#include <yt/yt/core/yson/yson_builder.h>
#include <yt/yt/core/yson/forwarding_consumer.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

class TPatchUnwrappingConsumer
    : public NYson::TForwardingYsonConsumer
{
public:
    using TBase = NYson::TForwardingYsonConsumer;

    struct TState
    {
        TBase::TState BaseState;
        NYson::TYsonStringBuilder::TCheckpoint BuilderCheckpoint;
        bool AttributesFragmentHasValue;
    };

public:
    explicit TPatchUnwrappingConsumer(NYson::IYsonConsumer* underlying);

    void OnMyBeginAttributes() override;

    void OnMyEndAttributes() override;

    void OnMyBeginMap() override;

    void OnMyEndMap() override;

    void OnMyKeyedItem(TStringBuf key) override;

    TState GetState();
    void SetState(const TState& state);

private:
    NYson::IYsonConsumer* const Underlying_;
    NYson::TYsonStringBuilder Builder_;
    std::optional<NYson::TYsonString> AttributesFragment_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
