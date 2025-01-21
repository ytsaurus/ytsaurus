#pragma once


#include <yt/yt/core/yson/yson_builder.h>
#include <yt/yt/core/yson/forwarding_consumer.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

class TPatchUnwrappingConsumer
    : public NYson::TForwardingYsonConsumer
{
public:
    explicit TPatchUnwrappingConsumer(NYson::IYsonConsumer* underlying);

    void OnMyBeginAttributes() override;

    void OnMyEndAttributes() override;

    void OnMyBeginMap() override;

    void OnMyEndMap() override;

    void OnMyKeyedItem(TStringBuf key) override;

private:
    NYson::IYsonConsumer* const Underlying_;
    NYson::TYsonStringBuilder Builder_;
    std::optional<NYson::TYsonString> AttributesFragment_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
