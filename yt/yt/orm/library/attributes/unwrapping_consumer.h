#pragma once

#include <yt/yt/core/yson/forwarding_consumer.h>

namespace NYT::NOrm::NAttributes {

////////////////////////////////////////////////////////////////////////////////

class TUnwrappingConsumer
    : public NYson::TForwardingYsonConsumer
{
public:
    explicit TUnwrappingConsumer(NYson::IYsonConsumer* underlying);

    void OnMyBeginMap() override;

    void OnMyEndMap() override;

    void OnMyEntity() override;

    void OnMyRaw(TStringBuf yson, NYson::EYsonType type) override;

private:
    NYson::IYsonConsumer* const Underlying_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NAttributes
