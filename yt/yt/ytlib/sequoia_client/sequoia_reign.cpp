#include "sequoia_reign.h"
#include "public.h"

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

ESequoiaReign GetCurrentSequoiaReign() noexcept
{
    return TEnumTraits<ESequoiaReign>::GetMaxValue();
}

////////////////////////////////////////////////////////////////////////////////

EGroundReign GetCurrentGroundReign()
{
    return TEnumTraits<EGroundReign>::GetMaxValue();
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> ValidateClusterGroundReign(const NApi::NNative::IClientPtr& client)
{
    auto attributesPath = SequoiaRootCypressPath + "/@";
    return client->GetNode(attributesPath)
        .Apply(BIND([] (NYson::TYsonString value) {
            auto attributes = NYTree::ConvertToAttributes(value);
            auto actualReign = EGroundReign(
                attributes->Get<int>("ground_reign", static_cast<int>(EGroundReign::Unknown)));
            auto expectedReign = GetCurrentGroundReign();
            THROW_ERROR_EXCEPTION_IF(actualReign != expectedReign,
                "Ground reigns differ (Expected: %v, Actual: %v)",
                expectedReign,
                actualReign);
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
