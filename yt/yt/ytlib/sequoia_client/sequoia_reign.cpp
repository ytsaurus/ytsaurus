#include "sequoia_reign.h"
#include "public.h"

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

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

TFuture<void> ValidateClusterGroundReign(
    const NApi::NNative::IClientPtr& client,
    const NYTree::TYPath& rootPath)
{
    auto attributesPath = rootPath + "/@";
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
