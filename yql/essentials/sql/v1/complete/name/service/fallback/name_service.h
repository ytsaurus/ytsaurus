#pragma once

#include <yql/essentials/sql/v1/complete/name/service/name_service.h>

namespace NSQLComplete {

    using TFallbackPolicy = std::function<bool(const std::exception&)>;

    inline TFallbackPolicy FallbackPolicyAlways = [](const std::exception&) { return true; };

    INameService::TPtr MakeFallbackNameService(
        INameService::TPtr primary,
        INameService::TPtr standby,
        TFallbackPolicy policy = FallbackPolicyAlways);

    INameService::TPtr MakeEmptyNameService();

    INameService::TPtr MakeSwallowingNameService(
        INameService::TPtr origin,
        TFallbackPolicy policy = FallbackPolicyAlways);

} // namespace NSQLComplete
