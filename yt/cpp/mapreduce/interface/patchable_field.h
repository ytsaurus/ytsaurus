#pragma once

#include "client.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// |NYT::TConfig| field supporting per-client overriding with cluster configuration.
// |T| must be one of |TNode| value types.
template <typename T>
class TPatchableField
{
public:
    static constexpr char ConfigProfile[] = "default";

    TPatchableField(TString name, T defaultValue)
        : Name_(move(name))
        , Value_(std::move(defaultValue))
    { }

    const T& Get(const IClientPtr& client)
    {
        if (!Patched_) {
            const auto& clusterConfig = client->GetDynamicConfiguration(ConfigProfile);
            auto iter = clusterConfig.find(Name_);
            if (!iter.IsEnd()) {
                Value_ = iter->second.As<T>();
            }
            Patched_ = true;
        }
        return Value_;
    }

    void Set(const T& value)
    {
        Value_ = value;
        Patched_ = true;
    }

private:
    const TString Name_;
    T Value_;
    bool Patched_ = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
