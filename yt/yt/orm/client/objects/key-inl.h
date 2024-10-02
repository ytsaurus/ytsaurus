#ifndef KEY_INL_H_
#error "Direct inclusion of this file is not allowed; include key.h"
// For the sake of sane code completion.
#include "key.h"
#endif

namespace NYT::NOrm::NClient::NObjects {

////////////////////////////////////////////////////////////////////////////////

template<typename... Ts>
TObjectKey::TObjectKey(Ts... keyFields)
    : KeyFields_({TKeyField(std::move(keyFields))...})
{ }

template<typename T>
T TObjectKey::GetWithDefault(size_t i, T defaultValue) const
{
    if (i < size()) {
        return std::get<T>(KeyFields_[i]);
    } else {
        return std::move(defaultValue);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NObjects

template <>
struct THash<NYT::NOrm::NClient::NObjects::TObjectKey>
{
    inline NYT::TFingerprint operator()(const NYT::NOrm::NClient::NObjects::TObjectKey& key) const
    {
        return key.CalculateHash();
    }
};
