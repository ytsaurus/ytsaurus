#ifndef NODE_INL_H_
#error "Direct inclusion of this file is not allowed, include node.h"
// For the sake of sane code completion.
#include "node.h"
#endif

namespace NYT::NCypressServer {

////////////////////////////////////////////////////////////////////////////////

template <class T>
TClonableBuiltinAttributePtr<T>::TClonableBuiltinAttributePtr() noexcept
{ }

template <class T>
TClonableBuiltinAttributePtr<T>::TClonableBuiltinAttributePtr(const T& value)
    : Value_(ConstructCopy(value))
{ }

template <class T>
TClonableBuiltinAttributePtr<T>::TClonableBuiltinAttributePtr(T&& value)
    : Value_(std::make_unique<T>(std::move(value)))
{ }

template <class T>
template <class... TArgs>
TClonableBuiltinAttributePtr<T>::TClonableBuiltinAttributePtr(std::in_place_t, TArgs&&... args)
    : Value_(std::make_unique<T>(std::forward<TArgs>(args)...))
{ }

template <class T>
TClonableBuiltinAttributePtr<T> TClonableBuiltinAttributePtr<T>::Clone() const
{
    return TClonableBuiltinAttributePtr(*Value_);
}

template <class T>
TClonableBuiltinAttributePtr<T>::operator bool() const noexcept
{
    return static_cast<bool>(Value_);
}

template <class T>
T* TClonableBuiltinAttributePtr<T>::operator->() const noexcept
{
    return Get();
}

template <class T>
T* TClonableBuiltinAttributePtr<T>::Get() const noexcept
{
    return Value_.get();
}

template <class T>
void TClonableBuiltinAttributePtr<T>::Save(NCellMaster::TSaveContext& context) const
{
    TUniquePtrSerializer<>::Save(context, Value_);
}

template <class T>
void TClonableBuiltinAttributePtr<T>::Load(NCellMaster::TLoadContext& context)
{
    TUniquePtrSerializer<>::Load(context, Value_);
}

template <class T>
std::unique_ptr<T> TClonableBuiltinAttributePtr<T>::ConstructCopy(const T& value)
{
    if constexpr (NObjectServer::CClonable<T>) {
        return std::make_unique<T>(value.Clone());
    } else {
        return std::make_unique<T>(value);
    }
}

////////////////////////////////////////////////////////////////////////////////

inline TCypressNodeDynamicData* TCypressNode::GetDynamicData() const
{
    return GetTypedDynamicData<TCypressNodeDynamicData>();
}

inline int TCypressNode::GetAccessStatisticsUpdateIndex() const
{
    return GetDynamicData()->AccessStatisticsUpdateIndex;
}

inline void TCypressNode::SetAccessStatisticsUpdateIndex(int value)
{
    GetDynamicData()->AccessStatisticsUpdateIndex = value;
}

inline int TCypressNode::GetTouchNodesIndex() const
{
    return GetDynamicData()->TouchNodesIndex;
}

inline void TCypressNode::SetTouchNodesIndex(int value)
{
    GetDynamicData()->TouchNodesIndex = value;
}

inline std::optional<TCypressNodeExpirationMap::iterator> TCypressNode::GetExpirationTimeIterator() const
{
    return GetDynamicData()->ExpirationTimeIterator;
}

inline void TCypressNode::SetExpirationTimeIterator(std::optional<TCypressNodeExpirationMap::iterator> value)
{
    GetDynamicData()->ExpirationTimeIterator = value;
}

inline std::optional<TCypressNodeExpirationMap::iterator> TCypressNode::GetExpirationTimeoutIterator() const
{
    return GetDynamicData()->ExpirationTimeoutIterator;
}

inline void TCypressNode::SetExpirationTimeoutIterator(std::optional<TCypressNodeExpirationMap::iterator> value)
{
    GetDynamicData()->ExpirationTimeoutIterator = value;
}

////////////////////////////////////////////////////////////////////////////////

inline bool TCypressNodeIdComparer::operator()(const TCypressNode* lhs, const TCypressNode* rhs) const
{
    return Compare(lhs, rhs);
}

inline bool TCypressNodeIdComparer::Compare(const TCypressNode* lhs, const TCypressNode* rhs)
{
    return lhs->GetVersionedId() < rhs->GetVersionedId();
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
T TVersionedBuiltinAttributeTraits<T>::ToRaw(T value)
{
    return std::move(value);
}

template <class T>
T TVersionedBuiltinAttributeTraits<T>::FromRaw(T value)
{
    return std::move(value);
}

template <class T>
T* TVersionedBuiltinAttributeTraits<NObjectServer::TStrongObjectPtr<T>>::ToRaw(const NObjectServer::TStrongObjectPtr<T>& value)
{
    return value.Get();
}

template <class T>
NObjectServer::TStrongObjectPtr<T> TVersionedBuiltinAttributeTraits<NObjectServer::TStrongObjectPtr<T>>::FromRaw(T* value)
{
    return NObjectServer::TStrongObjectPtr<T>(value);
}

template <class T>
T* TVersionedBuiltinAttributeTraits<TClonableBuiltinAttributePtr<T>>::ToRaw(const TClonableBuiltinAttributePtr<T>& value)
{
    return value.Get();
}

template <class T>
TClonableBuiltinAttributePtr<T> TVersionedBuiltinAttributeTraits<TClonableBuiltinAttributePtr<T>>::FromRaw(T* value)
{
    if (!value) {
        return {};
    }
    return TClonableBuiltinAttributePtr<T>(*value);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TVersionedBuiltinAttribute<T>::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, BoxedValue_);
}

template <class T>
void TVersionedBuiltinAttribute<T>::Save(TSerializeNodeContext& context) const
{
    using TTraits = TVersionedBuiltinAttributeTraits<T>;
    using NYT::Save;
    if constexpr (TTraits::SerializeAsRaw && TTraits::IsInherentlyNullable) {
        Save(context, BoxedValue_.transform([] (const auto& unboxedValue) {
            return TTraits::ToRaw(unboxedValue);
        }));
    } else {
        Save(context, BoxedValue_);
    }
}

template <class T>
void TVersionedBuiltinAttribute<T>::Load(TMaterializeNodeContext& context)
{
    using TTraits = TVersionedBuiltinAttributeTraits<T>;
    using NYT::Load;
    if constexpr (TTraits::SerializeAsRaw && TTraits::IsInherentlyNullable) {
        BoxedValue_ = Load<std::optional<TRawVersionedBuiltinAttributeType<T>>>(context)
            .transform([] (TRawVersionedBuiltinAttributeType<T>&& rawValue) {
                return TTraits::FromRaw(std::move(rawValue));
            });
    } else {
        Load(context, BoxedValue_);
    }
}

template <class T>
void TVersionedBuiltinAttribute<T>::Set(T value)
{
    if constexpr (TVersionedBuiltinAttributeTraits<T>::IsInherentlyNullable) {
        // default values are not allowed; Remove() should be used instead.
        YT_VERIFY(value);
    }

    BoxedValue_ = std::move(value);
}

template <class T>
void TVersionedBuiltinAttribute<T>::Reset()
{
    if constexpr (TVersionedBuiltinAttributeTraits<T>::IsInherentlyNullable) {
        BoxedValue_ = std::nullopt;
    } else {
        BoxedValue_ = TNullVersionedBuiltinAttribute{};
    }
}

template <class T>
void TVersionedBuiltinAttribute<T>::Remove()
{
    if constexpr (TVersionedBuiltinAttributeTraits<T>::IsInherentlyNullable) {
        BoxedValue_.emplace();
    } else {
        BoxedValue_ = TTombstonedVersionedBuiltinAttribute{};
    }
}

template <class T>
bool TVersionedBuiltinAttribute<T>::IsNull() const
{
    if constexpr (TVersionedBuiltinAttributeTraits<T>::IsInherentlyNullable) {
        return !BoxedValue_.has_value();
    } else {
        return std::holds_alternative<TNullVersionedBuiltinAttribute>(BoxedValue_);
    }
}

template <class T>
bool TVersionedBuiltinAttribute<T>::IsTombstoned() const
{
    if constexpr (TVersionedBuiltinAttributeTraits<T>::IsInherentlyNullable) {
        return BoxedValue_ && !*BoxedValue_;
    } else {
        return std::holds_alternative<TTombstonedVersionedBuiltinAttribute>(BoxedValue_);
    }
}

template <class T>
bool TVersionedBuiltinAttribute<T>::IsSet() const
{
    if constexpr (TVersionedBuiltinAttributeTraits<T>::IsInherentlyNullable) {
        return BoxedValue_ && *BoxedValue_;
    } else {
        return std::holds_alternative<T>(BoxedValue_);
    }
}

template <class T>
TRawVersionedBuiltinAttributeType<T> TVersionedBuiltinAttribute<T>::Unbox() const
{
    if constexpr (TVersionedBuiltinAttributeTraits<T>::IsInherentlyNullable) {
        const auto& result = *BoxedValue_;
        YT_VERIFY(result);
        return TVersionedBuiltinAttributeTraits<T>::ToRaw(result);
    } else {
        auto* result = std::get_if<T>(&BoxedValue_);
        YT_VERIFY(result);
        return TVersionedBuiltinAttributeTraits<T>::ToRaw(*result);
    }
}

template <class T>
std::optional<TRawVersionedBuiltinAttributeType<T>> TVersionedBuiltinAttribute<T>::ToOptional() const
{
    return IsSet() ? std::make_optional(Unbox()) : std::nullopt;
}

template <class T>
template <class TOwner>
/*static*/ TRawVersionedBuiltinAttributeType<T> TVersionedBuiltinAttribute<T>::Get(
    TVersionedBuiltinAttribute<T> TOwner::*member,
    const TOwner* node)
{
    auto result = TryGet(member, node);
    YT_VERIFY(result);
    return *std::move(result);
}

template <class T>
template <class TOwner>
/*static*/ TRawVersionedBuiltinAttributeType<T> TVersionedBuiltinAttribute<T>::Get(
    const TVersionedBuiltinAttribute<T>* (TOwner::*memberGetter)() const,
    const TOwner* node)
{
    auto result = TryGet(memberGetter, node);
    YT_VERIFY(result);
    return *std::move(result);
}

template <class T>
template <class TOwner>
/*static*/ std::optional<TRawVersionedBuiltinAttributeType<T>> TVersionedBuiltinAttribute<T>::TryGet(
    TVersionedBuiltinAttribute<T> TOwner::*member,
    const TOwner* node)
{
    auto* currentNode = node;
    for (;;) {
        const auto& attribute = currentNode->*member;
        if (!attribute.IsNull()) {
            if (attribute.IsTombstoned()) {
                return std::nullopt;
            }

            return attribute.Unbox();
        }

        auto* originator = currentNode->GetOriginator();
        if (!originator) {
            break;
        }
        currentNode = originator->template As<TOwner>();
    }

    return std::nullopt;
}

template <class T>
template <class TOwner>
/*static*/ std::optional<TRawVersionedBuiltinAttributeType<T>> TVersionedBuiltinAttribute<T>::TryGet(
    const TVersionedBuiltinAttribute<T>* (TOwner::*memberGetter)() const,
    const TOwner* node)
{
    auto* currentNode = node;
    for (;;) {
        auto* attribute = (currentNode->*memberGetter)();
        if (attribute && !attribute->IsNull()) {
            if (attribute->IsTombstoned()) {
                return std::nullopt;
            }

            return attribute->Unbox();
        }

        auto* originator = currentNode->GetOriginator();
        if (!originator) {
            break;
        }
        currentNode = originator->template As<TOwner>();
    }

    return std::nullopt;
}

template <class T>
void TVersionedBuiltinAttribute<T>::Merge(const TVersionedBuiltinAttribute& from, bool isTrunk)
{
    if (from.IsTombstoned()) {
        if (isTrunk) {
            Reset();
        } else {
            Remove();
        }
    } else if (!from.IsNull()) {
        Set(TVersionedBuiltinAttributeTraits<T>::FromRaw(from.Unbox()));
    } // NB: Null attributes are ignored.
}

template <class T>
TVersionedBuiltinAttribute<T> TVersionedBuiltinAttribute<T>::Clone() const
{
    TVersionedBuiltinAttribute<T> result;
    if constexpr (std::copyable<T>) {
        result.BoxedValue_ = BoxedValue_;
    } else {
        static_assert(NObjectServer::CClonable<T>);
        result.BoxedValue_ = BoxedValue_ ? std::optional(BoxedValue_->Clone()) : std::nullopt;
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

template <std::copyable TTime>
TCypressNode::TExpirationProperties<TTime>::TExpirationProperties(
    NSecurityServer::TUserPtr user, TTime expiration)
    : ExpirationValue_(std::in_place_index<0>, std::move(user), std::move(expiration))
{ }

template <std::copyable TTime>
TCypressNode::TExpirationProperties<TTime>::TExpirationProperties(
    TInstant lastResetTime)
    : ExpirationValue_(std::in_place_index<1>, lastResetTime)
{ }

template <std::copyable TTime>
TCypressNode::TExpirationProperties<TTime> TCypressNode::TExpirationProperties<TTime>::Clone() const
{
    return Visit(
        ExpirationValue_,
        [] (const TValue& value) {
            const auto& [user, expiration] = value;
            return TExpirationProperties(user.Clone(), expiration);
        },
        [] (const TLastResetInstant& lastResetInstant) {
            return TExpirationProperties(lastResetInstant);
        }
    );
}

template <std::copyable TTime>
void TCypressNode::TExpirationProperties<TTime>::Save(NCellMaster::TSaveContext& context) const
{
    using NYT::Save;
    Save(context, ExpirationValue_);
}

template <std::copyable TTime>
void TCypressNode::TExpirationProperties<TTime>::Load(NCellMaster::TLoadContext& context)
{
    using NYT::Load;
    Load(context, ExpirationValue_);
}

template <std::copyable TTime>
bool TCypressNode::TExpirationProperties<TTime>::IsReset() const
{
    return !std::holds_alternative<TValue>(ExpirationValue_);
}

template <std::copyable TTime>
std::optional<typename TCypressNode::TExpirationProperties<TTime>::TView> TCypressNode::TExpirationProperties<TTime>::AsView() const
{
    if (!std::holds_alternative<TValue>(ExpirationValue_)) {
        return std::nullopt;
    }

    const auto& [user, expiration] = std::get<TValue>(ExpirationValue_);
    return TView(user.Get(), expiration);
}

template <std::copyable TTime>
std::optional<TInstant> TCypressNode::TExpirationProperties<TTime>::GetLastResetTime() const
{
    if (!std::holds_alternative<TLastResetInstant>(ExpirationValue_)) {
        return std::nullopt;
    }

    return std::get<TLastResetInstant>(ExpirationValue_);
}

template <std::copyable TTime>
std::optional<NSecurityServer::TUserRawPtr> TCypressNode::TExpirationProperties<TTime>::GetUser() const
{
    if (!std::holds_alternative<TValue>(ExpirationValue_)) {
        return std::nullopt;
    }

    const auto& [user, expiration] = std::get<TValue>(ExpirationValue_);
    return user.Get();
}

template <std::copyable TTime>
std::optional<TTime> TCypressNode::TExpirationProperties<TTime>::GetExpiration() const
{
    if (!std::holds_alternative<TValue>(ExpirationValue_)) {
        return std::nullopt;
    }

    const auto& [user, expiration] = std::get<TValue>(ExpirationValue_);
    return expiration;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
