#ifndef NODE_INL_H_
#error "Direct inclusion of this file is not allowed, include node.h"
// For the sake of sane code completion.
#include "node.h"
#endif

namespace NYT::NCypressServer {

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

////////////////////////////////////////////////////////////////////////////////

template <class T>
void TVersionedBuiltinAttribute<T>::Persist(const NCellMaster::TPersistenceContext& context)
{
    using NYT::Persist;
    Persist(context, BoxedValue_);
}

template <class T>
void TVersionedBuiltinAttribute<T>::Save(TBeginCopyContext& context) const
{
    using NYT::Save;
    if constexpr (TVersionedBuiltinAttributeTraits<T>::IsPointer) {
        if (BoxedValue_) {
            Save(context, std::optional<TRawVersionedBuiltinAttributeType<T>>(TVersionedBuiltinAttributeTraits<T>::ToRaw(*BoxedValue_)));
        } else {
            Save(context, std::optional<TRawVersionedBuiltinAttributeType<T>>());
        }
    } else {
        Save(context, BoxedValue_);
    }
}

template <class T>
void TVersionedBuiltinAttribute<T>::Load(TEndCopyContext& context)
{
    using NYT::Load;
    if constexpr (TVersionedBuiltinAttributeTraits<T>::IsPointer) {
        if (auto optionalValue = Load<std::optional<TRawVersionedBuiltinAttributeType<T>>>(context)) {
            BoxedValue_ = TVersionedBuiltinAttributeTraits<T>::FromRaw(*optionalValue);
        } else {
            BoxedValue_ = std::nullopt;
        }
    } else {
        Load(context, BoxedValue_);
    }
}

template <class T>
void TVersionedBuiltinAttribute<T>::Set(T value)
{
    if constexpr (TVersionedBuiltinAttributeTraits<T>::IsPointer) {
        // nullptrs are not allowed; Remove() should be used instead.
        YT_VERIFY(value);
    }

    BoxedValue_ = std::move(value);
}

template <class T>
void TVersionedBuiltinAttribute<T>::Reset()
{
    if constexpr (TVersionedBuiltinAttributeTraits<T>::IsPointer) {
        BoxedValue_ = std::nullopt;
    } else {
        BoxedValue_ = TNullVersionedBuiltinAttribute{};
    }
}

template <class T>
void TVersionedBuiltinAttribute<T>::Remove()
{
    if constexpr (TVersionedBuiltinAttributeTraits<T>::IsPointer) {
        BoxedValue_ = TVersionedBuiltinAttributeTraits<T>::FromRaw(nullptr);
    } else {
        BoxedValue_ = TTombstonedVersionedBuiltinAttribute{};
    }
}

template <class T>
bool TVersionedBuiltinAttribute<T>::IsNull() const
{
    if constexpr (TVersionedBuiltinAttributeTraits<T>::IsPointer) {
        return !BoxedValue_.has_value();
    } else {
        return std::holds_alternative<TNullVersionedBuiltinAttribute>(BoxedValue_);
    }
}

template <class T>
bool TVersionedBuiltinAttribute<T>::IsTombstoned() const
{
    if constexpr (TVersionedBuiltinAttributeTraits<T>::IsPointer) {
        return BoxedValue_ && TVersionedBuiltinAttributeTraits<T>::ToRaw(*BoxedValue_) == nullptr;
    } else {
        return std::holds_alternative<TTombstonedVersionedBuiltinAttribute>(BoxedValue_);
    }
}

template <class T>
bool TVersionedBuiltinAttribute<T>::IsSet() const
{
    if constexpr (TVersionedBuiltinAttributeTraits<T>::IsPointer) {
        return BoxedValue_ && TVersionedBuiltinAttributeTraits<T>::ToRaw(*BoxedValue_) != nullptr;
    } else {
        return std::holds_alternative<T>(BoxedValue_);
    }
}

template <class T>
TRawVersionedBuiltinAttributeType<T> TVersionedBuiltinAttribute<T>::Unbox() const
{
    if constexpr (TVersionedBuiltinAttributeTraits<T>::IsPointer) {
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
    return TVersionedBuiltinAttributeTraits<T>::ToRaw(*result);
}

template <class T>
template <class TOwner>
/*static*/ TRawVersionedBuiltinAttributeType<T> TVersionedBuiltinAttribute<T>::Get(
    const TVersionedBuiltinAttribute<T>* (TOwner::*memberGetter)() const,
    const TOwner* node)
{
    auto result = TryGet(memberGetter, node);
    YT_VERIFY(result);
    return TVersionedBuiltinAttributeTraits<T>::ToRaw(*result);
}

template <class T>
template <class TOwner>
/*static*/ std::optional<TRawVersionedBuiltinAttributeType<T>> TVersionedBuiltinAttribute<T>::TryGet(
    TVersionedBuiltinAttribute<T> TOwner::*member,
    const TOwner* node)
{
    for (auto* currentNode = node; currentNode; currentNode = currentNode->GetOriginator()->template As<TOwner>()) {
        const auto& attribute = currentNode->*member;

        if (attribute.IsNull()) {
            continue;
        }

        if (attribute.IsTombstoned()) {
            return std::nullopt;
        }

        return attribute.Unbox();
    }

    return std::nullopt;
}

template <class T>
template <class TOwner>
/*static*/ std::optional<TRawVersionedBuiltinAttributeType<T>> TVersionedBuiltinAttribute<T>::TryGet(
    const TVersionedBuiltinAttribute<T>* (TOwner::*memberGetter)() const,
    const TOwner* node)
{
    for (auto* currentNode = node; currentNode; currentNode = currentNode->GetOriginator()->template As<TOwner>()) {
        auto* attribute = (currentNode->*memberGetter)();

        if (!attribute) {
            continue;
        }

        if (attribute->IsNull()) {
            continue;
        }

        if (attribute->IsTombstoned()) {
            return std::nullopt;
        }

        return attribute->Unbox();
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
    } // NB: null attributes are ignored.
}

template <class T>
TVersionedBuiltinAttribute<T> TVersionedBuiltinAttribute<T>::Clone() const
{
    TVersionedBuiltinAttribute<T> result;
    if constexpr (std::copyable<T>) {
        result.BoxedValue_ = BoxedValue_;
    } else if constexpr (NObjectServer::Clonable<T>) {
        result.BoxedValue_ = BoxedValue_ ? std::optional(BoxedValue_->Clone()) : std::nullopt;
    } else {
        static_assert("Unsupported type");
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
