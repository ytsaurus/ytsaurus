#ifndef CONSTRUCTION_SERIALIZABLE_INL_H_
#error "Direct inclusion of this file is not allowed, include construction_serializable.h"
// For the sake of sane code completion.
#include "construction_serializable.h"
#endif
#undef CONSTRUCTION_SERIALIZABLE_INL_H_

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

std::any GetOrCreateConstructionSerializable(const TString& guid, std::function<std::any()> createFunc);

template <typename T, auto CreatorFunc, typename ...TArgs>
TConstructionSerializablePtr<T> NewConstructionSerializableImpl(bool global, TArgs... args)
{
    TStringStream serialized;
    TConstructionSerializablePtr<T>::template SaveByCreationArgs<CreatorFunc>(&serialized, global, args...);
    TConstructionSerializablePtr<T> ptr;
    ptr.Load(&serialized);
    return ptr;
}

} // namespace NPrivate

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TConstructionSerializablePtr<NYT::TIntrusivePtr<T>>
{
    static_assert(NYT::TIntrusivePtr<T>::NotExistentField, "NYT::TIntrusivePtr<T> can not be template argument for TConstructionSerializablePtr, use just T.");
};

template <typename T>
template <auto CreatorFunc, typename ...TArgs>
void TConstructionSerializablePtr<T>::SaveByCreationArgs(IOutputStream* output, bool global, TArgs... args)
{
    TString guid;
    if (global) {
        guid = NYT::ToString(NYT::TGuid::Create());
    }
    TStringStream innerFrame;
    ::SaveMany(&innerFrame, guid, reinterpret_cast<ui64>(reinterpret_cast<void*>(&ParseAndCreate<CreatorFunc, TArgs...>)));
    ::SaveMany(&innerFrame, args...);
    ::Save(output, innerFrame.Str());
}

template <typename T>
void TConstructionSerializablePtr<T>::Save(IOutputStream* output) const
{
    if (!Storage_) {
        ::Save(output, TString{});
        return;
    }
    ::Save(output, Storage_->SerializedPtr);
}

template <typename T>
void TConstructionSerializablePtr<T>::Load(IInputStream* input)
{
    Reset();

    TString serializedPtr;
    ::Load(input, serializedPtr);
    if (serializedPtr.empty()) {
        Storage_ = nullptr;
        return;
    }
    TStringStream innerFrame(serializedPtr);
    TString guid;

    TParseAndCreateFunc parseFunc = nullptr;
    ::LoadMany(&innerFrame, guid, reinterpret_cast<ui64&>(reinterpret_cast<void*&>(parseFunc)));

    auto creator = [&] {
        TSelf newPtr;
        newPtr.Storage_ = NYT::New<TStorage>(TStorage{
            .Ptr = parseFunc(&innerFrame),
            .SerializedPtr = std::move(serializedPtr),
        });
        return newPtr;
    };

    if (guid.empty()) {
        *this = creator();
    } else {
        auto registeredItem = NRoren::NPrivate::GetOrCreateConstructionSerializable(guid, creator);
        *this = std::any_cast<TSelf>(registeredItem);
    }
}

template <typename T>
template <auto CreatorFunc, typename ...TArgs>
NYT::TIntrusivePtr<T> TConstructionSerializablePtr<T>::ParseAndCreate(IInputStream* input)
{
    return std::apply([&] (auto&& ...args) {
        ::LoadMany(input, args...);
        return CreatorFunc(args...);
    }, std::tuple<TArgs...>{});
}

template <typename T, typename ...TArgs>
TConstructionSerializablePtr<T> NewConstructionSerializable(TArgs... args)
{
    return NPrivate::NewConstructionSerializableImpl<T, [](TArgs ...args) { return NYT::New<T>(args...); }>(false, args...);
}

template <auto CreatorFunc, typename ...TArgs>
auto NewConstructionSerializable(TArgs... args) -> TConstructionSerializablePtr<
    typename NPrivate::TUnwrapIntrusivePtr<decltype(CreatorFunc(args...))>::TUnwrapped
>
{
    using T = typename NPrivate::TUnwrapIntrusivePtr<decltype(CreatorFunc(args...))>::TUnwrapped;
    return NPrivate::NewConstructionSerializableImpl<T, CreatorFunc>(false, args...);
}

template <typename T, typename ...TArgs>
TConstructionSerializablePtr<T> NewGlobalConstructionSerializable(TArgs... args)
{
    return NPrivate::NewConstructionSerializableImpl<T, [](TArgs ...args) { return NYT::New<T>(args...); }>(true, args...);
}

template <auto CreatorFunc, typename ...TArgs>
auto NewGlobalConstructionSerializable(TArgs... args) -> TConstructionSerializablePtr<
    typename NPrivate::TUnwrapIntrusivePtr<decltype(CreatorFunc(args...))>::TUnwrapped
>
{
    using T = typename NPrivate::TUnwrapIntrusivePtr<decltype(CreatorFunc(args...))>::TUnwrapped;
    return NPrivate::NewConstructionSerializableImpl<T, CreatorFunc>(true, args...);
}

template <typename T>
TConstructionSerializablePtr<T> NewFakeConstructionSerializable(NYT::TIntrusivePtr<T> ptr, bool saveWeakPtr) {
    if (nullptr == ptr) {
        return TConstructionSerializablePtr<T>{};
    }

    // Serialize ptr with failing parser.
    constexpr auto failingCreator = [](int) -> NYT::TIntrusivePtr<T> {
        YT_VERIFY(false && "This FakeConstructionSerializable was not serialized in this process");
    };
    TStringStream serializedItem;
    TConstructionSerializablePtr<T>::template SaveByCreationArgs<failingCreator>(&serializedItem, true, (int)0);


    // Insert ptr into process storage of global construction serializables.
    {
        using TStorage = typename TConstructionSerializablePtr<T>::TStorage;
        TStringStream serializedItemCopy = serializedItem;
        TString serializedPtr;
        ::Load(&serializedItemCopy, serializedPtr);
        YT_VERIFY(!serializedPtr.empty());
        TStringStream innerFrame(serializedPtr);
        TString guid;
        typename TConstructionSerializablePtr<T>::TParseAndCreateFunc parseFunc = nullptr;
        ::LoadMany(&innerFrame, guid, reinterpret_cast<ui64&>(reinterpret_cast<void*&>(parseFunc)));
        auto creator = [&] {
            TConstructionSerializablePtr<T> newPtr;

            newPtr.Storage_ = NYT::New<TStorage>(TStorage{
                .SerializedPtr = std::move(serializedPtr),
            });

            if (saveWeakPtr) {
                newPtr.Storage_->WeakPtr = ptr;
            } else {
                newPtr.Storage_->Ptr = ptr;
            }

            return newPtr;
        };
        NRoren::NPrivate::GetOrCreateConstructionSerializable(guid, creator);
    }

    // Load serialized ptr relying on getting ptr from storage without actual parsing.
    {
        TConstructionSerializablePtr<T> ptr;
        ptr.Load(&serializedItem);
        return ptr;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
