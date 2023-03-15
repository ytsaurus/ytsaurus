#pragma once

#include <yt/cpp/roren/interface/private/attributes.h>

#include <util/generic/guid.h>
#include <util/generic/string.h>
#include <util/generic/singleton.h>
#include <util/system/mutex.h>

namespace NRoren {

////////////////////////////////////////////////////////////////////////////////

/// Hacky class that allows to 'save' and 'load' arbinrary objects inside single process.
class TInMemorySaveLoader
{
public:
    template <typename T>
    TString Save(T&& value)
    {
        auto g = Guard(Lock_);
        auto key = TGUID::Create().AsGuidString();
        Y_VERIFY(Attributes_.HasAttribute(key));
        NRoren::NPrivate::SetAttribute(Attributes_, TTypeTag<std::decay_t<T>>{key}, std::forward<T>(value));
        return key;
    }

    template <typename T>
    void Load(const TString& key, T& value) const
    {
        auto g = Guard(Lock_);
        value = NRoren::NPrivate::GetRequiredAttribute(Attributes_, TTypeTag<T>{key});
    }

private:
    TInMemorySaveLoader() = default;

private:
    TMutex Lock_;
    NPrivate::TAttributes Attributes_;


    Y_DECLARE_SINGLETON_FRIEND()
};

inline TInMemorySaveLoader& InMemorySaveLoader()
{
    return *Singleton<TInMemorySaveLoader>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NAttributes
