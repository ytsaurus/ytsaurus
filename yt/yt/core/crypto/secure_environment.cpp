#include "secure_environment.h"

#include <yt/yt/core/ytree/helpers.h>

#include <yt/yt/library/undumpable/undumpable.h>

#include <library/cpp/yt/memory/chunked_memory_pool.h>
#include <library/cpp/yt/memory/leaky_singleton.h>

#include <util/system/env.h>

namespace NYT::NCrypto {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

struct TSecureMemoryPoolTag
{ };

////////////////////////////////////////////////////////////////////////////////

class TSecureMemoryChunkProvider
    : public IMemoryChunkProvider
{
public:
    std::unique_ptr<TAllocationHolder> Allocate(size_t size, TRefCountedTypeCookie cookie) override
    {
        return std::unique_ptr<THolder>(TAllocationHolder::Allocate<THolder>(size, cookie));
    }

private:
    struct THolder
        : public TAllocationHolder
    {
        THolder(TMutableRef ref, TRefCountedTypeCookie cookie)
            : TAllocationHolder(ref, cookie)
        {
            MarkUndumpableOob(ref.Begin(), ref.Size());
        }

        ~THolder() override
        {
            auto ref = GetRef();
            SecureZero(ref.Begin(), ref.Size());
            UnmarkUndumpableOob(ref.Begin());
        }
    };
};

////////////////////////////////////////////////////////////////////////////////

template <class TUnderlyingRef>
class TSecureZeroHolder
    : public TSharedRangeHolder
{
public:
    explicit TSecureZeroHolder(const TUnderlyingRef& ref)
        : Ref_(ref)
    { }

    ~TSecureZeroHolder()
    {
        SecureZero(Ref_.data(), Ref_.size() * sizeof(*Ref_.data()));
    }
private:
    const TUnderlyingRef Ref_;
};

////////////////////////////////////////////////////////////////////////////////

//! IAttributeDictionary implementation that securely stores YSON values in
//! undumpable memory allocated from a TChunkedMemoryPool and wiped at delete.
/*!
 *  Values are captured into pool memory via Capture() and wrapped
 *  in TSharedRef-backed TYsonString objects with a zeroing holder.
 *
 *  This relies on TChunkedMemoryPool never deallocating individual
 *  allocations and the pool outliving all TYsonString instances.
 *  This is always true when pool lives for the singleton lifetime.
 *
 *  This class is NOT thread-safe on its own.
 *  Thread-safety is provided by wrapping with CreateThreadSafeAttributes().
 *
 *  Right now access isn't always zero-copy and could leak content.
 *  TODO(khlebnikov): Add zero-copy Find<TStringBuf> or YPath resolver.
 */
class TSecureAttributeDictionary
    : public IAttributeDictionary
{
public:
    TSecureAttributeDictionary()
        : Pool_(
            GetRefCountedTypeCookie<TSecureMemoryPoolTag>(),
            New<TSecureMemoryChunkProvider>())
    { }

    std::vector<TKey> ListKeys() const override
    {
        std::vector<TKey> keys;
        keys.reserve(Map_.size());
        for (const auto& [key, value] : Map_) {
            keys.push_back(key);
        }
        return keys;
    }

    std::vector<TKeyValuePair> ListPairs() const override
    {
        std::vector<TKeyValuePair> pairs;
        pairs.reserve(Map_.size());
        for (const auto& pair : Map_) {
            pairs.push_back(pair);
        }
        return pairs;
    }

    TValue FindYson(TKeyView key) const override
    {
        auto it = Map_.find(key);
        return it == Map_.end() ? TYsonString() : it->second;
    }

    void SetYson(TKeyView key, const TValue& value) override
    {
        Map_[key] = Capture(value);
    }

    bool Remove(TKeyView key) override
    {
        return Map_.erase(key) > 0;
    }

private:
    TChunkedMemoryPool Pool_;
    THashMap<TKey, TYsonString, THash<std::string_view>, TEqualTo<std::string_view>> Map_;

    //! Captures the value content into the memory pool.
    TYsonString Capture(const TYsonString& value)
    {
        auto copy = Pool_.Capture(value.ToSharedRef());
        TSharedRef ref(copy.data(), copy.size(), New<TSecureZeroHolder<TMutableRange<char>>>(copy));
        return TYsonString(ref, value.GetType());
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSecureEnvironmentHolder
{
    IAttributeDictionaryPtr Underlying;
    IAttributeDictionaryPtr Dictionary;

    TSecureEnvironmentHolder()
    {
        Underlying = New<TSecureAttributeDictionary>();
        Dictionary = CreateThreadSafeAttributes(Underlying.Get());
    }
};

const IAttributeDictionaryPtr& GetSecureEnvironment()
{
    return LeakySingleton<TSecureEnvironmentHolder>()->Dictionary;
}

////////////////////////////////////////////////////////////////////////////////

void MoveToSecureEnvironment(const std::vector<TStringBuf>& names, const std::vector<TStringBuf>& namePrefixes)
{
    auto secureEnv = GetSecureEnvironment();
    IterateEnv([&](TStringBuf name, TStringBuf value) {
        if (std::ranges::find(names, name) != names.end() ||
            std::ranges::find_if(namePrefixes, [&] (TStringBuf prefix) { return name.StartsWith(prefix); }) != namePrefixes.end())
        {
            // TODO(khlebnikov): Add EYsonType::String and do zero-copy.
            // NOTE: Right now it works because "ReadUnquotedString" in YSON parser.
            // secureEnv->SetYson(name, TYsonString(value, EYsonType(-2)));
            secureEnv->Set(name, value);
            ::memset(const_cast<char*>(value.data()), '*', value.size());
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrypto
