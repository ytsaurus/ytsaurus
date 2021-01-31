#include "yson_intern_registry.h"
#include "private.h"

#include <yt/core/concurrency/spinlock.h>

namespace NYT::NObjectServer {

using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

class TInternedYsonStringData;

class TYsonInternRegistry
    : public IYsonInternRegistry
{
public:
    virtual TYsonString Intern(TYsonString value) override;

private:
    friend class TInternedYsonStringData;

    struct THash
    {
        size_t operator() (const TInternedYsonStringData* value) const;
        size_t operator() (TStringBuf data) const;
    };

    struct TEqual
    {
        bool operator() (const TInternedYsonStringData* lhs, const TInternedYsonStringData* rhs) const;
        bool operator() (const TInternedYsonStringData* lhs, TStringBuf rhs) const;
    };

    using TInternedSet = THashSet<TInternedYsonStringData*, THash, TEqual>;

    YT_DECLARE_SPINLOCK(TAdaptiveLock, Lock_);
    TInternedSet InternedValues_;

    void UnregisterInternedYsonStringData(TInternedSet::iterator iterator)
    {
        auto guard = Guard(Lock_);
        InternedValues_.erase(iterator);
    }
};

class TInternedYsonStringData
    : public TRefCounted
    , public TWithExtraSpace<TInternedYsonStringData>
{
public:
    static TIntrusivePtr<TInternedYsonStringData> Allocate(
        TStringBuf data,
        TYsonInternRegistry* registry)
    {
        return NewWithExtraSpace<TInternedYsonStringData>(
            data.length(),
            data,
            registry);
    }

    void SetIterator(TYsonInternRegistry::TInternedSet::iterator iterator)
    {
        Iterator_ = iterator;
    }

    TStringBuf ToStringBuf() const
    {
        return TStringBuf(static_cast<const char*>(GetExtraSpacePtr()), Length_);
    }

    TYsonString ToYsonString()
    {
        auto stringBuf = ToStringBuf();
        auto ref = TSharedRef(TRef(stringBuf.begin(), stringBuf.end()), MakeStrong(this));
        return TYsonString(std::move(ref));
    }

private:
    DECLARE_NEW_FRIEND()

    const size_t Length_;
    TYsonInternRegistry* const Registry_;
    TYsonInternRegistry::TInternedSet::iterator Iterator_;

    TInternedYsonStringData(
        TStringBuf data,
        TYsonInternRegistry* registry)
        : Length_(data.length())
        , Registry_(registry)
    {
        auto* extraSpace = static_cast<char*>(GetExtraSpacePtr());
        ::memcpy(extraSpace, data.data(), Length_);
    }

    ~TInternedYsonStringData()
    {
        Registry_->UnregisterInternedYsonStringData(Iterator_);
    }
};

size_t TYsonInternRegistry::THash::operator() (const TInternedYsonStringData* value) const
{
    return ::THash<TStringBuf>()(value->ToStringBuf());
}

size_t TYsonInternRegistry::THash::operator() (TStringBuf data) const
{
    return ::THash<TStringBuf>()(data);
}

bool TYsonInternRegistry::TEqual::operator() (const TInternedYsonStringData* lhs, const TInternedYsonStringData* rhs) const
{
    return lhs->ToStringBuf() == rhs->ToStringBuf();
}

bool TYsonInternRegistry::TEqual::operator() (const TInternedYsonStringData* lhs, TStringBuf rhs) const
{
    return lhs->ToStringBuf() == rhs;
}

TYsonString TYsonInternRegistry::Intern(TYsonString value)
{
    if (!value) {
        return {};
    }

    YT_ASSERT(value.GetType() == EYsonType::Node);

    auto data = value.AsStringBuf();
    if (data.length() < YsonStringInternLengthThreshold) {
        return value;
    }

    auto guard = Guard(Lock_);
    TInternedSet::insert_ctx context;
    if (auto it = InternedValues_.find(data, context)) {
        return (*it)->ToYsonString();
    } else {
        auto internedData = TInternedYsonStringData::Allocate(data, this);
        internedData->SetIterator(InternedValues_.insert_direct(internedData.Get(), context));
        return internedData->ToYsonString();
    }
}

IYsonInternRegistryPtr CreateYsonInternRegistry()
{
    return New<TYsonInternRegistry>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NObjectServer
