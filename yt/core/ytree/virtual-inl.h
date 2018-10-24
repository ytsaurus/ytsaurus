#pragma once
#ifndef VIRTUAL_INL_H_
#error "Direct inclusion of this file is not allowed, include virtual-inl.h"
// For the sake of sane code completion.
#include "virtual-inl.h"
#endif

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TDefaultConversionTraits
{ };

template <>
class TDefaultConversionTraits<TString>
{
public:
    static TString ConvertKeyToString(const TString& key)
    {
        return key;
    }

    static TString ConvertStringToKey(TStringBuf key)
    {
        return TString(key);
    }
};

template <class T, class TConversionTraits = TDefaultConversionTraits<typename T::key_type>>
class TCollectionBoundMapService
    : public TVirtualMapBase
{
public:
    TCollectionBoundMapService(const T& collection)
        : Collection_(collection)
    { }

    virtual i64 GetSize() const override
    {
        return Collection_.size();
    }

    virtual std::vector<TString> GetKeys(i64 limit) const override
    {
        std::vector<TString> keys;
        keys.reserve(limit);
        for (const auto& pair : Collection_) {
            if (static_cast<i64>(keys.size()) >= limit) {
                break;
            }
            keys.emplace_back(TConversionTraits::ConvertKeyToString(pair.first));
        }
        return keys;
    }

    virtual IYPathServicePtr FindItemService(TStringBuf key) const override
    {
        auto it = Collection_.find(TConversionTraits::ConvertStringToKey(key));
        if (it == Collection_.end()) {
            return nullptr;
        }
        return it->second->GetService();
    }

private:
    const T& Collection_;
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
class TCollectionBoundListService
    : public TVirtualListBase
{
public:
    TCollectionBoundListService(const T& collection)
        : Collection_(collection)
    { }

    virtual i64 GetSize() const override
    {
        return Collection_.size();
    }

    virtual IYPathServicePtr FindItemService(int index) const override
    {
        YCHECK(0 <= index && index < Collection_.size());
        return Collection_[index] ? Collection_[index]->GetService() : nullptr;
    }

private:
    const T& Collection_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
