#pragma once

#include "../type_tag.h"

#include <util/generic/string.h>
#include <util/generic/hash.h>

#include <any>

namespace NRoren::NPrivate {

////////////////////////////////////////////////////////////////////////////////

class TAttributes;

////////////////////////////////////////////////////////////////////////////////

class IWithAttributes
{
public:
    virtual ~IWithAttributes() = default;

private:
    virtual void SetAttribute(const TString& key, const std::any& value) = 0;
    virtual const std::any* GetAttribute(const TString& key) const = 0;

private:
    template <typename T>
    friend void SetAttribute(IWithAttributes& withAttributes, const TTypeTag<std::decay_t<T>>& key, T&& value);

    template <typename T>
    friend const T* GetAttribute(const IWithAttributes& withAttributes, const TTypeTag<T>& key);

    friend void MergeAttributes(IWithAttributes& destination, const TAttributes& source);
};

template <typename T>
void SetAttribute(IWithAttributes& withAttributes, const TTypeTag<std::decay_t<T>>& key, T&& value);

template <typename T>
const T* GetAttribute(const IWithAttributes& withAttributes, const TTypeTag<T>& key);

template <typename T>
const T& GetRequiredAttribute(const IWithAttributes& withAttributes, const TTypeTag<T>& key);

////////////////////////////////////////////////////////////////////////////////

class TAttributes
    : public IWithAttributes
{
public:
    void SetAttribute(const TString& key, const std::any& value) override
    {
        Y_ABORT_UNLESS(value.has_value());
        Attributes_[key] = value;
    }

    const std::any* GetAttribute(const TString& key) const override
    {
        auto it = Attributes_.find(key);
        if (it != Attributes_.end()) {
            return &it->second;
        } else {
            return nullptr;
        }
    }

    bool HasAttribute(const TString& key) const
    {
        return GetAttribute(key) == nullptr;
    }

    template <typename T>
    void SetAttribute(const TTypeTag<std::decay_t<T>>& key, T&& value)
    {
        NPrivate::SetAttribute(*this, key, std::forward<T>(value));
    }

    template <typename T>
    const T* GetAttribute(const TTypeTag<T>& key) const
    {
        return NPrivate::GetAttribute(*this, key);
    }

    template <typename T>
    const T& GetRequiredAttribute(const TTypeTag<T>& key) const
    {
        return NPrivate::GetRequiredAttribute(*this, key);
    }

private:
    THashMap<TString, std::any> Attributes_;

public:
    friend void MergeAttributes(IWithAttributes& destination, const TAttributes& source);
};

void MergeAttributes(IWithAttributes& destination, const TAttributes& source);

template <typename T>
void SetAttribute(IWithAttributes& withAttributes, const TTypeTag<std::decay_t<T>>& key, T&& value)
{
    GetAttribute(withAttributes, key); // check that we don't have value of different type
    withAttributes.SetAttribute(key.GetDescription(), std::any{std::forward<T>(value)});
}

template <typename T>
const T* GetAttribute(const IWithAttributes& withAttributes, const TTypeTag<T>& key)
{
    const auto* any = withAttributes.GetAttribute(key.GetDescription());
    if (any == nullptr) {
        return nullptr;
    }
    const T* result = std::any_cast<T>(any);
    Y_ABORT_UNLESS(result != nullptr, "bad any cast");
    return result;
}

template <typename T>
const T& GetRequiredAttribute(const IWithAttributes& withAttributes, const TTypeTag<T>& key)
{
    const auto* result = GetAttribute(withAttributes, key);
    Y_ABORT_UNLESS(result, "missing required attribute %s", key.GetDescription().c_str());
    return *result;
}

template <typename T>
T GetAttributeOrDefault(const IWithAttributes& withAttributes, const TTypeTag<T>& key, const T& defaultValue)
{
    const auto* attribute = GetAttribute(withAttributes, key);
    if (attribute) {
        return *attribute;
    } else {
        return defaultValue;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoren
