#include "interned_attributes.h"

#include <yt/core/misc/collection_helpers.h>

namespace NYT::NYTree {

///////////////////////////////////////////////////////////////////////////////

namespace {

class TInternedAttributeRegistry
{
public:
    void Intern(const TString& uninternedKey, TInternedAttributeKey internedKey)
    {
        YT_VERIFY(AttributeNameToIndex_.emplace(uninternedKey, internedKey).second);
        YT_VERIFY(AttributeIndexToName_.emplace(internedKey, uninternedKey).second);
    }

    TInternedAttributeKey GetInterned(TStringBuf uninternedKey)
    {
        auto it = AttributeNameToIndex_.find(uninternedKey);
        return it == AttributeNameToIndex_.end() ? InvalidInternedAttribute : it->second;
    }

    const TString& GetUninterned(TInternedAttributeKey internedKey)
    {
        return GetOrCrash(AttributeIndexToName_, internedKey);
    }

private:
    THashMap<TString, TInternedAttributeKey> AttributeNameToIndex_;
    THashMap<TInternedAttributeKey, TString> AttributeIndexToName_;
};

} // namespace

void InternAttribute(const TString& uninternedKey, TInternedAttributeKey internedKey)
{
    Singleton<TInternedAttributeRegistry>()->Intern(uninternedKey, internedKey);
}

TInternedAttributeKey GetInternedAttributeKey(TStringBuf uninternedKey)
{
    return Singleton<TInternedAttributeRegistry>()->GetInterned(uninternedKey);
}

const TString& GetUninternedAttributeKey(TInternedAttributeKey internedKey)
{
    return Singleton<TInternedAttributeRegistry>()->GetUninterned(internedKey);
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_INTERNED_ATTRIBUTE(count, CountInternedAttribute)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
