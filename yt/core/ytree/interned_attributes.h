#pragma once

#include "public.h"

namespace NYT::NYTree {

///////////////////////////////////////////////////////////////////////////////

using TInternedAttributeKey = int;

const TInternedAttributeKey InvalidInternedAttribute = 0;
const TInternedAttributeKey CountInternedAttribute = 1;

//! Interned attribute registry initialization. Should be called once per attribute.
//! Both interned and uninterned keys must be unique.
void InternAttribute(const TString& uninternedKey, TInternedAttributeKey internedKey);

// May return #InvalidInternedAttribute if the attribute is not interned.
TInternedAttributeKey GetInternedAttributeKey(const TString& uninternedKey);

const TString& GetUninternedAttributeKey(TInternedAttributeKey internedKey);

////////////////////////////////////////////////////////////////////////////////

#ifdef __GNUC__
    // Prevent the linker from throwing out static initializers.
    #define REGISTER_INTERNED_ATTRIBUTE_ATTRIBUTES __attribute__((used))
#else
    #define REGISTER_INTERNED_ATTRIBUTE_ATTRIBUTES
#endif

#define REGISTER_INTERNED_ATTRIBUTE(uninternedKey, internedKey) \
    REGISTER_INTERNED_ATTRIBUTE_ATTRIBUTES const void* InternedAttribute_##uninternedKey = [] () -> void* { \
    		::NYT::NYTree::InternAttribute(#uninternedKey, internedKey); \
    		return nullptr; \
    	} ();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
