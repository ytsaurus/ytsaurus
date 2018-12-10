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

struct TRegisterInternedAttribute
{
    TRegisterInternedAttribute(const TString& uninternedKey, TInternedAttributeKey internedKey);
};

// NB: this macro has to be expanded within a translation unit with functions or
// variables that are actually used somewhere else. Otherwise the compiler is
// well within its rights to eschew the initialization.
// See the standard at [basic.start.init] paragraph 4.
#define REGISTER_INTERNED_ATTRIBUTE(uninternedKey, internedKey) \
    static const ::NYT::NYTree::TRegisterInternedAttribute register_##uninternedKey(#uninternedKey, internedKey);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTree
