#pragma once

#include "public.h"

#include <yt/core/misc/small_vector.h>

#include <yt/core/ytree/attributes.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

//! Overlays attribute dictionaries on top of each other. This essentially gives
//! an appearance of merged dictionaries without actually merging them.
/*!
 *  Ownership depends on the template argument.
 *
 *  Underlying dictionaries may be null. For the purposes of reading, a null
 *  dictionary is equivalent to an empty one.
 *
 *  The class is const-correct in the sense that T may dereference to a const
 *  dictionary. Calling SetYson() or Remove() will cause runtime error in this
 *  case (compile-time diagnostics can't be achieved because of
 *  IAttributeDictionary's extension methods).
 *
 *  In a non-const overlay, removing an attribute is always supported and
 *  removes it from all underlying dictionaries. Setting an attribute is only
 *  supported iff at least one dictionary is not null. This sets the attribute
 *  in the topmost underlying dictionary and removes it from everywhere else.
 */
template <class T>
class TOverlaidAttributeDictionaries
    : public IAttributeDictionary
{
public:
    // Parameters go from topmost to bottommost (i.e. highest to lowest priority).
    template <class... Args>
    explicit TOverlaidAttributeDictionaries(Args&&... underlyingDicts);

    template <class U, class... Args>
    void PushBottom(U&& topmostUnderlyingDict, Args&&... underlyingDicts);

    template <class U>
    void PushBottom(U&& underlyingDict);

    virtual std::vector<TString> List() const override;
    virtual NYson::TYsonString FindYson(const TString& key) const override;
    virtual void SetYson(const TString& key, const NYson::TYsonString& value) override;
    virtual bool Remove(const TString& key) override;

private:
    void SetYson(IAttributeDictionary& dict, const TString& key, const NYson::TYsonString& value);
    void SetYson(const IAttributeDictionary& dict, const TString& key, const NYson::TYsonString& value);

    bool Remove(IAttributeDictionary& dict, const TString& key);
    bool Remove(const IAttributeDictionary& dict, const TString& key);

    // From top to bottom (the earlier the dictionary, the higher its priority).
    SmallVector<T, 2> UnderlyingDictionaries_;
};

////////////////////////////////////////////////////////////////////////////////

// Type deduction helper. All arguments are expected to be of same time.
template <class T, class... Args>
TOverlaidAttributeDictionaries<typename std::remove_reference<T>::type> OverlayAttributeDictionaries(
    T&& topmostUnderlyingDict,
    Args&&... underlyingDicts);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT

#define OVERLAID_ATTRIBUTE_DICTIONARIES_INL_H_
#include "overlaid_attribute_dictionaries-inl.h"
#undef OVERLAID_ATTRIBUTE_DICTIONARIES_INL_H_

