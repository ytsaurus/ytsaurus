#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TPatternFormatter
    : private TNonCopyable
{
public:
    void AddProperty(const TString& name, const TString& value);
    TString Format(const TString& pattern);

private:
    typedef THashMap<TString, TString> TPropertyMap;
    TPropertyMap PropertyMap;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
