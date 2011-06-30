#pragma once

#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TPatternFormatter
{
public:
    TPatternFormatter() { }

    void AddProperty(Stroka name, Stroka value);
    Stroka Format(Stroka pattern);

private:
    typedef yhash_map<Stroka, Stroka> TPropertyMap;
    TPropertyMap PropertyMap;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace
