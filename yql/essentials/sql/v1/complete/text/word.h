#pragma once

#include <util/charset/unidata.h>

namespace NSQLComplete {

    static const char WordBreakCharacters[] = " \t\v\f\a\b\r\n`~!@#$%^&*-=+[](){}\\|;:'\".,<>/?";

    bool IsWordBoundary(char ch);

    size_t LastWordIndex(TStringBuf text);

    TStringBuf LastWord(TStringBuf text);

} // namespace NSQLComplete
