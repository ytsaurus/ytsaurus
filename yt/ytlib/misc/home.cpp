#include "stdafx.h"

#include "home.h"

#ifdef _win_
#include <windows.h>
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

Stroka GetHomePath()
{
#ifdef _win_
    Stroka path;
    SHGetSpecialFolderPath(0, ~path, CSIDL_PROFILE, 0);
    return path;
#else
    return std::getenv("HOME");
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
