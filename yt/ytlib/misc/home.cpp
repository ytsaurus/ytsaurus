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
    char buffer[1024];
    SHGetSpecialFolderPath(0, buffer, CSIDL_PROFILE, 0);
    return Stroka(buffer);
#else
    return std::getenv("HOME");
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
