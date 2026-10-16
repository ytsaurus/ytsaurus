#include <util/system/platform.h>

#if defined(_linux_)
#   include "config-linux.h"
#elif defined(_darwin_)
#   include "config-darwin.h"
#elif defined(_win_)
#   include "config-win.h"
#else
#   error path/to/xxx: there is no config.h for this platform
#endif

#define OPEN_VCDIFF_VERSION "0.8.4.yandex.1"
