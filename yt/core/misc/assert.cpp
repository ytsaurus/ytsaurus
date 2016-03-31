#include "assert.h"
#include "raw_formatter.h"

#include <yt/core/logging/log_manager.h>

#ifdef _win_
    #include <io.h>
#else
    #include <unistd.h>
#endif

namespace NYT {
namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

void AssertTrapImpl(
    const char* trapType,
    const char* expr,
    const char* file,
    int line)
{
    TRawFormatter<1024> formatter;
    formatter.AppendString(trapType);
    formatter.AppendString("(");
    formatter.AppendString(expr);
    formatter.AppendString(") at ");
    formatter.AppendString(file);
    formatter.AppendString(":");
    formatter.AppendNumber(line);
    formatter.AppendString("\n");

    ssize_t size;
    do {
        size = ::write(2, formatter.GetData(), formatter.GetBytesWritten());
    } while (size == -1 && errno == EINTR);

    NLogging::TLogManager::Get()->Shutdown();

    BUILTIN_TRAP();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
