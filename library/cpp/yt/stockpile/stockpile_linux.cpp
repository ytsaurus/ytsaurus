#include "stockpile.h"

#include <thread>
#include <mutex>

#include <sys/mman.h>

#include <util/system/thread.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void RunStockpile(TStockpileOptions options)
{
    TThread::SetCurrentThreadName("Stockpile");

    constexpr auto MADV_STOCKPILE = 0x59410004;

    while (true) {
        madvise(nullptr, options.BufferSize, MADV_STOCKPILE);

        Sleep(options.StockpilePeriod);
    }
}

void StockpileMemory(TStockpileOptions options)
{
    static std::once_flag OnceFlag;
    std::call_once(OnceFlag, [options] {
        for (int i = 0; i < options.ThreadCount; i++) {
            std::thread(RunStockpile, options).detach();
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
