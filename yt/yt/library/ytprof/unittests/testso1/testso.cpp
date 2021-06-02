#include <atomic>

static std::atomic<int> CallCount{0};

extern "C" void CallOtherNext(void (*next)())
{
    next();
    CallCount++;
}
