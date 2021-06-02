#include <atomic>

static std::atomic<int> CallCount{0};

extern "C" void CallNext(void (*next)())
{
    next();
    CallCount++;
}
