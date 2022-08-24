#include "foobar.h"

void StopHere() {
    volatile int dummy;
    dummy = 0;
}

void AsyncStop(NYT::TIntrusivePtr<NYT::NConcurrency::TThreadPool>& threadPool) {
    auto future = BIND([&]() {
        StopHere();
    }).AsyncVia(threadPool->GetInvoker()).Run();
    Y_UNUSED(NYT::NConcurrency::WaitFor(future));
}

int main() {
    auto threadPool = NYT::New<NYT::NConcurrency::TThreadPool>(1, "test");
    auto future = BIND([&]() {
        Foo(threadPool, 10);
    }).AsyncVia(threadPool->GetInvoker()).Run();
    Y_UNUSED(NYT::NConcurrency::WaitFor(future));
    return 0;
}
