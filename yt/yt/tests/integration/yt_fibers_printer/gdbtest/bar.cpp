#include "foobar.h"

void Bar(NYT::TIntrusivePtr<NYT::NConcurrency::TThreadPool>& threadPool, int x) {
    if (x > 0) {
        Foo(threadPool, x - 1);
    }
    AsyncStop(threadPool);
    exit(0);
}
