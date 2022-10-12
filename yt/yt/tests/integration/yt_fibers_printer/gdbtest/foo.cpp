#include "foobar.h"

void Foo(NYT::TIntrusivePtr<NYT::NConcurrency::TThreadPool>& threadPool, int x) {
    if (x > 0) {
        Bar(threadPool, x - 1);
    }
    AsyncStop(threadPool);
    exit(0);
}
