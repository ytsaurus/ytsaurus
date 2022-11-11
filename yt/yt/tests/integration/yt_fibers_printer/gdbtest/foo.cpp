#include "foobar.h"

void Foo(NYT::NConcurrency::IThreadPoolPtr& threadPool, int x) {
    if (x > 0) {
        Bar(threadPool, x - 1);
    }
    AsyncStop(threadPool);
    exit(0);
}
