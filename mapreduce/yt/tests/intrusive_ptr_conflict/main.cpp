// NOTE: TIntrusivePtr is defined in server code of YT.
// if someone will try include both server-YT header and YT C++ wrapper
// that will create problems if there are any usages of `TIntrusivePtr'
// inside of wrapper (we should always use ::TIntrusivePtr)
// Check YT-4143 for some history.
//
// This file tests that our public headers always use ::TIntrusivePtr.

namespace NYT {
    template <class T>
    class TIntrusivePtr;
} // namespace NYT

#include <mapreduce/yt/interface/client.h>

int main(int /*argc*/, const char** /*argv*/){
    return 0;
}
