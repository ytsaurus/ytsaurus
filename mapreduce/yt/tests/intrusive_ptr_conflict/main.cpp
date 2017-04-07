// NOTE: TIntrusivePtr is defined in server code of YT.
// if someone will try include both server-YT header and YT C++ wrapper
// that will create problems if there are any usages of `TIntrusivePtr'
// inside of wrapper (we should always use ::TIntrusivePtr)
// Check YT-4143 for some history.
//
// This file tests that our public headers always use ::TIntrusivePtr.

namespace NYT {

    // We intentionaly use wrong template parameter here,
    // so any attempt to use NYT::TIntrusivePtr
    // inside following headers will lead to errors.
    template <int T>
    class TIntrusivePtr
    { };

} // namespace NYT

#include <mapreduce/yt/interface/client.h>

int main(int argc, const char** argv){
    NYT::Initialize(argc, argv);
    auto client = NYT::CreateClient("localhost");
    return 0;
}
