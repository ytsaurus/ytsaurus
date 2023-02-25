// NOTE: Some classes like:
//   - TIntrusivePtr
//   - TVariant
// are defined in server code of YT.
// If someone will try include both server-YT header and YT C++ wrapper
// that will create problems if there are any usages unqualified names
// inside of wrapper. So for example we must always use `::TIntrusivePtr'
// instead of `TIntrusivePtr'
//
// Check YT-4143 for some history.
//
// This file tests that our public headers always use qualified names.

namespace NYT {

    // We intentionaly use wrong template parameter here,
    // so any attempt to use these templates
    // inside following headers will lead to errors.

    template <int T>
    class TIntrusivePtr
    { };

    template <int T>
    class TVariant
    { };

} // namespace NYT

#include <yt/cpp/mapreduce/interface/client.h>

int main(int argc, const char** argv){
    NYT::Initialize(argc, argv);
    auto client = NYT::CreateClient("localhost");
    return 0;
}
