#include <library/cpp/yt/memory/poison.h>

#include <array>

using namespace NYT;

int main()
{
    std::array<char, 100> buf;
    auto ref = TMutableRef(buf.data(), buf.size());
    PoisonUninitializedMemory(ref);
    Cout << buf[0] << Endl;
    buf[0] = 'x';
    PoisonFreedMemory(ref);
    Cout << buf[0] << Endl;
    RecycleFreedMemory(ref);
    buf[0] = 'y';
    Cout << buf[0] << Endl;
}
