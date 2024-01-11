#include <library/cpp/yt/memory/new.h>
#include <library/cpp/yt/memory/ref.h>

#include <iostream>
#include <iomanip>
#include <vector>
#include <memory>
#include <map>

#include <yt/yt/library/ytprof/heap_profiler.h>

struct TMergedTag { };

using namespace NYT;

void CheckOverhead(int extraSize)
{
    int initialSize = 4 * 1024;

    for (int i = 0; i < 8; ++i) {
        auto size = (1 << i) * initialSize;
        auto total = size + extraSize;
        auto actual = nallocx(size + extraSize, 0);

        std::cout << "Payload size: " << size
            << "\textra: "<< extraSize
            << "\twould allocate: " << actual
            << "\toverhead: " << std::setprecision(4) << 100 * double(actual - total) / total << "%"
            << std::endl;
    }
}

void Reproduce()
{
    constexpr i64 Count = 8 * 1024;
    constexpr auto extraSize = 32;
    constexpr auto totalOverhead = extraSize;

    constexpr i64 Size = 32 * 1024;
    std::vector<TSharedMutableRef> list;
    list.reserve(Count);

    const std::vector<std::string> sensors = {
        "generic.physical_memory_used",
        "generic.bytes_in_use_by_app",
        "tcmalloc.required_bytes",
        "generic.heap_size",
        "tcmalloc.page_heap_free",
        "tcmalloc.metadata_bytes",
    };

    std::map<std::string, i64> metrics;
    for (const auto& sensor : sensors) {
        metrics[sensor] = *tcmalloc::MallocExtension::GetNumericProperty(sensor);
    }

    i64 totalSize = 0;

    for (i64 i = 0; i < Count; ++i) {
        auto buffer = TSharedMutableRef::Allocate<TMergedTag>(Size, {.InitializeStorage = true});
        totalSize += (buffer.Size() + totalOverhead);
        list.push_back(std::move(buffer));
    }

    std::cout << "Extra size: " <<  extraSize << std::endl
        << "Payload size: " << Size  << std::endl
        << "Payload count: " << Count << std::endl
        << "Expected total usage MB: " << double(totalSize) / 1024 / 1024 << std::endl
        << std::endl;

    for (auto& [sensor, data] : metrics) {
        auto current = *tcmalloc::MallocExtension::GetNumericProperty(sensor);;
        std::cout << sensor << " diff MB: " << double(current - data) / 1024 / 1024 << std::endl;

        if ("generic.bytes_in_use_by_app" == sensor) {
            std::cout << "total overhead: " << (double((current - data) - totalSize) / totalSize) * 100 << "%" << std::endl;
        }
    }
}

int main()
{
    CheckOverhead(0);
    std::cout << "----------" << std::endl;

    CheckOverhead(32);
    std::cout << "----------" << std::endl;

    std::cout << std::endl << "Reproducing..." << std::endl;
    Reproduce();

    return 0;
}
