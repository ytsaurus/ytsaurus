#include "heap_profiler.h"

#include "symbolize.h"
#include "backtrace.h"

#include <util/generic/hash_set.h>

#include <tcmalloc/malloc_extension.h>

namespace NYT::NYTProf {

////////////////////////////////////////////////////////////////////////////////

NProto::Profile ConvertAllocationProfile(const tcmalloc::Profile& snapshot)
{
    NProto::Profile profile;
    profile.add_string_table();

    profile.add_string_table("allocations");
    profile.add_string_table("count");
    profile.add_string_table("space");
    profile.add_string_table("bytes");
    profile.add_string_table("memory_tag");

    auto sampleType = profile.add_sample_type();
    sampleType->set_type(1);
    sampleType->set_unit(2);

    sampleType = profile.add_sample_type();
    sampleType->set_type(3);
    sampleType->set_unit(4);

    auto periodType = profile.mutable_period_type();
    periodType->set_type(3);
    periodType->set_unit(4);

    profile.set_period(snapshot.Period());

    THashMap<void*, ui64> locations;
    snapshot.Iterate([&] (const tcmalloc::Profile::Sample& sample) {
        auto sampleProto = profile.add_sample();
        sampleProto->add_value(sample.count);
        sampleProto->add_value(sample.sum);

        auto memoryTag = sample.stack[0];
        if (memoryTag != 0) {
            auto label = sampleProto->add_label();
            label->set_key(5);
            label->set_num(reinterpret_cast<i64>(memoryTag));
        }

        for (int i = 1; i < sample.depth; i++) {
            auto ip = sample.stack[i];

            auto it = locations.find(ip);
            if (it != locations.end()) {
                sampleProto->add_location_id(it->second);
                continue;
            }

            auto locationId = locations.size() + 1;

            auto location = profile.add_location();
            location->set_address(reinterpret_cast<ui64>(ip));
            location->set_id(locationId);

            sampleProto->add_location_id(locationId);
            locations[ip] = locationId;
        }
    });

    Symbolize(&profile, true);
    return profile;
}

NProto::Profile ReadHeapProfile(tcmalloc::ProfileType profileType)
{
    auto snapshot = tcmalloc::MallocExtension::SnapshotCurrent(profileType);
    return ConvertAllocationProfile(snapshot);
}

THashMap<TMemoryTag, ui64> GetEstimatedMemoryUsage()
{
    THashMap<TMemoryTag, ui64> usage;

    auto snapshot = tcmalloc::MallocExtension::SnapshotCurrent(tcmalloc::ProfileType::kHeap);
    snapshot.Iterate([&] (const tcmalloc::Profile::Sample& sample) {
        auto memoryTag = sample.stack[0];
        if (memoryTag != 0) {
            usage[reinterpret_cast<TMemoryTag>(memoryTag)] += sample.sum;
        }
    });

    return usage;
}

static thread_local TMemoryTag MemoryTag = 0;

TMemoryTag SetMemoryTag(TMemoryTag newTag)
{
    auto oldTag = MemoryTag;
    MemoryTag = newTag;
    return oldTag;
}

int AbslStackUnwinder(void** frames, int*,
                      int maxFrames, int skipFrames,
                      const void*,
                      int*)
{
    TUWCursor cursor;

    for (int i = 0; i < skipFrames + 1; ++i) {
        cursor.Next();
    }

    if (maxFrames > 0) {
        frames[0] = reinterpret_cast<void*>(MemoryTag);
    }

    int count = 1;
    for (int i = 1; i < maxFrames; ++i) {
        if (cursor.IsEnd()) {
            return count;
        }

        // IP point's to return address. Substract 1 to get accurate line information for profiler.
        frames[i] = reinterpret_cast<void*>(reinterpret_cast<uintptr_t>(cursor.GetIP()) - 1);
        count++;

        cursor.Next();
    }
    return count;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTProf
