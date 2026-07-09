# Insert-Only Concurrent Cache

`TInsertOnlyConcurrentCache` is a specialized concurrent hash map optimized for read-heavy, long-lived caches that eventually reach saturation (i.e., all possible elements are inserted).

## Key Characteristics

* **Insert-Only:** Elements can only be added, never removed or updated.
* **High-Performance Reads:** Read operations are extremely fast (comparable to `const THashMap`) and scale linearly with the number of threads.
* **Expensive Writes:** Insertions are performed under a lock, offering no concurrent scalability. Performance is comparable to a standard hash map protected by a mutex.
* **O(N) Memory:** Memory usage scales linearly with the number of elements.
* **Stable References:** Pointers and references to values remain valid for the entire lifetime of the cache.

## Ideal Use Cases

This container is perfect for scenarios where:
* Reads outnumber writes by orders of magnitude.
* The set of keys is relatively fixed or grows slowly until saturation.

## API Overview

```cpp
#include <library/cpp/containers/insert_only_concurrent_cache/cache.h>

TInsertOnlyConcurrentCache<TString, TMyValue> cache;

// Find an element or insert it if it doesn't exist.
// The initFunctor is called to construct the value if the key is missing.
const TMyValue& value = cache.FindOrInsert("my_key", []() {
    return TMyValue{...};
});

// Look up an element, returning a pointer or nullptr if not found.
const TMyValue* ptr = cache.FindPtr("my_key");
```

## Benchmarks

The following benchmark results were obtained using `--build=profile`:

**Naming Convention:** `TSmallHashMapFindFixture/<CacheType>/<SaturationElementCount>/threads:<ThreadCount>`

```
Benchmark                                                  Time             CPU   Iterations UserCounters...
------------------------------------------------------------------------------------------------------------
TSmallHashMapFindFixture/InsertOnlyCache/1/threads:1         73180 ns        25455 ns        28414 items_per_second=392.844M/s
TSmallHashMapFindFixture/InsertOnlyCache/1/threads:4         91923 ns        24582 ns        28284 items_per_second=1.62719G/s
TSmallHashMapFindFixture/InsertOnlyCache/64/threads:1        49212 ns        24862 ns        27562 items_per_second=402.226M/s
TSmallHashMapFindFixture/InsertOnlyCache/64/threads:4        96242 ns        25585 ns        28936 items_per_second=1.56342G/s
TSmallHashMapFindFixture/ConcurrentHashMap/1/threads:1       83632 ns        57679 ns        11699 items_per_second=173.372M/s
TSmallHashMapFindFixture/ConcurrentHashMap/1/threads:4     2107113 ns       139640 ns         4000 items_per_second=286.451M/s
TSmallHashMapFindFixture/ConcurrentHashMap/64/threads:1     132787 ns        55796 ns        11955 items_per_second=179.224M/s
TSmallHashMapFindFixture/ConcurrentHashMap/64/threads:4    2022755 ns       134124 ns         4000 items_per_second=298.231M/s
TSmallHashMapFindFixture/HashMap/1/threads:1                 35095 ns        23087 ns        29208 items_per_second=433.141M/s
TSmallHashMapFindFixture/HashMap/1/threads:4                103395 ns        23066 ns        30788 items_per_second=1.73415G/s
TSmallHashMapFindFixture/HashMap/64/threads:1                38166 ns        23731 ns        31223 items_per_second=421.394M/s
TSmallHashMapFindFixture/HashMap/64/threads:4               146537 ns        25141 ns        28992 items_per_second=1.59105G/s
TSmallHashMapFindFixture/HashMapLocked/1/threads:1           44386 ns        33476 ns        21052 items_per_second=298.718M/s
TSmallHashMapFindFixture/HashMapLocked/1/threads:4         1106744 ns       219272 ns         6852 items_per_second=182.422M/s
TSmallHashMapFindFixture/HashMapLocked/64/threads:1          49714 ns        32145 ns        21528 items_per_second=311.094M/s
TSmallHashMapFindFixture/HashMapLocked/64/threads:4         710951 ns       230508 ns         2516 items_per_second=173.53M/s
TSmallHashMapFindFixture/HarrisMichaelMap/1/threads:1      1104393 ns       744611 ns          913 items_per_second=13.4298M/s
TSmallHashMapFindFixture/HarrisMichaelMap/1/threads:4      4246802 ns       770375 ns          900 items_per_second=51.9228M/s
TSmallHashMapFindFixture/HarrisMichaelMap/64/threads:1     1711275 ns       791841 ns          871 items_per_second=12.6288M/s
TSmallHashMapFindFixture/HarrisMichaelMap/64/threads:4     6319795 ns       796464 ns          400 items_per_second=50.222M/s
```
