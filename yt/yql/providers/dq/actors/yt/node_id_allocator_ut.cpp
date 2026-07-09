#include "node_id_allocator.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;

Y_UNIT_TEST_SUITE(NodeIdAllocatorTest) {

    Y_UNIT_TEST(Allocate_NoDuplicates) {
        TNodeIdAllocator alloc(500, 1000);
        auto dups = alloc.Allocate({501, 502, 503});
        UNIT_ASSERT(dups.empty());
        UNIT_ASSERT(alloc.IsAllocated(501));
        UNIT_ASSERT(alloc.IsAllocated(502));
        UNIT_ASSERT(alloc.IsAllocated(503));
        UNIT_ASSERT(!alloc.IsAllocated(504));
    }

    Y_UNIT_TEST(Allocate_ReturnsDuplicates) {
        TNodeIdAllocator alloc(500, 1000);
        alloc.Allocate({501, 502});
        // 502 already taken, 503 is new
        auto dups = alloc.Allocate({502, 503});
        UNIT_ASSERT_VALUES_EQUAL(1u, dups.size());
        UNIT_ASSERT_VALUES_EQUAL(502u, dups[0]);
        // 503 was still inserted despite 502 being a duplicate
        UNIT_ASSERT(alloc.IsAllocated(503));
    }

    Y_UNIT_TEST(TryAllocate_Success) {
        TNodeIdAllocator alloc(500, 1000);
        UNIT_ASSERT(alloc.TryAllocate({501, 502, 503}));
        UNIT_ASSERT(alloc.IsAllocated(501));
        UNIT_ASSERT(alloc.IsAllocated(502));
        UNIT_ASSERT(alloc.IsAllocated(503));
    }

    Y_UNIT_TEST(TryAllocate_Conflict_AllocatesNothing) {
        TNodeIdAllocator alloc(500, 1000);
        alloc.Allocate({502});

        // 502 is already taken — TryAllocate must fail atomically: 501 and 503 must NOT be inserted
        UNIT_ASSERT(!alloc.TryAllocate({501, 502, 503}));
        UNIT_ASSERT(!alloc.IsAllocated(501));
        UNIT_ASSERT(!alloc.IsAllocated(503));
        UNIT_ASSERT(alloc.IsAllocated(502)); // original owner still holds it
    }

    Y_UNIT_TEST(Deallocate_FreesIds) {
        TNodeIdAllocator alloc(500, 1000);
        alloc.Allocate({501, 502, 503});
        alloc.Deallocate({502});
        UNIT_ASSERT(alloc.IsAllocated(501));
        UNIT_ASSERT(!alloc.IsAllocated(502));
        UNIT_ASSERT(alloc.IsAllocated(503));
        UNIT_ASSERT(alloc.TryAllocate({502}));
    }

    Y_UNIT_TEST(Clear_FreesAll) {
        TNodeIdAllocator alloc(500, 1000);
        alloc.Allocate({501, 502, 503});
        alloc.Clear();
        UNIT_ASSERT(!alloc.IsAllocated(501));
        UNIT_ASSERT(!alloc.IsAllocated(502));
        UNIT_ASSERT(!alloc.IsAllocated(503));
        UNIT_ASSERT(alloc.TryAllocate({501, 502, 503}));
    }

    Y_UNIT_TEST(AllocateCount_SkipsAlreadyAllocated) {
        TNodeIdAllocator alloc(500, 510);
        alloc.Allocate({500, 501, 502});
        TVector<ui32> result;
        alloc.Allocate(result, 3);
        UNIT_ASSERT_VALUES_EQUAL(3u, result.size());
        UNIT_ASSERT_VALUES_EQUAL(503u, result[0]);
        UNIT_ASSERT_VALUES_EQUAL(504u, result[1]);
        UNIT_ASSERT_VALUES_EQUAL(505u, result[2]);
    }

    Y_UNIT_TEST(AllocateCount_ReturnsLessIfPoolExhausted) {
        TNodeIdAllocator alloc(500, 503); // only 3 IDs: 500, 501, 502
        TVector<ui32> result;
        alloc.Allocate(result, 5);
        UNIT_ASSERT_VALUES_EQUAL(3u, result.size());
    }
}
