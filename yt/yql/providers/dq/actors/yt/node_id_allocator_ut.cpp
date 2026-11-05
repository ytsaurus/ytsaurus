#include "node_id_allocator.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

using namespace NYql;

Y_UNIT_TEST_SUITE(NodeIdAllocatorTest) {

    Y_UNIT_TEST(OverlappingClaimsSurviveOwnerSpecificRelease) {
        TNodeIdAllocator alloc(501, 503);

        UNIT_ASSERT(alloc.RestoreClaim("first", {501}).Valid);
        const auto secondClaim = alloc.RestoreClaim("second", {501});
        UNIT_ASSERT(secondClaim.Valid);
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui32>({501}), secondClaim.ConflictingNodeIds);
        UNIT_ASSERT_VALUES_EQUAL(2, alloc.GetClaimCount());
        UNIT_ASSERT_VALUES_EQUAL(1, alloc.GetClaimedNodeIdCount());

        UNIT_ASSERT(alloc.Release("first"));
        UNIT_ASSERT_VALUES_EQUAL(1, alloc.GetClaimCount());
        UNIT_ASSERT_VALUES_EQUAL(1, alloc.GetClaimedNodeIdCount());

        TVector<ui32> nodes;
        UNIT_ASSERT(!alloc.Allocate("all", 2, &nodes));
        UNIT_ASSERT(nodes.empty());
        UNIT_ASSERT(alloc.Allocate("free", 1, &nodes));
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui32>({502}), nodes);

        UNIT_ASSERT(alloc.Release("second"));
        UNIT_ASSERT(alloc.Release("free"));
        UNIT_ASSERT_VALUES_EQUAL(0, alloc.GetClaimedNodeIdCount());
        UNIT_ASSERT(alloc.Allocate("all", 2, &nodes));
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui32>({501, 502}), nodes);
        UNIT_ASSERT_VALUES_EQUAL(2, alloc.GetClaimedNodeIdCount());
    }

    Y_UNIT_TEST(RestoreClaimIsIdempotentAndRejectsInvalidChanges) {
        TNodeIdAllocator alloc(500, 505);

        UNIT_ASSERT(alloc.RestoreClaim("owner", {501, 502}).Valid);
        UNIT_ASSERT(alloc.RestoreClaim("owner", {502, 501}).Valid);
        UNIT_ASSERT(!alloc.RestoreClaim("owner", {501, 503}).Valid);
        UNIT_ASSERT(!alloc.RestoreClaim("duplicate", {503, 503}).Valid);
        UNIT_ASSERT(alloc.RestoreClaim("below-range", {499}).Valid);
        UNIT_ASSERT(alloc.RestoreClaim("above-range", {505}).Valid);
        UNIT_ASSERT(!alloc.RestoreClaim("empty", {}).Valid);
        UNIT_ASSERT_VALUES_EQUAL(4, alloc.GetClaimCount());

        TVector<ui32> nodes;
        UNIT_ASSERT(alloc.Allocate("remaining", 3, &nodes));
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui32>({500, 503, 504}), nodes);
    }

    Y_UNIT_TEST(ExactAllocationDoesNotPartiallyClaimOnExhaustion) {
        TNodeIdAllocator alloc(500, 505);
        UNIT_ASSERT(alloc.RestoreClaim("blocker", {500}).Valid);

        TVector<ui32> nodes;
        UNIT_ASSERT(!alloc.Allocate("fresh", 5, &nodes));
        UNIT_ASSERT(nodes.empty());
        UNIT_ASSERT_VALUES_EQUAL(1, alloc.GetClaimCount());

        UNIT_ASSERT(alloc.Release("blocker"));
        UNIT_ASSERT(alloc.Allocate("fresh", 5, &nodes));
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui32>({500, 501, 502, 503, 504}), nodes);
        UNIT_ASSERT_VALUES_EQUAL(5, alloc.GetClaimCount());
    }

    Y_UNIT_TEST(IncidentGeometryKeepsAllLiveClaims) {
        constexpr ui32 MinNodeId = 512;
        constexpr ui32 RangeCount = 45;
        constexpr ui32 NodeIdsPerRange = 5;
        constexpr ui32 MaxNodeId = MinNodeId + RangeCount * NodeIdsPerRange;

        TNodeIdAllocator alloc(MinNodeId, MaxNodeId);
        TVector<TString> liveOwners;
        TVector<TString> terminalOwners;

        for (ui32 rangeIndex = 0; rangeIndex < RangeCount; ++rangeIndex) {
            TVector<ui32> nodeIds;
            nodeIds.reserve(NodeIdsPerRange);
            for (ui32 offset = 0; offset < NodeIdsPerRange; ++offset) {
                nodeIds.push_back(MinNodeId + rangeIndex * NodeIdsPerRange + offset);
            }

            const TString liveOwner = TStringBuilder() << "live-" << rangeIndex;
            liveOwners.push_back(liveOwner);
            UNIT_ASSERT(alloc.RestoreClaim(liveOwner, nodeIds).Valid);

            const ui32 terminalOwnerCount = rangeIndex + 1 == RangeCount ? 2 : 1;
            for (ui32 ownerIndex = 0; ownerIndex < terminalOwnerCount; ++ownerIndex) {
                const TString terminalOwner = TStringBuilder()
                    << "terminal-" << rangeIndex << "-" << ownerIndex;
                terminalOwners.push_back(terminalOwner);
                UNIT_ASSERT(alloc.RestoreClaim(terminalOwner, nodeIds).Valid);
            }
        }

        UNIT_ASSERT_VALUES_EQUAL((2 * RangeCount + 1) * NodeIdsPerRange, alloc.GetClaimCount());
        UNIT_ASSERT_VALUES_EQUAL(RangeCount + 1, terminalOwners.size());

        for (const auto& owner : terminalOwners) {
            UNIT_ASSERT(alloc.Release(owner));
        }
        UNIT_ASSERT_VALUES_EQUAL(RangeCount * NodeIdsPerRange, alloc.GetClaimCount());

        TVector<ui32> nodes;
        UNIT_ASSERT(!alloc.Allocate("replacement", 1, &nodes));
        UNIT_ASSERT(nodes.empty());

        for (const auto& owner : liveOwners) {
            UNIT_ASSERT(alloc.Release(owner));
        }
        UNIT_ASSERT(alloc.Allocate("replacement", RangeCount * NodeIdsPerRange, &nodes));
        UNIT_ASSERT_VALUES_EQUAL(RangeCount * NodeIdsPerRange, nodes.size());
    }

    Y_UNIT_TEST(ClearDropsAllOwnerClaims) {
        TNodeIdAllocator alloc(500, 503);
        UNIT_ASSERT(alloc.RestoreClaim("first", {500, 501}).Valid);

        alloc.Clear();
        UNIT_ASSERT_VALUES_EQUAL(0, alloc.GetClaimCount());

        TVector<ui32> nodes;
        UNIT_ASSERT(alloc.Allocate("second", 3, &nodes));
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui32>({500, 501, 502}), nodes);
    }
}
