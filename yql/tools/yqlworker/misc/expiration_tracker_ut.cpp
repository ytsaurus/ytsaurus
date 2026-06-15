#include "expiration_tracker.h"
#include <library/cpp/testing/unittest/registar.h>

using namespace NYql;
namespace {
class TTestTracker : public TExpirationTracker<TString> {
public:
    size_t CollectExpired(TInstant now) {
        auto oldSize = CollectedItems.size();

        Process(now, [this](auto s) { CollectedItems.push_back(s); });

        auto newSize = CollectedItems.size();
        return newSize - oldSize;
    }

    TVector<TString> CollectedItems;
};
}

Y_UNIT_TEST_SUITE(TExpirationTrackerTest) {

    static TInstant GetTime(int c) {
        return TInstant::Seconds(c);
    }

    Y_UNIT_TEST(Empty) {
        TTestTracker tracker;
        UNIT_ASSERT_VALUES_EQUAL(0, tracker.GetSize());
        UNIT_ASSERT(tracker.IsEmpty());

        UNIT_ASSERT_VALUES_EQUAL(0, tracker.CollectExpired(GetTime(0)));
        UNIT_ASSERT_VALUES_EQUAL(0, tracker.CollectExpired(GetTime(1)));

        tracker.Remove("a");
        tracker.Remove("b");
        UNIT_ASSERT(tracker.IsEmpty());
    }

    Y_UNIT_TEST(AddSeveral) {
        TTestTracker tracker;
        tracker.Add(GetTime(10), "a");
        tracker.Add(GetTime(20), "b");
        tracker.Add(GetTime(30), "c");
        tracker.Add(GetTime(40), "d");
        tracker.Add(GetTime(50), "e");

        UNIT_ASSERT_VALUES_EQUAL(5, tracker.GetSize());
        UNIT_ASSERT(!tracker.IsEmpty());

        UNIT_ASSERT_VALUES_EQUAL(0, tracker.CollectExpired(GetTime(0)));
        UNIT_ASSERT_VALUES_EQUAL(0, tracker.CollectExpired(GetTime(5)));
        UNIT_ASSERT_VALUES_EQUAL(0, tracker.CollectExpired(GetTime(9)));

        UNIT_ASSERT_VALUES_EQUAL(1, tracker.CollectExpired(GetTime(10)));
        UNIT_ASSERT_VALUES_EQUAL("a", tracker.CollectedItems.at(0));
        UNIT_ASSERT_VALUES_EQUAL(4, tracker.GetSize());

        // again 10
        UNIT_ASSERT_VALUES_EQUAL(0, tracker.CollectExpired(GetTime(10)));
        UNIT_ASSERT_VALUES_EQUAL(0, tracker.CollectExpired(GetTime(19)));

        UNIT_ASSERT_VALUES_EQUAL(1, tracker.CollectExpired(GetTime(20)));
        UNIT_ASSERT_VALUES_EQUAL("b", tracker.CollectedItems.at(1));

        UNIT_ASSERT_VALUES_EQUAL(2, tracker.CollectExpired(GetTime(44)));
        UNIT_ASSERT_VALUES_EQUAL("c", tracker.CollectedItems.at(2));
        UNIT_ASSERT_VALUES_EQUAL("d", tracker.CollectedItems.at(3));
        UNIT_ASSERT_VALUES_EQUAL(1, tracker.GetSize());

        UNIT_ASSERT_VALUES_EQUAL(1, tracker.CollectExpired(GetTime(80)));
        UNIT_ASSERT_VALUES_EQUAL("e", tracker.CollectedItems.at(4));

        UNIT_ASSERT(tracker.IsEmpty());
    }

    Y_UNIT_TEST(AddRemoveSeveral) {
        TTestTracker tracker;
        tracker.Add(GetTime(10), "a");
        tracker.Add(GetTime(20), "b");
        tracker.Add(GetTime(30), "c");
        tracker.Add(GetTime(40), "d");
        tracker.Add(GetTime(50), "e");

        UNIT_ASSERT_VALUES_EQUAL(1, tracker.CollectExpired(GetTime(10)));
        UNIT_ASSERT_VALUES_EQUAL("a", tracker.CollectedItems.at(0));
        tracker.Remove("a");

        UNIT_ASSERT_VALUES_EQUAL(1, tracker.CollectExpired(GetTime(22)));
        UNIT_ASSERT_VALUES_EQUAL("b", tracker.CollectedItems.at(1));
        tracker.Remove("d");

        UNIT_ASSERT_VALUES_EQUAL(2, tracker.CollectExpired(GetTime(55)));
        UNIT_ASSERT_VALUES_EQUAL("c", tracker.CollectedItems.at(2));
        UNIT_ASSERT_VALUES_EQUAL("e", tracker.CollectedItems.at(3));

        UNIT_ASSERT(tracker.IsEmpty());
    }
}
