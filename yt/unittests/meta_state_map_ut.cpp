#include "stdafx.h"

#include "../ytlib/meta_state/map.h"

#include "../ytlib/misc/serialize.h"

#include <util/random/random.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NUnitTest {

////////////////////////////////////////////////////////////////////////////////

namespace {
    typedef Stroka TKey;
    
    struct TMyInt
    {
        static THolder<Event> SaveEvent;

        int Value;

        TMyInt()
        { }

        TMyInt(const TMyInt& other)
            : Value(other.Value)
        { }

        TMyInt(int value)
            : Value(value)
        { }

        TAutoPtr<TMyInt> Clone() const
        {
            return new TMyInt(*this);
        }

        void Save(TOutputStream* output) const
        {
            SaveEvent->Wait();
            Write(*output, Value);
        }

        static TAutoPtr<TMyInt> Load(const TKey& /* fake */, TInputStream* input)
        {
            int value;
            Read(*input, &value);
            return new TMyInt(value);
        }
    };

    THolder<Event> TMyInt::SaveEvent;

    class TMetaStateMapTest: public ::testing::Test
    { };

    typedef TMyInt TValue;
} // namespace <anonymous>

////////////////////////////////////////////////////////////////////////////////

TEST_F(TMetaStateMapTest, BasicsInNormalMode)
{
    NMetaState::TMetaStateMap<TKey, TValue> map;

    map.Insert("a", new TValue(42)); // add
    EXPECT_EQ(map.Find("a")->Value, 42);

    ASSERT_DEATH(map.Insert("a", new TValue(21)), ".*"); // add existing
    EXPECT_EQ(map.Find("a")->Value, 42);

    map.Remove("a"); // remove
    EXPECT_EQ(map.Find("a") == NULL, true);

    ASSERT_DEATH(map.Remove("a"), ".*"); // remove non existing

    map.Insert("a", new TValue(10));
    TValue* ptr = map.FindForUpdate("a");
    EXPECT_EQ(ptr->Value, 10);
    ptr->Value = 100; // update value
    EXPECT_EQ(map.Find("a")->Value, 100);

    map.Clear();
    EXPECT_EQ(map.Find("a") == NULL, true);
}

TEST_F(TMetaStateMapTest, BasicsInSavingSnapshotMode)
{
    Stroka snapshotData;
    TStringOutput output(snapshotData);

    NMetaState::TMetaStateMap<TKey, TValue> map;
    TActionQueue::TPtr actionQueue = New<TActionQueue>();
    IInvoker::TPtr invoker = actionQueue->GetInvoker();
    TMyInt::SaveEvent.Reset(new Event());
    auto& saveEvent = *TMyInt::SaveEvent;

    TFuture<TVoid>::TPtr asyncResult;

    asyncResult = map.Save(invoker, &output);
    map.Insert("b", new TValue(42)); // add to temp table
    EXPECT_EQ(map.Find("b")->Value, 42); // check find in temp tables

    saveEvent.Signal();
    asyncResult->Get();
    EXPECT_EQ(map.Find("b")->Value, 42); // check find in main table

    saveEvent.Reset();
    asyncResult = map.Save(invoker, &output);
    ASSERT_DEATH(map.Insert("b", new TValue(21)), ".*"); // add existing
    saveEvent.Signal();
    asyncResult->Get();
    EXPECT_EQ(map.Find("b")->Value, 42); // check find in main table

    saveEvent.Reset();
    asyncResult = map.Save(invoker, &output);
    map.Remove("b"); // remove
    EXPECT_EQ(map.Find("b") == NULL, true); // check find in temp table

    saveEvent.Signal();
    asyncResult->Get();
    EXPECT_EQ(map.Find("b") == NULL, true); // check find in main table

    saveEvent.Reset();
    asyncResult = map.Save(invoker, &output);
    ASSERT_DEATH(map.Remove("b"), ".*"); // remove non existing
    saveEvent.Signal();
    asyncResult->Get();

    // update in temp table
    saveEvent.Reset();
    asyncResult = map.Save(invoker, &output);
    map.Insert("b", new TValue(999));

    TValue* ptr;
    ptr = map.FindForUpdate("b");
    EXPECT_EQ(ptr->Value, 999);
    ptr->Value = 9000; // update value
    EXPECT_EQ(map.Find("b")->Value, 9000);

    saveEvent.Signal();
    asyncResult->Get();
    EXPECT_EQ(map.Find("b")->Value, 9000);

    // update in main table
    saveEvent.Reset();
    asyncResult = map.Save(invoker, &output);
    ptr = map.FindForUpdate("b");
    EXPECT_EQ(ptr->Value, 9000);
    ptr->Value = -1; // update value
    EXPECT_EQ(map.Find("b")->Value, -1);

    saveEvent.Signal();
    asyncResult->Get();
    EXPECT_EQ(map.Find("b")->Value, -1);
}

TEST_F(TMetaStateMapTest, SaveAndLoad)
{
    srand(42); // set seed
    yhash_map<TKey, int> checkMap;
    TActionQueue::TPtr actionQueue = New<TActionQueue>();
    IInvoker::TPtr invoker = actionQueue->GetInvoker();
    TMyInt::SaveEvent.Reset(new Event());
    TMyInt::SaveEvent->Signal();
    Stroka snapshotData;
    {
        NMetaState::TMetaStateMap<TKey, TValue> map;

        const int valueCount = 10000;
        const int valueRange = 1000;
        for (int i = 0; i < valueCount; ++i) {
            TKey key = ToString(rand() % valueRange);
            int value = rand();
            bool result = checkMap.insert(MakePair(key, value)).second;
            if (result) {
                map.Insert(key, new TValue(value));
            } else {
                EXPECT_EQ(map.Get(key).Value, checkMap[key]);
            }
        }
        TStringOutput output(snapshotData);
        map.Save(invoker, &output)->Get();
    }
    {
        NMetaState::TMetaStateMap<TKey, TValue> map;
        TStringInput input(snapshotData);
        map.Load(&input);

        // assert checkMap \subseteq map
        FOREACH(const auto& pair, checkMap) {
            EXPECT_EQ(map.Find(pair.first)->Value, pair.second);
        }

        // assert map \subseteq checkMap
        FOREACH(const auto& pair, map) {
            EXPECT_EQ(checkMap.find(pair.first)->second, pair.second->Value);
        }
    }
}

TEST_F(TMetaStateMapTest, StressSave)
{
    srand(42); // set seed
    Stroka snapshotData;
    TStringOutput output(snapshotData);

    yhash_map<TKey, int> checkMap;
    TActionQueue::TPtr actionQueue = New<TActionQueue>();
    IInvoker::TPtr invoker = actionQueue->GetInvoker();
    NMetaState::TMetaStateMap<TKey, TValue> map;

    const int valueCount = 100000;
    const int insertRange = 100000;

    for (int i = 0; i < valueCount; ++i) {
        TKey key = ToString(rand() % insertRange);
        int value = rand();
        bool result = checkMap.insert(MakePair(key, value)).second;
        if (result) {
            map.Insert(key, new TValue(value));
        } else {
            EXPECT_EQ(map.Get(key).Value, checkMap[key]);
        }
    }
    TMyInt::SaveEvent.Reset(new Event());
    TFuture<TVoid>::TPtr asyncResult = map.Save(invoker, &output);

    const int actionCount = 100000;
    const int selectRange = 200000;
    
    for (int i = 0; i < actionCount; ++i) {
        TKey key = ToString(rand() % selectRange);
        int value = rand();

        int action = rand() % 3;
        switch (action) {
            case 0: {
                SCOPED_TRACE("Performing Insert");

                bool result = checkMap.insert(MakePair(key, value)).second;
                if (result) {
                    map.Insert(key, new TValue(value));
                } else {
                    EXPECT_EQ(map.Get(key).Value, checkMap[key]);
                }
            }
            case 1: {
                SCOPED_TRACE("Performing Update");

                TValue* ptr = map.FindForUpdate(key);
                auto it = checkMap.find(key);
                if (it == checkMap.end()) {
                    EXPECT_IS_TRUE(ptr == NULL);
                } else {
                    EXPECT_EQ(ptr->Value, it->second);
                    it->second = value;
                    ptr->Value = value;
                }
            }
            case 2: {
                SCOPED_TRACE("Performing Remove");

                bool result = checkMap.erase(key) == 1;
                if (result) {
                    map.Remove(key);
                } else {
                    EXPECT_IS_TRUE(map.Find(key) == NULL);
                }
            }
        }
    }
    TMyInt::SaveEvent->Signal();
    asyncResult->Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NUnitTest
} // namespace NYT
