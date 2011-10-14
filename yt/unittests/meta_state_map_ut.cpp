#include "../ytlib/meta_state/map.h"

#include "../ytlib/misc/serialize.h"

#include <util/random/random.h>
#include <util/system/tempfile.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NUnitTest {

////////////////////////////////////////////////////////////////////////////////

struct TMyInt
{
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
        Write(*output, Value);
    }

    static TAutoPtr<TMyInt> Load(TInputStream* input)
    {
        int value;
        Read(*input, &value);
        return new TMyInt(value);
    }
};

class TMetaStateMapTest: public ::testing::Test
{ };

typedef Stroka TKey;
typedef TMyInt TValue;


TEST_F(TMetaStateMapTest, BasicsInNormalMode)
{
    NMetaState::TMetaStateMap<TKey, TValue> map;

    map.Insert("a", new TValue(42)); // add
    EXPECT_EQ(map.Find("a")->Value, 42);

    ASSERT_DEATH(map.Insert("a", new TValue(21)), ".*"); // add existing
    EXPECT_EQ(map.Find("a")->Value, 42);

    map.Remove("a"); // remove
    EXPECT_EQ(map.Find("a") == NULL, true);

    ASSERT_DEATH(map.Remove("a"), ".*"); // remove non exisiting

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
    TTempFileHandle file(GenerateRandomFileName("MetaStateMap"));
    TBufferedFileOutput output(file);
    TOutputStream* stream = &output;

    NMetaState::TMetaStateMap<TKey, TValue> map;
    TActionQueue::TPtr actionQueue = New<TActionQueue>();
    IInvoker::TPtr invoker = actionQueue->GetInvoker();

    TFuture<TVoid>::TPtr asyncResult;

    asyncResult = map.Save(invoker, stream);
    map.Insert("b", new TValue(42)); // add to temp table
    EXPECT_EQ(map.Find("b")->Value, 42); // check find in temp tables

    asyncResult->Get();
    EXPECT_EQ(map.Find("b")->Value, 42); // check find in main table

    asyncResult = map.Save(invoker, stream);
    ASSERT_DEATH(map.Insert("b", new TValue(21)), ".*"); // add existing
    asyncResult->Get();
    EXPECT_EQ(map.Find("b")->Value, 42); // check find in main table

    asyncResult = map.Save(invoker, stream);
    map.Remove("b"); // remove
    EXPECT_EQ(map.Find("b") == NULL, true); // check find in temp table

    asyncResult->Get();
    EXPECT_EQ(map.Find("b") == NULL, true); // check find in main table

    asyncResult = map.Save(invoker, stream);
    ASSERT_DEATH(map.Remove("b"), ".*"); // remove non existing
    asyncResult->Get();

    // update in temp table
    asyncResult = map.Save(invoker, stream);
    map.Insert("b", new TValue(999));

    TValue* ptr;
    ptr = map.FindForUpdate("b");
    EXPECT_EQ(ptr->Value, 999);
    ptr->Value = 9000; // update value
    EXPECT_EQ(map.Find("b")->Value, 9000);

    asyncResult->Get();
    EXPECT_EQ(map.Find("b")->Value, 9000);

    // update in main table
    asyncResult = map.Save(invoker, stream);
    ptr = map.FindForUpdate("b");
    EXPECT_EQ(ptr->Value, 9000);
    ptr->Value = -1; // update value
    EXPECT_EQ(map.Find("b")->Value, -1);

    asyncResult->Get();
    EXPECT_EQ(map.Find("b")->Value, -1);
}

TEST_F(TMetaStateMapTest, SaveAndLoad)
{
    srand(42); // set seed
    TTempFileHandle file(GenerateRandomFileName("MetaStateMap"));
    yhash_map<TKey, int> checkMap;
    TActionQueue::TPtr actionQueue = New<TActionQueue>();
    IInvoker::TPtr invoker = actionQueue->GetInvoker();
    {
        NMetaState::TMetaStateMap<TKey, TValue> map;

        int numValues = 10000;
        int range = 1000;
        for (int i = 0; i < numValues; ++i) {
            TKey key = ToString(rand() % range);
            int value = rand();
            bool result = checkMap.insert(MakePair(key, value)).second;
            if (result) {
                map.Insert(key, new TValue(value));
            } else {
                EXPECT_EQ(map.Get(key).Value, checkMap[key]);
            }
        }
        TBufferedFileOutput output(file);
        TOutputStream* stream = &output;
        map.Save(invoker, stream)->Get();
    }
    {
        NMetaState::TMetaStateMap<TKey, TValue> map;
        TBufferedFileInput input(file);
        TInputStream* stream = &input;

        map.Load(invoker, stream)->Get();

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
    TTempFileHandle file(GenerateRandomFileName("MetaStateMap"));
    TBufferedFileOutput output(file);
    TOutputStream* stream = &output;

    yhash_map<TKey, int> checkMap;
    TActionQueue::TPtr actionQueue = New<TActionQueue>();
    IInvoker::TPtr invoker = actionQueue->GetInvoker();
    NMetaState::TMetaStateMap<TKey, TValue> map;

    int numValues = 100000;
    int range = 100000;

    for (int i = 0; i < numValues; ++i) {
        TKey key = ToString(rand() % range);
        int value = rand();
        bool result = checkMap.insert(MakePair(key, value)).second;
        if (result) {
            map.Insert(key, new TValue(value));
        } else {
            EXPECT_EQ(map.Get(key).Value, checkMap[key]);
        }
    }
    TFuture<TVoid>::TPtr asyncResult = map.Save(invoker, stream);

    int numActions = 100000;
    range = 200000;
    for (int i = 0; i < numActions; ++i) {
        TKey key = ToString(rand() % range);
        int value = rand();

        int action = rand() % 3;
        if (action == 0) {
            // insert
            bool result = checkMap.insert(MakePair(key, value)).second;
            if (result) {
                map.Insert(key, new TValue(value));
            } else {
                EXPECT_EQ(map.Get(key).Value, checkMap[key]);
            }
        }
        if (action == 1) {
            // update
            TValue* ptr = map.FindForUpdate(key);
            auto it = checkMap.find(key);
            if (it == checkMap.end()) {
                EXPECT_IS_TRUE(ptr == NULL);
            } else {
                EXPECT_EQ(ptr->Value, it->second);
            }
            it->second = value;
            ptr->Value = value;
        }
        if (action == 2) {
            // remove
            bool result = checkMap.erase(key) == 1;
            if (result) {
                map.Remove(key);
            } else {
                EXPECT_IS_TRUE(map.Find(key) == NULL);
            }
        }
    }
    asyncResult->Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NUnitTest
} // namespace NYT
