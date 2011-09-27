#include "../ytlib/meta_state/map.h"

#include <util/random/random.h>
#include <util/system/tempfile.h>

#include "framework/framework.h"

namespace NYT {
namespace NUnitTest {

////////////////////////////////////////////////////////////////////////////////

struct TIntRefCounted
{
    int Value;

    TIntRefCounted()
    { }

    TIntRefCounted(const TIntRefCounted& other)
        : Value(other.Value)
    { }

    TIntRefCounted(int value)
        : Value(value)
    { }
};

class TMetaStateMapTest: public ::testing::Test
{ };

typedef Stroka TKey;
typedef TIntRefCounted TValue;


TEST_F(TMetaStateMapTest, BasicsInNormalMode)
{
    TMetaStateMap<TKey, TValue> map;

    EXPECT_IS_TRUE(map.Insert("a", TValue(42))); // add
    EXPECT_EQ(map.Find("a")->Value, 42);

    EXPECT_IS_FALSE(map.Insert("a", TValue(21))); // add existing
    EXPECT_EQ(map.Find("a")->Value, 42);

    EXPECT_IS_TRUE(map.Remove("a")); // remove
    EXPECT_EQ(map.Find("a") == NULL, true);

    EXPECT_IS_FALSE(map.Remove("a")); // remove non exisiting

    EXPECT_IS_TRUE(map.Insert("a", TValue(10)));
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

    TMetaStateMap<TKey, TValue> map;
    IInvoker::TPtr invoker = new TActionQueue();

    TFuture<TVoid>::TPtr asyncResult;

    asyncResult = map.Save(invoker, stream);
    EXPECT_IS_TRUE(map.Insert("b", TValue(42))); // add to temp table
    EXPECT_EQ(map.Find("b")->Value, 42); // check find in temp tables

    asyncResult->Get();
    EXPECT_EQ(map.Find("b")->Value, 42); // check find in main table

    asyncResult = map.Save(invoker, stream);
    EXPECT_IS_FALSE(map.Insert("b", TValue(21))); // add existing
    asyncResult->Get();
    EXPECT_EQ(map.Find("b")->Value, 42); // check find in main table

    asyncResult = map.Save(invoker, stream);
    EXPECT_IS_TRUE(map.Remove("b")); // remove
    EXPECT_EQ(map.Find("b") == NULL, true); // check find in temp table

    asyncResult->Get();
    EXPECT_EQ(map.Find("b") == NULL, true); // check find in main table

    asyncResult = map.Save(invoker, stream);
    EXPECT_IS_FALSE(map.Remove("b")); // remove non existing
    asyncResult->Get();

    // update in temp table
    asyncResult = map.Save(invoker, stream);
    EXPECT_IS_TRUE(map.Insert("b", TValue(999)));

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
    IInvoker::TPtr invoker = new TActionQueue();
    {
        TMetaStateMap<TKey, TValue> map;

        int numValues = 10000;
        int range = 1000;
        for (int i = 0; i < numValues; ++i) {
            TKey key = ToString(rand() % range);
            int value = rand();
            bool result = checkMap.insert(MakePair(key, value)).second;
            EXPECT_EQ(map.Insert(key, TValue(value)), result);
        }
        TBufferedFileOutput output(file);
        TOutputStream* stream = &output;
        map.Save(invoker, stream)->Get();
    }
    {
        TMetaStateMap<TKey, TValue> map;
        TBufferedFileInput input(file);
        TInputStream* stream = &input;

        map.Load(invoker, stream)->Get();

        // assert checkMap \subseteq map
        FOREACH(const auto& pair, checkMap) {
            EXPECT_EQ(map.Find(pair.first)->Value, pair.second);
        }

        // assert map \subseteq checkMap
        FOREACH(const auto& pair, map) {
            EXPECT_EQ(checkMap.find(pair.first)->second, pair.second.Value);
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
    IInvoker::TPtr invoker = new TActionQueue();
    TMetaStateMap<TKey, TValue> map;

    int numValues = 100000;
    int range = 100000;

    for (int i = 0; i < numValues; ++i) {
        TKey key = ToString(rand() % range);
        int value = rand();
        bool result = checkMap.insert(MakePair(key, value)).second;
        EXPECT_EQ(map.Insert(key, TValue(value)), result);
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
            EXPECT_EQ(
                map.Insert(key, TValue(value)),
                checkMap.insert(MakePair(key, value)).second);
        }
        if (action == 1) {
            // update
            TValue* ptr = map.FindForUpdate(key);
            auto it = checkMap.find(key);
            if (it == checkMap.end()) {
                EXPECT_EQ(ptr == NULL, true);
            } else {
                EXPECT_EQ(ptr->Value, it->second);
            }
            it->second = value;
            ptr->Value = value;
        }
        if (action == 2) {
            // remove
            EXPECT_EQ(map.Remove(key), checkMap.erase(key) == 1);
        }
    }
    asyncResult->Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NUnitTest
} // namespace NYT
