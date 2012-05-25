#include "stdafx.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/actions/action_queue.h>
#include <ytlib/meta_state/map.h>

#include <util/random/random.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NUnitTest {

////////////////////////////////////////////////////////////////////////////////

namespace {
    typedef Stroka TKey;
    
    struct TMyInt
    {
        int Value;

        TMyInt()
        { }

        TMyInt(const Stroka&)
        { }

        TMyInt(int value)
            : Value(value)
        { }

        void Save(TOutputStream* output) const
        {
            WritePod(*output, Value);
        }

        void Load(TVoid, TInputStream* input)
        {
            ReadPod(*input, Value);
        }
    };

    class TMetaStateMapTest: public ::testing::Test
    { };

    typedef TMyInt TValue;
} // namespace

////////////////////////////////////////////////////////////////////////////////

TEST_F(TMetaStateMapTest, BasicsInNormalMode)
{
    NMetaState::TMetaStateMap<TKey, TValue> map;

    map.Insert("a", new TValue(42)); // add
    EXPECT_EQ(map.Find("a")->Value, 42);

    // TODO(babenko): this won't crash in release, think about this
    //ASSERT_DEATH(map.Insert("a", new TValue(21)), ".*"); // add existing
    EXPECT_EQ(map.Find("a")->Value, 42);

    map.Remove("a"); // remove
    EXPECT_EQ(map.Find("a") == NULL, true);

    ASSERT_DEATH(map.Remove("a"), ".*"); // remove non existing

    map.Insert("a", new TValue(10));
    TValue* ptr = map.Find("a");
    EXPECT_EQ(ptr->Value, 10);
    ptr->Value = 100; // update value
    EXPECT_EQ(map.Find("a")->Value, 100);

    map.Clear();
    EXPECT_EQ(map.Find("a") == NULL, true);
}

TEST_F(TMetaStateMapTest, SaveAndLoad)
{
    srand(42); // set seed
    yhash_map<TKey, int> checkMap;
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
        map.SaveKeys(&output);
        map.SaveValues(&output);
    }
    {
        NMetaState::TMetaStateMap<TKey, TValue> map;
        TStringInput input(snapshotData);
        map.LoadKeys(&input);
        TVoid context;
        map.LoadValues(context, &input);

        // assert checkMap \subseteq map
        FOREACH (const auto& pair, checkMap) {
            EXPECT_EQ(map.Find(pair.first)->Value, pair.second);
        }

        // assert map \subseteq checkMap
        FOREACH (const auto& pair, map) {
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
    map.SaveKeys(&output);
    map.SaveValues(&output);

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

                TValue* ptr = map.Find(key);
                auto it = checkMap.find(key);
                if (it == checkMap.end()) {
                    EXPECT_IS_TRUE(!ptr);
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

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NUnitTest
} // namespace NYT
