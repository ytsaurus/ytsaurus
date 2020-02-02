#include "lock_free_hash_table_and_concurrent_cache_helpers.h"

#include <yt/core/test_framework/framework.h>

#include <yt/core/misc/lock_free_hash_table.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT::NConcurrency;

TEST(TLockFreeHashTableTest, Simple)
{
    size_t keyColumnCount = 3;
    size_t columnCount = 5;

    TLockFreeHashTable<TElement, TDefaultAllocator> table(1000);
    TDefaultAllocator allocator;

    THash<TElement> hash;
    TEqualTo<TElement> equalTo;

    std::vector<TRefCountedPtr<TElement>> checkTable;

    size_t iterations = 1000;

    TRandomCharGenerator randomChar(0);

    for (size_t index = 0; index < iterations; ++index) {
        auto item = CreateObjectWithExtraSpace<TElement>(&allocator, columnCount);
        {
            item->Size = keyColumnCount;
            for (size_t pos = 0; pos < columnCount; ++pos) {
                item->Data[pos] = randomChar();
            }
            item->Hash = THash<TStringBuf>{}(TStringBuf(&item->Data[0], keyColumnCount));
        }
        checkTable.push_back(item);
    }

    std::sort(checkTable.begin(), checkTable.end(), [] (
        const TRefCountedPtr<TElement>& lhs,
        const TRefCountedPtr<TElement>& rhs)
    {
        return memcmp(lhs->Data, rhs->Data, lhs->Size) < 0;
    });

    auto it = std::unique(checkTable.begin(), checkTable.end(), [&] (
        const TRefCountedPtr<TElement>& lhs,
        const TRefCountedPtr<TElement>& rhs)
    {
        return equalTo(lhs.Get(), rhs.Get());
    });

    checkTable.erase(it, checkTable.end());

    std::random_shuffle(checkTable.begin(), checkTable.end());

    for (const auto& item : checkTable) {
        auto fingerprint = hash(item.Get());
        table.Insert(fingerprint, item);
    }

    for (const auto& item : checkTable) {
        auto fingerprint = hash(item.Get());
        auto found = table.Find(fingerprint, item.Get());

        EXPECT_TRUE(found);
    }

    std::vector<TRefCountedPtr<TElement>> updateTable;
    for (size_t index = 0; index < checkTable.size(); index += 2) {
        const auto& current = checkTable[index];

        auto item = CreateObjectWithExtraSpace<TElement>(&allocator, columnCount);
        memcpy(item.Get(), current.Get(), sizeof(TElement) + columnCount);
        std::swap(item->Data[columnCount - 1], item->Data[columnCount - 2]);

        updateTable.push_back(item);
    }

    for (const auto& item : updateTable) {
        auto fingerprint = hash(item.Get());
        EXPECT_TRUE(table.Update(fingerprint, item));
    }

    for (const auto& item : checkTable) {
        auto fingerprint = hash(item.Get());
        auto found = table.Find(fingerprint, item.Get());

        EXPECT_TRUE(found);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
