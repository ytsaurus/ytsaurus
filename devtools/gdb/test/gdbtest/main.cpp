#include <vector>
#include <bitset>
#include <list>
#include <map>
#include <set>
#include <deque>
#include <array>
#include <unordered_map>
#include <unordered_set>
#include <optional>
#include <string_view>

#include <util/generic/hash.h>
#include <util/generic/hash_multi_map.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/variant.h>
#include <util/generic/vector.h>
#include <util/generic/maybe.h>
#include <library/cpp/enumbitset/enumbitset.h>

std::tuple<int, int> test_tuple = {1, 2};

std::vector<int> test_vector = {1, 2, 3, 4};

std::bitset<8> test_bitset(0xA5);

std::map<int, int> test_map = {{1, 2}, {3, 4}, {5, 6}, {7, 8}, {9, 10}};

std::multimap<int, int> test_multimap = {{1, 2}, {3, 4}, {1, 4}};

std::set<int> test_set = {1, 2, 3, 4, 5, 6};

std::unordered_map<int, int> test_unordered_map = {{1, 2}};
std::unordered_set<int> test_unordered_set = {1};

std::deque<int> test_deque = {1, 2, 3, 4};

std::pair<int, int> test_pair = {1, 2};

std::array<int, 4> test_array = {{1, 2, 3, 4}};

std::string test_string = "Это тест.";
std::string_view test_string_view(test_string);

std::unique_ptr<int> test_unique_int(new int(1));
std::unique_ptr<int> test_unique_empty;

std::shared_ptr<int> test_shared_int(new int(1));
std::shared_ptr<int> test_shared_empty;

std::variant<int, std::string> test_variant_default;  // Untested.
std::variant<int, std::string> test_variant_int(10);  // Untested.
std::variant<int, std::string> test_variant_string(test_string);  // Untested.

TVector<int> test_tvector = {1, 2, 3, 4};
THashMap<int, int> test_hashmap = {{1, 2}, {3, 4}};
THashSet<int> test_hashset = {1, 2, 3};
THashMultiMap<int, int> test_hashmultimap = {{1, 2}, {3, 4}, {1, 4}};
THashMap<TString, TString> test_tstring_hashmap = {{"Это", "тест"}};

TString test_tstring = "Это тест.";
TUtf16String test_tutf16string = u"Это тест.";
TUtf32String test_tutf32string = U"Это тест.";  // Untested.

TStringBuf test_tstringbuf = "Это тест.";

using TVariantType = std::variant<int, TString>;

TVariantType test_tvariant_default;
TVariantType test_tvariant_int(10);
TVariantType test_tvariant_string(test_tstring);

using TMaybeType = TMaybe<int>;

TMaybeType test_maybe(465);
TMaybeType test_maybe_empty;

enum TEnumType: int {
    A = 0,
    B = 1,
    SIZE = 2
};

TEnumBitSet<TEnumType, TEnumType::A, TEnumType::SIZE> test_enumbitset = {A, B};

int test_int = 1;
THolder<int> test_holder_int(&test_int);
THolder<int> test_holder_empty;
TCowPtr<TSimpleSharedPtr<int>> test_cow_shared_empty;
TCowPtr<TSimpleSharedPtr<int>> test_cow_shared{new int(1)};

THolder<std::string> test_holder_string_empty;

std::map<int, int>::iterator test_map_iterator;
std::multimap<int, int>::iterator test_multimap_iterator;
std::set<int>::iterator test_set_iterator;
std::deque<int>::iterator test_deque_iterator;
std::unordered_map<int, int>::iterator test_unordered_map_iterator;
std::unordered_set<int>::iterator test_unordered_set_iterator;
std::optional<int> test_optional_int_empty;
std::optional<int> test_optional_int(123);

std::atomic<int> test_atomic_int(12);
std::atomic<std::array<int64_t, 3>> test_atomic_array(std::array<int64_t, 3>{1, 2, 3});

std::list<int> test_list_empty;
std::list<int> test_list{1, 2, 3};

// Variable which can't be statically initialized due to undetermined order
// of static initialization in C++.
void init() {
    test_map_iterator = test_map.begin();
    test_multimap_iterator = test_multimap.begin();
    test_set_iterator = test_set.begin();
    test_deque_iterator = test_deque.begin();
    test_unordered_map_iterator = test_unordered_map.begin();
    test_unordered_set_iterator = test_unordered_set.begin();
}

void stop_here() {
    [[maybe_unused]] volatile int dummy;
    dummy = 0;
}

int main() {
    init();
    stop_here();
}
