#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <climits>
#include <cstdint>
#include <cstring>
#include <immintrin.h>
#include <iostream>
#include <random>
#include <stdexcept>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <iterator>
#include "yt/yt/library/query/engine/simd_hashtable.h"
// std::array<__m256i, 256> perm{};

// bool perm_init = false;

// void init_perm() {
//   perm_init = true;
//   for (int i = 0; i < 256; i++) {
//     int i_copy = i;
//     std::vector<int> pos_with_one;
//     std::vector<int> pos_with_zero;
//     for (int index = 0; index < 8; index++) {
//       if (i_copy % 2 == 0) {
//         pos_with_zero.push_back(7 - index);
//       } else {
//         // std::cout << "push one" << std::endl;
//         pos_with_one.push_back(7 - index);
//       }
//       i_copy /= 2;
//     }
//     std::array<int, 8> init_result{};
//     for (int j = 0; j < pos_with_one.size(); j++) {
//       init_result[j] = pos_with_one[j];
//     }
//     for (int j = 0; j < pos_with_zero.size(); j++) {
//       init_result[pos_with_one.size() + j] = pos_with_zero[j];
//     }
//     perm[i] = _mm256_setr_epi32(init_result[0], init_result[1], init_result[2],
//                                 init_result[3], init_result[4], init_result[5],
//                                 init_result[6], init_result[7]);
//   }
// }

// template <size_t ind>
// bool scalar(__m256i invalid, __m256i keys, __m256i hash,
//             const std::vector<int> &table_32, const int table_32_size,
//             int *out_keys) {
//   auto invalid_ind = _mm256_extract_epi32(invalid, ind);
//   if (invalid_ind != 0) {
//     auto pos = _mm256_extract_epi32(hash, ind);
//     auto key = _mm256_extract_epi32(keys, ind);
//     while (table_32[pos] != INT_MIN && table_32[pos] != key) {
//       pos = (pos + 1) % table_32_size;
//     }
//     if (table_32[pos] == key) {
//       *out_keys = key;
//       return true;
//     }
//   }
//   return false;
// }

// void dump_simd(const std::string &prefix, __m256i simd) {
//   std::cout << prefix << ":" << _mm256_extract_epi32(simd, 0) << ","
//             << _mm256_extract_epi32(simd, 1) << ","
//             << _mm256_extract_epi32(simd, 2) << ","
//             << _mm256_extract_epi32(simd, 3) << ","
//             << _mm256_extract_epi32(simd, 4) << ","
//             << _mm256_extract_epi32(simd, 5) << ","
//             << _mm256_extract_epi32(simd, 6) << ","
//             << _mm256_extract_epi32(simd, 7) << std::endl;
// }

// const int PRIME = 31;

// int scalar_hash(int key, const std::vector<int> &table) {
//   return (key * PRIME) % (table.size());
// }

// uint8_t reverse_byte(unsigned char x) {
//   static const uint8_t table[] = {
//       0x00, 0x80, 0x40, 0xc0, 0x20, 0xa0, 0x60, 0xe0, 0x10, 0x90, 0x50, 0xd0,
//       0x30, 0xb0, 0x70, 0xf0, 0x08, 0x88, 0x48, 0xc8, 0x28, 0xa8, 0x68, 0xe8,
//       0x18, 0x98, 0x58, 0xd8, 0x38, 0xb8, 0x78, 0xf8, 0x04, 0x84, 0x44, 0xc4,
//       0x24, 0xa4, 0x64, 0xe4, 0x14, 0x94, 0x54, 0xd4, 0x34, 0xb4, 0x74, 0xf4,
//       0x0c, 0x8c, 0x4c, 0xcc, 0x2c, 0xac, 0x6c, 0xec, 0x1c, 0x9c, 0x5c, 0xdc,
//       0x3c, 0xbc, 0x7c, 0xfc, 0x02, 0x82, 0x42, 0xc2, 0x22, 0xa2, 0x62, 0xe2,
//       0x12, 0x92, 0x52, 0xd2, 0x32, 0xb2, 0x72, 0xf2, 0x0a, 0x8a, 0x4a, 0xca,
//       0x2a, 0xaa, 0x6a, 0xea, 0x1a, 0x9a, 0x5a, 0xda, 0x3a, 0xba, 0x7a, 0xfa,
//       0x06, 0x86, 0x46, 0xc6, 0x26, 0xa6, 0x66, 0xe6, 0x16, 0x96, 0x56, 0xd6,
//       0x36, 0xb6, 0x76, 0xf6, 0x0e, 0x8e, 0x4e, 0xce, 0x2e, 0xae, 0x6e, 0xee,
//       0x1e, 0x9e, 0x5e, 0xde, 0x3e, 0xbe, 0x7e, 0xfe, 0x01, 0x81, 0x41, 0xc1,
//       0x21, 0xa1, 0x61, 0xe1, 0x11, 0x91, 0x51, 0xd1, 0x31, 0xb1, 0x71, 0xf1,
//       0x09, 0x89, 0x49, 0xc9, 0x29, 0xa9, 0x69, 0xe9, 0x19, 0x99, 0x59, 0xd9,
//       0x39, 0xb9, 0x79, 0xf9, 0x05, 0x85, 0x45, 0xc5, 0x25, 0xa5, 0x65, 0xe5,
//       0x15, 0x95, 0x55, 0xd5, 0x35, 0xb5, 0x75, 0xf5, 0x0d, 0x8d, 0x4d, 0xcd,
//       0x2d, 0xad, 0x6d, 0xed, 0x1d, 0x9d, 0x5d, 0xdd, 0x3d, 0xbd, 0x7d, 0xfd,
//       0x03, 0x83, 0x43, 0xc3, 0x23, 0xa3, 0x63, 0xe3, 0x13, 0x93, 0x53, 0xd3,
//       0x33, 0xb3, 0x73, 0xf3, 0x0b, 0x8b, 0x4b, 0xcb, 0x2b, 0xab, 0x6b, 0xeb,
//       0x1b, 0x9b, 0x5b, 0xdb, 0x3b, 0xbb, 0x7b, 0xfb, 0x07, 0x87, 0x47, 0xc7,
//       0x27, 0xa7, 0x67, 0xe7, 0x17, 0x97, 0x57, 0xd7, 0x37, 0xb7, 0x77, 0xf7,
//       0x0f, 0x8f, 0x4f, 0xcf, 0x2f, 0xaf, 0x6f, 0xef, 0x1f, 0x9f, 0x5f, 0xdf,
//       0x3f, 0xbf, 0x7f, 0xff,
//   };
//   return table[x];
// }

// int scalar_insert(int key, std::vector<int> &table_32) {
//   int hash = scalar_hash(key, table_32);
//   int offset = 0;
//   int start_index = hash;
//   while (table_32[(offset + hash) % table_32.size()] != INT_MIN) {
//     // std::cout << "scalar_insert" << std::endl;
//     offset++;
//     if (((offset + hash) % table_32.size()) == start_index) {
//       throw std::runtime_error("scalar insert cycle");
//     }
//   }
//   table_32[(offset + hash) % table_32.size()] = key;
//   return 0;
// }

// bool scalar_find(int key, const std::vector<int> &table_32) {
//   int hash = scalar_hash(key, table_32);
//   int offset = 0;
//   while (table_32[(offset + hash) % table_32.size()] != INT_MIN &&
//          table_32[(offset + hash) % table_32.size()] != key) {
//     offset++;
//   }
//   return table_32[(offset + hash) % table_32.size()] == key;
// }

// int simd_find(const std::vector<int> &S_keys, std::vector<int> &S_found,
//          const std::vector<int> &table_32) {
//   if (!perm_init) {
//     throw std::runtime_error("perm is not initted");
//   }
//   const int table_32_size = table_32.size();
//   assert(table_32_size % 2 == 0);
//   int S_keys_ind = 0;
//   int S_found_ind = 0;
//   __m256i invalid;
//   invalid = _mm256_cmpeq_epi32(invalid, invalid); // set mask to one

//   __m256i offset;
//   offset = _mm256_xor_si256(offset, offset); // reset vector to zero

//   __m256i key;

//   __m256i PRIMES =
//       _mm256_set_epi32(PRIME, PRIME, PRIME, PRIME, PRIME, PRIME, PRIME, PRIME);
//   __m256i ONES = _mm256_set_epi32(1, 1, 1, 1, 1, 1, 1, 1);
//   __m256i EMPTYS = _mm256_set_epi32(INT32_MIN, INT32_MIN, INT32_MIN, INT32_MIN,
//                                     INT32_MIN, INT32_MIN, INT32_MIN, INT32_MIN);

//   __m256i table_key;

//   __m256i module =
//       _mm256_set_epi32(table_32_size - 1, table_32_size - 1, table_32_size - 1,
//                        table_32_size - 1, table_32_size - 1, table_32_size - 1,
//                        table_32_size - 1, table_32_size - 1);

//   __m256i hash;
//   if (S_keys.size() < 8) {
//     assert(false); // only starting from 8
//   }
//   while (S_keys_ind + 7 < S_keys.size()) {
//     // std::cout << "iter" << std::endl;
//     // std::cout << "find " << S_keys_ind << std::endl;
//     // load only where inv = 1
//     auto new_key = _mm256_maskload_epi32(&S_keys.data()[S_keys_ind], invalid);
//     key = _mm256_andnot_si256(invalid, key);
//     key = _mm256_or_si256(key, new_key);
//     // dump_simd("key", key);
//     // add offset only to position where inv != 0
//     hash = _mm256_mullo_epi32(key, PRIMES);
//     offset = _mm256_add_epi32(offset, ONES);
//     offset = _mm256_andnot_si256(invalid, offset);
//     hash = _mm256_add_epi32(hash, offset);
//     hash = _mm256_and_si256(hash, module);

//     // dump_simd("hash", hash);
//     // gather data to table_key only where inv = 1
//     table_key = _mm256_mask_i32gather_epi32(table_key, table_32.data(), hash,
//                                             invalid, 4);
//     // dump_simd("table_key", table_key);

//     // invalid = 1 where search ends
//     invalid = _mm256_cmpeq_epi32(table_key, EMPTYS);

//     // store found keys
//     auto eq_mask = _mm256_cmpeq_epi32(table_key, key);
//     // dump_simd("eq_mask", eq_mask);
//     auto eq_mask_as_int =
//         reverse_byte(_mm256_movemask_ps(_mm256_castsi256_ps(eq_mask)));
//     // std::cout << "eq mask as int " << std::to_string(eq_mask_as_int)
//     //           << std::endl;
//     auto permutation = _mm256_load_si256(&perm[eq_mask_as_int]);
//     // dump_simd("permutation", permutation);
//     auto permuted_key = _mm256_permutevar8x32_epi32(key, permutation);
//     // dump_simd("permuted key", permuted_key);
//     auto permuted_eq_mask = _mm256_permutevar8x32_epi32(eq_mask, permutation);
//     // dump_simd("permuted_key", permuted_key);
//     // dump_simd("permuted_eq_mask", permuted_eq_mask);
//     _mm256_maskstore_epi32(&S_found.data()[S_found_ind], permuted_eq_mask,
//                            permuted_key);
//     S_found_ind += _mm_popcnt_u32(eq_mask_as_int);

//     invalid = _mm256_or_si256(invalid, eq_mask);
//     // dump_simd("invalud", invalid);
//     // dump_simd("invalid mask 1", invalid);
//     auto invalid_mask =
//         reverse_byte(_mm256_movemask_ps(_mm256_castsi256_ps(invalid)));
//     // std::cout << "invalid mask as int " << std::to_string(invalid_mask)
//     //           << std::endl;
//     // std::cout << "final permute mask " << std::to_string(255 ^ invalid_mask)
//     //           << std::endl;
//     auto final_permutation = perm[255 ^ invalid_mask];
//     // dump_simd("permute", final_permutation);
//     // dump_simd("final permute mask", final_permutation);
//     key = _mm256_permutevar8x32_epi32(key, final_permutation);
//     invalid = _mm256_permutevar8x32_epi32(invalid, final_permutation);
//     S_keys_ind += _mm_popcnt_u32(invalid_mask);
//     // dump_simd("invalid end iteration", invalid);
//     // dump_simd("key end iteration", key);
//   }

//   auto invalid_mask_int =
//       reverse_byte(_mm256_movemask_ps(_mm256_castsi256_ps(invalid)));
//   // std::cout << "invalid mask int " << std::to_string(invalid_mask_int)
//   //           << std::endl;
//   // std::cout << "S_found_ind " << S_found_ind << std::endl;
//   if (invalid_mask_int != 255) {
//     if (_mm256_extract_epi32(invalid, 0) == 0) {
//       // std::cout << "extract 0" << std::endl;
//       auto key_scalar = _mm256_extract_epi32(key, 0);
//       if (scalar_find(key_scalar, table_32)) {
//         S_found[S_found_ind] = key_scalar;
//         S_found_ind++;
//       }
//       S_keys_ind++;
//     }
//     if (_mm256_extract_epi32(invalid, 1) == 0) {
//       auto key_scalar = _mm256_extract_epi32(key, 1);
//       if (scalar_find(key_scalar, table_32)) {
//         S_found[S_found_ind] = key_scalar;
//         S_found_ind++;
//       }
//       S_keys_ind++;
//     }
//     if (_mm256_extract_epi32(invalid, 2) == 0) {
//       auto key_scalar = _mm256_extract_epi32(key, 2);
//       if (scalar_find(key_scalar, table_32)) {
//         S_found[S_found_ind] = key_scalar;
//         S_found_ind++;
//       }
//       S_keys_ind++;
//     }
//     if (_mm256_extract_epi32(invalid, 3) == 0) {
//       auto key_scalar = _mm256_extract_epi32(key, 3);
//       if (scalar_find(key_scalar, table_32)) {
//         S_found[S_found_ind] = key_scalar;
//         S_found_ind++;
//       }
//       S_keys_ind++;
//     }
//     if (_mm256_extract_epi32(invalid, 4) == 0) {
//       auto key_scalar = _mm256_extract_epi32(key, 4);
//       if (scalar_find(key_scalar, table_32)) {
//         S_found[S_found_ind] = key_scalar;
//         S_found_ind++;
//       }
//       S_keys_ind++;
//     }
//     if (_mm256_extract_epi32(invalid, 5) == 0) {
//       auto key_scalar = _mm256_extract_epi32(key, 5);
//       if (scalar_find(key_scalar, table_32)) {
//         S_found[S_found_ind] = key_scalar;
//         S_found_ind++;
//       }
//       S_keys_ind++;
//     }
//     if (_mm256_extract_epi32(invalid, 6) == 0) {
//       auto key_scalar = _mm256_extract_epi32(key, 6);
//       if (scalar_find(key_scalar, table_32)) {
//         S_found[S_found_ind] = key_scalar;
//         S_found_ind++;
//       }
//       S_keys_ind++;
//     }
//     if (_mm256_extract_epi32(invalid, 7) == 0) {
//       std::cout << "extract 7" << std::endl;
//       auto key_scalar = _mm256_extract_epi32(key, 7);
//       if (scalar_find(key_scalar, table_32)) {
//         S_found[S_found_ind] = key_scalar;
//         S_found_ind++;
//       }
//       S_keys_ind++;
//     }
//   }
//   while (S_keys_ind < S_keys.size()) {
//     if (scalar_find(S_keys[S_keys_ind], table_32)) {
//       S_found[S_found_ind] = S_keys[S_keys_ind];
//       S_found_ind++;
//     }
//     S_keys_ind++;
//   }
//   return S_found_ind;
// }

void test(uint seed, int exp, int modulo, int in_map_cnt, int in_query_cnt, double prob_that_query_exists) {
  std::random_device rd;
  if (seed == 0) {
    seed = rd();
  }
  std::cout << "seed for test: " << seed << ", exp=" << exp << std::endl;
  std::mt19937 mt(seed);

  // int hashtable_size = 1 << exp;
  auto hashtable = simd_hashtable_t(exp);
  // std::vector<int> hashtable(hashtable_size, INT_MIN);
  std::unordered_set<int> put_in_map;
  std::uniform_int_distribution<int> distr(0, modulo);
  assert(in_map_cnt < hashtable.size());
  for (int i = 0; i < in_map_cnt; i++) {
    // std::cout << "test1" << std::endl;
    auto rand_key = distr(mt);
    put_in_map.insert(rand_key);
    hashtable.scalar_insert(rand_key);
  }
  std::vector<int> keys_to_find;
  for (int i = 0; i < in_query_cnt; i++) {
    // std::cout << "test2" << std::endl;
    std::bernoulli_distribution bd(prob_that_query_exists);
    if (bd(mt)) {
      std::uniform_int_distribution<int> distr(0, put_in_map.size() - 1);
      auto it = put_in_map.begin();
      std::advance(it, distr(mt));
      keys_to_find.push_back(*it);
    } else {
      keys_to_find.push_back(modulo + 1 + i);
    }
  }
  std::vector<int> keys_to_find_res(keys_to_find.size(), 0);
  // std::cout << "keys_to_find: ";
  // for (auto& x : keys_to_find) {
  //   std::cout << x << " ";
  // }
  // std::cout << "\n";
  // std::cout << "hashtable: ";
  // for (auto& x : hashtable) {
  //   if (x == INT32_MIN) {
  //     std::cout << "e" << " ";  
  //   } else {
  //     std::cout << x << " ";
  //   }
  // }
  // std::cout << "\n";
  auto actual = hashtable.simd_find(keys_to_find, keys_to_find_res);
  auto expected = std::count_if(keys_to_find.begin(), keys_to_find.end(), [&](const auto& x) {
    return hashtable.scalar_find(x) == true;
  });
  if (actual != expected) {
    std::cout << "actual = " << std::to_string(actual)
              << ", expected = " << std::to_string(expected) << std::endl;
  }
  assert(actual == expected);
}

// // all keys success
// void test_case1() {
//   int tt = 10;
//   while (tt-- > 0) {
//     int exponent = 4;
//     std::vector<int> hashtable(1 << exponent, INT_MIN);
//     std::unordered_set<int> inserted_keys;
//     for (int i = 0; i < 12; i++) {
//       int rand_key = rand();
//       while (inserted_keys.find(rand_key) != inserted_keys.end()) {
//         rand_key = rand();
//       }
//       inserted_keys.insert(rand_key);
//       scalar_insert(rand_key, hashtable);
//     }
//     std::vector<int> keys(inserted_keys.begin(), inserted_keys.end());
//     std::vector<int> found_keys(keys.size(), 0);
//     auto res = simd_find(keys, found_keys, hashtable);
//     if (res != keys.size()) {
//       std::cout << "res " << res << ", keys " << keys.size() << std::endl;
//     }
//     assert(res == keys.size());
//   }
//   std::cout << "test_case 1 success" << std::endl;
// }

// // all keys success, hashtable is bigger
// void test_case2() {
//   int tt = 10;
//   while (tt-- > 0) {
//     int exponent = 5;
//     // std::vector<int> hashtable(1 << exponent, INT_MIN);
//     std::unordered_set<int> inserted_keys;
//     for (int i = 0; i < 12; i++) {
//       int rand_key = rand();
//       while (inserted_keys.find(rand_key) != inserted_keys.end()) {
//         rand_key = rand();
//       }
//       inserted_keys.insert(rand_key);
//       hashtable.scalar_insert(rand_key);
//     }
//     std::vector<int> keys(inserted_keys.begin(), inserted_keys.end());
//     std::vector<int> found_keys(keys.size(), 0);
//     auto res = simd_find(keys, found_keys);
//     if (res != keys.size()) {
//       std::cout << "res " << res << ", keys " << keys.size() << std::endl;
//     }
//     assert(res == keys.size());
//   }
//   std::cout << "test_case 2 success" << std::endl;
// }

// // all keys success, hashtable is bigger
// void test_case3() {
//   int tt = 10;
//   while (tt-- > 0) {
//     int exponent = 4;
//     std::vector<int> hashtable(1 << exponent, INT_MIN);
//     std::unordered_set<int> inserted_keys;
//     for (int i = 0; i < 8; i++) {
//       int rand_key = rand();
//       while (inserted_keys.find(rand_key) != inserted_keys.end()) {
//         rand_key = rand();
//       }
//       inserted_keys.insert(rand_key);
//       scalar_insert(rand_key, hashtable);
//     }

//     std::unordered_set<int> missing_keys;
//     for (int i = 0; i < 8; i++) {
//       int rand_key = rand();
//       while (inserted_keys.find(rand_key) != inserted_keys.end() ||
//              missing_keys.find(rand_key) != missing_keys.end()) {
//         rand_key = rand();
//       }
//       missing_keys.insert(rand_key);
//     }

//     std::vector<int> keys(inserted_keys.begin(), inserted_keys.end());
//     keys.insert(std::end(keys), missing_keys.begin(), missing_keys.end());
//     auto rng = std::default_random_engine{};
//     std::shuffle(std::begin(keys), std::end(keys), rng);

//     std::vector<int> found_keys(keys.size(), 0);
//     auto res = simd_find(keys, found_keys, hashtable);
//     if (res != inserted_keys.size()) {
//       std::cout << "res " << res << ", keys " << keys.size() << std::endl;
//     }
//     assert(res == inserted_keys.size());
//   }
//   std::cout << "test_case 3 success" << std::endl;
// }

// // hashtable is empty
// void test_case4() {
//   int tt = 10;
//   while (tt-- > 0) {
//     int exponent = 6;
//     std::vector<int> hashtable(1 << exponent, INT_MIN);
//     std::unordered_set<int> inserted_keys;
//     for (int i = 0; i < 8; i++) {
//       int rand_key = rand();
//       while (inserted_keys.find(rand_key) != inserted_keys.end()) {
//         rand_key = rand();
//       }
//       inserted_keys.insert(rand_key);
//     }
//     std::vector<int> keys(inserted_keys.begin(), inserted_keys.end());
//     std::vector<int> found(inserted_keys.begin(), inserted_keys.end());
//     auto res = simd_find(keys, found, hashtable);
//     assert(res == 0);
//   }
//   std::cout << "test_case 4 success" << std::endl;
// }

void test_main() {
  // init_perm();
  // test_case1();
  // test_case2();
  // test_case3();
  // test_case4();

  // test(15, 16, 13000, 16, 8, 0.5);
  // test(15, 16, 13000, 8, 4, 0.5);
  // std::random_device rd;
  // std::uniform_int_distribution<> d(8, 64);
  // std::mt19937 mt(rd());
  for (int i = 0; i < 100; i++) {
    std::cout << "iter " << i << std::endl;
    for (int in_query_cnt = 8; in_query_cnt < 350; in_query_cnt++) {
      for (int in_map_cnt = 1; i < in_query_cnt * 2; i++) {
        for (double prob_that_query_exists = 0; prob_that_query_exists <= 1; prob_that_query_exists += 0.1) {
          std::cout << "prob_that_query_exists " << prob_that_query_exists
                    << std::endl;
          test(0, 16, 13000, in_map_cnt, in_query_cnt, prob_that_query_exists);  
          test(0, 10, 13000, in_map_cnt, in_query_cnt, prob_that_query_exists);  
        }
      }
    }
    // test(0, 16, 13000, 16, 8, 0.5);
    // test(0, 5, 13000, 16, 8, 0.5);
    // test(0, 16, 13000, 32, 15, 0.5);
      // test(0, 16, 13000, 32, 13, 0.5);
  }
}

int main() {
  // init_perm();
  // seed for test: 434323706, exp=5
  // test(434323706, 5, 13000, 16, 8, 0.5);
  test_main();
  // int exponent = 15;
  // std::vector<int> hashtable(1 << exponent, INT_MIN);

  // std::unordered_set<int> keys;
  // for (int i = 0; i < 4000; i++) {
  //   int rand_key = rand() % 8000;
  //   while (keys.find(rand_key) != keys.end()) {
  //     rand_key = rand();
  //   }
  //   keys.insert(rand_key);
  //   scalar_insert(rand_key, hashtable);
  // }

  // std::vector<int> keys_to_search;
  // for (int i = 0; i < 4000; i++) {
  //   int rand_key = rand() % 8000;
  //   keys_to_search.push_back(rand_key);
  // }

  // auto scalar_begin = std::chrono::steady_clock::now();
  // for (auto &x : keys_to_search) {
  //   volatile auto res = keys.find(keys_to_search[x]);
  // }
  // auto scalar_end = std::chrono::steady_clock::now();
  // std::cout << (scalar_end - scalar_begin).count() << std::endl;

  // std::vector<int> found_key_res(keys_to_search.size(), 0);
  // auto vector_begin = std::chrono::steady_clock::now();
  // volatile auto res = simd_find(keys_to_search, found_key_res, hashtable);
  // auto vector_end = std::chrono::steady_clock::now();
  // std::cout << (vector_end - vector_begin).count() << std::endl;
}
