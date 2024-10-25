#pragma once

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <climits>
#include <cstdint>
#include <cstring>
#include <immintrin.h>
#include <iostream>
#include <iterator>
#include <random>
#include <stdexcept>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

struct simd_hashtable_t {
  using element_t = int32_t;
  static constexpr element_t min_element = INT32_MIN;
  static constexpr int simd_size = 256;
  static constexpr int elements_in_simd = simd_size / sizeof(element_t);

  std::array<__m256i, 256> perm{};

  std::vector<element_t> table;

  const int PRIME = 31;

  void init_perm() {
    for (int i = 0; i < simd_size; i++) {
      int i_copy = i;
      std::vector<int> pos_with_one;
      std::vector<int> pos_with_zero;
      for (int index = 0; index < elements_in_simd; index++) {
        if (i_copy % 2 == 0) {
          pos_with_zero.push_back(elements_in_simd - 1 - index);
        } else {
          // std::cout << "push one" << std::endl;
          pos_with_one.push_back(elements_in_simd - 1 - index);
        }
        i_copy /= 2;
      }
      std::array<int, elements_in_simd> init_result{};
      for (size_t j = 0; j < pos_with_one.size(); j++) {
        init_result[j] = pos_with_one[j];
      }
      for (size_t j = 0; j < pos_with_zero.size(); j++) {
        init_result[pos_with_one.size() + j] = pos_with_zero[j];
      }
      perm[i] = _mm256_setr_epi32(
          init_result[0], init_result[1], init_result[2], init_result[3],
          init_result[4], init_result[5], init_result[6], init_result[7]);
    }
  }

  simd_hashtable_t(int exp) {
    init_perm();
    table = std::vector<element_t>(1 << exp, min_element);
  }

  uint8_t reverse_byte(unsigned char x) const {
    static const uint8_t table[] = {
        0x00, 0x80, 0x40, 0xc0, 0x20, 0xa0, 0x60, 0xe0, 0x10, 0x90, 0x50, 0xd0,
        0x30, 0xb0, 0x70, 0xf0, 0x08, 0x88, 0x48, 0xc8, 0x28, 0xa8, 0x68, 0xe8,
        0x18, 0x98, 0x58, 0xd8, 0x38, 0xb8, 0x78, 0xf8, 0x04, 0x84, 0x44, 0xc4,
        0x24, 0xa4, 0x64, 0xe4, 0x14, 0x94, 0x54, 0xd4, 0x34, 0xb4, 0x74, 0xf4,
        0x0c, 0x8c, 0x4c, 0xcc, 0x2c, 0xac, 0x6c, 0xec, 0x1c, 0x9c, 0x5c, 0xdc,
        0x3c, 0xbc, 0x7c, 0xfc, 0x02, 0x82, 0x42, 0xc2, 0x22, 0xa2, 0x62, 0xe2,
        0x12, 0x92, 0x52, 0xd2, 0x32, 0xb2, 0x72, 0xf2, 0x0a, 0x8a, 0x4a, 0xca,
        0x2a, 0xaa, 0x6a, 0xea, 0x1a, 0x9a, 0x5a, 0xda, 0x3a, 0xba, 0x7a, 0xfa,
        0x06, 0x86, 0x46, 0xc6, 0x26, 0xa6, 0x66, 0xe6, 0x16, 0x96, 0x56, 0xd6,
        0x36, 0xb6, 0x76, 0xf6, 0x0e, 0x8e, 0x4e, 0xce, 0x2e, 0xae, 0x6e, 0xee,
        0x1e, 0x9e, 0x5e, 0xde, 0x3e, 0xbe, 0x7e, 0xfe, 0x01, 0x81, 0x41, 0xc1,
        0x21, 0xa1, 0x61, 0xe1, 0x11, 0x91, 0x51, 0xd1, 0x31, 0xb1, 0x71, 0xf1,
        0x09, 0x89, 0x49, 0xc9, 0x29, 0xa9, 0x69, 0xe9, 0x19, 0x99, 0x59, 0xd9,
        0x39, 0xb9, 0x79, 0xf9, 0x05, 0x85, 0x45, 0xc5, 0x25, 0xa5, 0x65, 0xe5,
        0x15, 0x95, 0x55, 0xd5, 0x35, 0xb5, 0x75, 0xf5, 0x0d, 0x8d, 0x4d, 0xcd,
        0x2d, 0xad, 0x6d, 0xed, 0x1d, 0x9d, 0x5d, 0xdd, 0x3d, 0xbd, 0x7d, 0xfd,
        0x03, 0x83, 0x43, 0xc3, 0x23, 0xa3, 0x63, 0xe3, 0x13, 0x93, 0x53, 0xd3,
        0x33, 0xb3, 0x73, 0xf3, 0x0b, 0x8b, 0x4b, 0xcb, 0x2b, 0xab, 0x6b, 0xeb,
        0x1b, 0x9b, 0x5b, 0xdb, 0x3b, 0xbb, 0x7b, 0xfb, 0x07, 0x87, 0x47, 0xc7,
        0x27, 0xa7, 0x67, 0xe7, 0x17, 0x97, 0x57, 0xd7, 0x37, 0xb7, 0x77, 0xf7,
        0x0f, 0x8f, 0x4f, 0xcf, 0x2f, 0xaf, 0x6f, 0xef, 0x1f, 0x9f, 0x5f, 0xdf,
        0x3f, 0xbf, 0x7f, 0xff,
    };
    return table[x];
  }

  void dump_simd(const std::string &prefix, __m256i simd) {
    std::cout << prefix << ":" << _mm256_extract_epi32(simd, 0) << ","
              << _mm256_extract_epi32(simd, 1) << ","
              << _mm256_extract_epi32(simd, 2) << ","
              << _mm256_extract_epi32(simd, 3) << ","
              << _mm256_extract_epi32(simd, 4) << ","
              << _mm256_extract_epi32(simd, 5) << ","
              << _mm256_extract_epi32(simd, 6) << ","
              << _mm256_extract_epi32(simd, 7) << std::endl;
  }

  int scalar_insert(int key) {
    int hash = scalar_hash(key);
    int offset = 0;
    int start_index = hash;
    while (table[(offset + hash) % table.size()] != INT_MIN) {
      offset++;
      if (((offset + hash) % table.size()) == start_index) {
        throw std::runtime_error("scalar insert cycle");
      }
    }
    table[(offset + hash) % table.size()] = key;
    return 0;
  }

  size_t size() const {
    return table.size();
  }

  bool scalar_find(int key) const {
    int hash = scalar_hash(key);
    int offset = 0;
    while (table[(offset + hash) % table.size()] != INT_MIN &&
           table[(offset + hash) % table.size()] != key) {
      offset++;
    }
    return table[(offset + hash) % table.size()] == key;
  }

  int scalar_hash(element_t key) const {
    return (key * PRIME) % (table.size());
  }

  size_t simd_find(const std::vector<element_t> &S_keys, std::vector<element_t> &S_found) const {
    const int table_size = table.size();
    assert(table_size % 2 == 0);
    size_t S_keys_ind = 0;
    size_t S_found_ind = 0;
    __m256i invalid{};
    invalid = _mm256_cmpeq_epi32(invalid, invalid); // set mask to one

    __m256i offset{};
    offset = _mm256_xor_si256(offset, offset); // reset vector to zero

    __m256i key{};

    __m256i PRIMES = _mm256_set_epi32(PRIME, PRIME, PRIME, PRIME, PRIME, PRIME,
                                      PRIME, PRIME);
    __m256i ONES = _mm256_set_epi32(1, 1, 1, 1, 1, 1, 1, 1);
    __m256i EMPTYS =
        _mm256_set_epi32(min_element, min_element, min_element, min_element, min_element,
                         min_element, min_element, min_element);

    __m256i table_key{};

    __m256i module = _mm256_set_epi32(table_size - 1, table_size - 1,
                                      table_size - 1, table_size - 1,
                                      table_size - 1, table_size - 1,
                                      table_size - 1, table_size - 1);

    __m256i hash{};
    if (S_keys.size() < 8) {
      assert(false); // only starting from 8
    }
    while (S_keys_ind + 7 < S_keys.size()) {
      // std::cout << "iter" << std::endl;
      // std::cout << "find " << S_keys_ind << std::endl;
      // load only where inv = 1
      auto new_key = _mm256_maskload_epi32(&S_keys.data()[S_keys_ind], invalid);
      key = _mm256_andnot_si256(invalid, key);
      key = _mm256_or_si256(key, new_key);
      // dump_simd("key", key);
      // add offset only to position where inv != 0
      hash = _mm256_mullo_epi32(key, PRIMES);
      offset = _mm256_add_epi32(offset, ONES);
      offset = _mm256_andnot_si256(invalid, offset);
      hash = _mm256_add_epi32(hash, offset);
      hash = _mm256_and_si256(hash, module);

      // dump_simd("hash", hash);
      // gather data to table_key only where inv = 1
      table_key = _mm256_mask_i32gather_epi32(table_key, table.data(), hash,
                                              invalid, sizeof(element_t));
      // dump_simd("table_key", table_key);

      // invalid = 1 where search ends
      invalid = _mm256_cmpeq_epi32(table_key, EMPTYS);

      // store found keys
      auto eq_mask = _mm256_cmpeq_epi32(table_key, key);
      // dump_simd("eq_mask", eq_mask);
      auto eq_mask_as_int =
          reverse_byte(_mm256_movemask_ps(_mm256_castsi256_ps(eq_mask)));
      // std::cout << "eq mask as int " << std::to_string(eq_mask_as_int)
      //           << std::endl;
      auto permutation = _mm256_load_si256(&perm[eq_mask_as_int]);
      // dump_simd("permutation", permutation);
      auto permuted_key = _mm256_permutevar8x32_epi32(key, permutation);
      // dump_simd("permuted key", permuted_key);
      auto permuted_eq_mask = _mm256_permutevar8x32_epi32(eq_mask, permutation);
      // dump_simd("permuted_key", permuted_key);
      // dump_simd("permuted_eq_mask", permuted_eq_mask);
      _mm256_maskstore_epi32(&S_found.data()[S_found_ind], permuted_eq_mask,
                             permuted_key);
      S_found_ind += _mm_popcnt_u32(eq_mask_as_int);

      invalid = _mm256_or_si256(invalid, eq_mask);
      // dump_simd("invalud", invalid);
      // dump_simd("invalid mask 1", invalid);
      auto invalid_mask =
          reverse_byte(_mm256_movemask_ps(_mm256_castsi256_ps(invalid)));
      // std::cout << "invalid mask as int " << std::to_string(invalid_mask)
      //           << std::endl;
      // std::cout << "final permute mask " << std::to_string(255 ^
      // invalid_mask)
      //           << std::endl;
      auto final_permutation = perm[255 ^ invalid_mask];
      // dump_simd("permute", final_permutation);
      // dump_simd("final permute mask", final_permutation);
      key = _mm256_permutevar8x32_epi32(key, final_permutation);
      invalid = _mm256_permutevar8x32_epi32(invalid, final_permutation);
      S_keys_ind += _mm_popcnt_u32(invalid_mask);
      // dump_simd("invalid end iteration", invalid);
      // dump_simd("key end iteration", key);
    }

    auto invalid_mask_int =
        reverse_byte(_mm256_movemask_ps(_mm256_castsi256_ps(invalid)));
    // std::cout << "invalid mask int " << std::to_string(invalid_mask_int)
    //           << std::endl;
    // std::cout << "S_found_ind " << S_found_ind << std::endl;
    if (invalid_mask_int != 255) {
      if (_mm256_extract_epi32(invalid, 0) == 0) {
        // std::cout << "extract 0" << std::endl;
        auto key_scalar = _mm256_extract_epi32(key, 0);
        if (scalar_find(key_scalar)) {
          S_found[S_found_ind] = key_scalar;
          S_found_ind++;
        }
        S_keys_ind++;
      }
      if (_mm256_extract_epi32(invalid, 1) == 0) {
        auto key_scalar = _mm256_extract_epi32(key, 1);
        if (scalar_find(key_scalar)) {
          S_found[S_found_ind] = key_scalar;
          S_found_ind++;
        }
        S_keys_ind++;
      }
      if (_mm256_extract_epi32(invalid, 2) == 0) {
        auto key_scalar = _mm256_extract_epi32(key, 2);
        if (scalar_find(key_scalar)) {
          S_found[S_found_ind] = key_scalar;
          S_found_ind++;
        }
        S_keys_ind++;
      }
      if (_mm256_extract_epi32(invalid, 3) == 0) {
        auto key_scalar = _mm256_extract_epi32(key, 3);
        if (scalar_find(key_scalar)) {
          S_found[S_found_ind] = key_scalar;
          S_found_ind++;
        }
        S_keys_ind++;
      }
      if (_mm256_extract_epi32(invalid, 4) == 0) {
        auto key_scalar = _mm256_extract_epi32(key, 4);
        if (scalar_find(key_scalar)) {
          S_found[S_found_ind] = key_scalar;
          S_found_ind++;
        }
        S_keys_ind++;
      }
      if (_mm256_extract_epi32(invalid, 5) == 0) {
        auto key_scalar = _mm256_extract_epi32(key, 5);
        if (scalar_find(key_scalar)) {
          S_found[S_found_ind] = key_scalar;
          S_found_ind++;
        }
        S_keys_ind++;
      }
      if (_mm256_extract_epi32(invalid, 6) == 0) {
        auto key_scalar = _mm256_extract_epi32(key, 6);
        if (scalar_find(key_scalar)) {
          S_found[S_found_ind] = key_scalar;
          S_found_ind++;
        }
        S_keys_ind++;
      }
      if (_mm256_extract_epi32(invalid, 7) == 0) {
        std::cout << "extract 7" << std::endl;
        auto key_scalar = _mm256_extract_epi32(key, 7);
        if (scalar_find(key_scalar)) {
          S_found[S_found_ind] = key_scalar;
          S_found_ind++;
        }
        S_keys_ind++;
      }
    }
    while (S_keys_ind < S_keys.size()) {
      if (scalar_find(S_keys[S_keys_ind])) {
        S_found[S_found_ind] = S_keys[S_keys_ind];
        S_found_ind++;
      }
      S_keys_ind++;
    }
    return S_found_ind;
  }
};
