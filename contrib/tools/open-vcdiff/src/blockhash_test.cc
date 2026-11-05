// Copyright 2008 The open-vcdiff Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <config.h>
#include "blockhash.h"
#include <limits.h>  // INT_MIN
#include <string.h>  // memcpy, memcmp, strlen
#include <iostream>
#include "google/encodetable.h"
#include "rolling_hash.h"
#include "testing.h"
#include "unique_ptr.h" // auto_ptr, unique_ptr

namespace open_vcdiff {


template<class BlockHash>
class BlockHashTest : public testing::Test {
 protected:
  static constexpr size_t kBlockSize = BlockHash::kBlockSize;

  BlockHashTest() {
    dh_.reset(BlockHash::CreateDictionaryHash(sample_text,
                                              strlen(sample_text)));
    th_.reset(BlockHash::CreateTargetHash(sample_text, strlen(sample_text), 0));
    EXPECT_TRUE(dh_.get() != NULL);
    EXPECT_TRUE(th_.get() != NULL);
  }

  // BlockHashTest is a friend to BlockHash.  Expose the protected functions
  // that will be tested by the children of BlockHashTest.
  static bool BlockContentsMatch(const char* block1, const char* block2) {
    return BlockHash::BlockContentsMatch(block1, block2);
  }

  int FirstMatchingBlock(const BlockHash& block_hash,
                         uint32_t hash_value,
                         const char* block_ptr) const {
    int blk = block_hash.FirstMatchingBlock(hash_value, block_ptr);
    if (blk >= 0) {
      EXPECT_EQ(std::string(block_ptr, kBlockSize),
                std::string(&block_hash.source_data_[blk * kBlockSize],
                            kBlockSize));
    }
    return blk;
  }

  int NextMatchingBlock(const BlockHash& block_hash,
                        int block_number,
                        const char* block_ptr) const {
    int blk = block_hash.NextMatchingBlock(block_number);
    if (blk >= 0) {
      EXPECT_EQ(std::string(block_ptr, kBlockSize),
                std::string(&block_hash.source_data_[blk * kBlockSize],
                            kBlockSize));
    }
    return blk;
  }

  static int MatchingBytesToLeft(const char* source_match_start,
                                 const char* target_match_start,
                                 size_t max_bytes) {
    return BlockHash::MatchingBytesToLeft(source_match_start,
                                          target_match_start,
                                          max_bytes);
  }

  static int MatchingBytesToRight(const char* source_match_end,
                                  const char* target_match_end,
                                  size_t max_bytes) {
    return BlockHash::MatchingBytesToRight(source_match_end,
                                           target_match_end,
                                           max_bytes);
  }

  static int StringLengthAsInt(const char* s) {
    return static_cast<int>(strlen(s));
  }

  // Copy sample_text_without_spaces and search_string_without_spaces
  // into newly allocated sample_text and search_string buffers,
  // but pad them with space characters so that every character
  // in sample_text_without_spaces matches (kBlockSize - 1)
  // space characters in sample_text, followed by that character.
  // For example:
  // Since sample_text_without_spaces begins "The only thing"...,
  // if kBlockSize is 4, then 3 space characters will be inserted
  // between each letter of sample_text, as follows:
  // "   T   h   e       o   n   l   y       t   h   i   n   g"...
  // This makes testing simpler, because finding a kBlockSize-byte match
  // between the sample text and search string only depends on the
  // trailing letter in each block.
  static void MakeEachLetterABlock(const char* string_without_spaces,
                                   const char** result) {
    const size_t length_without_spaces = strlen(string_without_spaces);
    char* padded_text = new char[(kBlockSize * length_without_spaces) + 1];
    memset(padded_text, ' ', kBlockSize * length_without_spaces);
    char* padded_text_ptr = padded_text + (kBlockSize - 1);
    for (size_t i = 0; i < length_without_spaces; ++i) {
      *padded_text_ptr = string_without_spaces[i];
      padded_text_ptr += kBlockSize;
    }
    padded_text[kBlockSize * length_without_spaces] = '\0';
    *result = padded_text;
  }

  static void SetUpTestCase() {
    MakeEachLetterABlock(sample_text_without_spaces, &sample_text);
    MakeEachLetterABlock(search_string_without_spaces, &search_string);
    MakeEachLetterABlock(search_string_altered_without_spaces,
                         &search_string_altered);
    MakeEachLetterABlock(search_to_end_without_spaces, &search_to_end_string);
    MakeEachLetterABlock(search_to_beginning_without_spaces,
                         &search_to_beginning_string);
    MakeEachLetterABlock(sample_text_many_matches_without_spaces,
                         &sample_text_many_matches);
    MakeEachLetterABlock(search_string_many_matches_without_spaces,
                         &search_string_many_matches);
    MakeEachLetterABlock("y", &test_string_y);
    MakeEachLetterABlock("e", &test_string_e);
    char* new_test_string_unaligned_e = new char[kBlockSize];
    memset(new_test_string_unaligned_e, ' ', kBlockSize);
    new_test_string_unaligned_e[kBlockSize - 2] = 'e';
    test_string_unaligned_e = new_test_string_unaligned_e;
    char* new_test_string_all_Qs = new char[kBlockSize];
    memset(new_test_string_all_Qs, 'Q', kBlockSize);
    test_string_all_Qs = new_test_string_all_Qs;
    hashed_y = RollingHash<kBlockSize>::Hash(test_string_y);
    hashed_e = RollingHash<kBlockSize>::Hash(test_string_e);
    hashed_f =
        RollingHash<kBlockSize>::Hash(&search_string[index_of_f_in_fearsome]);
    hashed_unaligned_e = RollingHash<kBlockSize>::Hash(test_string_unaligned_e);
    hashed_all_Qs = RollingHash<kBlockSize>::Hash(test_string_all_Qs);
  }

  static void TearDownTestCase() {
    delete[] sample_text;
    delete[] search_string;
    delete[] search_string_altered;
    delete[] search_to_end_string;
    delete[] search_to_beginning_string;
    delete[] sample_text_many_matches;
    delete[] search_string_many_matches;
    delete[] test_string_y;
    delete[] test_string_e;
    delete[] test_string_unaligned_e;
    delete[] test_string_all_Qs;
  }

  // Each block in the sample text and search string is kBlockSize bytes long,
  // and consists of (kBlockSize - 1) space characters
  // followed by a single letter of text.

  // Block numbers of certain characters within the sample text:
  // All six occurrences of "e", in order.
  static constexpr int block_of_first_e = 2;
  static constexpr int block_of_second_e = 16;
  static constexpr int block_of_third_e = 21;
  static constexpr int block_of_fourth_e = 27;
  static constexpr int block_of_fifth_e = 35;
  static constexpr int block_of_sixth_e = 42;

  static constexpr int block_of_y_in_only = 7;
  // The block number is multiplied by kBlockSize to arrive at the
  // index, which points to the (kBlockSize - 1) space characters before
  // the letter specified.
  // Indices of certain characters within the sample text.
  static constexpr int index_of_first_e = block_of_first_e * kBlockSize;
  static constexpr int index_of_fourth_e = block_of_fourth_e * kBlockSize;
  static constexpr int index_of_sixth_e = block_of_sixth_e * kBlockSize;
  static constexpr int index_of_y_in_only = block_of_y_in_only * kBlockSize;
  static constexpr int index_of_space_before_fear_is_fear = 25 * kBlockSize;
  static constexpr int index_of_longest_match_ear_is_fear = 27 * kBlockSize;
  static constexpr int index_of_i_in_fear_is_fear = 31 * kBlockSize;
  static constexpr int index_of_space_before_fear_itself = 33 * kBlockSize;
  static constexpr int index_of_space_before_itself = 38 * kBlockSize;
  static constexpr int index_of_ababc = 4 * kBlockSize;

  // Indices of certain characters within the search strings.
  static constexpr int index_of_second_w_in_what_we = 5 * kBlockSize;
  static constexpr int index_of_second_e_in_what_we_hear = 9 * kBlockSize;
  static constexpr int index_of_f_in_fearsome = 16 * kBlockSize;
  static constexpr int index_of_space_in_eat_itself =  12 * kBlockSize;
  static constexpr int index_of_i_in_itself = 13 * kBlockSize;
  static constexpr int index_of_t_in_use_the = 4 * kBlockSize;
  static constexpr int index_of_o_in_online = 8 * kBlockSize;

  static constexpr char sample_text_without_spaces[] =
      "The only thing we have to fear is fear itself";

  static constexpr char search_string_without_spaces[] =
      "What we hear is fearsome";

  static constexpr char search_string_altered_without_spaces[] =
      "Vhat ve hear is fearsomm";

  static constexpr char search_to_end_without_spaces[] =
      "Pop will eat itself, eventually";

  static constexpr char search_to_beginning_without_spaces[] =
      "Use The online dictionary";

  static constexpr char sample_text_many_matches_without_spaces[] =
      "ababababcab";

  static constexpr char search_string_many_matches_without_spaces[] =
      "ababc";

  static const char* sample_text;
  static const char* search_string;
  static const char* search_string_altered;
  static const char* search_to_end_string;
  static const char* search_to_beginning_string;
  static const char* sample_text_many_matches;
  static const char* search_string_many_matches;

  static const char* test_string_y;
  static const char* test_string_e;
  static const char* test_string_all_Qs;
  static const char* test_string_unaligned_e;

  static uint32_t hashed_y;
  static uint32_t hashed_e;
  static uint32_t hashed_f;
  static uint32_t hashed_unaligned_e;
  static uint32_t hashed_all_Qs;

  UNIQUE_PTR<const BlockHash> dh_;  // hash table is populated at startup
  UNIQUE_PTR<BlockHash> th_;  // hash table not populated;
                              // used to test incremental adds

  BlockHashBase::Match best_match_;
  int prime_result_;
};

#ifdef GTEST_HAS_DEATH_TEST
template<class BlockHash>
using BlockHashDeathTest = BlockHashTest<BlockHash>;
#endif  // GTEST_HAS_DEATH_TEST

template<class BlockHash>
const char* BlockHashTest<BlockHash>::sample_text = NULL;
template<class BlockHash>
const char* BlockHashTest<BlockHash>::search_string = NULL;
template<class BlockHash>
const char* BlockHashTest<BlockHash>::search_string_altered = NULL;
template<class BlockHash>
const char* BlockHashTest<BlockHash>::search_to_end_string = NULL;
template<class BlockHash>
const char* BlockHashTest<BlockHash>::search_to_beginning_string = NULL;
template<class BlockHash>
const char* BlockHashTest<BlockHash>::sample_text_many_matches = NULL;
template<class BlockHash>
const char* BlockHashTest<BlockHash>::search_string_many_matches = NULL;

template<class BlockHash>
const char* BlockHashTest<BlockHash>::test_string_y = NULL;
template<class BlockHash>
const char* BlockHashTest<BlockHash>::test_string_e = NULL;
template<class BlockHash>
const char* BlockHashTest<BlockHash>::test_string_unaligned_e = NULL;
template<class BlockHash>
const char* BlockHashTest<BlockHash>::test_string_all_Qs = NULL;

template<class BlockHash>
uint32_t BlockHashTest<BlockHash>::hashed_y = 0;
template<class BlockHash>
uint32_t BlockHashTest<BlockHash>::hashed_e = 0;
template<class BlockHash>
uint32_t BlockHashTest<BlockHash>::hashed_f = 0;
template<class BlockHash>
uint32_t BlockHashTest<BlockHash>::hashed_unaligned_e = 0;
template<class BlockHash>
uint32_t BlockHashTest<BlockHash>::hashed_all_Qs = 0;


typedef testing::Types<BlockHash<8>, BlockHash<16>,
                       BlockHash<32>, BlockHash<64>> BlockHashTypes;
TYPED_TEST_SUITE(BlockHashTest, BlockHashTypes);

// The two strings passed to BlockHash::MatchingBytesToLeft do have matching
// characters -- in fact, they're the same string -- but since max_bytes is zero
// or negative, BlockHash::MatchingBytesToLeft should not read from the strings
// and should return 0.
TYPED_TEST(BlockHashTest, MaxBytesZeroDoesNothing) {
  EXPECT_EQ(0, this->MatchingBytesToLeft(
      &this->search_string[this->index_of_f_in_fearsome],
      &this->search_string[this->index_of_f_in_fearsome],
      0));
  EXPECT_EQ(0, this->MatchingBytesToRight(
      &this->search_string[this->index_of_f_in_fearsome],
      &this->search_string[this->index_of_f_in_fearsome],
      0));
}

TYPED_TEST(BlockHashTest, MaxBytesOneMatch) {
  EXPECT_EQ(1, this->MatchingBytesToLeft(
      &this->search_string[this->index_of_f_in_fearsome],
      &this->search_string[this->index_of_f_in_fearsome],
      1));
  EXPECT_EQ(1, this->MatchingBytesToRight(
      &this->search_string[this->index_of_f_in_fearsome],
      &this->search_string[this->index_of_f_in_fearsome],
      1));
}

TYPED_TEST(BlockHashTest, MaxBytesOneNoMatch) {
  EXPECT_EQ(0, this->MatchingBytesToLeft(
      &this->search_string[this->index_of_f_in_fearsome],
      &this->search_string[this->index_of_second_e_in_what_we_hear],
      1));
  EXPECT_EQ(0, this->MatchingBytesToRight(
      &this->search_string[this->index_of_f_in_fearsome],
      &this->search_string[this->index_of_second_e_in_what_we_hear - 1],
      1));
}

TYPED_TEST(BlockHashTest, LeftLimitedByMaxBytes) {
  // The number of bytes that match between the original "we hear is fearsome"
  // and the altered "ve hear is fearsome".
  const int expected_length = TypeParam::kBlockSize * this->StringLengthAsInt("e hear is ");
  const int max_bytes = expected_length - 1;
  EXPECT_EQ(max_bytes, this->MatchingBytesToLeft(
      &this->search_string[this->index_of_f_in_fearsome],
      &this->search_string_altered[this->index_of_f_in_fearsome],
      max_bytes));
}

TYPED_TEST(BlockHashTest, LeftNotLimited) {
  // The number of bytes that match between the original "we hear is fearsome"
  // and the altered "ve hear is fearsome".
  const int expected_length = TypeParam::kBlockSize * this->StringLengthAsInt("e hear is ");
  const int max_bytes = expected_length + 1;
  EXPECT_EQ(expected_length, this->MatchingBytesToLeft(
      &this->search_string[this->index_of_f_in_fearsome],
      &this->search_string_altered[this->index_of_f_in_fearsome],
      max_bytes));
  EXPECT_EQ(expected_length, this->MatchingBytesToLeft(
      &this->search_string[this->index_of_f_in_fearsome],
      &this->search_string_altered[this->index_of_f_in_fearsome],
      INT_MAX));
}

TYPED_TEST(BlockHashTest, RightLimitedByMaxBytes) {
  // The number of bytes that match between the original "fearsome"
  // and the altered "fearsomm".
  const int expected_length = (TypeParam::kBlockSize * this->StringLengthAsInt("fearsom"))
                              + (TypeParam::kBlockSize - 1);  // spacing between letters
  const int max_bytes = expected_length - 1;
  EXPECT_EQ(max_bytes, this->MatchingBytesToRight(
      &this->search_string[this->index_of_f_in_fearsome],
      &this->search_string_altered[this->index_of_f_in_fearsome],
      max_bytes));
}

TYPED_TEST(BlockHashTest, RightNotLimited) {
  // The number of bytes that match between the original "we hear is fearsome"
  // and the altered "ve hear is fearsome".
  const int expected_length = (TypeParam::kBlockSize * this->StringLengthAsInt("fearsom"))
                              + (TypeParam::kBlockSize - 1);  // spacing between letters
  const int max_bytes = expected_length + 1;
  EXPECT_EQ(expected_length, this->MatchingBytesToRight(
      &this->search_string[this->index_of_f_in_fearsome],
      &this->search_string_altered[this->index_of_f_in_fearsome],
      max_bytes));
  EXPECT_EQ(expected_length, this->MatchingBytesToRight(
      &this->search_string[this->index_of_f_in_fearsome],
      &this->search_string_altered[this->index_of_f_in_fearsome],
      INT_MAX));
}

TYPED_TEST(BlockHashTest, FindFailsBeforeHashing) {
  EXPECT_EQ(-1, this->FirstMatchingBlock(*this->th_, this->hashed_y, this->test_string_y));
}

TYPED_TEST(BlockHashTest, HashOneFindOne) {
  for (int i = 0; i <= this->index_of_y_in_only; ++i) {
    this->th_->AddOneIndexHash(i, RollingHash<TypeParam::kBlockSize>::Hash(&this->sample_text[i]));
  }
  EXPECT_EQ(this->block_of_y_in_only, this->FirstMatchingBlock(*this->th_, this->hashed_y,
                                                   this->test_string_y));
  EXPECT_EQ(-1, this->NextMatchingBlock(*this->th_, this->block_of_y_in_only, this->test_string_y));
}

TYPED_TEST(BlockHashTest, HashAllFindOne) {
  EXPECT_EQ(this->block_of_y_in_only, this->FirstMatchingBlock(*this->dh_, this->hashed_y,
                                                   this->test_string_y));
  EXPECT_EQ(-1, this->NextMatchingBlock(*this->dh_, this->block_of_y_in_only, this->test_string_y));
}

TYPED_TEST(BlockHashTest, NonMatchingTextNotFound) {
  EXPECT_EQ(-1, this->FirstMatchingBlock(*this->dh_, this->hashed_all_Qs, this->test_string_all_Qs));
}

// Search for unaligned text.  The test string is contained in the
// sample text (unlike the non-matching string in NonMatchingTextNotFound,
// above), but it is not aligned on a block boundary.  FindMatchingBlock
// will only work if the test string is aligned on a block boundary.
//
//    "   T   h   e       o   n   l   y"
//              ^^^^ Here is the test string
//
TYPED_TEST(BlockHashTest, UnalignedTextNotFound) {
  EXPECT_EQ(-1, this->FirstMatchingBlock(*this->dh_, this->hashed_unaligned_e,
                                   this->test_string_unaligned_e));
}

TYPED_TEST(BlockHashTest, FindSixMatches) {
  EXPECT_EQ(this->block_of_first_e, this->FirstMatchingBlock(*this->dh_, this->hashed_e,
                                                 this->test_string_e));
  EXPECT_EQ(this->block_of_second_e, this->NextMatchingBlock(*this->dh_, this->block_of_first_e,
                                                 this->test_string_e));
  EXPECT_EQ(this->block_of_third_e, this->NextMatchingBlock(*this->dh_, this->block_of_second_e,
                                                this->test_string_e));
  EXPECT_EQ(this->block_of_fourth_e, this->NextMatchingBlock(*this->dh_, this->block_of_third_e,
                                                 this->test_string_e));
  EXPECT_EQ(this->block_of_fifth_e, this->NextMatchingBlock(*this->dh_, this->block_of_fourth_e,
                                                this->test_string_e));
  EXPECT_EQ(this->block_of_sixth_e, this->NextMatchingBlock(*this->dh_, this->block_of_fifth_e,
                                                this->test_string_e));
  EXPECT_EQ(-1, this->NextMatchingBlock(*this->dh_, this->block_of_sixth_e, this->test_string_e));

  // Starting over gives same result
  EXPECT_EQ(this->block_of_first_e, this->FirstMatchingBlock(*this->dh_, this->hashed_e,
                                                 this->test_string_e));
}

TYPED_TEST(BlockHashTest, AddRangeFindThreeMatches) {
  // Add hash values only for those characters before the fourth instance
  // of "e" in the sample text.  Tests that the ending index
  // of AddAllBlocksThroughIndex() is not inclusive: only three matches
  // for "e" should be found.
  this->th_->AddAllBlocksThroughIndex(this->index_of_fourth_e);
  EXPECT_EQ(this->block_of_first_e, this->FirstMatchingBlock(*this->th_, this->hashed_e,
                                                 this->test_string_e));
  EXPECT_EQ(this->block_of_second_e, this->NextMatchingBlock(*this->th_, this->block_of_first_e,
                                                 this->test_string_e));
  EXPECT_EQ(this->block_of_third_e, this->NextMatchingBlock(*this->th_, this->block_of_second_e,
                                                this->test_string_e));
  EXPECT_EQ(-1, this->NextMatchingBlock(*this->th_, this->block_of_third_e, this->test_string_e));

  // Starting over gives same result
  EXPECT_EQ(this->block_of_first_e, this->FirstMatchingBlock(*this->th_, this->hashed_e,
                                                 this->test_string_e));
}

// Try indices that are not even multiples of the block size.
// Add three ranges and verify the results after each
// call to AddAllBlocksThroughIndex().
TYPED_TEST(BlockHashTest, AddRangeWithUnalignedIndices) {
  this->th_->AddAllBlocksThroughIndex(this->index_of_first_e + 1);
  EXPECT_EQ(this->block_of_first_e, this->FirstMatchingBlock(*this->th_, this->hashed_e,
                                                 this->test_string_e));
  EXPECT_EQ(-1, this->NextMatchingBlock(*this->th_, this->block_of_first_e, this->test_string_e));

  // Starting over gives same result
  EXPECT_EQ(this->block_of_first_e, this->FirstMatchingBlock(*this->th_, this->hashed_e,
                                                 this->test_string_e));

  // Add the second range to expand the result set
  this->th_->AddAllBlocksThroughIndex(this->index_of_fourth_e - 3);
  EXPECT_EQ(this->block_of_first_e, this->FirstMatchingBlock(*this->th_, this->hashed_e,
                                                 this->test_string_e));
  EXPECT_EQ(this->block_of_second_e, this->NextMatchingBlock(*this->th_, this->block_of_first_e,
                                                 this->test_string_e));
  EXPECT_EQ(this->block_of_third_e, this->NextMatchingBlock(*this->th_, this->block_of_second_e,
                                                this->test_string_e));
  EXPECT_EQ(-1, this->NextMatchingBlock(*this->th_, this->block_of_third_e, this->test_string_e));

  // Starting over gives same result
  EXPECT_EQ(this->block_of_first_e, this->FirstMatchingBlock(*this->th_, this->hashed_e,
                                                 this->test_string_e));

  // Add the third range to expand the result set
  this->th_->AddAllBlocksThroughIndex(this->index_of_fourth_e + 1);

  EXPECT_EQ(this->block_of_first_e, this->FirstMatchingBlock(*this->th_, this->hashed_e,
                                                 this->test_string_e));
  EXPECT_EQ(this->block_of_second_e, this->NextMatchingBlock(*this->th_, this->block_of_first_e,
                                                 this->test_string_e));
  EXPECT_EQ(this->block_of_third_e, this->NextMatchingBlock(*this->th_, this->block_of_second_e,
                                                this->test_string_e));
  EXPECT_EQ(this->block_of_fourth_e, this->NextMatchingBlock(*this->th_, this->block_of_third_e,
                                                 this->test_string_e));
  EXPECT_EQ(-1, this->NextMatchingBlock(*this->th_, this->block_of_fourth_e, this->test_string_e));

  // Starting over gives same result
  EXPECT_EQ(this->block_of_first_e, this->FirstMatchingBlock(*this->th_, this->hashed_e,
                                                 this->test_string_e));
}

#ifdef GTEST_HAS_DEATH_TEST

TYPED_TEST_SUITE(BlockHashDeathTest, BlockHashTypes);

TYPED_TEST(BlockHashDeathTest, AddingRangesInDescendingOrderNoEffect) {
  this->th_->AddAllBlocksThroughIndex(this->index_of_fourth_e + 1);

  EXPECT_EQ(this->block_of_first_e, this->FirstMatchingBlock(*this->th_, this->hashed_e,
                                                 this->test_string_e));
  EXPECT_EQ(this->block_of_second_e, this->NextMatchingBlock(*this->th_, this->block_of_first_e,
                                                 this->test_string_e));
  EXPECT_EQ(this->block_of_third_e, this->NextMatchingBlock(*this->th_, this->block_of_second_e,
                                                this->test_string_e));
  EXPECT_EQ(this->block_of_fourth_e, this->NextMatchingBlock(*this->th_, this->block_of_third_e,
                                                 this->test_string_e));
  EXPECT_EQ(-1, this->NextMatchingBlock(*this->th_, this->block_of_fourth_e, this->test_string_e));

  // Starting over gives same result
  EXPECT_EQ(this->block_of_first_e, this->FirstMatchingBlock(*this->th_, this->hashed_e,
                                                 this->test_string_e));

  // These calls will produce DFATAL error messages, and will do nothing,
  // since the ranges have already been added.
  EXPECT_DEBUG_DEATH(this->th_->AddAllBlocksThroughIndex(this->index_of_fourth_e - 3),
                     "<");
  EXPECT_DEBUG_DEATH(this->th_->AddAllBlocksThroughIndex(this->index_of_first_e + 1),
                     "<");

  EXPECT_EQ(this->block_of_first_e, this->FirstMatchingBlock(*this->th_, this->hashed_e,
                                                 this->test_string_e));
  EXPECT_EQ(this->block_of_second_e, this->NextMatchingBlock(*this->th_, this->block_of_first_e,
                                                 this->test_string_e));
  EXPECT_EQ(this->block_of_third_e, this->NextMatchingBlock(*this->th_, this->block_of_second_e,
                                                this->test_string_e));
  EXPECT_EQ(this->block_of_fourth_e, this->NextMatchingBlock(*this->th_, this->block_of_third_e,
                                                 this->test_string_e));
  EXPECT_EQ(-1, this->NextMatchingBlock(*this->th_, this->block_of_fourth_e, this->test_string_e));

  // Starting over gives same result
  EXPECT_EQ(this->block_of_first_e, this->FirstMatchingBlock(*this->th_, this->hashed_e,
                                                 this->test_string_e));
}
#endif  // GTEST_HAS_DEATH_TEST

TYPED_TEST(BlockHashTest, AddEntireRangeFindSixMatches) {
  this->th_->AddAllBlocksThroughIndex(this->StringLengthAsInt(this->sample_text));
  EXPECT_EQ(this->block_of_first_e, this->FirstMatchingBlock(*this->th_, this->hashed_e,
                                                 this->test_string_e));
  EXPECT_EQ(this->block_of_second_e, this->NextMatchingBlock(*this->th_, this->block_of_first_e,
                                                 this->test_string_e));
  EXPECT_EQ(this->block_of_third_e, this->NextMatchingBlock(*this->th_, this->block_of_second_e,
                                                this->test_string_e));
  EXPECT_EQ(this->block_of_fourth_e, this->NextMatchingBlock(*this->th_, this->block_of_third_e,
                                                 this->test_string_e));
  EXPECT_EQ(this->block_of_fifth_e, this->NextMatchingBlock(*this->th_, this->block_of_fourth_e,
                                                this->test_string_e));
  EXPECT_EQ(this->block_of_sixth_e, this->NextMatchingBlock(*this->th_, this->block_of_fifth_e,
                                                this->test_string_e));
  EXPECT_EQ(-1, this->NextMatchingBlock(*this->th_, this->block_of_sixth_e, this->test_string_e));

  // Starting over gives same result
  EXPECT_EQ(this->block_of_first_e, this->FirstMatchingBlock(*this->th_, this->hashed_e,
                                                 this->test_string_e));
}

TYPED_TEST(BlockHashTest, ZeroSizeSourceAccepted) {
  BlockHash<TypeParam::kBlockSize> zero_sized_hash(0);
  EXPECT_EQ(true, zero_sized_hash.Init(this->sample_text, 0, true));
  EXPECT_EQ(-1, this->FirstMatchingBlock(zero_sized_hash, this->hashed_y, this->test_string_y));
}

TYPED_TEST(BlockHashTest, NullSource) {
  BlockHash<TypeParam::kBlockSize> null_source_hash(0);
  EXPECT_EQ(true, null_source_hash.Init(NULL, 0, true));
  EXPECT_EQ(-1, this->FirstMatchingBlock(null_source_hash, this->hashed_y, this->test_string_y));
}

#ifdef GTEST_HAS_DEATH_TEST
TYPED_TEST(BlockHashDeathTest, BadNextMatchingBlockReturnsNoMatch) {
  EXPECT_DEBUG_DEATH(EXPECT_EQ(-1, this->NextMatchingBlock(*this->dh_, 0xFFFFFFFE, "    ")),
                     "invalid");
}

TYPED_TEST(BlockHashDeathTest, CallingInitTwiceIsIllegal) {
  BlockHash<TypeParam::kBlockSize> bh(0);
  EXPECT_TRUE(bh.Init(this->sample_text, strlen(this->sample_text), false));
  EXPECT_DEBUG_DEATH(EXPECT_FALSE(bh.Init(this->sample_text, strlen(this->sample_text), false)), "twice");
}

TYPED_TEST(BlockHashDeathTest, CallingAddBlockBeforeInitIsIllegal) {
  BlockHash<TypeParam::kBlockSize> bh(0);
  EXPECT_DEBUG_DEATH(bh.AddAllBlocksThroughIndex(this->index_of_first_e),
                     "called before");
}

TYPED_TEST(BlockHashDeathTest, AddAllBlocksThroughIndexOutOfRange) {
  EXPECT_DEBUG_DEATH(
      this->th_->AddAllBlocksThroughIndex(strlen(this->sample_text) + 1),
      "higher than end");
}
#endif  // GTEST_HAS_DEATH_TEST

TYPED_TEST(BlockHashTest, UnknownFingerprintReturnsNoMatch) {
  EXPECT_EQ(-1, this->FirstMatchingBlock(*this->dh_, 0xFAFAFAFA, "FAFA"));
}

TYPED_TEST(BlockHashTest, FindBestMatch) {
  this->best_match_ = this->dh_->FindBestMatch(
                     this->hashed_f,
                     &this->search_string[this->index_of_f_in_fearsome],
                     this->search_string,
                     strlen(this->search_string));
  EXPECT_EQ(this->index_of_longest_match_ear_is_fear, this->best_match_.source_offset());
  EXPECT_EQ(this->index_of_second_e_in_what_we_hear, this->best_match_.target_offset());
  // The match includes the spaces after the final character,
  // which is why (TypeParam::kBlockSize - 1) is added to the expected best size.
  EXPECT_EQ((strlen("ear is fear") * TypeParam::kBlockSize) + (TypeParam::kBlockSize - 1),
            this->best_match_.size());
}

TYPED_TEST(BlockHashTest, FindBestMatchWithStartingOffset) {
  BlockHash<TypeParam::kBlockSize> th2(0x10000);
  th2.Init(this->sample_text, strlen(this->sample_text), true);  // hash all blocks
  this->best_match_ = th2.FindBestMatch(
                    this->hashed_f,
                    &this->search_string[this->index_of_f_in_fearsome],
                    this->search_string,
                    strlen(this->search_string));
  // Offset should begin with dictionary_size
  EXPECT_EQ(0x10000 + (this->index_of_longest_match_ear_is_fear),
            this->best_match_.source_offset());
  EXPECT_EQ(this->index_of_second_e_in_what_we_hear, this->best_match_.target_offset());
  // The match includes the spaces after the final character,
  // which is why (TypeParam::kBlockSize - 1) is added to the expected best size.
  EXPECT_EQ((strlen("ear is fear") * TypeParam::kBlockSize) + (TypeParam::kBlockSize - 1),
            this->best_match_.size());
}

TYPED_TEST(BlockHashTest, BestMatchReachesEndOfDictionary) {
  // Hash the "i" in "fear itself"
  uint32_t hash_value = RollingHash<TypeParam::kBlockSize>::Hash(
      &this->search_to_end_string[this->index_of_i_in_itself]);
  this->best_match_ = this->dh_->FindBestMatch(
                     hash_value,
                     &this->search_to_end_string[this->index_of_i_in_itself],
                     this->search_to_end_string,
                     strlen(this->search_to_end_string));
  EXPECT_EQ(this->index_of_space_before_itself, this->best_match_.source_offset());
  EXPECT_EQ(this->index_of_space_in_eat_itself, this->best_match_.target_offset());
  EXPECT_EQ(strlen(" itself") * TypeParam::kBlockSize, this->best_match_.size());
}

TYPED_TEST(BlockHashTest, BestMatchReachesStartOfDictionary) {
  // Hash the "i" in "fear itself"
  uint32_t hash_value = RollingHash<TypeParam::kBlockSize>::Hash(
      &this->search_to_beginning_string[this->index_of_o_in_online]);
  this->best_match_ = this->dh_->FindBestMatch(
                     hash_value,
                     &this->search_to_beginning_string[this->index_of_o_in_online],
                     this->search_to_beginning_string,
                     strlen(this->search_to_beginning_string));
  EXPECT_EQ(0, this->best_match_.source_offset());  // beginning of dictionary
  EXPECT_EQ(this->index_of_t_in_use_the, this->best_match_.target_offset());
  // The match includes the spaces after the final character,
  // which is why (TypeParam::kBlockSize - 1) is added to the expected best size.
  EXPECT_EQ((strlen("The onl") * TypeParam::kBlockSize) + (TypeParam::kBlockSize - 1),
            this->best_match_.size());
}

TYPED_TEST(BlockHashTest, BestMatchWithManyMatches) {
  BlockHash<TypeParam::kBlockSize> many_matches_hash(0);
  EXPECT_TRUE(many_matches_hash.Init(this->sample_text_many_matches,
                                     strlen(this->sample_text_many_matches),
                                     true));
  // Hash the "   a" at the beginning of the search string "ababc"
  uint32_t hash_value =
      RollingHash<TypeParam::kBlockSize>::Hash(this->search_string_many_matches);
  this->best_match_ = many_matches_hash.FindBestMatch(
                                  hash_value,
                                  this->search_string_many_matches,
                                  this->search_string_many_matches,
                                  strlen(this->search_string_many_matches));
  EXPECT_EQ(this->index_of_ababc, this->best_match_.source_offset());
  EXPECT_EQ(0, this->best_match_.target_offset());
  EXPECT_EQ(strlen(this->search_string_many_matches), this->best_match_.size());
}

TYPED_TEST(BlockHashTest, HashCollisionFindsNoMatch) {
  char* collision_search_string = new char[strlen(this->search_string) + 1];
  memcpy(collision_search_string, this->search_string, strlen(this->search_string) + 1);
  char* fearsome_location = &collision_search_string[this->index_of_f_in_fearsome];

  // Tweak the collision string so that it has the same hash value
  // but different text.  The last four characters of the search string
  // should be "   f", and the bytes given below have the same hash value
  // as those characters.
  CHECK_GE(TypeParam::kBlockSize, 5);
  fearsome_location[TypeParam::kBlockSize - 5] = 'g';
  fearsome_location[TypeParam::kBlockSize - 4] = 0x02;
  fearsome_location[TypeParam::kBlockSize - 3] = 0xCE;
  fearsome_location[TypeParam::kBlockSize - 2] = 0x01;
  fearsome_location[TypeParam::kBlockSize - 1] = 0xAE;
  EXPECT_EQ(this->hashed_f, RollingHash<TypeParam::kBlockSize>::Hash(fearsome_location));
  EXPECT_NE(0, memcmp(&this->search_string[this->index_of_f_in_fearsome],
                      fearsome_location,
                      TypeParam::kBlockSize));
  // No match should be found this time.
  this->best_match_ = this->dh_->FindBestMatch(this->hashed_f,
      fearsome_location,
      collision_search_string,
      strlen(this->search_string));  // since collision_search_string has embedded \0
  EXPECT_EQ(-1, this->best_match_.source_offset());
  EXPECT_EQ(-1, this->best_match_.target_offset());
  EXPECT_EQ(0U, this->best_match_.size());
  delete[] collision_search_string;
}

// If the footprint passed to FindBestMatch does not actually match
// the search string, it should not find any matches.
TYPED_TEST(BlockHashTest, WrongFootprintFindsNoMatch) {
  this->best_match_ = this->dh_->FindBestMatch(
                     this->hashed_e,  // Using hashed value of "e" instead of "f"!
                     &this->search_string[this->index_of_f_in_fearsome],
                     this->search_string,
                     strlen(this->search_string));
  EXPECT_EQ(-1, this->best_match_.source_offset());
  EXPECT_EQ(-1, this->best_match_.target_offset());
  EXPECT_EQ(0U, this->best_match_.size());
}

// Use a dictionary containing 1M copies of the letter 'Q',
// and target data that also contains 1M Qs.  If FindBestMatch
// is not throttled to find a maximum number of matches, this
// will take a very long time -- several seconds at least.
// If this test appears to hang, it is because the throttling code
// (see BlockHash::kMaxMatchesToCheck for details) is not working.
TYPED_TEST(BlockHashTest, SearchStringFindsTooManyMatches) {
  const int kTestSize = 1 << 20;  // 1M
  char* huge_dictionary = new char[kTestSize];
  memset(huge_dictionary, 'Q', kTestSize);
  BlockHash<TypeParam::kBlockSize> huge_bh(0);
  EXPECT_TRUE(huge_bh.Init(huge_dictionary, kTestSize,
                           /* populate_hash_table = */ true));
  char* huge_target = new char[kTestSize];
  memset(huge_target, 'Q', kTestSize);
  CycleTimer timer;
  timer.Start();
  this->best_match_ = huge_bh.FindBestMatch(
                        this->hashed_all_Qs,
                        huge_target + (kTestSize / 2),  // middle of target
                        huge_target,
                        kTestSize);
  timer.Stop();
  double elapsed_time_in_us = static_cast<double>(timer.GetInUsec());
  std::cout << "Time to search for best match with 1M matches: "
            << elapsed_time_in_us << " us" << std::endl;
  // All blocks match the candidate block.  FindBestMatch should have checked
  // a certain number of matches before giving up.  The best match
  // should include at least half the source and target, since the candidate
  // block was in the middle of the target data.
  EXPECT_GT((kTestSize / 2), this->best_match_.source_offset());
  EXPECT_GT((kTestSize / 2), this->best_match_.target_offset());
  EXPECT_LT(static_cast<size_t>(kTestSize / 2), this->best_match_.size());
  EXPECT_GT(5000000, elapsed_time_in_us);  // < 5 seconds
#ifdef NDEBUG
  EXPECT_GT(1000000, elapsed_time_in_us);  // < 1 second
#endif  // NDEBUG
  delete[] huge_target;
  delete[] huge_dictionary;
}

#ifdef GTEST_HAS_DEATH_TEST
TYPED_TEST(BlockHashDeathTest, AddTooManyBlocks) {
  for (int i = 0; i < this->StringLengthAsInt(this->sample_text_without_spaces); ++i) {
    this->th_->AddOneIndexHash(i * TypeParam::kBlockSize, this->hashed_e);
  }
  // Didn't expect another block to be added
  EXPECT_DEBUG_DEATH(this->th_->AddOneIndexHash(this->StringLengthAsInt(this->sample_text),
                                          this->hashed_e),
                     "AddBlock");
}
#endif  // GTEST_HAS_DEATH_TEST

}  //  namespace open_vcdiff
