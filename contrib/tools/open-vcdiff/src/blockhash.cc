// Copyright 2006, 2008 The open-vcdiff Authors. All Rights Reserved.
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
#include <stdint.h>  // uint32_t
#include <algorithm>  // std::min
#include "compile_assert.h"
#include "google/vcencoder.h"
#include "logging.h"
#include "rolling_hash.h"

namespace open_vcdiff {

BlockHashBase::BlockHashBase(int starting_offset, size_t block_size)
    : hash_table_mask_(0),
      starting_offset_(starting_offset),
      next_block_to_add_(0),
      block_size_(block_size) {
}

BlockHashBase::~BlockHashBase() { }

bool BlockHashBase::Init(const char* source_data,
                         size_t source_size,
                         bool populate_hash_table) {
  source_data_ = source_data;
  source_size_ = source_size;
  if (!hash_table_.empty() ||
      !next_block_table_.empty()) {
    VCD_DFATAL << "Init() called twice for same BlockHash object" << VCD_ENDL;
    return false;
  }
  if (source_size_ > kMaxDictSize) {
    VCD_DFATAL << "Dictionary size " << source_size_ << " is greater than "
               "allowed " << kMaxDictSize << VCD_ENDL;
    return false;
  }
  const size_t table_size = CalcTableSize(source_size_);
  if (table_size == 0) {
    VCD_DFATAL << "Error finding table size for source size " << source_size_
               << VCD_ENDL;
    return false;
  }
  // Since table_size is a power of 2, (table_size - 1) is a bit mask
  // containing all the bits below table_size.
  hash_table_mask_ = static_cast<uint32_t>(table_size - 1);
  hash_table_.resize(table_size, -1);
  next_block_table_.resize(GetNumberOfBlocks());
  if (populate_hash_table) {
    AddAllBlocks();
  }
  return true;
}

template<size_t block_size>
const BlockHash<block_size>* BlockHash<block_size>::CreateDictionaryHash(
    const char* dictionary_data,
    size_t dictionary_size) {
  BlockHash* new_dictionary_hash = new BlockHash(0);
  if (!new_dictionary_hash->Init(dictionary_data,
                                 dictionary_size,
                                 /* populate_hash_table = */ true)) {
    delete new_dictionary_hash;
    return NULL;
  } else {
    return new_dictionary_hash;
  }
}

template<size_t block_size>
BlockHash<block_size>* BlockHash<block_size>::CreateTargetHash(
    const char* target_data,
    size_t target_size,
    size_t dictionary_size) {
  BlockHash* new_target_hash = new BlockHash(static_cast<int>(dictionary_size));
  if (!new_target_hash->Init(target_data,
                             target_size,
                             /* populate_hash_table = */ false)) {
    delete new_target_hash;
    return NULL;
  } else {
    return new_target_hash;
  }
}

// Returns zero if an error occurs.
size_t BlockHashBase::CalcTableSize(const size_t dictionary_size) {
  // Overallocate the hash table by making it the same size (in bytes)
  // as the source data.  This is a trade-off between space and time:
  // the empty entries in the hash table will reduce the
  // probability of a hash collision to (sizeof(int) / kblockSize),
  // and so save time comparing false matches.
  const size_t min_size = (dictionary_size / sizeof(int)) + 1;  // NOLINT
  size_t table_size = 1;
  // Find the smallest power of 2 that is >= min_size, and assign
  // that value to table_size.
  while (table_size < min_size) {
    table_size <<= 1;
    // Guard against an infinite loop
    if (table_size <= 0) {
      VCD_DFATAL << "Internal error: CalcTableSize(dictionary_size = "
                 << dictionary_size
                 << "): resulting table_size " << table_size
                 << " is zero or negative" << VCD_ENDL;
      return 0;
    }
  }
  // Check size sanity
  if ((table_size & (table_size - 1)) != 0) {
    VCD_DFATAL << "Internal error: CalcTableSize(dictionary_size = "
               << dictionary_size
               << "): resulting table_size " << table_size
               << " is not a power of 2" << VCD_ENDL;
    return 0;
  }
  // The loop above tries to find the smallest power of 2 that is >= min_size.
  // That value must lie somewhere between min_size and (min_size * 2),
  // except for the case (dictionary_size == 0, table_size == 1).
  if ((dictionary_size > 0) && (table_size > (min_size * 2))) {
    VCD_DFATAL << "Internal error: CalcTableSize(dictionary_size = "
               << dictionary_size
               << "): resulting table_size " << table_size
               << " is too large" << VCD_ENDL;
    return 0;
  }
  return table_size;
}

// If the hash value is already available from the rolling hash,
// call this function to save time.
template<size_t block_size>
void BlockHash<block_size>::AddBlock(uint32_t hash_value) {
  if (hash_table_.empty()) {
    VCD_DFATAL << "BlockHash::AddBlock() called before BlockHash::Init()"
               << VCD_ENDL;
    return;
  }
  int block_number = static_cast<int>(next_block_to_add_);
  const int total_blocks =
      static_cast<int>(source_size_ / kBlockSize);  // round down
  if (block_number >= total_blocks) {
    VCD_DFATAL << "BlockHash::AddBlock() called"
                  " with block number " << block_number
               << " that is past last block " << (total_blocks - 1)
               << VCD_ENDL;
    return;
  }
  if (next_block_table_[block_number].blk != -1) {
    VCD_DFATAL << "Internal error in BlockHash::AddBlock(): "
                  "block number = " << block_number
               << ", next block should be -1 but is "
               << next_block_table_[block_number].blk << VCD_ENDL;
    return;
  }
  next_block_to_add_ = block_number + 1;
  const uint32_t hash_table_index = GetHashTableIndex(hash_value);
  const int first_matching_block = hash_table_[hash_table_index];
  if (first_matching_block < 0) {
    // This is the first entry with this hash value
    hash_table_[hash_table_index] = block_number;
  } else {
    // Find a chain of blocks with identical data (or add a new chain)
    int ins_block = first_matching_block;
    while (!BlockContentsMatch(&source_data_[block_number * kBlockSize],
                               &source_data_[ins_block * kBlockSize])) {
      int next_block = next_block_table_[ins_block].diff_str_blk;
      if (next_block == -1) {
        next_block_table_[ins_block].diff_str_blk = block_number;
        return;
      }
      ins_block = next_block;
    }
    // Add this entry at the end of the chain of blocks with identical data.
    // If there was just one block in the chain, just add the second one.
    int next_block = next_block_table_[ins_block].blk;
    if (next_block == -1) {
      next_block_table_[ins_block].blk = block_number;
      return;
    }
    // If there were two blocks, init stripe_size, then add the third block.
    // (stripe_size always lives within the second block of the chain)
    next_block = next_block_table_[ins_block = next_block].blk;
    if (next_block == -1) {
      next_block_table_[ins_block].blk = block_number;
      next_block_table_[ins_block].stripe_size = 3;
      next_block_table_[block_number].last_block = block_number;
      return;
    }
    if (next_block_table_[ins_block].stripe_size > kMaxMatchesToCheck) {
      return;
    }
    // With 3+ blocks, use last_block to add blocks to the end
    ++next_block_table_[ins_block].stripe_size;
    auto &last_block = next_block_table_[next_block].last_block;

    if (next_block_table_[last_block].blk != -1) {
      VCD_DFATAL << "Internal error in BlockHash::AddBlock(): "
                    "first matching block = " << first_matching_block
                 << ", ins_block = " << ins_block
                 << ", next_block = " << next_block
                 << ", next_block = " << last_block
                 << ", [last_block].blk be -1 but is "
                 << next_block_table_[last_block].blk << VCD_ENDL;
      return;
    }
    next_block_table_[last_block].blk = block_number;
    last_block = block_number;
  }
}

template<size_t block_size>
void BlockHash<block_size>::AddAllBlocks() {
  // kBlockSize must be at least 2 to be meaningful.  Since it's a compile-time
  // constant, check its value at compile time rather than wasting CPU cycles
  // on runtime checks.
  VCD_COMPILE_ASSERT(kBlockSize >= 2, kBlockSize_must_be_at_least_2);

  // kBlockSize is required to be a power of 2 because multiplication
  // (n * kBlockSize), division (n / kBlockSize) and MOD (n % kBlockSize)
  // are commonly-used operations.  If kBlockSize is a compile-time
  // constant and a power of 2, the compiler can convert these three operations
  // into bit-shift (>> or <<) and bitwise-AND (&) operations, which are much
  // more efficient than executing full integer multiply, divide, or remainder
  // instructions.
  VCD_COMPILE_ASSERT((kBlockSize & (kBlockSize - 1)) == 0,
                     kBlockSize_must_be_a_power_of_2);

  if (block_size_ != kBlockSize) {
    VCD_DFATAL << "Block size mismatch: constructed with " << block_size_
               << ", but AddAllBlocks() is called with " << kBlockSize
               << VCD_ENDL;
    return;
  }
  AddAllBlocksThroughIndex(source_size_);
}

template<size_t block_size>
void BlockHash<block_size>::AddAllBlocksThroughIndex(size_t end_index) {
  if (hash_table_.empty()) {
    VCD_DFATAL << "BlockHash::AddAllBlocksThroughIndex() called before "
                  "BlockHash::Init()" << VCD_ENDL;
    return;
  }
  if (end_index > source_size_) {
    VCD_DFATAL << "BlockHash::AddAllBlocksThroughIndex() called"
                  " with index " << end_index
               << " higher than end index  " << source_size_ << VCD_ENDL;
    return;
  }
  if (end_index < NextIndexToAdd()) {
    VCD_DFATAL << "BlockHash::AddAllBlocksThroughIndex() called"
                  " with index " << end_index
               << " < next index to add ( " << NextIndexToAdd()
               << ")" << VCD_ENDL;
    return;
  }
  if (source_size() < kBlockSize) {
    // Exit early if the source data is small enough that it does not contain
    // any blocks.  This avoids negative values of last_legal_hash_index.
    // See: https://github.com/google/open-vcdiff/issues/40
    return;
  }
  size_t end_limit = end_index;
  // Don't allow reading any indices at or past source_size_.
  // The Hash function extends (kBlockSize - 1) bytes past the index,
  // so leave a margin of that size.
  size_t last_legal_hash_index = source_size() - kBlockSize + 1;
  if (end_limit > last_legal_hash_index) {
    end_limit = last_legal_hash_index;
  }
  const char* block_ptr = source_data() + NextIndexToAdd();
  const char* const end_ptr = source_data() + end_limit;
  while (block_ptr < end_ptr) {
    AddBlock(RollingHash<kBlockSize>::Hash(block_ptr));
    block_ptr += kBlockSize;
  }
}

// Init() must have been called and returned true before using
// FirstMatchingBlock or NextMatchingBlock.  No check is performed
// for this condition; the code will crash if this condition is violated.
template<size_t block_size>
int BlockHash<block_size>::FirstMatchingBlock(uint32_t hash_value,
                                              const char* block_ptr) const {
  return SkipNonMatchingBlocks(hash_table_[GetHashTableIndex(hash_value)],
                               block_ptr);
}

template<size_t block_size>
int BlockHash<block_size>::NextMatchingBlock(int block_number) const {
  if (static_cast<size_t>(block_number) >= next_block_table_.size()) {
    VCD_DFATAL << "NextMatchingBlock called for invalid block number "
               << block_number << VCD_ENDL;
    return -1;
  }
  return next_block_table_[block_number].blk;
}

// Returns the number of bytes to the left of source_match_start
// that match the corresponding bytes to the left of target_match_start.
// Will not examine more than max_bytes bytes, which is to say that
// the return value will be in the range [0, max_bytes] inclusive.
template<size_t block_size>
size_t BlockHash<block_size>::MatchingBytesToLeft(const char* src_match_start,
                                                  const char* tgt_match_start,
                                                  size_t max_bytes) {
  const char* source_ptr = src_match_start;
  const char* target_ptr = tgt_match_start;
  size_t bytes_found = 0;
  while (bytes_found < max_bytes) {
    --source_ptr;
    --target_ptr;
    if (*source_ptr != *target_ptr) {
      break;
    }
    ++bytes_found;
  }
  return bytes_found;
}

// Returns the number of bytes starting at source_match_end
// that match the corresponding bytes starting at target_match_end.
// Will not examine more than max_bytes bytes, which is to say that
// the return value will be in the range [0, max_bytes] inclusive.
template<size_t block_size>
size_t BlockHash<block_size>::MatchingBytesToRight(const char* src_match_end,
                                                   const char* tgt_match_end,
                                                   size_t max_bytes) {
  const char* source_ptr = src_match_end;
  const char* target_ptr = tgt_match_end;
  size_t bytes_found = 0;
  // We can get a lot of matching bytes on the right, so optimize a little bit.
  // First try bigger portions.
  while ((bytes_found + kBlockSize <= max_bytes) &&
         BlockContentsMatch(source_ptr, target_ptr)) {
    bytes_found += kBlockSize;
    source_ptr += kBlockSize;
    target_ptr += kBlockSize;
  }
  // Then byte-by-byte.
  while ((bytes_found < max_bytes) && (*source_ptr == *target_ptr)) {
    ++bytes_found;
    ++source_ptr;
    ++target_ptr;
  }
  return bytes_found;
}

// No NULL checks are performed on the pointer arguments.  The caller
// must guarantee that none of the arguments is NULL, or a crash will occur.
//
// The vast majority of calls to FindBestMatch enter the loop *zero* times,
// which is to say that most candidate blocks find no matches in the dictionary.
// The important sections for optimization are therefore the code outside the
// loop and the code within the loop conditions.  Keep this to a minimum.
template<size_t block_size>
BlockHashBase::Match BlockHash<block_size>::FindBestMatchImpl(
    ptrdiff_t block_number,
    const char* target_candidate_start,
    const char* target_start,
    size_t target_size) const {
  // assert(block_number >= 0);
  Match best_match;
  do {
    size_t source_match_offset = size_t(block_number * kBlockSize);
    const size_t source_match_end = source_match_offset + kBlockSize;

    size_t target_match_offset = target_candidate_start - target_start;
    const size_t target_match_end = target_match_offset + kBlockSize;

    size_t match_size = kBlockSize;
    {
      // Extend match start towards beginning of unencoded data
      const size_t limit_bytes_to_left = std::min(source_match_offset,
                                                  target_match_offset);
      const size_t matching_bytes_to_left =
          MatchingBytesToLeft(source_data_ + source_match_offset,
                              target_start + target_match_offset,
                              limit_bytes_to_left);
      source_match_offset -= matching_bytes_to_left;
      target_match_offset -= matching_bytes_to_left;
      match_size += matching_bytes_to_left;
    }
    {
      // Extend match end towards end of unencoded data
      const size_t source_bytes_to_right = source_size_ - source_match_end;
      const size_t target_bytes_to_right = target_size - target_match_end;
      const size_t limit_bytes_to_right = std::min(source_bytes_to_right,
                                                   target_bytes_to_right);
      match_size +=
          MatchingBytesToRight(source_data_ + source_match_end,
                               target_start + target_match_end,
                               limit_bytes_to_right);
    }
    // Update out parameter if the best match found was better
    // than any match already stored in best_match.
    best_match.ReplaceIfBetterMatch(match_size,
                                     source_match_offset + starting_offset_,
                                     target_match_offset);

    block_number = next_block_table_[block_number].blk;
  } while (block_number >= 0);
  return best_match;
}

template class BlockHash<8>;
template class BlockHash<16>;
template class BlockHash<32>;
template class BlockHash<64>;

}  // namespace open_vcdiff
