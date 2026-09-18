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
#include "vcdiffengine.h"
#include <stdint.h>  // uint32_t
#include <string.h>  // memcpy
#include "blockhash.h"
#include "google/codetablewriter_interface.h"
#include "google/vcencoder.h"
#include "logging.h"
#include "rolling_hash.h"

namespace open_vcdiff {

VCDiffEngine::VCDiffEngine(const char* dictionary, size_t dictionary_size,
                           CompressionOptions comp_opt)
    // If dictionary_size == 0, then dictionary could be NULL.  Guard against
    // using a NULL value.
    : dictionary_((dictionary_size > 0) ? new char[dictionary_size] : ""),
      dictionary_size_(dictionary_size),
      comp_opt_(comp_opt),
      //block_size_(block_size),
      min_match_size_(comp_opt == COMP_MIN_8_BLOCK_8 ? 8 :
                      comp_opt == COMP_MIN_16_BLOCK_8 ||
                        comp_opt == COMP_MIN_16_BLOCK_16 ? 16 :
                      comp_opt == COMP_MIN_32_BLOCK_16 ||
                        comp_opt == COMP_MIN_32_BLOCK_32 ? 32 :
                      comp_opt == COMP_MIN_64_BLOCK_32 ||
                        comp_opt == COMP_MIN_64_BLOCK_64 ? 64 :
                      comp_opt == COMP_MIN_128_BLOCK_64 ? 128 : 32),
      hashed_dictionary_buf_("") {
  if (dictionary_size > 0) {
    memcpy(const_cast<char*>(dictionary_), dictionary, dictionary_size);
  }
  switch (comp_opt_) {
    case COMP_MIN_8_BLOCK_8: case COMP_MIN_16_BLOCK_8:
      new(const_cast<char*>(hashed_dictionary_buf_)) BlockHash<8>(0);
      break;
    default:
    case COMP_MIN_16_BLOCK_16: case COMP_MIN_32_BLOCK_16:
      new(const_cast<char*>(hashed_dictionary_buf_)) BlockHash<16>(0);
      break;
    case COMP_MIN_32_BLOCK_32: case COMP_MIN_64_BLOCK_32:
      new(const_cast<char*>(hashed_dictionary_buf_)) BlockHash<32>(0);
      break;
    case COMP_MIN_64_BLOCK_64: case COMP_MIN_128_BLOCK_64:
      new(const_cast<char*>(hashed_dictionary_buf_)) BlockHash<64>(0);
      break;
  }
}

VCDiffEngine::~VCDiffEngine() {
  if (dictionary_size_ > 0) {
    delete[] dictionary_;
  }
  switch (comp_opt_) {
    case COMP_MIN_8_BLOCK_8: case COMP_MIN_16_BLOCK_8:
      hashed_dictionary_mutable<8>().~BlockHash<8>();
      break;
    default:
    case COMP_MIN_16_BLOCK_16: case COMP_MIN_32_BLOCK_16:
      hashed_dictionary_mutable<16>().~BlockHash<16>();
      break;
    case COMP_MIN_32_BLOCK_32: case COMP_MIN_64_BLOCK_32:
      hashed_dictionary_mutable<32>().~BlockHash<32>();
      break;
    case COMP_MIN_64_BLOCK_64: case COMP_MIN_128_BLOCK_64:
      hashed_dictionary_mutable<64>().~BlockHash<64>();
      break;
  }
}

bool VCDiffEngine::Init() {
  bool res = false;
  switch (comp_opt_) {
    case COMP_MIN_8_BLOCK_8: case COMP_MIN_16_BLOCK_8:
      res = hashed_dictionary_mutable<8>().Init(
                dictionary_, dictionary_size_, true);
      break;
    default:
    case COMP_MIN_16_BLOCK_16: case COMP_MIN_32_BLOCK_16:
      res = hashed_dictionary_mutable<16>().Init(
                dictionary_, dictionary_size_, true);
      break;
    case COMP_MIN_32_BLOCK_32: case COMP_MIN_64_BLOCK_32:
      res = hashed_dictionary_mutable<32>().Init(
                dictionary_, dictionary_size_, true);
      break;
    case COMP_MIN_64_BLOCK_64: case COMP_MIN_128_BLOCK_64:
      res = hashed_dictionary_mutable<64>().Init(
                dictionary_, dictionary_size_, true);
      break;
  }
  if (!res) {
    VCD_DFATAL << "Creation of dictionary hash failed" << VCD_ENDL;
    return false;
  }
  return true;
}

// This helper function tries to find an appropriate match within
// hashed_dictionary_ for the block starting at the current target position.
// If target_hash is not NULL, this function will also look for a match
// within the previously encoded target data.
//
// If a match is found, this function will generate an ADD instruction
// for all unencoded data that precedes the match,
// and a COPY instruction for the match itself; then it returns
// the number of bytes processed by both instructions,
// which is guaranteed to be > 0.
// If no appropriate match is found, the function returns 0.
//
// The first four parameters are input parameters which are passed
// directly to BlockHash::FindBestMatch; please see that function
// for a description of their allowable values.
template<bool look_for_target_matches, size_t block_size>
inline size_t VCDiffEngine::EncodeCopyForBestMatch(
    uint32_t hash_value,
    const char* candidate_pos,
    const char* unencoded_target_start,
    size_t unencoded_target_size,
    const BlockHash<block_size>* target_hash,
    CodeTableWriterInterface* coder) const {
  // When FindBestMatch() comes up with a match for a candidate block,
  // it will populate best_match with the size, source offset,
  // and target offset of the match.

  // First look for a match in the dictionary.
  BlockHashBase::Match best_match =
    hashed_dictionary<block_size>().FindBestMatch(hash_value,
                                     candidate_pos,
                                     unencoded_target_start,
                                     unencoded_target_size);
  // If target matching is enabled, then see if there is a better match
  // within the target data that has been encoded so far.
  if (look_for_target_matches) {
    BlockHashBase::Match best_match_t =
      target_hash->FindBestMatch(hash_value,
                                 candidate_pos,
                                 unencoded_target_start,
                                 unencoded_target_size);
    best_match.ReplaceIfBetterMatch(best_match_t.size(),
                                    best_match_t.source_offset(),
                                    best_match_t.target_offset());
  }
  if (!best_match.size() || best_match.size() < min_match_size_) {
    return 0;
  }
  if (best_match.target_offset() > 0) {
    // Create an ADD instruction to encode all target bytes
    // from the end of the last COPY match, if any, up to
    // the beginning of this COPY match.
    coder->Add(unencoded_target_start, best_match.target_offset());
  }
  coder->Copy(best_match.source_offset(), best_match.size());
  return best_match.target_offset()  // ADD size
       + best_match.size();          // + COPY size
}

// Once the encoder loop has finished checking for matches in the target data,
// this function creates an ADD instruction to encode all target bytes
// from the end of the last COPY match, if any, through the end of
// the target data.  In the worst case, if no matches were found at all,
// this function will create one big ADD instruction
// for the entire buffer of target data.
inline void VCDiffEngine::AddUnmatchedRemainder(
    const char* unencoded_target_start,
    size_t unencoded_target_size,
    CodeTableWriterInterface* coder) const {
  if (unencoded_target_size > 0) {
    coder->Add(unencoded_target_start, unencoded_target_size);
  }
}

template<bool look_for_target_matches, size_t block_size>
void VCDiffEngine::EncodeInternal(const char* target_data,
                                  size_t target_size,
                                  OutputStringInterface* diff,
                                  CodeTableWriterInterface* coder) const {
  // Special case for really small input
  if (target_size < block_size) {
    AddUnmatchedRemainder(target_data, target_size, coder);
    coder->Output(diff);
    return;
  }
  RollingHash<block_size> hasher;
  BlockHash<block_size>* target_hash = NULL;
  if (look_for_target_matches) {
    // Check matches against previously encoded target data
    // in this same target window, as well as against the dictionary
    target_hash = BlockHash<block_size>::CreateTargetHash(target_data,
                                              target_size,
                                              dictionary_size());
    if (!target_hash) {
      VCD_DFATAL << "Instantiation of target hash failed" << VCD_ENDL;
      return;
    }
  }
  const char* const target_end = target_data + target_size;
  const char* const start_of_last_block = target_end - block_size;
  // Offset of next bytes in string to ADD if NOT copied (i.e., not found in
  // dictionary)
  const char* next_encode = target_data;
  // candidate_pos points to the start of the block_size-byte block that may
  // begin a match with the dictionary or previously encoded target data.
  const char* candidate_pos = target_data;
  uint32_t hash_value = hasher.Hash(candidate_pos);
  while (1) {
    const size_t bytes_encoded =
        EncodeCopyForBestMatch<look_for_target_matches>(
            hash_value,
            candidate_pos,
            next_encode,
            (target_end - next_encode),
            target_hash,
            coder);
    if (bytes_encoded > 0) {
      next_encode += bytes_encoded;  // Advance past COPYed data
      candidate_pos = next_encode;
      if (candidate_pos > start_of_last_block) {
        break;  // Reached end of target data
      }
      // candidate_pos has jumped ahead by bytes_encoded bytes, so UpdateHash
      // can't be used to calculate the hash value at its new position.
      hash_value = hasher.Hash(candidate_pos);
      if (look_for_target_matches) {
        // Update the target hash for the ADDed and COPYed data
        target_hash->AddAllBlocksThroughIndex(next_encode - target_data);
      }
    } else {
      // No match, or match is too small to be worth a COPY instruction.
      // Move to the next position in the target data.
      if ((candidate_pos + 1) > start_of_last_block) {
        break;  // Reached end of target data
      }
      if (look_for_target_matches) {
        target_hash->AddOneIndexHash(candidate_pos - target_data, hash_value);
      }
      hash_value = hasher.UpdateHash(hash_value,
                                     candidate_pos[0],
                                     candidate_pos[block_size]);
      ++candidate_pos;
    }
  }
  AddUnmatchedRemainder(next_encode, target_end - next_encode, coder);
  coder->Output(diff);
  delete target_hash;
}

void VCDiffEngine::Encode(const char* target_data,
                          size_t target_size,
                          bool look_for_target_matches,
                          OutputStringInterface* diff,
                          CodeTableWriterInterface* coder) const {
  if (target_size == 0) {
    return;  // Do nothing for empty target
  }

  switch (comp_opt_) {
    case COMP_MIN_8_BLOCK_8: case COMP_MIN_16_BLOCK_8:
      if (!hashed_dictionary<8>().IsInitialized())
        break;
      if (look_for_target_matches) {
        EncodeInternal<true, 8>(target_data, target_size, diff, coder);
      } else {
        EncodeInternal<false, 8>(target_data, target_size, diff, coder);
      }
      return;
    default:
    case COMP_MIN_16_BLOCK_16: case COMP_MIN_32_BLOCK_16:
      if (!hashed_dictionary<16>().IsInitialized())
        break;
      if (look_for_target_matches) {
        EncodeInternal<true, 16>(target_data, target_size, diff, coder);
      } else {
        EncodeInternal<false, 16>(target_data, target_size, diff, coder);
      }
      return;
    case COMP_MIN_32_BLOCK_32: case COMP_MIN_64_BLOCK_32:
      if (!hashed_dictionary<32>().IsInitialized())
        break;
      if (look_for_target_matches) {
        EncodeInternal<true, 32>(target_data, target_size, diff, coder);
      } else {
        EncodeInternal<false, 32>(target_data, target_size, diff, coder);
      }
      return;
    case COMP_MIN_64_BLOCK_64: case COMP_MIN_128_BLOCK_64:
      if (!hashed_dictionary<64>().IsInitialized())
        break;
      if (look_for_target_matches) {
        EncodeInternal<true, 64>(target_data, target_size, diff, coder);
      } else {
        EncodeInternal<false, 64>(target_data, target_size, diff, coder);
      }
      return;
  }
  VCD_DFATAL << "Internal error: VCDiffEngine::Encode() "
                "called before VCDiffEngine::Init()" << VCD_ENDL;
}

}  // namespace open_vcdiff
