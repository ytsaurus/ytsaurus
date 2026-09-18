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
#include "vcdiffengine.h"
#include <string.h>  // memset, strlen
#include <algorithm>
#include <string>
#include "addrcache.h"
#include "blockhash.h"
#include "google/encodetable.h"
#include "google/output_string.h"
#include "google/vcencoder.h"
#include "rolling_hash.h"
#include "testing.h"
#include "varint_bigendian.h"
#include "vcdiff_defs.h"

namespace open_vcdiff {

namespace {

class VCDiffEngineTestBase : public testing::TestWithParam<size_t> {
 protected:
  typedef std::string string;

  // Some common definitions and helper functions used in the various tests
  // for VCDiffEngine.

  VCDiffEngineTestBase() : interleaved_(false),
                           diff_output_string_(&diff_),
                           verify_position_(0),
                           saved_total_size_position_(0),
                           saved_delta_encoding_position_(0),
                           saved_section_sizes_position_(0),
                           data_bytes_(0),
                           instruction_bytes_(0),
                           address_bytes_(0),
                           block_size_(VCDiffEngine(NULL, 0,
                             CompressionOptions(GetParam())).
                               min_match_size()) { }

  virtual ~VCDiffEngineTestBase() { }

  virtual void TearDown() {
  }

  // Copy string_without_spaces into newly allocated result buffer,
  // but pad its contents with space characters so that every character
  // in string_without_spaces corresponds to (block_size - 1)
  // spaces in the result, followed by that character.
  // For example:
  // If string_without_spaces begins "The only thing"... and block_size is 4,
  // then 3 space characters will be inserted
  // between each letter in the result, as follows:
  // "   T   h   e       o   n   l   y       t   h   i   n   g"...
  // This makes testing simpler, because finding a block_size-byte match
  // between the dictionary and target only depends on the
  // trailing letter in each block.
  // If no_initial_padding is true, then the first letter will not have
  // spaces added in front of it.
  static char *MakeEachLetterABlock(const char* string_without_spaces,
                                    int block_size,
                                    bool no_initial_padding) {
    const size_t length_without_spaces = strlen(string_without_spaces);
    char* padded_text = new char[(block_size * length_without_spaces) + 1];
    memset(padded_text, ' ', block_size * length_without_spaces);
    char* padded_text_ptr = padded_text;
    if (!no_initial_padding) {
      padded_text_ptr += block_size - 1;
    }
    for (size_t i = 0; i < length_without_spaces; ++i) {
      *padded_text_ptr = string_without_spaces[i];
      padded_text_ptr += block_size;
    }
    *(padded_text_ptr - block_size + 1)  = '\0';
    return padded_text;
  }

  // These functions iterate through the decoded output and expect
  // simple elements: bytes or variable-length integers.
  void ExpectByte(char byte) {
    EXPECT_GT(diff_.size(), verify_position_);
    EXPECT_EQ(byte, diff_[verify_position_]);
    ++verify_position_;
  }

  size_t ExpectVarint(int32_t expected_value) {
    EXPECT_GT(diff_.size(), verify_position_);
    const char* const original_position = &diff_[verify_position_];
    const char* new_position = original_position;
    const size_t expected_length = VarintBE<int32_t>::Length(expected_value);
    int32_t parsed_value = VarintBE<int32_t>::Parse(diff_.data() + diff_.size(),
                                                    &new_position);
    EXPECT_LE(0, parsed_value);
    size_t parsed_length = new_position - original_position;
    EXPECT_EQ(expected_value, parsed_value);
    EXPECT_EQ(expected_length, parsed_length);
    verify_position_ += parsed_length;
    return parsed_length;
  }

  size_t ExpectSize(size_t size) {
    return ExpectVarint(static_cast<int32_t>(size));
  }

  size_t ExpectStringLength(const char* s) {
    return ExpectSize(strlen(s));
  }

  void SkipVarint() {
    EXPECT_GT(diff_.size(), verify_position_);
    const char* const original_position = &diff_[verify_position_];
    const char* new_position = original_position;
    VarintBE<int32_t>::Parse(diff_.data() + diff_.size(), &new_position);
    size_t parsed_length = new_position - original_position;
    verify_position_ += parsed_length;
  }

  void ExpectDataByte(char byte) {
    ExpectByte(byte);
    if (interleaved_) {
      ++instruction_bytes_;
    } else {
      ++data_bytes_;
    }
  }

  void ExpectDataString(const char *expected_string) {
    const char* ptr = expected_string;
    while (*ptr) {
      ExpectDataByte(*ptr);
      ++ptr;
    }
  }

  void ExpectDataStringWithBlockSpacing(const char *expected_string,
                                        bool trailing_spaces) {
    const char* ptr = expected_string;
    while (*ptr) {
      for (size_t i = 0; i < (block_size_ - 1); ++i) {
        ExpectDataByte(' ');
      }
      ExpectDataByte(*ptr);
      ++ptr;
    }
    if (trailing_spaces) {
      for (size_t i = 0; i < (block_size_ - 1); ++i) {
        ExpectDataByte(' ');
      }
    }
  }

  void ExpectInstructionByte(char byte) {
    ExpectByte(byte);
    ++instruction_bytes_;
  }

  void ExpectInstructionVarint(int32_t value) {
    instruction_bytes_ += ExpectVarint(value);
  }

  void ExpectAddressByte(char byte) {
    ExpectByte(byte);
    if (interleaved_) {
      ++instruction_bytes_;
    } else {
      ++address_bytes_;
    }
  }

  void ExpectAddressVarint(int32_t value) {
    if (interleaved_) {
      instruction_bytes_ += ExpectVarint(value);
    } else {
      address_bytes_ += ExpectVarint(value);
    }
  }

  // The following functions leverage the fact that the encoder uses
  // the default code table and cache sizes.  They are able to search for
  // instructions of a particular size.  The logic for mapping from
  // instruction type, mode, and size to opcode value is very different here
  // from the logic used in encodetable.{h,cc}, so hopefully
  // this version will help validate that the other is correct.
  // This version uses conditional statements, while encodetable.h
  // looks up values in a mapping table.
  void ExpectAddress(int32_t address, int copy_mode) {
    if (default_cache_.WriteAddressAsVarintForMode(copy_mode)) {
      ExpectAddressVarint(address);
    } else {
      ExpectAddressByte(address);
    }
  }

  void ExpectAddInstruction(int size) {
    if (size <= 18) {
      ExpectInstructionByte(0x01 + size);
    } else {
      ExpectInstructionByte(0x01);
      ExpectInstructionVarint(size);
    }
  }

  void ExpectCopyInstruction(int size, int mode) {
    if ((size >= 4) && (size <= 16)) {
      ExpectInstructionByte(0x10 + (0x10 * mode) + size);
    } else {
      ExpectInstructionByte(0x13 + (0x10 * mode));
      ExpectInstructionVarint(size);
    }
  }

  bool ExpectAddCopyInstruction(int add_size, int copy_size, int copy_mode) {
    if (!default_cache_.IsSameMode(copy_mode) &&
        (add_size <= 4) &&
        (copy_size >= 4) &&
        (copy_size <= 6)) {
      ExpectInstructionByte(0x9C +
                            (0x0C * copy_mode) +
                            (0x03 * add_size) +
                            copy_size);
      return true;
    } else if (default_cache_.IsSameMode(copy_mode) &&
               (add_size <= 4) &&
               (copy_size == 4)) {
      ExpectInstructionByte(0xD2 + (0x04 * copy_mode) + add_size);
      return true;
    } else {
      ExpectAddInstruction(add_size);
      return false;
    }
  }

  void ExpectAddInstructionForStringLength(const char* s) {
    ExpectAddInstruction(static_cast<int>(strlen(s)));
  }

  // Call this function before beginning to iterate through the diff string
  // using the Expect... functions.
  // text must be NULL-terminated.
  void VerifyHeaderForDictionaryAndTargetText(const char* dictionary,
                                              const char* target_text) {
    ExpectByte(0x01);  // Win_Indicator: VCD_SOURCE (dictionary)
    ExpectStringLength(dictionary);
    ExpectByte(0x00);  // Source segment position: start of dictionary
    saved_total_size_position_ = verify_position_;
    SkipVarint();  // Length of the delta encoding
    saved_delta_encoding_position_ = verify_position_;
    ExpectStringLength(target_text);
    ExpectByte(0x00);  // Delta_indicator (no compression)
    saved_section_sizes_position_ = verify_position_;
    SkipVarint();  // length of data for ADDs and RUNs
    SkipVarint();  // length of instructions section
    SkipVarint();  // length of addresses for COPYs
  }

  // Call this function before beginning to iterating through the entire
  // diff string using the Expect... functions.  It makes sure that the
  // size totals in the window header match the number of bytes that
  // were parsed.
  void VerifySizes() {
    EXPECT_EQ(verify_position_, diff_.size());
    const size_t delta_encoding_size = verify_position_ -
                                       saved_delta_encoding_position_;
    verify_position_ = saved_total_size_position_;
    ExpectSize(delta_encoding_size);
    verify_position_ = saved_section_sizes_position_;
    ExpectSize(data_bytes_);
    ExpectSize(instruction_bytes_);
    ExpectSize(address_bytes_);
  }

  bool interleaved_;
  string diff_;
  OutputString<string> diff_output_string_;
  size_t verify_position_;
  size_t saved_total_size_position_;
  size_t saved_delta_encoding_position_;
  size_t saved_section_sizes_position_;
  size_t data_bytes_;
  size_t instruction_bytes_;
  size_t address_bytes_;
  size_t block_size_;
  VCDiffAddressCache default_cache_;  // Used for finding mode values
};

class VCDiffEngineTest : public VCDiffEngineTestBase {
 protected:
  VCDiffEngineTest() :
      dictionary_(MakeEachLetterABlock(dictionary_without_spaces_,
                                       block_size_, false)),
      target_(MakeEachLetterABlock(target_without_spaces_, block_size_, false)),
      engine_(dictionary_, strlen(dictionary_),
              CompressionOptions(GetParam())) {
    EXPECT_TRUE(const_cast<VCDiffEngine*>(&engine_)->Init());
  }

  virtual ~VCDiffEngineTest() {
    delete[] dictionary_;
    delete[] target_;
  }

  void EncodeNothing(bool interleaved, bool target_matching) {
    interleaved_ = interleaved;
    VCDiffCodeTableWriter coder(interleaved, false);
    coder.Init(engine_.dictionary(), engine_.dictionary_size());
    engine_.Encode("", 0, target_matching, &diff_output_string_, &coder);
    EXPECT_TRUE(diff_.empty());
  }

  // text must be NULL-terminated
  void EncodeText(const char* text, bool interleaved, bool target_matching) {
    interleaved_ = interleaved;
    VCDiffCodeTableWriter coder(interleaved, false);
    coder.Init(engine_.dictionary(), engine_.dictionary_size());
    engine_.Encode(text,
                   strlen(text),
                   target_matching,
                   &diff_output_string_,
                   &coder);
  }

  void Encode(bool interleaved, bool target_matching) {
    EncodeText(target_, interleaved, target_matching);
    VerifyHeader();
  }

  void VerifyHeader() {
    VerifyHeaderForDictionaryAndTargetText(dictionary_, target_);
  }

  static const char dictionary_without_spaces_[];
  static const char target_without_spaces_[];

  const char* dictionary_ = nullptr;
  const char* target_ = nullptr;

  const VCDiffEngine engine_;
};

#ifdef GTEST_HAS_DEATH_TEST
typedef VCDiffEngineTest VCDiffEngineDeathTest;
#endif  // GTEST_HAS_DEATH_TEST

const char VCDiffEngineTest::dictionary_without_spaces_[] =
    "The only thing we have to fear is fear itself";

const char VCDiffEngineTest::target_without_spaces_[] =
    "What we hear is fearsome";

#ifdef GTEST_HAS_DEATH_TEST
TEST_P(VCDiffEngineDeathTest, InitCalledTwice) {
  EXPECT_DEBUG_DEATH(EXPECT_FALSE(const_cast<VCDiffEngine*>(&engine_)->Init()),
                     "twice");
}

INSTANTIATE_TEST_SUITE_P(AllOpts, VCDiffEngineDeathTest,
                         ::testing::Values<size_t>(COMP_DEFAULT));
#endif  // GTEST_HAS_DEATH_TEST

TEST_P(VCDiffEngineTest, EngineEncodeNothing) {
  EncodeNothing(/* interleaved = */ false, /* target matching = */ false);
}

TEST_P(VCDiffEngineTest, EngineEncodeNothingInterleaved) {
  EncodeNothing(/* interleaved = */ true, /* target matching = */ false);
}

TEST_P(VCDiffEngineTest, EngineEncodeNothingTarget) {
  EncodeNothing(/* interleaved = */ false, /* target matching = */ true);
}

TEST_P(VCDiffEngineTest, EngineEncodeNothingTargetInterleaved) {
  EncodeNothing(/* interleaved = */ true, /* target matching = */ true);
}

TEST_P(VCDiffEngineTest, EngineEncodeSmallerThanOneBlock) {
  const char* small_text = "  ";
  EncodeText(small_text,
             /* interleaved = */ false,
             /* target matching = */ false);
  VerifyHeaderForDictionaryAndTargetText(dictionary_, small_text);
  // Data for ADDs
  ExpectDataString(small_text);
  // Instructions and sizes
  ExpectAddInstructionForStringLength(small_text);
}

TEST_P(VCDiffEngineTest, EngineEncodeSmallerThanOneBlockInterleaved) {
  const char* small_text = "  ";
  EncodeText(small_text,
             /* interleaved = */ true,
             /* target matching = */ false);
  VerifyHeaderForDictionaryAndTargetText(dictionary_, small_text);
  // Interleaved section
  ExpectAddInstructionForStringLength(small_text);
  ExpectDataString(small_text);
}

TEST_P(VCDiffEngineTest, EngineEncodeSampleText) {
  if (GetParam() == COMP_MIN_64_BLOCK_32 || GetParam() == COMP_MIN_128_BLOCK_64)
    return;
  Encode(/* interleaved = */ false, /* target matching = */ false);
  // Data for ADDs
  ExpectDataStringWithBlockSpacing("W", false);
  ExpectDataByte('t');
  ExpectDataByte('s');
  ExpectDataByte('m');
  // Instructions and sizes
  if (!ExpectAddCopyInstruction(block_size_, (3 * block_size_) - 1,
                                VCD_SELF_MODE)) {
    ExpectCopyInstruction((3 * block_size_) - 1, VCD_SELF_MODE);
  }
  ExpectAddInstruction(1);
  ExpectCopyInstruction((6 * block_size_) - 1, VCD_SELF_MODE);
  ExpectCopyInstruction(11 * block_size_,
                        default_cache_.FirstNearMode());
  if (!ExpectAddCopyInstruction(1, (2 * block_size_) - 1, VCD_SELF_MODE)) {
    ExpectCopyInstruction((2 * block_size_) - 1, VCD_SELF_MODE);
  }
  if (!ExpectAddCopyInstruction(1, block_size_, VCD_SELF_MODE)) {
    ExpectCopyInstruction(block_size_, VCD_SELF_MODE);
  }
  // Addresses for COPY
  ExpectAddressVarint(18 * block_size_);  // "ha"
  ExpectAddressVarint(14 * block_size_);  // " we h"
  ExpectAddressVarint((9 * block_size_) + (block_size_ - 1));  // "ear is fear"
  ExpectAddressVarint(4 * block_size_);  // "o" from "The only"
  ExpectAddressVarint(2 * block_size_);  // "e" from "The only"
  VerifySizes();
}

TEST_P(VCDiffEngineTest, EngineEncodeSampleTextInterleaved) {
  if (GetParam() == COMP_MIN_64_BLOCK_32 || GetParam() == COMP_MIN_128_BLOCK_64)
    return;
  Encode(/* interleaved = */ true, /* target matching = */ false);
  // Interleaved section
  if (!ExpectAddCopyInstruction(block_size_, (3 * block_size_) - 1,
                                VCD_SELF_MODE)) {
    ExpectDataStringWithBlockSpacing("W", false);
    ExpectCopyInstruction((3 * block_size_) - 1, VCD_SELF_MODE);
  } else {
    ExpectDataStringWithBlockSpacing("W", false);
  }
  ExpectAddressVarint(18 * block_size_);  // "ha"
  ExpectAddInstruction(1);
  ExpectDataByte('t');
  ExpectCopyInstruction((6 * block_size_) - 1, VCD_SELF_MODE);
  ExpectAddressVarint(14 * block_size_);  // " we h"
  ExpectCopyInstruction(11 * block_size_,
                        default_cache_.FirstNearMode());
  ExpectAddressVarint((9 * block_size_) + (block_size_ - 1));  // "ear is fear"
  if (!ExpectAddCopyInstruction(1, (2 * block_size_) - 1, VCD_SELF_MODE)) {
    ExpectDataByte('s');
    ExpectCopyInstruction((2 * block_size_) - 1, VCD_SELF_MODE);
  } else {
    ExpectDataByte('s');
  }
  ExpectAddressVarint(4 * block_size_);  // "o" from "The only"
  if (!ExpectAddCopyInstruction(1, block_size_, VCD_SELF_MODE)) {
    ExpectDataByte('m');
    ExpectCopyInstruction(block_size_, VCD_SELF_MODE);
  } else {
    ExpectDataByte('m');
  }
  ExpectAddressVarint(2 * block_size_);  // "e" from "The only"
  VerifySizes();
}

TEST_P(VCDiffEngineTest, EngineEncodeSampleTextWithTargetMatching) {
  if (GetParam() == COMP_MIN_64_BLOCK_32 || GetParam() == COMP_MIN_128_BLOCK_64)
    return;
  Encode(/* interleaved = */ false, /* target matching = */ true);
  // Data for ADDs
  ExpectDataStringWithBlockSpacing("W", false);
  ExpectDataByte('t');
  ExpectDataByte('s');
  ExpectDataByte('m');
  // Instructions and sizes
  if (!ExpectAddCopyInstruction(block_size_, (3 * block_size_) - 1,
                                VCD_SELF_MODE)) {
    ExpectCopyInstruction((3 * block_size_) - 1, VCD_SELF_MODE);
  }
  ExpectAddInstruction(1);
  ExpectCopyInstruction((6 * block_size_) - 1, VCD_SELF_MODE);
  ExpectCopyInstruction(11 * block_size_,
                        default_cache_.FirstNearMode());
  if (!ExpectAddCopyInstruction(1, (2 * block_size_) - 1, VCD_SELF_MODE)) {
    ExpectCopyInstruction((2 * block_size_) - 1, VCD_SELF_MODE);
  }
  if (!ExpectAddCopyInstruction(1, block_size_, VCD_SELF_MODE)) {
    ExpectCopyInstruction(block_size_, VCD_SELF_MODE);
  }
  // Addresses for COPY
  ExpectAddressVarint(18 * block_size_);  // "ha"
  ExpectAddressVarint(14 * block_size_);  // " we h"
  ExpectAddressVarint((9 * block_size_) + (block_size_ - 1));  // "ear is fear"
  ExpectAddressVarint(4 * block_size_);  // "o" from "The only"
  ExpectAddressVarint(2 * block_size_);  // "e" from "The only"
  VerifySizes();
}

TEST_P(VCDiffEngineTest, EngineEncodeSampleTextInterleavedWithTargetMatching) {
  if (GetParam() == COMP_MIN_64_BLOCK_32 || GetParam() == COMP_MIN_128_BLOCK_64)
    return;
  Encode(/* interleaved = */ true, /* target matching = */ true);
  // Interleaved section
  if (!ExpectAddCopyInstruction(block_size_, (3 * block_size_) - 1,
                                VCD_SELF_MODE)) {
    ExpectDataStringWithBlockSpacing("W", false);
    ExpectCopyInstruction((3 * block_size_) - 1, VCD_SELF_MODE);
  } else {
    ExpectDataStringWithBlockSpacing("W", false);
  }
  ExpectAddressVarint(18 * block_size_);  // "ha"
  ExpectAddInstruction(1);
  ExpectDataByte('t');
  ExpectCopyInstruction((6 * block_size_) - 1, VCD_SELF_MODE);
  ExpectAddressVarint(14 * block_size_);  // " we h"
  ExpectCopyInstruction(11 * block_size_,
                        default_cache_.FirstNearMode());
  ExpectAddressVarint((9 * block_size_) + (block_size_ - 1));  // "ear is fear"
  if (!ExpectAddCopyInstruction(1, (2 * block_size_) - 1, VCD_SELF_MODE)) {
    ExpectDataByte('s');
    ExpectCopyInstruction((2 * block_size_) - 1, VCD_SELF_MODE);
  } else {
    ExpectDataByte('s');
  }
  ExpectAddressVarint(4 * block_size_);  // "o" from "The only"
  if (!ExpectAddCopyInstruction(1, block_size_, VCD_SELF_MODE)) {
    ExpectDataByte('m');
    ExpectCopyInstruction(block_size_, VCD_SELF_MODE);
  } else {
    ExpectDataByte('m');
  }
  ExpectAddressVarint(2 * block_size_);  // "e" from "The only"
  VerifySizes();
}

INSTANTIATE_TEST_SUITE_P(AllOpts, VCDiffEngineTest,
                         ::testing::Range<size_t>(0, COMP_VALUE_MAX + 1));

// This test case takes a dictionary containing several instances of the string
// "weasel", and a target string which is identical to the dictionary
// except that all instances of "weasel" have been replaced with the string
// "moon-pie".  It tests that COPY instructions are generated for all
// boilerplate text (that is, the text between the "moon-pie" instances in
// the target) and, if target matching is enabled, that each instance of
// "moon-pie" (except the first one) is encoded using a COPY instruction
// rather than an ADD.
class WeaselsToMoonpiesTest : public VCDiffEngineTestBase {
 protected:
  // kCompressibleTestBlockSize:
  // The size of the block to create for each letter in the
  // dictionary and search string for the "compressible text" test.
  // See MakeEachLetterABlock, below.
  // If we use kCompressibleTestBlockSize = block_size_, then the
  // encoder will find one match per unique letter in the HTML text.
  // There are too many examples of "<" in the text for the encoder
  // to iterate through them all, and some matches are not found.
  // If we use kCompressibleTextBlockSize = 1, then the boilerplate
  // text between "weasel" strings in the dictionary and "moon-pie"
  // strings in the target may not be long enough to be found by
  // the encoder's block-hash algorithm.  A good value, that will give
  // reproducible results across all block sizes, will be somewhere
  // in between these extremes.
  const int kCompressibleTestBlockSize = block_size_ / 4;
  const int kTrailingSpaces = kCompressibleTestBlockSize - 1;

  WeaselsToMoonpiesTest() :
      dictionary_(MakeEachLetterABlock(dictionary_without_spaces_,
                           kCompressibleTestBlockSize,
                           false)),
      target_(MakeEachLetterABlock(target_without_spaces_,
                           kCompressibleTestBlockSize,
                           false)),
      weasel_text_(MakeEachLetterABlock(weasel_text_without_spaces_,
                           kCompressibleTestBlockSize,
                           true)),
      moonpie_text_(MakeEachLetterABlock(moonpie_text_without_spaces_,
                           kCompressibleTestBlockSize,
                           true)),
      engine_(dictionary_, strlen(dictionary_),
              CompressionOptions(GetParam())),
      match_index_(0),
      search_dictionary_(dictionary_, strlen(dictionary_)),
      copied_moonpie_address_(0) {
    EXPECT_TRUE(const_cast<VCDiffEngine*>(&engine_)->Init());
    weasel_positions_[0] = 0;
    after_weasel_[0] = 0;
    moonpie_positions_[0] = 0;
    after_moonpie_[0] = 0;
  }

  virtual ~WeaselsToMoonpiesTest() {
    delete[] dictionary_;
    delete[] target_;
    delete[] weasel_text_;
    delete[] moonpie_text_;
  }

  // text must be NULL-terminated
  void EncodeText(const char* text, bool interleaved, bool target_matching) {
    interleaved_ = interleaved;
    VCDiffCodeTableWriter coder(interleaved, false);
    coder.Init(engine_.dictionary(), engine_.dictionary_size());
    engine_.Encode(text,
                   strlen(text),
                   target_matching,
                   &diff_output_string_,
                   &coder);
  }

  void Encode(bool interleaved, bool target_matching) {
    EncodeText(target_, interleaved, target_matching);
    VerifyHeader();
  }

  void VerifyHeader() {
    VerifyHeaderForDictionaryAndTargetText(dictionary_, target_);
  }

  void ExpectCopyForSize(size_t size, int mode) {
    ExpectCopyInstruction(static_cast<int>(size), mode);
  }

  void ExpectAddForSize(size_t size) {
    ExpectAddInstruction(static_cast<int>(size));
  }

  void ExpectAddressVarintForSize(size_t value) {
    ExpectAddressVarint(static_cast<int32_t>(value));
  }

  void FindNextMoonpie(bool include_trailing_spaces) {
    ++match_index_;
    SetCurrentWeaselPosition(search_dictionary_.find(weasel_text_,
                                                     AfterLastWeasel()));
    if (CurrentWeaselPosition() == string::npos) {
      SetCurrentMoonpiePosition(string::npos);
    } else {
      SetCurrentAfterWeaselPosition(CurrentWeaselPosition()
                                        + strlen(weasel_text_)
                                        + (include_trailing_spaces ?
                                            kTrailingSpaces : 0));
      SetCurrentMoonpiePosition(AfterLastMoonpie()
                                    + CurrentBoilerplateLength());
      SetCurrentAfterMoonpiePosition(CurrentMoonpiePosition()
                                        + strlen(moonpie_text_)
                                        + (include_trailing_spaces ?
                                            kTrailingSpaces : 0));
    }
  }
  bool NoMoreMoonpies() const {
    return CurrentMoonpiePosition() == string::npos;
  }
  size_t CurrentWeaselPosition() const {
    return weasel_positions_[match_index_];
  }
  size_t LastWeaselPosition() const {
    return weasel_positions_[match_index_ - 1];
  }
  size_t CurrentMoonpiePosition() const {
    return moonpie_positions_[match_index_];
  }
  size_t LastMoonpiePosition() const {
    return moonpie_positions_[match_index_ - 1];
  }
  size_t AfterLastWeasel() const {
    CHECK_GE(match_index_, 1);
    return after_weasel_[match_index_ - 1];
  }
  size_t AfterPreviousWeasel() const {
    CHECK_GE(match_index_, 2);
    return after_weasel_[match_index_ - 2];
  }
  size_t AfterLastMoonpie() const {
    CHECK_GE(match_index_, 1);
    return after_moonpie_[match_index_ - 1];
  }
  size_t AfterPreviousMoonpie() const {
    CHECK_GE(match_index_, 2);
    return after_moonpie_[match_index_ - 2];
  }

  void SetCurrentWeaselPosition(size_t value) {
    weasel_positions_[match_index_] = value;
  }
  void SetCurrentAfterWeaselPosition(size_t value) {
    after_weasel_[match_index_] = value;
  }
  void SetCurrentMoonpiePosition(size_t value) {
    moonpie_positions_[match_index_] = value;
  }
  void SetCurrentAfterMoonpiePosition(size_t value) {
    after_moonpie_[match_index_] = value;
  }

  // Find the length of the text in between the "weasel" strings in the
  // compressible dictionary, which is the same as the text between
  // the "moon-pie" strings in the compressible target.
  size_t CurrentBoilerplateLength() const {
    CHECK_GE(match_index_, 1);
    return CurrentWeaselPosition() - AfterLastWeasel();
  }
  size_t DistanceFromLastWeasel() const {
    CHECK_GE(match_index_, 1);
    return CurrentWeaselPosition() - LastWeaselPosition();
  }
  size_t DistanceFromLastMoonpie() const {
    CHECK_GE(match_index_, 1);
    return CurrentMoonpiePosition() - LastMoonpiePosition();
  }
  size_t DistanceBetweenLastTwoWeasels() const {
    CHECK_GE(match_index_, 2);
    return AfterLastWeasel() - AfterPreviousWeasel();
  }
  size_t DistanceBetweenLastTwoMoonpies() const {
    CHECK_GE(match_index_, 2);
    return AfterLastMoonpie() - AfterPreviousMoonpie();
  }

  int32_t FindBoilerplateAddressForCopyMode(int copy_mode) const;
  int UpdateCopyModeForMoonpie(int copy_mode) const;
  int32_t FindMoonpieAddressForCopyMode(int copy_mode) const;

  void CopyBoilerplateAndAddMoonpie(int copy_mode);
  void CopyBoilerplateAndCopyMoonpie(int copy_mode, int moonpie_copy_mode);

  static const char dictionary_without_spaces_[];
  static const char target_without_spaces_[];
  static const char weasel_text_without_spaces_[];
  static const char moonpie_text_without_spaces_[];

  const char* dictionary_;
  const char* target_;
  const char* weasel_text_;
  const char* moonpie_text_;

  const VCDiffEngine engine_;
  size_t weasel_positions_[128];
  size_t after_weasel_[128];
  size_t moonpie_positions_[128];
  size_t after_moonpie_[128];
  int match_index_;
  string search_dictionary_;
  size_t copied_moonpie_address_;
};

// Care is taken in the formulation of the dictionary
// to ensure that the surrounding letters do not match; for example,
// there are not two instances of the string "weasels".  Otherwise,
// the matching behavior would not be as predictable.
const char WeaselsToMoonpiesTest::dictionary_without_spaces_[] =
  "<html>\n"
  "<head>\n"
  "<meta content=\"text/html; charset=ISO-8859-1\"\n"
  "http-equiv=\"content-type\">\n"
  "<title>All about weasels</title>\n"
  "</head>\n"
  "<!-- You will notice that the word \"weasel\" may be replaced"
  " with something else -->\n"
  "<body>\n"
  "<h1>All about the weasel: highly compressible HTML text</h1>"
  "<ul>\n"
  "<li>Don\'t look a gift weasel in its mouth.</li>\n"
  "<li>This item makes sure the next occurrence is found.</li>\n"
  "<li>Don\'t count your weasel, before it\'s hatched.</li>\n"
  "</ul>\n"
  "<br>\n"
  "</body>\n"
  "</html>\n";

const char WeaselsToMoonpiesTest::target_without_spaces_[] =
  "<html>\n"
  "<head>\n"
  "<meta content=\"text/html; charset=ISO-8859-1\"\n"
  "http-equiv=\"content-type\">\n"
  "<title>All about moon-pies</title>\n"
  "</head>\n"
  "<!-- You will notice that the word \"moon-pie\" may be replaced"
  " with something else -->\n"
  "<body>\n"
  "<h1>All about the moon-pie: highly compressible HTML text</h1>"
  "<ul>\n"
  "<li>Don\'t look a gift moon-pie in its mouth.</li>\n"
  "<li>This item makes sure the next occurrence is found.</li>\n"
  "<li>Don\'t count your moon-pie, before it\'s hatched.</li>\n"
  "</ul>\n"
  "<br>\n"
  "</body>\n"
  "</html>\n";

const char WeaselsToMoonpiesTest::weasel_text_without_spaces_[] = "weasel";
const char WeaselsToMoonpiesTest::moonpie_text_without_spaces_[] = "moon-pie";

int32_t WeaselsToMoonpiesTest::FindBoilerplateAddressForCopyMode(
    int copy_mode) const {
  size_t copy_address = 0;
  if (default_cache_.IsSelfMode(copy_mode)) {
    copy_address = AfterLastWeasel();
  } else if (default_cache_.IsNearMode(copy_mode)) {
    copy_address = DistanceBetweenLastTwoWeasels();
  } else if (default_cache_.IsSameMode(copy_mode)) {
    copy_address = AfterLastWeasel() % 256;
  }
  return static_cast<int32_t>(copy_address);
}

int WeaselsToMoonpiesTest::UpdateCopyModeForMoonpie(int copy_mode) const {
  if (copy_mode == default_cache_.FirstSameMode()) {
    return default_cache_.FirstSameMode()
        + static_cast<int>((copied_moonpie_address_ / 256) % 3);
  } else {
    return copy_mode;
  }
}

int32_t WeaselsToMoonpiesTest::FindMoonpieAddressForCopyMode(
    int copy_mode) const {
  size_t copy_address = 0;
  if (default_cache_.IsHereMode(copy_mode)) {
    copy_address = DistanceFromLastMoonpie();
  } else if (default_cache_.IsNearMode(copy_mode)) {
    copy_address = DistanceBetweenLastTwoMoonpies() - kTrailingSpaces;
  } else if (default_cache_.IsSameMode(copy_mode)) {
    copy_address = copied_moonpie_address_ % 256;
  }
  return static_cast<int32_t>(copy_address);
}

// Expect one dictionary instance of "weasel" to be replaced with "moon-pie"
// in the encoding.
void WeaselsToMoonpiesTest::CopyBoilerplateAndAddMoonpie(int copy_mode) {
  EXPECT_FALSE(NoMoreMoonpies());
  ExpectCopyForSize(CurrentBoilerplateLength(), copy_mode);
  ExpectAddress(FindBoilerplateAddressForCopyMode(copy_mode), copy_mode);
  ExpectAddInstructionForStringLength(moonpie_text_);
  ExpectDataString(moonpie_text_);
}

// Expect one dictionary instance of "weasel" to be replaced with "moon-pie"
// in the encoding.  The "moon-pie" text will be copied from the previously
// encoded target.
void WeaselsToMoonpiesTest::CopyBoilerplateAndCopyMoonpie(
    int copy_mode,
    int moonpie_copy_mode) {
  EXPECT_FALSE(NoMoreMoonpies());
  ExpectCopyForSize(CurrentBoilerplateLength(), copy_mode);
  ExpectAddress(FindBoilerplateAddressForCopyMode(copy_mode), copy_mode);
  moonpie_copy_mode = UpdateCopyModeForMoonpie(moonpie_copy_mode);
  ExpectCopyForSize(strlen(moonpie_text_) + kTrailingSpaces, moonpie_copy_mode);
  ExpectAddress(FindMoonpieAddressForCopyMode(moonpie_copy_mode),
                moonpie_copy_mode);
  copied_moonpie_address_ = strlen(dictionary_) + LastMoonpiePosition();
}

TEST_P(WeaselsToMoonpiesTest, EngineEncodeCompressibleNoTargetMatching) {
  Encode(/* interleaved = */ true, /* target matching = */ false);
  FindNextMoonpie(false);
  // Expect all five "weasel"s to be replaced with "moon-pie"s
  CopyBoilerplateAndAddMoonpie(default_cache_.FirstSameMode());
  FindNextMoonpie(false);
  CopyBoilerplateAndAddMoonpie(VCD_SELF_MODE);
  FindNextMoonpie(false);
  CopyBoilerplateAndAddMoonpie(default_cache_.FirstNearMode() + 1);
  FindNextMoonpie(false);
  CopyBoilerplateAndAddMoonpie(default_cache_.FirstNearMode() + 2);
  FindNextMoonpie(false);
  CopyBoilerplateAndAddMoonpie(default_cache_.FirstNearMode() + 3);
  FindNextMoonpie(false);
  EXPECT_TRUE(NoMoreMoonpies());
  ExpectCopyForSize(strlen(dictionary_) - AfterLastWeasel(),
                    default_cache_.FirstNearMode());
  ExpectAddressVarintForSize(DistanceBetweenLastTwoWeasels());
  VerifySizes();
}

TEST_P(WeaselsToMoonpiesTest, EngineEncodeCompressibleWithTargetMatching) {
  if (GetParam() != COMP_DEFAULT)
    return;
  Encode(/* interleaved = */ true, /* target matching = */ true);
  // Expect all five "weasel"s to be replaced with "moon-pie"s.
  // Every "moon-pie" after the first one should be copied from the
  // previously encoded target text.
  FindNextMoonpie(false);
  CopyBoilerplateAndAddMoonpie(default_cache_.FirstSameMode());
  FindNextMoonpie(true);
  CopyBoilerplateAndCopyMoonpie(VCD_SELF_MODE, VCD_HERE_MODE);
  FindNextMoonpie(true);
  CopyBoilerplateAndCopyMoonpie(default_cache_.FirstNearMode() + 1,
                                default_cache_.FirstSameMode());
  FindNextMoonpie(true);
  CopyBoilerplateAndCopyMoonpie(default_cache_.FirstNearMode() + 3,
                                VCD_HERE_MODE);
  FindNextMoonpie(true);
  CopyBoilerplateAndCopyMoonpie(default_cache_.FirstNearMode() + 1,
                                default_cache_.FirstSameMode());
  FindNextMoonpie(true);
  EXPECT_TRUE(NoMoreMoonpies());
  ExpectCopyForSize(strlen(dictionary_) - AfterLastWeasel(),
                    default_cache_.FirstNearMode() + 3);
  ExpectAddressVarintForSize(DistanceBetweenLastTwoWeasels());
  VerifySizes();
}

INSTANTIATE_TEST_SUITE_P(AllOpts, WeaselsToMoonpiesTest,
                         ::testing::Range<size_t>(0, COMP_VALUE_MAX + 1));

}  //  anonymous namespace
}  //  namespace open-vcdiff
