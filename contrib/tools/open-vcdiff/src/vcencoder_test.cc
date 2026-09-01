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
#include "google/vcencoder.h"
#include <stdlib.h>  // free, posix_memalign
#include <string.h>  // memcpy
#include <algorithm>
#include <string>
#include <vector>
#include "blockhash.h"
#include "checksum.h"
#include "testing.h"
#include "varint_bigendian.h"
#include "google/vcdecoder.h"
#include "google/jsonwriter.h"
#include "vcdiff_defs.h"
#include "vcdiffengine.h"

#ifdef HAVE_EXT_ROPE
#error #include <ext/rope>
#include "output_string_crope.h"
using __gnu_cxx::crope;
#endif  // HAVE_EXT_ROPE

#ifdef HAVE_MALLOC_H
#include <malloc.h>
#endif  // HAVE_MALLOC_H

#ifdef HAVE_SYS_MMAN_H
#if !defined(_XOPEN_SOURCE) || _XOPEN_SOURCE < 600
#undef  _XOPEN_SOURCE
#define _XOPEN_SOURCE 600  // posix_memalign
#endif
#include <sys/mman.h>  // mprotect
#endif  // HAVE_SYS_MMAN_H

#ifdef HAVE_UNISTD_H
#include <unistd.h>  // getpagesize
#endif  // HAVE_UNISTD_H

namespace open_vcdiff {
namespace {

static const size_t kFileHeaderSize = sizeof(DeltaFileHeader);

// This is to check the maximum possible encoding size
// if using a single ADD instruction, so assume that the
// dictionary size, the length of the ADD data, the size
// of the target window, and the length of the delta window
// are all two-byte Varints, that is, 128 <= length < 4096.
// This figure includes three extra bytes for a zero-sized
// ADD instruction with a two-byte Varint explicit size.
// Any additional COPY & ADD instructions must reduce
// the length of the encoding from this maximum.
static const size_t kWindowHeaderSize = 21;

class VerifyEncodedBytesTest : public testing::TestWithParam<size_t> {
 public:
  typedef std::string string;

  VerifyEncodedBytesTest()
      : delta_index_(0),
        block_size_(VCDiffEngine(NULL, 0, CompressionOptions(GetParam())).
            min_match_size()) { }
  virtual ~VerifyEncodedBytesTest() { }

  void ExpectByte(unsigned char b) {
    EXPECT_EQ(b, static_cast<unsigned char>(delta_[delta_index_]));
    ++delta_index_;
  }

  void ExpectString(const char* s) {
    const size_t size = strlen(s);  // don't include terminating NULL char
    EXPECT_EQ(s, string(delta_data() + delta_index_, size));
    delta_index_ += size;
  }

  void ExpectNoMoreBytes() {
    EXPECT_EQ(delta_index_, delta_size());
  }

  void ExpectSize(size_t size) {
    const char* delta_size_pos = &delta_[delta_index_];
    EXPECT_EQ(size,
              static_cast<size_t>(
                  VarintBE<int32_t>::Parse(delta_data() + delta_size(),
                                           &delta_size_pos)));
    delta_index_ = delta_size_pos - delta_data();
  }

  void ExpectChecksum(VCDChecksum checksum) {
    const char* delta_checksum_pos = &delta_[delta_index_];
    EXPECT_EQ(checksum,
              static_cast<VCDChecksum>(
                  VarintBE<int64_t>::Parse(delta_data() + delta_size(),
                                           &delta_checksum_pos)));
    delta_index_ = delta_checksum_pos - delta_data();
  }

  const string& delta_as_const() const { return delta_; }
  string* delta() { return &delta_; }

  const char* delta_data() const { return delta_as_const().data(); }
  size_t delta_size() const { return delta_as_const().size(); }

 private:
  string delta_;
  size_t delta_index_;

 protected:
  size_t block_size_;
};

class VCDiffEncoderTest : public VerifyEncodedBytesTest {
 protected:
  static const char kDictionary[];
  static const char kTarget[];
  static const char kJSONDiff[];
  static const uint8_t kNonAscii[];

  VCDiffEncoderTest();
  virtual ~VCDiffEncoderTest() { }

  void TestWithFixedChunkSize(VCDiffStreamingEncoder *encoder,
                              VCDiffStreamingDecoder *decoder,
                              size_t chunk_size,
                              bool sdch3dm = false);
  void TestWithEncodedChunkVector(size_t chunk_size);

  HashedDictionary hashed_dictionary_;
  VCDiffStreamingEncoder encoder_;
  VCDiffStreamingDecoder decoder_;
  VCDiffEncoder simple_encoder_;
  VCDiffDecoder simple_decoder_;
  VCDiffStreamingEncoder json_encoder_;
  VCDiffStreamingEncoder external_encoder_;
  VCDiffEncoder nonascii_simple_encoder_;

  string result_target_;
  string result_sdch3_dict_;
};

const char VCDiffEncoderTest::kDictionary[] =
    "\"Just the place for a Snark!\" the Bellman cried,\n"
    "As he landed his crew with care;\n"
    "Supporting each man on the top of the tide\n"
    "By a finger entwined in his hair.\n";

const char VCDiffEncoderTest::kTarget[] =
    "\"Just the place for a Snark! I have said it twice:\n"
    "That alone should encourage the crew.\n"
    "Just the place for a Snark! I have said it thrice:\n"
    "What I tell you three times is true.\"\n";

const char VCDiffEncoderTest::kJSONDiff[] =
    "[\"\\\"Just the place for a Snark! I have said it twice:\\n"
    "That alone should encourage the crew.\\n\","
    "161,44,"
    "\"hrice:\\nWhat I tell you three times is true.\\\"\\n\"]";

// NonASCII string "foo\x128".
const uint8_t VCDiffEncoderTest::kNonAscii[] = {102, 111, 111, 128, 0};

VCDiffEncoderTest::VCDiffEncoderTest()
    : hashed_dictionary_(kDictionary, sizeof(kDictionary),
                         CompressionOptions(GetParam())),
      encoder_(&hashed_dictionary_,
               VCD_FORMAT_INTERLEAVED | VCD_FORMAT_CHECKSUM,
               /* look_for_target_matches = */ true),
      simple_encoder_(kDictionary, sizeof(kDictionary),
                      CompressionOptions(GetParam())),
      json_encoder_(&hashed_dictionary_, VCD_FORMAT_JSON,
                    /* look_for_target_matches = */ true),
      external_encoder_(&hashed_dictionary_, 0,
                        /* look_for_target_matches = */ true,
                        new JSONCodeTableWriter()),
      nonascii_simple_encoder_(reinterpret_cast<const char *>(kNonAscii),
                               sizeof(kNonAscii),
                               CompressionOptions(GetParam())) {
  EXPECT_TRUE(hashed_dictionary_.Init());
}

TEST_P(VCDiffEncoderTest, EncodeBeforeStartEncoding) {
  EXPECT_FALSE(encoder_.EncodeChunk(kTarget, strlen(kTarget), delta()));
}

TEST_P(VCDiffEncoderTest, FinishBeforeStartEncoding) {
  EXPECT_FALSE(encoder_.FinishEncoding(delta()));
}

TEST_P(VCDiffEncoderTest, EncodeDecodeNothing) {
  HashedDictionary nothing_dictionary("", 0);
  EXPECT_TRUE(nothing_dictionary.Init());
  VCDiffStreamingEncoder nothing_encoder(&nothing_dictionary,
                                         VCD_STANDARD_FORMAT,
                                         false);
  EXPECT_TRUE(nothing_encoder.StartEncoding(delta()));
  EXPECT_TRUE(nothing_encoder.FinishEncoding(delta()));
  decoder_.StartDecoding("", 0);
  EXPECT_TRUE(decoder_.DecodeChunk(delta_data(),
                                   delta_size(),
                                   &result_target_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_TRUE(result_target_.empty());
}

TEST_P(VCDiffEncoderTest, EncodeNothingJSON) {
  HashedDictionary nothing_dictionary("", 0);
  EXPECT_TRUE(nothing_dictionary.Init());
  VCDiffStreamingEncoder nothing_encoder(&nothing_dictionary,
                                         VCD_FORMAT_JSON,
                                         false);
  EXPECT_TRUE(nothing_encoder.StartEncoding(delta()));
  EXPECT_TRUE(nothing_encoder.FinishEncoding(delta()));
  EXPECT_EQ("", delta_as_const());
}

// A NULL dictionary pointer is legal as long as the dictionary size is 0.
TEST_P(VCDiffEncoderTest, EncodeDecodeNullDictionaryPtr) {
  HashedDictionary null_dictionary(NULL, 0);
  EXPECT_TRUE(null_dictionary.Init());
  VCDiffStreamingEncoder null_encoder(&null_dictionary,
                                      VCD_STANDARD_FORMAT,
                                      false);
  EXPECT_TRUE(null_encoder.StartEncoding(delta()));
  EXPECT_TRUE(null_encoder.EncodeChunk(kTarget, strlen(kTarget), delta()));
  EXPECT_TRUE(null_encoder.FinishEncoding(delta()));
  EXPECT_GE(strlen(kTarget) + kFileHeaderSize + kWindowHeaderSize,
            delta_size());
  decoder_.StartDecoding(NULL, 0);
  EXPECT_TRUE(decoder_.DecodeChunk(delta_data(),
                                   delta_size(),
                                   &result_target_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(kTarget, result_target_);
}

TEST_P(VCDiffEncoderTest, EncodeDecodeSimple) {
  EXPECT_TRUE(simple_encoder_.Encode(kTarget, strlen(kTarget), delta()));
  EXPECT_GE(strlen(kTarget) + kFileHeaderSize + kWindowHeaderSize,
            delta_size());
  EXPECT_TRUE(simple_decoder_.Decode(kDictionary,
                                     sizeof(kDictionary),
                                     delta_as_const(),
                                     &result_target_));
  EXPECT_EQ(kTarget, result_target_);
}

TEST_P(VCDiffEncoderTest, EncodeDecodeInterleaved) {
  simple_encoder_.SetFormatFlags(VCD_FORMAT_INTERLEAVED);
  EXPECT_TRUE(simple_encoder_.Encode(kTarget, strlen(kTarget), delta()));
  EXPECT_GE(strlen(kTarget) + kFileHeaderSize + kWindowHeaderSize,
            delta_size());
  EXPECT_TRUE(simple_decoder_.Decode(kDictionary,
                                     sizeof(kDictionary),
                                     delta_as_const(),
                                     &result_target_));
  EXPECT_EQ(kTarget, result_target_);
}

TEST_P(VCDiffEncoderTest, EncodeDecodeInterleavedChecksum) {
  simple_encoder_.SetFormatFlags(VCD_FORMAT_INTERLEAVED | VCD_FORMAT_CHECKSUM);
  EXPECT_TRUE(simple_encoder_.Encode(kTarget,
                                     strlen(kTarget),
                                     delta()));
  EXPECT_GE(strlen(kTarget) + kFileHeaderSize + kWindowHeaderSize,
            delta_size());
  EXPECT_TRUE(simple_decoder_.Decode(kDictionary,
                                     sizeof(kDictionary),
                                     delta_as_const(),
                                     &result_target_));
  EXPECT_EQ(kTarget, result_target_);
}

TEST_P(VCDiffEncoderTest, EncodeDecodeSingleChunk) {
  EXPECT_TRUE(encoder_.StartEncoding(delta()));
  EXPECT_TRUE(encoder_.EncodeChunk(kTarget, strlen(kTarget), delta()));
  EXPECT_TRUE(encoder_.FinishEncoding(delta()));
  EXPECT_GE(strlen(kTarget) + kFileHeaderSize + kWindowHeaderSize,
            delta_size());
  decoder_.StartDecoding(kDictionary, sizeof(kDictionary));
  EXPECT_TRUE(decoder_.DecodeChunk(delta_data(),
                                   delta_size(),
                                   &result_target_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(kTarget, result_target_);
}

TEST_P(VCDiffEncoderTest, EncodeSimpleJSON) {
  if (GetParam() != COMP_DEFAULT)
    return;
  EXPECT_TRUE(json_encoder_.StartEncoding(delta()));
  EXPECT_TRUE(json_encoder_.EncodeChunk(kTarget, strlen(kTarget), delta()));
  EXPECT_TRUE(json_encoder_.FinishEncoding(delta()));
  EXPECT_EQ(kJSONDiff, delta_as_const());
}

TEST_P(VCDiffEncoderTest, EncodeSimpleExternalJSON) {
  if (GetParam() != COMP_DEFAULT)
    return;
  EXPECT_TRUE(external_encoder_.StartEncoding(delta()));
  EXPECT_TRUE(external_encoder_.EncodeChunk(kTarget, strlen(kTarget), delta()));
  EXPECT_TRUE(external_encoder_.FinishEncoding(delta()));
  EXPECT_EQ(kJSONDiff, delta_as_const());
}

TEST_P(VCDiffEncoderTest, EncodeDecodeSeparate) {
  string delta_start, delta_encode, delta_finish;
  EXPECT_TRUE(encoder_.StartEncoding(&delta_start));
  EXPECT_TRUE(encoder_.EncodeChunk(kTarget, strlen(kTarget), &delta_encode));
  EXPECT_TRUE(encoder_.FinishEncoding(&delta_finish));
  EXPECT_GE(strlen(kTarget) + kFileHeaderSize + kWindowHeaderSize,
            delta_start.size() + delta_encode.size() + delta_finish.size());
  decoder_.StartDecoding(kDictionary, sizeof(kDictionary));
  EXPECT_TRUE(decoder_.DecodeChunk(delta_start.data(),
                                   delta_start.size(),
                                   &result_target_));
  EXPECT_TRUE(decoder_.DecodeChunk(delta_encode.data(),
                                   delta_encode.size(),
                                   &result_target_));
  EXPECT_TRUE(decoder_.DecodeChunk(delta_finish.data(),
                                   delta_finish.size(),
                                   &result_target_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(kTarget, result_target_);
}

TEST_P(VCDiffEncoderTest, NonasciiDictionary) {
  EXPECT_TRUE(nonascii_simple_encoder_.Encode(kTarget,
                                              strlen(kTarget),
                                              delta()));
}

TEST_P(VCDiffEncoderTest, NonasciiDictionaryWithJSON) {
  nonascii_simple_encoder_.SetFormatFlags(VCD_FORMAT_JSON);
  EXPECT_FALSE(nonascii_simple_encoder_.Encode(kTarget,
                                               strlen(kTarget),
                                               delta()));
}

TEST_P(VCDiffEncoderTest, NonasciiTarget) {
  EXPECT_TRUE(simple_encoder_.Encode(reinterpret_cast<const char *>(kNonAscii),
                                     sizeof(kNonAscii), delta()));
}

TEST_P(VCDiffEncoderTest, NonasciiTargetWithJSON) {
  simple_encoder_.SetFormatFlags(VCD_FORMAT_JSON);
  EXPECT_FALSE(simple_encoder_.Encode(reinterpret_cast<const char *>(kNonAscii),
                                      sizeof(kNonAscii), delta()));
}

#ifdef HAVE_EXT_ROPE
// Test that the crope class can be used in place of a string for encoding
// and decoding.
TEST_P(VCDiffEncoderTest, EncodeDecodeCrope) {
  crope delta_crope, result_crope;
  EXPECT_TRUE(encoder_.StartEncoding(&delta_crope));
  EXPECT_TRUE(encoder_.EncodeChunk(kTarget, strlen(kTarget), &delta_crope));
  EXPECT_TRUE(encoder_.FinishEncoding(&delta_crope));
  EXPECT_GE(strlen(kTarget) + kFileHeaderSize + kWindowHeaderSize,
            delta_crope.size());
  decoder_.StartDecoding(kDictionary, sizeof(kDictionary));
  // crope can't guarantee that its characters are contiguous, so the decoding
  // has to be done byte-by-byte.
  for (crope::const_iterator it = delta_crope.begin();
       it != delta_crope.end(); it++) {
    const char this_char = *it;
    EXPECT_TRUE(decoder_.DecodeChunk(&this_char, 1, &result_crope));
  }
  EXPECT_TRUE(decoder_.FinishDecoding());
  crope expected_target(kTarget);
  EXPECT_EQ(expected_target, result_crope);
}
#endif  // HAVE_EXT_ROPE

// Test the encoding and decoding with a fixed chunk size.
// If decoder is null, only test the encoding.
void VCDiffEncoderTest::TestWithFixedChunkSize(VCDiffStreamingEncoder *encoder,
                                               VCDiffStreamingDecoder *decoder,
                                               size_t chunk_size,
                                               bool sdch3dm) {
  delta()->clear();
  EXPECT_TRUE(encoder->StartEncoding(delta()));
  for (size_t chunk_start_index = 0;
       chunk_start_index < strlen(kTarget);
       chunk_start_index += chunk_size) {
    size_t this_chunk_size = chunk_size;
    const size_t bytes_available = strlen(kTarget) - chunk_start_index;
    if (this_chunk_size > bytes_available) {
      this_chunk_size = bytes_available;
    }
    EXPECT_TRUE(encoder->EncodeChunk(&kTarget[chunk_start_index],
                                     this_chunk_size,
                                     delta()));
  }
  EXPECT_TRUE(encoder->FinishEncoding(delta()));
  if (!sdch3dm) {
    const size_t num_windows = (strlen(kTarget) / chunk_size) + 1;
    const size_t size_of_windows =
        strlen(kTarget) + (kWindowHeaderSize * num_windows);
    EXPECT_GE(kFileHeaderSize + size_of_windows, delta_size());
  }
  result_target_.clear();

  if (!decoder) return;

  if (sdch3dm) {
    decoder->StartSdch3DmDecoding(&result_sdch3_dict_);
  } else {
    decoder->StartDecoding(kDictionary, sizeof(kDictionary));
  }
  for (size_t chunk_start_index = 0;
       chunk_start_index < delta_size();
       chunk_start_index += chunk_size) {
    size_t this_chunk_size = chunk_size;
    const size_t bytes_available = delta_size() - chunk_start_index;
    if (this_chunk_size > bytes_available) {
      this_chunk_size = bytes_available;
    }
    EXPECT_TRUE(decoder->DecodeChunk(delta_data() + chunk_start_index,
                                     this_chunk_size,
                                     &result_target_));
  }
  EXPECT_TRUE(decoder->FinishDecoding());
  EXPECT_EQ(kTarget, result_target_);
  if (sdch3dm) {
    // Tests use sizeof(kDictionary) everywhere so there is actually
    // a nul character included in the dictionary
    EXPECT_EQ(std::string_view(kDictionary, sizeof(kDictionary)),
      result_sdch3_dict_);
  }
}

TEST_P(VCDiffEncoderTest, EncodeDecodeFixedChunkSizes) {
  // These specific chunk sizes have failed in the past
  TestWithFixedChunkSize(&encoder_, &decoder_, 6);
  TestWithFixedChunkSize(&encoder_, &decoder_, 45);
  TestWithFixedChunkSize(&encoder_, &decoder_, 60);

  // Now loop through all possible chunk sizes
  for (size_t chunk_size = 1; chunk_size < strlen(kTarget); ++chunk_size) {
    TestWithFixedChunkSize(&encoder_, &decoder_, chunk_size);
  }
}

TEST_P(VCDiffEncoderTest, EncodeDecodeSdch3FixedChunkSizes) {
  VCDiffStreamingEncoder encoder(&hashed_dictionary_,
      VCD_FORMAT_INTERLEAVED | VCD_FORMAT_CHECKSUM | VCD_FORMAT_SDCH3DM,
      /* look_for_target_matches = */ true);
  // Now loop through all possible chunk sizes
  for (size_t chunk_size = 1; chunk_size < strlen(kTarget); ++chunk_size) {
    TestWithFixedChunkSize(&encoder, &decoder_, chunk_size, /* sdch3 */ true);
  }
}

TEST_P(VCDiffEncoderTest, EncodeFixedChunkSizesJSON) {
  if (GetParam() != COMP_DEFAULT)
    return;
  // There is no JSON decoder; these diffs are created by hand.
  TestWithFixedChunkSize(&json_encoder_, NULL, 6);
  EXPECT_EQ("[\"\\\"Just \",\"the pl\",\"ace fo\",\"r a Sn\",\"ark! I\","
            "\" have \",\"said i\",\"t twic\",\"e:\\nTha\",\"t alon\","
            "\"e shou\",\"ld enc\",\"ourage\",\" the c\",\"rew.\\nJ\","
            "\"ust th\",\"e plac\",\"e for \",\"a Snar\",\"k! I h\","
            "\"ave sa\",\"id it \",\"thrice\",\":\\nWhat\",\" I tel\","
            "\"l you \",\"three \",\"times \",\"is tru\",\"e.\\\"\\n\"]",
            delta_as_const());
  TestWithFixedChunkSize(&json_encoder_, NULL, 45);
  EXPECT_EQ("[\"\\\"Just the place for a Snark! I have said it t\","
            "\"wice:\\nThat alone should encourage the crew.\\nJ\","
            "\"ust the place for a Snark! I have said it thr\",\"ice:\\n"
            "What I tell you three times is true.\\\"\\n\"]",
            delta_as_const());
  TestWithFixedChunkSize(&json_encoder_, NULL, 60);
  EXPECT_EQ("[\"\\\"Just the place for a Snark! I have said it twice:\\n"
            "That alon\",\"e should encourage the crew.\\n"
            "Just the place for a Snark! I h\",\"ave said it thrice:\\n"
            "What I tell you three times is true.\\\"\\n\"]",
            delta_as_const());
}


// If --allow_vcd_target=false is specified, the decoder will throw away some of
// the internally-stored decoded target beyond the current window.  Try
// different numbers of encoded window sizes to make sure that this behavior
// does not affect the results.
TEST_P(VCDiffEncoderTest, EncodeDecodeFixedChunkSizesNoVcdTarget) {
  decoder_.SetAllowVcdTarget(false);
  // Loop through all possible chunk sizes
  for (size_t chunk_size = 1; chunk_size < strlen(kTarget); ++chunk_size) {
    TestWithFixedChunkSize(&encoder_, &decoder_, chunk_size);
  }
}

// Splits the text to be encoded into fixed-size chunks.  Encodes each
// chunk and puts it into a vector of strings.  Then decodes each string
// in the vector and appends the result into result_target_.
void VCDiffEncoderTest::TestWithEncodedChunkVector(size_t chunk_size) {
  std::vector<string> encoded_chunks;
  string this_encoded_chunk;
  size_t total_chunk_size = 0;
  EXPECT_TRUE(encoder_.StartEncoding(&this_encoded_chunk));
  encoded_chunks.push_back(this_encoded_chunk);
  total_chunk_size += this_encoded_chunk.size();
  for (size_t chunk_start_index = 0;
       chunk_start_index < strlen(kTarget);
       chunk_start_index += chunk_size) {
    size_t this_chunk_size = chunk_size;
    const size_t bytes_available = strlen(kTarget) - chunk_start_index;
    if (this_chunk_size > bytes_available) {
      this_chunk_size = bytes_available;
    }
    this_encoded_chunk.clear();
    EXPECT_TRUE(encoder_.EncodeChunk(&kTarget[chunk_start_index],
                                     this_chunk_size,
                                     &this_encoded_chunk));
    encoded_chunks.push_back(this_encoded_chunk);
    total_chunk_size += this_encoded_chunk.size();
  }
  this_encoded_chunk.clear();
  EXPECT_TRUE(encoder_.FinishEncoding(&this_encoded_chunk));
  encoded_chunks.push_back(this_encoded_chunk);
  total_chunk_size += this_encoded_chunk.size();
  const size_t num_windows = (strlen(kTarget) / chunk_size) + 1;
  const size_t size_of_windows =
      strlen(kTarget) + (kWindowHeaderSize * num_windows);
  EXPECT_GE(kFileHeaderSize + size_of_windows, total_chunk_size);
  result_target_.clear();
  decoder_.StartDecoding(kDictionary, sizeof(kDictionary));
  for (std::vector<string>::iterator it = encoded_chunks.begin();
       it != encoded_chunks.end(); ++it) {
    EXPECT_TRUE(decoder_.DecodeChunk(it->data(), it->size(), &result_target_));
  }
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(kTarget, result_target_);
}

TEST_P(VCDiffEncoderTest, EncodeDecodeStreamOfChunks) {
  // Loop through all possible chunk sizes
  for (size_t chunk_size = 1; chunk_size < strlen(kTarget); ++chunk_size) {
    TestWithEncodedChunkVector(chunk_size);
  }
}

// Verify that HashedDictionary stores a copy of the dictionary text,
// rather than just storing a pointer to it.  If the dictionary buffer
// is overwritten after creating a HashedDictionary from it, it shouldn't
// affect an encoder that uses that HashedDictionary.
TEST_P(VCDiffEncoderTest, DictionaryBufferOverwritten) {
  string dictionary_copy(kDictionary, sizeof(kDictionary));
  HashedDictionary hd_copy(dictionary_copy.data(), dictionary_copy.size());
  EXPECT_TRUE(hd_copy.Init());
  VCDiffStreamingEncoder copy_encoder(&hd_copy,
                                      VCD_FORMAT_INTERLEAVED
                                          | VCD_FORMAT_CHECKSUM,
                                      /* look_for_target_matches = */ true);
  // Produce a reference version of the encoded text.
  string delta_before;
  EXPECT_TRUE(copy_encoder.StartEncoding(&delta_before));
  EXPECT_TRUE(copy_encoder.EncodeChunk(kTarget,
                                       strlen(kTarget),
                                       &delta_before));
  EXPECT_TRUE(copy_encoder.FinishEncoding(&delta_before));
  EXPECT_GE(strlen(kTarget) + kFileHeaderSize + kWindowHeaderSize,
            delta_before.size());

  // Overwrite the dictionary text with all 'Q' characters.
  dictionary_copy.replace(0,
                          dictionary_copy.size(),
                          dictionary_copy.size(),
                          'Q');
  // When the encoder is used on the same target text after overwriting
  // the dictionary, it should produce the same encoded output.
  string delta_after;
  EXPECT_TRUE(copy_encoder.StartEncoding(&delta_after));
  EXPECT_TRUE(copy_encoder.EncodeChunk(kTarget, strlen(kTarget), &delta_after));
  EXPECT_TRUE(copy_encoder.FinishEncoding(&delta_after));
  EXPECT_EQ(delta_before, delta_after);
}

// Binary data test part 1: The dictionary and target data should not
// be treated as NULL-terminated.  An embedded NULL should be handled like
// any other byte of data.
TEST_P(VCDiffEncoderTest, DictionaryHasEmbeddedNULLs) {
  const uint8_t embedded_null_dictionary_text[] =
      { 0x00, 0xFF, 0xFE, 0xFD, 0x00, 0xFD, 0xFE, 0xFF, 0x00, 0x03 };
  const uint8_t embedded_null_target[] =
      { 0xFD, 0x00, 0xFD, 0xFE, 0x03, 0x00, 0x01, 0x00 };
  CHECK_EQ(10, sizeof(embedded_null_dictionary_text));
  CHECK_EQ(8, sizeof(embedded_null_target));
  HashedDictionary embedded_null_dictionary(
      reinterpret_cast<const char *>(embedded_null_dictionary_text),
      sizeof(embedded_null_dictionary_text));
  EXPECT_TRUE(embedded_null_dictionary.Init());
  VCDiffStreamingEncoder embedded_null_encoder(&embedded_null_dictionary,
      VCD_FORMAT_INTERLEAVED | VCD_FORMAT_CHECKSUM,
      /* look_for_target_matches = */ true);
  EXPECT_TRUE(embedded_null_encoder.StartEncoding(delta()));
  EXPECT_TRUE(embedded_null_encoder.EncodeChunk(
      reinterpret_cast<const char *>(embedded_null_target),
      sizeof(embedded_null_target), delta()));
  EXPECT_TRUE(embedded_null_encoder.FinishEncoding(delta()));
  decoder_.StartDecoding(
      reinterpret_cast<const char *>(embedded_null_dictionary_text),
      sizeof(embedded_null_dictionary_text));
  EXPECT_TRUE(decoder_.DecodeChunk(delta_data(),
                                   delta_size(),
                                   &result_target_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(sizeof(embedded_null_target), result_target_.size());
  EXPECT_EQ(string(reinterpret_cast<const char*>(embedded_null_target),
                   sizeof(embedded_null_target)),
            result_target_);
}

// Binary data test part 2: An embedded CR or LF should be handled like
// any other byte of data.  No text-processing of the data should occur.
TEST_P(VCDiffEncoderTest, DictionaryHasEmbeddedNewlines) {
  const uint8_t embedded_null_dictionary_text[] =
      { 0x0C, 0xFF, 0xFE, 0x0C, 0x00, 0x0A, 0xFE, 0xFF, 0x00, 0x0A };
  const uint8_t embedded_null_target[] =
      { 0x0C, 0x00, 0x0A, 0xFE, 0x03, 0x00, 0x0A, 0x00 };
  CHECK_EQ(10, sizeof(embedded_null_dictionary_text));
  CHECK_EQ(8, sizeof(embedded_null_target));
  HashedDictionary embedded_null_dictionary(
      reinterpret_cast<const char *>(embedded_null_dictionary_text),
      sizeof(embedded_null_dictionary_text));
  EXPECT_TRUE(embedded_null_dictionary.Init());
  VCDiffStreamingEncoder embedded_null_encoder(&embedded_null_dictionary,
      VCD_FORMAT_INTERLEAVED | VCD_FORMAT_CHECKSUM,
      /* look_for_target_matches = */ true);
  EXPECT_TRUE(embedded_null_encoder.StartEncoding(delta()));
  EXPECT_TRUE(embedded_null_encoder.EncodeChunk(
      reinterpret_cast<const char *>(embedded_null_target),
      sizeof(embedded_null_target), delta()));
  EXPECT_TRUE(embedded_null_encoder.FinishEncoding(delta()));
  decoder_.StartDecoding(
      reinterpret_cast<const char *>(embedded_null_dictionary_text),
      sizeof(embedded_null_dictionary_text));
  EXPECT_TRUE(
      decoder_.DecodeChunk(delta_data(), delta_size(), &result_target_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(sizeof(embedded_null_target), result_target_.size());
  EXPECT_EQ(string(reinterpret_cast<const char *>(embedded_null_target),
                   sizeof(embedded_null_target)),
            result_target_);
}

TEST_P(VCDiffEncoderTest, UsingWideCharacters) {
  const wchar_t wchar_dictionary_text[] =
      L"\"Just the place for a Snark!\" the Bellman cried,\n"
      L"As he landed his crew with care;\n"
      L"Supporting each man on the top of the tide\n"
      L"By a finger entwined in his hair.\n";

  const wchar_t wchar_target[] =
      L"\"Just the place for a Snark! I have said it twice:\n"
      L"That alone should encourage the crew.\n"
      L"Just the place for a Snark! I have said it thrice:\n"
      L"What I tell you three times is true.\"\n";

  HashedDictionary wchar_dictionary((const char*) wchar_dictionary_text,
                                    sizeof(wchar_dictionary_text));
  EXPECT_TRUE(wchar_dictionary.Init());
  VCDiffStreamingEncoder wchar_encoder(&wchar_dictionary,
                                       VCD_FORMAT_INTERLEAVED
                                           | VCD_FORMAT_CHECKSUM,
                                       /* look_for_target_matches = */ false);
  EXPECT_TRUE(wchar_encoder.StartEncoding(delta()));
  EXPECT_TRUE(wchar_encoder.EncodeChunk((const char*) wchar_target,
                                        sizeof(wchar_target),
                                        delta()));
  EXPECT_TRUE(wchar_encoder.FinishEncoding(delta()));
  decoder_.StartDecoding((const char*) wchar_dictionary_text,
                         sizeof(wchar_dictionary_text));
  EXPECT_TRUE(decoder_.DecodeChunk(delta_data(),
                                   delta_size(),
                                   &result_target_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  const wchar_t* result_as_wchar = (const wchar_t*) result_target_.data();
  EXPECT_EQ(wcslen(wchar_target), wcslen(result_as_wchar));
  EXPECT_EQ(0, wcscmp(wchar_target, result_as_wchar));
}

#if defined(HAVE_MPROTECT) && \
   (defined(HAVE_MEMALIGN) || defined(HAVE_POSIX_MEMALIGN))
// Bug 1220602: Make sure the encoder doesn't read past the end of the input
// buffer.
TEST_P(VCDiffEncoderTest, ShouldNotReadPastEndOfBuffer) {
  const size_t target_size = strlen(kTarget);

  // Allocate two memory pages.
  const int page_size = getpagesize();
  void* two_pages = NULL;
#ifdef HAVE_POSIX_MEMALIGN
  posix_memalign(&two_pages, page_size, 2 * page_size);
#else  // !HAVE_POSIX_MEMALIGN
  two_pages = memalign(page_size, 2 * page_size);
#endif  // HAVE_POSIX_MEMALIGN
  char* const first_page = reinterpret_cast<char*>(two_pages);
  char* const second_page = first_page + page_size;

  // Place the target string at the end of the first page.
  char* const target_with_guard = second_page - target_size;
  memcpy(target_with_guard, kTarget, target_size);

  // Make the second page unreadable.
  mprotect(second_page, page_size, PROT_NONE);

  // Now perform the encode operation, which will cause a segmentation fault
  // if it reads past the end of the buffer.
  EXPECT_TRUE(encoder_.StartEncoding(delta()));
  EXPECT_TRUE(encoder_.EncodeChunk(target_with_guard, target_size, delta()));
  EXPECT_TRUE(encoder_.FinishEncoding(delta()));

  // Undo the mprotect.
  mprotect(second_page, page_size, PROT_READ|PROT_WRITE);
  free(two_pages);
}

TEST_P(VCDiffEncoderTest, ShouldNotReadPastBeginningOfBuffer) {
  const size_t target_size = strlen(kTarget);

  // Allocate two memory pages.
  const int page_size = getpagesize();
  void* two_pages = NULL;
#ifdef HAVE_POSIX_MEMALIGN
  posix_memalign(&two_pages, page_size, 2 * page_size);
#else  // !HAVE_POSIX_MEMALIGN
  two_pages = memalign(page_size, 2 * page_size);
#endif  // HAVE_POSIX_MEMALIGN
  char* const first_page = reinterpret_cast<char*>(two_pages);
  char* const second_page = first_page + page_size;

  // Make the first page unreadable.
  mprotect(first_page, page_size, PROT_NONE);

  // Place the target string at the beginning of the second page.
  char* const target_with_guard = second_page;
  memcpy(target_with_guard, kTarget, target_size);

  // Now perform the encode operation, which will cause a segmentation fault
  // if it reads past the beginning of the buffer.
  EXPECT_TRUE(encoder_.StartEncoding(delta()));
  EXPECT_TRUE(encoder_.EncodeChunk(target_with_guard, target_size, delta()));
  EXPECT_TRUE(encoder_.FinishEncoding(delta()));

  // Undo the mprotect.
  mprotect(first_page, page_size, PROT_READ|PROT_WRITE);
  free(two_pages);
}
#endif  // HAVE_MPROTECT && (HAVE_MEMALIGN || HAVE_POSIX_MEMALIGN)

INSTANTIATE_TEST_SUITE_P(AllOpts, VCDiffEncoderTest,
                         ::testing::Range<size_t>(0, COMP_VALUE_MAX + 1));

class VCDiffHTML1Test : public VerifyEncodedBytesTest {
 protected:
  static const char kDictionary[];
  static const char kTarget[];
  static const char kRedundantTarget[];

  VCDiffHTML1Test();
  virtual ~VCDiffHTML1Test() { }

  void SimpleEncode();
  void StreamingEncode();

  HashedDictionary hashed_dictionary_;
  VCDiffStreamingEncoder encoder_;
  VCDiffStreamingDecoder decoder_;
  VCDiffEncoder simple_encoder_;
  VCDiffDecoder simple_decoder_;

  string result_target_;
};

const char VCDiffHTML1Test::kDictionary[] =
    "<html><font color=red>This part from the dict</font><br>";

const char VCDiffHTML1Test::kTarget[] =
    "<html><font color=red>This part from the dict</font><br>\n"
    "And this part is not...</html>";

const char VCDiffHTML1Test::kRedundantTarget[] =
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";  // 256

VCDiffHTML1Test::VCDiffHTML1Test()
    : hashed_dictionary_(kDictionary, sizeof(kDictionary),
                         CompressionOptions(GetParam())),
      encoder_(&hashed_dictionary_,
               VCD_FORMAT_INTERLEAVED | VCD_FORMAT_CHECKSUM,
               /* look_for_target_matches = */ true),
      simple_encoder_(kDictionary, sizeof(kDictionary),
                      CompressionOptions(GetParam())) {
  EXPECT_TRUE(hashed_dictionary_.Init());
}

void VCDiffHTML1Test::SimpleEncode() {
  EXPECT_TRUE(simple_encoder_.Encode(kTarget, strlen(kTarget), delta()));
  EXPECT_GE(strlen(kTarget) + kFileHeaderSize + kWindowHeaderSize,
            delta_size());
  EXPECT_TRUE(simple_decoder_.Decode(kDictionary,
                                     sizeof(kDictionary),
                                     delta_as_const(),
                                     &result_target_));
  EXPECT_EQ(kTarget, result_target_);
}

void VCDiffHTML1Test::StreamingEncode() {
  EXPECT_TRUE(encoder_.StartEncoding(delta()));
  EXPECT_TRUE(encoder_.EncodeChunk(kTarget, strlen(kTarget), delta()));
  EXPECT_TRUE(encoder_.FinishEncoding(delta()));
}

TEST_P(VCDiffHTML1Test, CheckOutputOfSimpleEncoder) {
  SimpleEncode();
  // These values do not depend on the block size used for encoding
  ExpectByte(0xD6);  // 'V' | 0x80
  ExpectByte(0xC3);  // 'C' | 0x80
  ExpectByte(0xC4);  // 'D' | 0x80
  ExpectByte(0x00);  // Simple encoder never uses interleaved format
  ExpectByte(0x00);  // Hdr_Indicator
  ExpectByte(VCD_SOURCE);  // Win_Indicator: VCD_SOURCE (dictionary)
  ExpectByte(sizeof(kDictionary));  // Dictionary length
  ExpectByte(0x00);  // Source segment position: start of dictionary
  if (block_size_ < 16) {
    // A medium block size will catch the "his part " match.
    ExpectByte(0x22);  // Length of the delta encoding
    ExpectSize(strlen(kTarget));  // Size of the target window
    ExpectByte(0x00);  // Delta_indicator (no compression)
    ExpectByte(0x16);  // Length of the data section
    ExpectByte(0x05);  // Length of the instructions section
    ExpectByte(0x02);  // Length of the address section
    // Data section
    ExpectString("\nAnd t");      // Data for 1st ADD
    ExpectString("is not...</html>");  // Data for 2nd ADD
    // Instructions section
    ExpectByte(0x73);  // COPY size 0 mode VCD_SAME(0)
    ExpectByte(0x38);  // COPY size (56)
    ExpectByte(0x07);  // ADD size 6
    ExpectByte(0x19);  // COPY size 9 mode VCD_SELF
    ExpectByte(0x11);  // ADD size 16
    // Address section
    ExpectByte(0x00);  // COPY address (0) mode VCD_SAME(0)
    ExpectByte(0x17);  // COPY address (23) mode VCD_SELF
  } else if (block_size_ <= 56) {
    // Any block size up to 56 will catch the matching prefix string.
    ExpectByte(0x29);  // Length of the delta encoding
    ExpectSize(strlen(kTarget));  // Size of the target window
    ExpectByte(0x00);  // Delta_indicator (no compression)
    ExpectByte(0x1F);  // Length of the data section
    ExpectByte(0x04);  // Length of the instructions section
    ExpectByte(0x01);  // Length of the address section
    ExpectString("\nAnd this part is not...</html>");  // Data for ADD
    // Instructions section
    ExpectByte(0x73);  // COPY size 0 mode VCD_SAME(0)
    ExpectByte(0x38);  // COPY size (56)
    ExpectByte(0x01);  // ADD size 0
    ExpectByte(0x1F);  // Size of ADD (31)
    // Address section
    ExpectByte(0x00);  // COPY address (0) mode VCD_SAME(0)
  } else {
    // The matching string is 56 characters long, and the block size is
    // 64 or greater, so no match should be found.
    ExpectSize(strlen(kTarget) + 7);  // Delta encoding len
    ExpectSize(strlen(kTarget));  // Size of the target window
    ExpectByte(0x00);  // Delta_indicator (no compression)
    ExpectSize(strlen(kTarget));  // Length of the data section
    ExpectByte(0x02);  // Length of the instructions section
    ExpectByte(0x00);  // Length of the address section
    // Data section
    ExpectString(kTarget);
    ExpectByte(0x01);  // ADD size 0
    ExpectSize(strlen(kTarget));
  }
  ExpectNoMoreBytes();
}

TEST_P(VCDiffHTML1Test, SimpleEncoderPerformsTargetMatching) {
  EXPECT_TRUE(simple_encoder_.Encode(kRedundantTarget,
                                     strlen(kRedundantTarget),
                                     delta()));
  EXPECT_GE(strlen(kRedundantTarget) + kFileHeaderSize + kWindowHeaderSize,
            delta_size());
  EXPECT_TRUE(simple_decoder_.Decode(kDictionary,
                                     sizeof(kDictionary),
                                     delta_as_const(),
                                     &result_target_));
  EXPECT_EQ(kRedundantTarget, result_target_);
  // These values do not depend on the block size used for encoding
  ExpectByte(0xD6);  // 'V' | 0x80
  ExpectByte(0xC3);  // 'C' | 0x80
  ExpectByte(0xC4);  // 'D' | 0x80
  ExpectByte(0x00);  // Simple encoder never uses interleaved format
  ExpectByte(0x00);  // Hdr_Indicator
  ExpectByte(VCD_SOURCE);  // Win_Indicator: VCD_SOURCE (dictionary)
  ExpectByte(sizeof(kDictionary));  // Dictionary length
  ExpectByte(0x00);  // Source segment position: start of dictionary
  ExpectByte(0x0C);  // Length of the delta encoding
  ExpectSize(strlen(kRedundantTarget));  // Size of the target window
  ExpectByte(0x00);  // Delta_indicator (no compression)
  ExpectByte(0x01);  // Length of the data section
  ExpectByte(0x04);  // Length of the instructions section
  ExpectByte(0x01);  // Length of the address section
  // Data section
  ExpectString("A");      // Data for ADD
  // Instructions section
  ExpectByte(0x02);  // ADD size 1
  ExpectByte(0x23);  // COPY size 0 mode VCD_HERE
  ExpectSize(strlen(kRedundantTarget) - 1);  // COPY size 255
  // Address section
  ExpectByte(0x01);  // COPY address (1) mode VCD_HERE
  ExpectNoMoreBytes();
}

TEST_P(VCDiffHTML1Test, SimpleEncoderWithoutTargetMatching) {
  simple_encoder_.SetTargetMatching(false);
  EXPECT_TRUE(simple_encoder_.Encode(kRedundantTarget,
                                     strlen(kRedundantTarget),
                                     delta()));
  EXPECT_GE(strlen(kRedundantTarget) + kFileHeaderSize + kWindowHeaderSize,
            delta_size());
  EXPECT_TRUE(simple_decoder_.Decode(kDictionary,
                                     sizeof(kDictionary),
                                     delta_as_const(),
                                     &result_target_));
  EXPECT_EQ(kRedundantTarget, result_target_);
  // These values do not depend on the block size used for encoding
  ExpectByte(0xD6);  // 'V' | 0x80
  ExpectByte(0xC3);  // 'C' | 0x80
  ExpectByte(0xC4);  // 'D' | 0x80
  ExpectByte(0x00);  // Simple encoder never uses interleaved format
  ExpectByte(0x00);  // Hdr_Indicator
  ExpectByte(VCD_SOURCE);  // Win_Indicator: VCD_SOURCE (dictionary)
  ExpectByte(sizeof(kDictionary));  // Dictionary length
  ExpectByte(0x00);  // Source segment position: start of dictionary
  ExpectSize(strlen(kRedundantTarget) + 0x0A);  // Length of the delta encoding
  ExpectSize(strlen(kRedundantTarget));  // Size of the target window
  ExpectByte(0x00);  // Delta_indicator (no compression)
  ExpectSize(strlen(kRedundantTarget));  // Length of the data section
  ExpectByte(0x03);  // Length of the instructions section
  ExpectByte(0x00);  // Length of the address section
  // Data section
  ExpectString(kRedundantTarget);      // Data for ADD
  // Instructions section
  ExpectByte(0x01);  // ADD size 0
  ExpectSize(strlen(kRedundantTarget));  // ADD size
  // Address section empty
  ExpectNoMoreBytes();
}

INSTANTIATE_TEST_SUITE_P(AllOpts, VCDiffHTML1Test,
                         ::testing::Range<size_t>(0, COMP_VALUE_MAX + 1));

class VCDiffHTML2Test : public VerifyEncodedBytesTest {
 protected:
  static const char kDictionary[];
  static const char kTarget[];

  VCDiffHTML2Test();
  virtual ~VCDiffHTML2Test() { }

  void SimpleEncode();
  void StreamingEncode();

  HashedDictionary hashed_dictionary_;
  VCDiffStreamingEncoder encoder_;
  VCDiffStreamingDecoder decoder_;
  VCDiffEncoder simple_encoder_;
  VCDiffDecoder simple_decoder_;

  string result_target_;
};

const char VCDiffHTML2Test::kDictionary[] = "10\nThis is a test";

const char VCDiffHTML2Test::kTarget[] = "This is a test!!!\n";

VCDiffHTML2Test::VCDiffHTML2Test()
    : hashed_dictionary_(kDictionary, sizeof(kDictionary),
                         CompressionOptions(GetParam())),
      encoder_(&hashed_dictionary_,
               VCD_FORMAT_INTERLEAVED | VCD_FORMAT_CHECKSUM,
               /* look_for_target_matches = */ true),
      simple_encoder_(kDictionary, sizeof(kDictionary),
                      CompressionOptions(GetParam())) {
  EXPECT_TRUE(hashed_dictionary_.Init());
}

void VCDiffHTML2Test::SimpleEncode() {
  EXPECT_TRUE(simple_encoder_.Encode(kTarget, strlen(kTarget), delta()));
  EXPECT_GE(strlen(kTarget) + kFileHeaderSize + kWindowHeaderSize,
            delta_size());
  EXPECT_TRUE(simple_decoder_.Decode(kDictionary,
                                     sizeof(kDictionary),
                                     delta_as_const(),
                                     &result_target_));
  EXPECT_EQ(kTarget, result_target_);
}

void VCDiffHTML2Test::StreamingEncode() {
  EXPECT_TRUE(encoder_.StartEncoding(delta()));
  EXPECT_TRUE(encoder_.EncodeChunk(kTarget, strlen(kTarget), delta()));
  EXPECT_GE(strlen(kTarget) + kFileHeaderSize + kWindowHeaderSize,
            delta_size());
  EXPECT_TRUE(simple_decoder_.Decode(kDictionary,
                                     sizeof(kDictionary),
                                     delta_as_const(),
                                     &result_target_));
  EXPECT_EQ(kTarget, result_target_);
}

TEST_P(VCDiffHTML2Test, VerifyOutputOfSimpleEncoder) {
  SimpleEncode();
  // These values do not depend on the block size used for encoding
  ExpectByte(0xD6);  // 'V' | 0x80
  ExpectByte(0xC3);  // 'C' | 0x80
  ExpectByte(0xC4);  // 'D' | 0x80
  ExpectByte(0x00);  // Simple encoder never uses interleaved format
  ExpectByte(0x00);  // Hdr_Indicator
  ExpectByte(VCD_SOURCE);  // Win_Indicator: VCD_SOURCE (dictionary)
  ExpectByte(sizeof(kDictionary));  // Dictionary length
  ExpectByte(0x00);  // Source segment position: start of dictionary
  if (block_size_ <= 8) {
    ExpectByte(12);  // Length of the delta encoding
    ExpectSize(strlen(kTarget));  // Size of the target window
    ExpectByte(0x00);  // Delta_indicator (no compression)
    ExpectByte(0x04);  // Length of the data section
    ExpectByte(0x02);  // Length of the instructions section
    ExpectByte(0x01);  // Length of the address section
    ExpectByte('!');
    ExpectByte('!');
    ExpectByte('!');
    ExpectByte('\n');
    ExpectByte(0x1E);  // COPY size 14 mode VCD_SELF
    ExpectByte(0x05);  // ADD size 4
    ExpectByte(0x03);  // COPY address (3) mode VCD_SELF
  } else {
    // Larger block sizes will not catch any matches.
    ExpectSize(strlen(kTarget) + 7);  // Delta encoding len
    ExpectSize(strlen(kTarget));  // Size of the target window
    ExpectByte(0x00);  // Delta_indicator (no compression)
    ExpectSize(strlen(kTarget));  // Length of the data section
    ExpectByte(0x02);  // Length of the instructions section
    ExpectByte(0x00);  // Length of the address section
    // Data section
    ExpectString(kTarget);
    ExpectByte(0x01);  // ADD size 0
    ExpectSize(strlen(kTarget));
  }
  ExpectNoMoreBytes();
}

TEST_P(VCDiffHTML2Test, VerifyOutputWithChecksum) {
  StreamingEncode();
  const VCDChecksum html2_checksum = ComputeAdler32(kTarget, strlen(kTarget));
  CHECK_EQ(5, VarintBE<int64_t>::Length(html2_checksum));
  // These values do not depend on the block size used for encoding
  ExpectByte(0xD6);  // 'V' | 0x80
  ExpectByte(0xC3);  // 'C' | 0x80
  ExpectByte(0xC4);  // 'D' | 0x80
  ExpectByte('S');  // Format extensions
  ExpectByte(0x00);  // Hdr_Indicator
  ExpectByte(VCD_SOURCE | VCD_CHECKSUM);  // Win_Indicator
  ExpectByte(sizeof(kDictionary));  // Dictionary length
  ExpectByte(0x00);  // Source segment position: start of dictionary
  if (block_size_ <= 8) {
    ExpectByte(17);  // Length of the delta encoding
    ExpectSize(strlen(kTarget));  // Size of the target window
    ExpectByte(0x00);  // Delta_indicator (no compression)
    ExpectByte(0x00);  // Length of the data section
    ExpectByte(0x07);  // Length of the instructions section
    ExpectByte(0x00);  // Length of the address section
    ExpectChecksum(html2_checksum);
    ExpectByte(0x1E);  // COPY size 14 mode VCD_SELF
    ExpectByte(0x03);  // COPY address (3) mode VCD_SELF
    ExpectByte(0x05);  // ADD size 4
    ExpectByte('!');
    ExpectByte('!');
    ExpectByte('!');
    ExpectByte('\n');
  } else {
    // Larger block sizes will not catch any matches.
    ExpectSize(strlen(kTarget) + 12);  // Delta encoding len
    ExpectSize(strlen(kTarget));  // Size of the target window
    ExpectByte(0x00);  // Delta_indicator (no compression)
    ExpectByte(0x00);  // Length of the data section
    ExpectSize(0x02 + strlen(kTarget));  // Interleaved
    ExpectByte(0x00);  // Length of the address section
    ExpectChecksum(html2_checksum);
    // Data section
    ExpectByte(0x01);  // ADD size 0
    ExpectSize(strlen(kTarget));
    ExpectString(kTarget);
  }
  ExpectNoMoreBytes();
}

INSTANTIATE_TEST_SUITE_P(AllOpts, VCDiffHTML2Test,
                         ::testing::Range<size_t>(0, COMP_VALUE_MAX + 1));

}  // anonymous namespace
}  // namespace open_vcdiff
