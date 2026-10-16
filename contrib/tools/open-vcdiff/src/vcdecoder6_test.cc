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
#include "google/vcdecoder.h"
#include <string>
#include "buffer_edge_test.h"
#include "testing.h"
#include "varint_bigendian.h"
#include "vcdecoder_test.h"
#include "vcdiff_defs.h"  // VCD_SOURCE

namespace open_vcdiff {

// Test headers, valid and invalid.

TEST_F(VCDiffSdch3DecoderTest, DecodeHeaderWithStartDecoding) {
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_header_.data(),
                                   delta_file_header_.size(),
                                   &output_));
  EXPECT_FALSE(decoder_.FinishDecoding());
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffSdch3DecoderTest, DecodeNonSdch3HeaderWithStartSdch3DmDecoding) {
  UseInterleavedFileHeader();
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_header_.data(),
                                   delta_file_header_.size(),
                                   &output_));
  EXPECT_FALSE(decoder_.FinishDecoding());
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffSdch3DecoderTest, DecodeHeaderOnly) {
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_header_.data(),
                                   delta_file_header_.size(),
                                   &output_));
  EXPECT_FALSE(decoder_.FinishDecoding());
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffSdch3DecoderTest, DecodeHeaderDictLargerThanLimit) {
  decoder_.StartSdch3DmDecoding(&sdch3dict_, 20);
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_header_.data(),
                                   delta_file_header_.size(),
                                   &output_));
  EXPECT_FALSE(decoder_.FinishDecoding());
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffSdch3DecoderTest, PartialHeaderNotEnough) {
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_header_.data(),
                                   delta_file_header_.size() - 2,
                                   &output_));
  EXPECT_FALSE(decoder_.FinishDecoding());
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffSdch3DecoderTest, Decode) {
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_.data(),
                                   delta_file_.size(),
                                   &output_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
  EXPECT_EQ(sdch3dict_, dictionary_);
}

TEST_F(VCDiffSdch3DecoderTest, DecodeWithChecksum) {
  ComputeAndAddChecksum();
  InitializeDeltaFile();
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_.data(),
                                   delta_file_.size(),
                                   &output_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

// Remove one byte from the length of the chunk to process, and
// verify that an error is returned for FinishDecoding().
TEST_F(VCDiffSdch3DecoderTest, FinishAfterDecodingPartialWindow) {
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_.data(),
                                   delta_file_.size() - 1,
                                   &output_));
  EXPECT_FALSE(decoder_.FinishDecoding());
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

TEST_F(VCDiffSdch3DecoderTest, FinishAfterDecodingPartialWindowHeader) {
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_.data(),
                                   delta_file_header_.size()
                                       + delta_window_header_.size() - 1,
                                   &output_));
  EXPECT_FALSE(decoder_.FinishDecoding());
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

// Fuzz bits to make sure decoder does not violently crash.
// This test has no expected behavior except that no crashes should occur.
// In some cases, changing bits will still decode to the correct target;
// for example, changing unused bits within a bitfield.
TEST_F(VCDiffSdch3DecoderTest, FuzzBits) {
  while (FuzzOneByteInDeltaFile()) {
    decoder_.StartSdch3DmDecoding(&sdch3dict_);
    if (decoder_.DecodeChunk(delta_file_.data(),
                             delta_file_.size(),
                             &output_)) {
      decoder_.FinishDecoding();
    }
    InitializeDeltaFile();
    output_.clear();
  }
}

// If a checksum is present, then fuzzing any of the bits may produce an error,
// but it should not result in an incorrect target being produced without
// an error.
TEST_F(VCDiffSdch3DecoderTest, FuzzBitsWithChecksum) {
  ComputeAndAddChecksum();
  InitializeDeltaFile();
  while (FuzzOneByteInDeltaFile()) {
    decoder_.StartSdch3DmDecoding(&sdch3dict_);
    if (decoder_.DecodeChunk(delta_file_.data(),
                             delta_file_.size(),
                             &output_)) {
      if (decoder_.FinishDecoding()) {
        // Decoding succeeded.  Make sure the correct target was produced.
        EXPECT_EQ(expected_target_.c_str(), output_);
      }
    } else {
      EXPECT_EQ("", output_);
    }
    InitializeDeltaFile();
    output_.clear();
  }
}

TEST_F(VCDiffSdch3DecoderTest, CopyMoreThanExpectedTarget) {
  delta_file_[delta_file_header_.size() + 0x0E] =
      FirstByteOfStringLength(kExpectedTarget);
  delta_file_[delta_file_header_.size() + 0x0F] =
      SecondByteOfStringLength(kExpectedTarget) + 1;
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffSdch3DecoderTest, CopySizeZero) {
  delta_file_[delta_file_header_.size() + 0x0E] = 0;
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffSdch3DecoderTest, CopySizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 0x0E];
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffSdch3DecoderTest, CopySizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 0x0E];
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffSdch3DecoderTest, CopyAddressBeyondHereAddress) {
  delta_file_[delta_file_header_.size() + 0x0F] =
      FirstByteOfStringLength(kDictionary);
  delta_file_[delta_file_header_.size() + 0x10] =
      SecondByteOfStringLength(kDictionary);
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

#ifdef HAVE_BUFFER_EDGE_TEST
TEST_F(VCDiffSdch3DecoderTest, ShouldNotReadPastEndOfBuffer) {
  BufferEdgeTestHelper buffer(delta_file_.data(), delta_file_.size(),
      BufferEdgeTestHelper::AFTER_END);
  // Now perform the decode operation, which will cause a segmentation fault
  // if it reads past the end of the buffer.
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  EXPECT_TRUE(decoder_.DecodeChunk(buffer.data(),
                                   delta_file_.size(),
                                   &output_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

TEST_F(VCDiffSdch3DecoderTest, ShouldNotReadPastBeginningOfBuffer) {
  BufferEdgeTestHelper buffer(delta_file_.data(), delta_file_.size(),
      BufferEdgeTestHelper::BEFORE_BEGIN);
  // Now perform the decode operation, which will cause a segmentation fault
  // if it reads past the beginning of the buffer.
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  EXPECT_TRUE(decoder_.DecodeChunk(buffer.data(),
                                   delta_file_.size(),
                                   &output_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}
#endif  // HAVE_BUFFER_EDGE_TEST

// These are the same tests as for VCDiffSdch3DecoderTest, with the added
// complication that instead of calling DecodeChunk() once with the entire data
// set, DecodeChunk() is called once for each byte of input.  This is intended
// to shake out any bugs with rewind and resume while parsing chunked data.

typedef VCDiffSdch3DecoderTest VCDiffSdch3DecoderTestByteByByte;

// Test headers, valid and invalid.

TEST_F(VCDiffSdch3DecoderTestByteByByte, DecodeHeaderOnly) {
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  for (size_t i = 0; i < delta_file_header_.size(); ++i) {
    EXPECT_TRUE(decoder_.DecodeChunk(&delta_file_header_[i], 1, &output_));
  }
  EXPECT_FALSE(decoder_.FinishDecoding());
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffSdch3DecoderTestByteByByte, PartialHeaderNotEnough) {
  delta_file_.resize(delta_file_header_.size() - 2);
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    EXPECT_TRUE(decoder_.DecodeChunk(&delta_file_[i], 1, &output_));
  }
  EXPECT_FALSE(decoder_.FinishDecoding());
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffSdch3DecoderTestByteByByte, Decode) {
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    EXPECT_TRUE(decoder_.DecodeChunk(&delta_file_[i], 1, &output_));
  }
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
  EXPECT_EQ(sdch3dict_, dictionary_);
}

TEST_F(VCDiffSdch3DecoderTestByteByByte, DecodeWithChecksum) {
  ComputeAndAddChecksum();
  InitializeDeltaFile();
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    EXPECT_TRUE(decoder_.DecodeChunk(&delta_file_[i], 1, &output_));
  }
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

// Fuzz bits to make sure decoder does not violently crash.
// This test has no expected behavior except that no crashes should occur.
// In some cases, changing bits will still decode to the correct target;
// for example, changing unused bits within a bitfield.
TEST_F(VCDiffSdch3DecoderTestByteByByte, FuzzBits) {
  while (FuzzOneByteInDeltaFile()) {
    decoder_.StartSdch3DmDecoding(&sdch3dict_);
    bool failed = false;
    for (size_t i = 0; i < delta_file_.size(); ++i) {
      if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
        failed = true;
        break;
      }
    }
    if (!failed) {
      decoder_.FinishDecoding();
    }
    InitializeDeltaFile();
    output_.clear();
  }
}

// If a checksum is present, then fuzzing any of the bits may produce an error,
// but it should not result in an incorrect target being produced without
// an error.
TEST_F(VCDiffSdch3DecoderTestByteByByte, FuzzBitsWithChecksum) {
  ComputeAndAddChecksum();
  InitializeDeltaFile();
  while (FuzzOneByteInDeltaFile()) {
    decoder_.StartSdch3DmDecoding(&sdch3dict_);
    bool failed = false;
    for (size_t i = 0; i < delta_file_.size(); ++i) {
      if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
        failed = true;
        break;
      }
    }
    if (!failed) {
      if (decoder_.FinishDecoding()) {
        // Decoding succeeded.  Make sure the correct target was produced.
        EXPECT_EQ(expected_target_.c_str(), output_);
      }
    }
    // The decoder should not create more target bytes than were expected.
    EXPECT_GE(expected_target_.size(), output_.size());
    InitializeDeltaFile();
    output_.clear();
  }
}

TEST_F(VCDiffSdch3DecoderTestByteByByte,
       CopyInstructionsShouldFailIfNoSourceSegment) {
  // Replace the Win_Indicator and the source size and source offset with a
  // single 0 byte (a Win_Indicator for a window with no source segment.)
  delta_window_header_.replace(0, 4, "\0", 1);
  InitializeDeltaFile();
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // The first COPY instruction should fail.
      EXPECT_EQ(delta_file_header_.size() + delta_window_header_.size() + 2, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffSdch3DecoderTestByteByByte, CopyMoreThanExpectedTarget) {
  delta_file_[delta_file_header_.size() + 0x0E] =
      FirstByteOfStringLength(kExpectedTarget);
  delta_file_[delta_file_header_.size() + 0x0F] =
      SecondByteOfStringLength(kExpectedTarget) + 1;
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail at the position that was altered
      EXPECT_EQ(delta_file_header_.size() + 0x0F, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

// A COPY instruction with an explicit size of 0 is not illegal according to the
// standard, although it is inefficient and should not be generated by any
// reasonable encoder.  Changing the size of a COPY instruction to zero will
// cause a failure because the generated target window size will not match the
// expected target size.
TEST_F(VCDiffSdch3DecoderTestByteByByte, CopySizeZero) {
  delta_file_[delta_file_header_.size() + 0x0E] = 0;
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      break;
    }
  }
  EXPECT_TRUE(failed);
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

TEST_F(VCDiffSdch3DecoderTestByteByByte, CopySizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 0x0E];
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      break;
    }
  }
  EXPECT_TRUE(failed);
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

TEST_F(VCDiffSdch3DecoderTestByteByByte, CopySizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 0x0E];
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      break;
    }
  }
  EXPECT_TRUE(failed);
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

TEST_F(VCDiffSdch3DecoderTestByteByByte, CopyAddressBeyondHereAddress) {
  delta_file_[delta_file_header_.size() + 0x0F] =
      FirstByteOfStringLength(kDictionary);
  delta_file_[delta_file_header_.size() + 0x10] =
      SecondByteOfStringLength(kDictionary);
  decoder_.StartSdch3DmDecoding(&sdch3dict_);
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail at the position that was altered
      EXPECT_EQ(delta_file_header_.size() + 0x10, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

}  // namespace open_vcdiff
