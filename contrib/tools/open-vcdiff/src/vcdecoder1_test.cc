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
#include "testing.h"
#include "vcdecoder_test.h"
#include "vcdiff_defs.h"

namespace open_vcdiff {

TEST_F(VCDiffStandardDecoderTest, DecodeHeaderOnly) {
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_header_.data(),
                                   delta_file_header_.size(),
                                   &output_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, Decode) {
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_.data(),
                                   delta_file_.size(),
                                   &output_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

// If we add a checksum to a standard-format delta file (without using format
// extensions), it will be interpreted as random bytes inserted into the middle
// of the file.  The decode operation should fail, but where exactly it fails is
// not easy to predict.
TEST_F(VCDiffStandardDecoderTest, StandardFormatDoesNotSupportChecksum) {
  ComputeAndAddChecksum();
  InitializeDeltaFile();
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

// Remove one byte from the length of the chunk to process, and
// verify that an error is returned for FinishDecoding().
TEST_F(VCDiffStandardDecoderTest, FinishAfterDecodingPartialWindow) {
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_.data(),
                                   delta_file_.size() - 1,
                                   &output_));
  EXPECT_FALSE(decoder_.FinishDecoding());
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

TEST_F(VCDiffStandardDecoderTest, FinishAfterDecodingPartialWindowHeader) {
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_.data(),
                                   delta_file_header_.size()
                                       + delta_window_header_.size() - 1,
                                   &output_));
  EXPECT_FALSE(decoder_.FinishDecoding());
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, TargetMatchesWindowSizeLimit) {
  decoder_.SetMaximumTargetWindowSize(expected_target_.size());
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_.data(),
                                   delta_file_.size(),
                                   &output_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

TEST_F(VCDiffStandardDecoderTest, TargetMatchesFileSizeLimit) {
  decoder_.SetMaximumTargetFileSize(expected_target_.size());
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_.data(),
                                   delta_file_.size(),
                                   &output_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

TEST_F(VCDiffStandardDecoderTest, TargetExceedsWindowSizeLimit) {
  decoder_.SetMaximumTargetWindowSize(expected_target_.size() - 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, TargetExceedsFileSizeLimit) {
  decoder_.SetMaximumTargetFileSize(expected_target_.size() - 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

// Fuzz bits to make sure decoder does not violently crash.
// This test has no expected behavior except that no crashes should occur.
// In some cases, changing bits will still decode to the correct target;
// for example, changing unused bits within a bitfield.
TEST_F(VCDiffStandardDecoderTest, FuzzBits) {
  while (FuzzOneByteInDeltaFile()) {
    decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
    if (decoder_.DecodeChunk(delta_file_.data(),
                             delta_file_.size(),
                             &output_)) {
      decoder_.FinishDecoding();
    }
    InitializeDeltaFile();
    output_.clear();
  }
}

// Change each element of the delta file window to an erroneous value
// and make sure it's caught as an error.

TEST_F(VCDiffStandardDecoderTest, WinIndicatorHasBothSourceAndTarget) {
  delta_file_[delta_file_header_.size()] = VCD_SOURCE + VCD_TARGET;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, OkayToSetUpperBitsOfWinIndicator) {
  // It is not an error to set any of the other bits in Win_Indicator
  // besides VCD_SOURCE, VCD_TARGET, VCD_CHECKSUM and VCD_FINAL.
  delta_file_[delta_file_header_.size()] = 0xF5;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_.data(),
                                   delta_file_.size(),
                                   &output_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

TEST_F(VCDiffStandardDecoderTest, CopyInstructionsShouldFailIfNoSourceSegment) {
  // Replace the Win_Indicator and the source size and source offset with a
  // single 0 byte (a Win_Indicator for a window with no source segment.)
  delta_window_header_.replace(0, 4, "\0", 1);
  InitializeDeltaFile();
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  // The first COPY instruction should fail, so there should be no output
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, SourceSegmentSizeExceedsDictionarySize) {
  ++delta_file_[delta_file_header_.size() + 2];  // increment size
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, SourceSegmentSizeMaxInt) {
  WriteMaxVarintAtOffset(1, 2);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, SourceSegmentSizeNegative) {
  WriteNegativeVarintAtOffset(1, 2);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, SourceSegmentSizeInvalid) {
  WriteInvalidVarintAtOffset(1, 2);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, SourceSegmentEndExceedsDictionarySize) {
  ++delta_file_[delta_file_header_.size() + 3];  // increment start pos
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, SourceSegmentPosMaxInt) {
  WriteMaxVarintAtOffset(3, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, SourceSegmentPosNegative) {
  WriteNegativeVarintAtOffset(3, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, SourceSegmentPosInvalid) {
  WriteInvalidVarintAtOffset(3, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, DeltaEncodingLengthZero) {
  delta_file_[delta_file_header_.size() + 4] = 0;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, DeltaEncodingLengthTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 4];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, DeltaEncodingLengthTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 4];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, DeltaEncodingLengthMaxInt) {
  WriteMaxVarintAtOffset(4, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, DeltaEncodingLengthNegative) {
  WriteNegativeVarintAtOffset(4, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, DeltaEncodingLengthInvalid) {
  WriteInvalidVarintAtOffset(4, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, TargetWindowSizeZero) {
  static const char zero_size[] = { 0x00 };
  delta_file_.replace(delta_file_header_.size() + 5, 2, zero_size, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, TargetWindowSizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 6];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, TargetWindowSizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 6];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, TargetWindowSizeMaxInt) {
  WriteMaxVarintAtOffset(5, 2);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, TargetWindowSizeNegative) {
  WriteNegativeVarintAtOffset(5, 2);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, TargetWindowSizeInvalid) {
  WriteInvalidVarintAtOffset(5, 2);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, OkayToSetUpperBitsOfDeltaIndicator) {
  delta_file_[delta_file_header_.size() + 7] = 0xF8;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_.data(),
                                   delta_file_.size(),
                                   &output_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

TEST_F(VCDiffStandardDecoderTest, DataCompressionNotSupported) {
  delta_file_[delta_file_header_.size() + 7] = 0x01;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, InstructionCompressionNotSupported) {
  delta_file_[delta_file_header_.size() + 7] = 0x02;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, AddressCompressionNotSupported) {
  delta_file_[delta_file_header_.size() + 7] = 0x04;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, AddRunDataSizeZero) {
  delta_file_[delta_file_header_.size() + 8] = 0;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, AddRunDataSizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 8];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, AddRunDataSizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 8];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, AddRunDataSizeMaxInt) {
  WriteMaxVarintAtOffset(8, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, AddRunDataSizeNegative) {
  WriteNegativeVarintAtOffset(8, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, AddRunDataSizeInvalid) {
  WriteInvalidVarintAtOffset(8, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, InstructionsSizeZero) {
  delta_file_[delta_file_header_.size() + 9] = 0;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, InstructionsSizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 9];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, InstructionsSizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 9];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, InstructionsSizeMaxInt) {
  WriteMaxVarintAtOffset(9, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, InstructionsSizeNegative) {
  WriteNegativeVarintAtOffset(9, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, InstructionsSizeInvalid) {
  WriteInvalidVarintAtOffset(9, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, CopyAddressSizeZero) {
  delta_file_[delta_file_header_.size() + 10] = 0;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, CopyAddressSizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 10];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, CopyAddressSizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 10];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, CopyAddressSizeMaxInt) {
  WriteMaxVarintAtOffset(10, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, CopyAddressSizeNegative) {
  WriteNegativeVarintAtOffset(10, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, CopyAddressSizeInvalid) {
  WriteInvalidVarintAtOffset(10, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, InstructionsEndEarly) {
  --delta_file_[delta_file_header_.size() + 9];
  ++delta_file_[delta_file_header_.size() + 10];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

// From this point on, the tests should also be run against the interleaved
// format.

TEST_F(VCDiffStandardDecoderTest, CopyMoreThanExpectedTarget) {
  delta_file_[delta_file_header_.size() + 0x70] =
      FirstByteOfStringLength(kExpectedTarget);
  delta_file_[delta_file_header_.size() + 0x71] =
      SecondByteOfStringLength(kExpectedTarget) + 1;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, CopySizeZero) {
  delta_file_[delta_file_header_.size() + 0x70] = 0;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, CopySizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 0x70];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, CopySizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 0x70];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, CopySizeMaxInt) {
  WriteMaxVarintAtOffset(0x70, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, CopySizeNegative) {
  WriteNegativeVarintAtOffset(0x70, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, CopySizeInvalid) {
  WriteInvalidVarintAtOffset(0x70, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, CopyAddressBeyondHereAddress) {
  delta_file_[delta_file_header_.size() + 0x7B] =
      FirstByteOfStringLength(kDictionary);
  delta_file_[delta_file_header_.size() + 0x7C] =
      SecondByteOfStringLength(kDictionary);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, CopyAddressMaxInt) {
  WriteMaxVarintAtOffset(0x7B, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, CopyAddressNegative) {
  WriteNegativeVarintAtOffset(0x70, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, CopyAddressInvalid) {
  WriteInvalidVarintAtOffset(0x70, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, AddMoreThanExpectedTarget) {
  delta_file_[delta_file_header_.size() + 0x72] =
      FirstByteOfStringLength(kExpectedTarget);
  delta_file_[delta_file_header_.size() + 0x73] =
      SecondByteOfStringLength(kExpectedTarget) + 1;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, AddSizeZero) {
  delta_file_[delta_file_header_.size() + 0x72] = 0;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, AddSizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 0x72];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, AddSizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 0x72];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, AddSizeMaxInt) {
  WriteMaxVarintAtOffset(0x72, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, AddSizeNegative) {
  WriteNegativeVarintAtOffset(0x72, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, AddSizeInvalid) {
  WriteInvalidVarintAtOffset(0x72, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, RunMoreThanExpectedTarget) {
  delta_file_[delta_file_header_.size() + 0x78] =
      FirstByteOfStringLength(kExpectedTarget);
  delta_file_[delta_file_header_.size() + 0x79] =
      SecondByteOfStringLength(kExpectedTarget) + 1;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, RunSizeZero) {
  delta_file_[delta_file_header_.size() + 0x78] = 0;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, RunSizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 0x78];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, RunSizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 0x78];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, RunSizeMaxInt) {
  WriteMaxVarintAtOffset(0x78, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, RunSizeNegative) {
  WriteNegativeVarintAtOffset(0x78, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTest, RunSizeInvalid) {
  WriteInvalidVarintAtOffset(0x78, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

}  // namespace open_vcdiff
