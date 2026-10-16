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
#include "testing.h"
#include "vcdecoder_test.h"
#include "vcdiff_defs.h"  // VCD_SOURCE

namespace open_vcdiff {

// These are the same tests as for VCDiffStandardDecoderTest, with the added
// complication that instead of calling DecodeChunk() once with the entire data
// set, DecodeChunk() is called once for each byte of input.  This is intended
// to shake out any bugs with rewind and resume while parsing chunked data.

typedef VCDiffStandardDecoderTest VCDiffStandardDecoderTestByteByByte;

TEST_F(VCDiffStandardDecoderTestByteByByte, DecodeHeaderOnly) {
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  for (size_t i = 0; i < delta_file_header_.size(); ++i) {
    EXPECT_TRUE(decoder_.DecodeChunk(&delta_file_header_[i], 1, &output_));
  }
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, Decode) {
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    EXPECT_TRUE(decoder_.DecodeChunk(&delta_file_[i], 1, &output_));
  }
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, DecodeNoVcdTarget) {
  decoder_.SetAllowVcdTarget(false);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    EXPECT_TRUE(decoder_.DecodeChunk(&delta_file_[i], 1, &output_));
  }
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

// Remove one byte from the length of the chunk to process, and
// verify that an error is returned for FinishDecoding().
TEST_F(VCDiffStandardDecoderTestByteByByte, FinishAfterDecodingPartialWindow) {
  delta_file_.resize(delta_file_.size() - 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    EXPECT_TRUE(decoder_.DecodeChunk(&delta_file_[i], 1, &output_));
  }
  EXPECT_FALSE(decoder_.FinishDecoding());
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

TEST_F(VCDiffStandardDecoderTestByteByByte,
       FinishAfterDecodingPartialWindowHeader) {
  delta_file_.resize(delta_file_header_.size()
                       + delta_window_header_.size() - 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    EXPECT_TRUE(decoder_.DecodeChunk(&delta_file_[i], 1, &output_));
  }
  EXPECT_FALSE(decoder_.FinishDecoding());
  EXPECT_EQ("", output_);
}

// If we add a checksum to a standard-format delta file (without using format
// extensions), it will be interpreted as random bytes inserted into the middle
// of the file.  The decode operation should fail, but where exactly it fails is
// undefined.
TEST_F(VCDiffStandardDecoderTestByteByByte,
       StandardFormatDoesNotSupportChecksum) {
  ComputeAndAddChecksum();
  InitializeDeltaFile();
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, TargetMatchesWindowSizeLimit) {
  decoder_.SetMaximumTargetWindowSize(expected_target_.size());
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    EXPECT_TRUE(decoder_.DecodeChunk(&delta_file_[i], 1, &output_));
  }
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, TargetMatchesFileSizeLimit) {
  decoder_.SetMaximumTargetFileSize(expected_target_.size());
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    EXPECT_TRUE(decoder_.DecodeChunk(&delta_file_[i], 1, &output_));
  }
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, TargetExceedsWindowSizeLimit) {
  decoder_.SetMaximumTargetWindowSize(expected_target_.size() - 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, TargetExceedsFileSizeLimit) {
  decoder_.SetMaximumTargetFileSize(expected_target_.size() - 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

// Fuzz bits to make sure decoder does not violently crash.
// This test has no expected behavior except that no crashes should occur.
// In some cases, changing bits will still decode to the correct target;
// for example, changing unused bits within a bitfield.
TEST_F(VCDiffStandardDecoderTestByteByByte, FuzzBits) {
  while (FuzzOneByteInDeltaFile()) {
    decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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
    // The decoder should not create more target bytes than were expected.
    EXPECT_GE(expected_target_.size(), output_.size());
    InitializeDeltaFile();
    output_.clear();
  }
}

// Change each element of the delta file window to an erroneous value
// and make sure it's caught as an error.

TEST_F(VCDiffStandardDecoderTestByteByByte,
       WinIndicatorHasBothSourceAndTarget) {
  delta_file_[delta_file_header_.size()] = VCD_SOURCE + VCD_TARGET;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail at the position that was altered
      EXPECT_EQ(delta_file_header_.size(), i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, OkayToSetUpperBitsOfWinIndicator) {
  // It is not an error to set any of the other bits in Win_Indicator
  // besides VCD_SOURCE, VCD_TARGET, VCD_CHECKSUM and VCD_FINAL.
  delta_file_[delta_file_header_.size()] = 0xF5;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    EXPECT_TRUE(decoder_.DecodeChunk(&delta_file_[i], 1, &output_));
  }
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte,
       CopyInstructionsShouldFailIfNoSourceSegment) {
  // Replace the Win_Indicator and the source size and source offset with a
  // single 0 byte (a Win_Indicator for a window with no source segment.)
  delta_window_header_.replace(0, 4, "\0", 1);
  InitializeDeltaFile();
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // The first COPY instruction should fail.  With the standard format,
      // it may need to see the whole delta window before knowing that it is
      // invalid.
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte,
       SourceSegmentSizeExceedsDictionarySize) {
  ++delta_file_[delta_file_header_.size() + 2];  // increment size
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the source segment size
      EXPECT_EQ(delta_file_header_.size() + 2, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, SourceSegmentSizeMaxInt) {
  WriteMaxVarintAtOffset(1, 2);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the source segment size
      EXPECT_EQ(delta_file_header_.size() + 5, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, SourceSegmentSizeNegative) {
  WriteNegativeVarintAtOffset(1, 2);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the source segment size
      EXPECT_EQ(delta_file_header_.size() + 4, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, SourceSegmentSizeInvalid) {
  WriteInvalidVarintAtOffset(1, 2);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the source segment size
      EXPECT_GE(delta_file_header_.size() + 6, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte,
       SourceSegmentEndExceedsDictionarySize) {
  ++delta_file_[delta_file_header_.size() + 3];  // increment start pos
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the source segment end
      EXPECT_EQ(delta_file_header_.size() + 3, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, SourceSegmentPosMaxInt) {
  WriteMaxVarintAtOffset(3, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the source segment pos
      EXPECT_EQ(delta_file_header_.size() + 7, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, SourceSegmentPosNegative) {
  WriteNegativeVarintAtOffset(3, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the source segment pos
      EXPECT_EQ(delta_file_header_.size() + 6, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, SourceSegmentPosInvalid) {
  WriteInvalidVarintAtOffset(3, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the source segment pos
      EXPECT_GE(delta_file_header_.size() + 8, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, DeltaEncodingLengthZero) {
  delta_file_[delta_file_header_.size() + 4] = 0;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the copy address segment size
      EXPECT_EQ(delta_file_header_.size() + 10, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, DeltaEncodingLengthTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 4];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the copy address segment size
      EXPECT_EQ(delta_file_header_.size() + 10, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, DeltaEncodingLengthTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 4];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the copy address segment size
      EXPECT_EQ(delta_file_header_.size() + 10, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, DeltaEncodingLengthMaxInt) {
  WriteMaxVarintAtOffset(4, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail before finishing the window header
      EXPECT_GE(delta_file_header_.size() + delta_window_header_.size() + 4,
                i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, DeltaEncodingLengthNegative) {
  WriteNegativeVarintAtOffset(4, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the delta encoding length
      EXPECT_EQ(delta_file_header_.size() + 7, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, DeltaEncodingLengthInvalid) {
  WriteInvalidVarintAtOffset(4, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the delta encoding length
      EXPECT_GE(delta_file_header_.size() + 9, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, TargetWindowSizeZero) {
  static const char zero_size[] = { 0x00 };
  delta_file_.replace(delta_file_header_.size() + 5, 2, zero_size, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, TargetWindowSizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 6];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, TargetWindowSizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 6];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, TargetWindowSizeMaxInt) {
  WriteMaxVarintAtOffset(5, 2);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the target window size
      EXPECT_EQ(delta_file_header_.size() + 9, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, TargetWindowSizeNegative) {
  WriteNegativeVarintAtOffset(5, 2);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the target window size
      EXPECT_EQ(delta_file_header_.size() + 8, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, TargetWindowSizeInvalid) {
  WriteInvalidVarintAtOffset(5, 2);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the target window size
      EXPECT_GE(delta_file_header_.size() + 10, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte,
       OkayToSetUpperBitsOfDeltaIndicator) {
  delta_file_[delta_file_header_.size() + 7] = 0xF8;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    EXPECT_TRUE(decoder_.DecodeChunk(&delta_file_[i], 1, &output_));
  }
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, DataCompressionNotSupported) {
  delta_file_[delta_file_header_.size() + 7] = 0x01;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the delta indicator
      EXPECT_EQ(delta_file_header_.size() + 7, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte,
       InstructionCompressionNotSupported) {
  delta_file_[delta_file_header_.size() + 7] = 0x02;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the delta indicator
      EXPECT_EQ(delta_file_header_.size() + 7, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, AddressCompressionNotSupported) {
  delta_file_[delta_file_header_.size() + 7] = 0x04;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the delta indicator
      EXPECT_EQ(delta_file_header_.size() + 7, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, AddRunDataSizeZero) {
  delta_file_[delta_file_header_.size() + 8] = 0;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the copy address segment size
      EXPECT_EQ(delta_file_header_.size() + 10, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, AddRunDataSizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 8];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the copy address segment size
      EXPECT_EQ(delta_file_header_.size() + 10, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, AddRunDataSizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 8];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the copy address segment size
      EXPECT_EQ(delta_file_header_.size() + 10, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, AddRunDataSizeMaxInt) {
  WriteMaxVarintAtOffset(8, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail before finishing the window header
      EXPECT_GE(delta_file_header_.size() + delta_window_header_.size() + 4,
                i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, AddRunDataSizeNegative) {
  WriteNegativeVarintAtOffset(8, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the add/run data segment size
      EXPECT_EQ(delta_file_header_.size() + 11, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, AddRunDataSizeInvalid) {
  WriteInvalidVarintAtOffset(8, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the add/run data segment size
      EXPECT_GE(delta_file_header_.size() + 13, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, InstructionsSizeZero) {
  delta_file_[delta_file_header_.size() + 9] = 0;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the copy address segment size
      EXPECT_EQ(delta_file_header_.size() + 10, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, InstructionsSizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 9];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the copy address segment size
      EXPECT_EQ(delta_file_header_.size() + 10, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, InstructionsSizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 9];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the copy address segment size
      EXPECT_EQ(delta_file_header_.size() + 10, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, InstructionsSizeMaxInt) {
  WriteMaxVarintAtOffset(9, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail before finishing the window header
      EXPECT_GE(delta_file_header_.size() + delta_window_header_.size() + 4,
                i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, InstructionsSizeNegative) {
  WriteNegativeVarintAtOffset(9, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the instructions segment size
      EXPECT_EQ(delta_file_header_.size() + 12, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, InstructionsSizeInvalid) {
  WriteInvalidVarintAtOffset(9, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the instructions segment size
      EXPECT_GE(delta_file_header_.size() + 14, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, CopyAddressSizeZero) {
  delta_file_[delta_file_header_.size() + 10] = 0;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the copy address segment size
      EXPECT_EQ(delta_file_header_.size() + 10, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, CopyAddressSizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 10];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the copy address segment size
      EXPECT_EQ(delta_file_header_.size() + 10, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, CopyAddressSizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 10];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the copy address segment size
      EXPECT_EQ(delta_file_header_.size() + 10, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, CopyAddressSizeMaxInt) {
  WriteMaxVarintAtOffset(10, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the copy address segment size
      EXPECT_EQ(delta_file_header_.size() + 14, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, CopyAddressSizeNegative) {
  WriteNegativeVarintAtOffset(10, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the copy address segment size
      EXPECT_EQ(delta_file_header_.size() + 13, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, CopyAddressSizeInvalid) {
  WriteInvalidVarintAtOffset(10, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the copy address segment size
      EXPECT_GE(delta_file_header_.size() + 15, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffStandardDecoderTestByteByByte, InstructionsEndEarly) {
  --delta_file_[delta_file_header_.size() + 9];
  ++delta_file_[delta_file_header_.size() + 10];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

// From this point on, the tests should also be run against the interleaved
// format.

TEST_F(VCDiffStandardDecoderTestByteByByte, CopyMoreThanExpectedTarget) {
  delta_file_[delta_file_header_.size() + 0x70] =
      FirstByteOfStringLength(kExpectedTarget);
  delta_file_[delta_file_header_.size() + 0x71] =
      SecondByteOfStringLength(kExpectedTarget) + 1;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, CopySizeZero) {
  delta_file_[delta_file_header_.size() + 0x70] = 0;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, CopySizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 0x70];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, CopySizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 0x70];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, CopySizeMaxInt) {
  WriteMaxVarintAtOffset(0x70, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, CopySizeNegative) {
  WriteNegativeVarintAtOffset(0x70, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, CopySizeInvalid) {
  WriteInvalidVarintAtOffset(0x70, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, CopyAddressBeyondHereAddress) {
  delta_file_[delta_file_header_.size() + 0x7B] =
      FirstByteOfStringLength(kDictionary);
  delta_file_[delta_file_header_.size() + 0x7C] =
      SecondByteOfStringLength(kDictionary);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, CopyAddressMaxInt) {
  WriteMaxVarintAtOffset(0x7B, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, CopyAddressNegative) {
  WriteNegativeVarintAtOffset(0x70, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, CopyAddressInvalid) {
  WriteInvalidVarintAtOffset(0x70, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, AddMoreThanExpectedTarget) {
  delta_file_[delta_file_header_.size() + 0x72] =
      FirstByteOfStringLength(kExpectedTarget);
  delta_file_[delta_file_header_.size() + 0x73] =
      SecondByteOfStringLength(kExpectedTarget) + 1;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, AddSizeZero) {
  delta_file_[delta_file_header_.size() + 0x72] = 0;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, AddSizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 0x72];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, AddSizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 0x72];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, AddSizeMaxInt) {
  WriteMaxVarintAtOffset(0x72, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, AddSizeNegative) {
  WriteNegativeVarintAtOffset(0x72, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, AddSizeInvalid) {
  WriteInvalidVarintAtOffset(0x72, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, RunMoreThanExpectedTarget) {
  delta_file_[delta_file_header_.size() + 0x78] =
      FirstByteOfStringLength(kExpectedTarget);
  delta_file_[delta_file_header_.size() + 0x79] =
      SecondByteOfStringLength(kExpectedTarget) + 1;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, RunSizeZero) {
  delta_file_[delta_file_header_.size() + 0x78] = 0;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, RunSizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 0x78];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, RunSizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 0x78];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, RunSizeMaxInt) {
  WriteMaxVarintAtOffset(0x78, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, RunSizeNegative) {
  WriteNegativeVarintAtOffset(0x78, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffStandardDecoderTestByteByByte, RunSizeInvalid) {
  WriteInvalidVarintAtOffset(0x78, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

}  // namespace open_vcdiff
