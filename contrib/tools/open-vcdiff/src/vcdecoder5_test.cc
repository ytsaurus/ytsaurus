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
#include "codetable.h"
#include "testing.h"
#include "vcdecoder_test.h"

namespace open_vcdiff {
namespace {


// Decode an encoding that uses a RUN instruction to allocate 64MB.
class VCDiffLargeTargetTest : public VCDiffDecoderTest {
 protected:
  VCDiffLargeTargetTest();
  virtual ~VCDiffLargeTargetTest() {}

  static const uint8_t kLargeRunWindow[];
};

const uint8_t VCDiffLargeTargetTest::kLargeRunWindow[] = {
    0x00,  // Win_Indicator: no source segment
    0x0E,  // Length of the delta encoding
    0xA0,  // Size of the target window (0x4000000)
    0x80,  // Size of the target window cont'd
    0x80,  // Size of the target window cont'd
    0x00,  // Size of the target window cont'd
    0x00,  // Delta_indicator (no compression)
    0x00,  // length of data for ADDs and RUNs
    0x06,  // length of instructions section
    0x00,  // length of addresses for COPYs
    // Interleaved segment
    0x00,  // VCD_RUN size 0
    0xA0,  // Size of RUN (0x4000000)
    0x80,  // Size of RUN cont'd
    0x80,  // Size of RUN cont'd
    0x00,  // Size of RUN cont'd
    0xBE,  // Data for RUN
};

VCDiffLargeTargetTest::VCDiffLargeTargetTest() {
  UseInterleavedFileHeader();
}

// Ensure that, with allow_vcd_target set to false, we can decode any number of
// 64MB windows without running out of memory.
TEST_F(VCDiffLargeTargetTest, Decode) {
  // 50 x 64MB = 3.2GB, which should be too large if memory usage accumulates
  // during each iteration.
  const int kIterations = 50;
  decoder_.SetAllowVcdTarget(false);
  decoder_.SetMaximumTargetFileSize(0x4000000UL * 50);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_header_.data(),
                                   delta_file_header_.size(),
                                   &output_));
  EXPECT_EQ("", output_);
  for (int i = 0; i < kIterations; i++) {
    EXPECT_TRUE(
        decoder_.DecodeChunk(reinterpret_cast<const char *>(kLargeRunWindow),
                             sizeof(kLargeRunWindow), &output_));
    EXPECT_EQ(0x4000000U, output_.size());
    EXPECT_EQ(static_cast<char>(0xBE), output_[0]);
    EXPECT_EQ(static_cast<char>(0xBE),
              output_[output_.size() / 2]);  // middle element
    EXPECT_EQ(static_cast<char>(0xBE),
              output_[output_.size() - 1]);  // last element
    output_.clear();
  }
  EXPECT_TRUE(decoder_.FinishDecoding());
}

// If we don't increase the maximum target file size first, the same test should
// produce an error.
TEST_F(VCDiffLargeTargetTest, DecodeReachesMaxFileSize) {
  decoder_.SetAllowVcdTarget(false);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_header_.data(),
                                   delta_file_header_.size(),
                                   &output_));
  EXPECT_EQ("", output_);
  // The default maximum target file size is 64MB, which just matches the target
  // data produced by a single iteration.
  EXPECT_TRUE(
      decoder_.DecodeChunk(reinterpret_cast<const char *>(kLargeRunWindow),
                           sizeof(kLargeRunWindow), &output_));
  EXPECT_EQ(0x4000000U, output_.size());
  EXPECT_EQ(static_cast<char>(0xBE), output_[0]);
  EXPECT_EQ(static_cast<char>(0xBE),
            output_[output_.size() / 2]);  // middle element
  EXPECT_EQ(static_cast<char>(0xBE),
            output_[output_.size() - 1]);  // last element
  output_.clear();
  // Trying to decode a second window should exceed the target file size limit.
  EXPECT_FALSE(
      decoder_.DecodeChunk(reinterpret_cast<const char *>(kLargeRunWindow),
                           sizeof(kLargeRunWindow), &output_));
}

}  // unnamed namespace
}  // namespace open_vcdiff
