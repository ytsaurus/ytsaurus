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

#ifndef OPEN_VCDIFF_VCDECODER_TEST_H_
#define OPEN_VCDIFF_VCDECODER_TEST_H_

#include "google/vcdecoder.h"
#include <stdint.h>  // utf8_t
#include <string>
#include "checksum.h"
#include "testing.h"

namespace open_vcdiff {

// A base class used for all the decoder tests.  Most tests use the same
// dictionary and target and construct the delta file in the same way.
// Those elements are provided as string members and can be modified or
// overwritten by each specific decoder test as needed.
class VCDiffDecoderTest : public testing::Test {
 protected:
  typedef std::string string;

  static const char kDictionary[];
  static const char kExpectedTarget[];

  // A value smaller than kSdch3DmBlockSize is used for tests
  static const int kSdch3TestBlockSize = 8;

  VCDiffDecoderTest();

  virtual ~VCDiffDecoderTest() {}

  virtual void SetUp();

  // These functions populate delta_file_header_ with a standard, interleaved,
  // or sdch3-enabled file header. For sdch3, must be called after
  // setting/changing the dictionary.
  void UseStandardFileHeader();
  void UseInterleavedFileHeader();
  void UseSdch3FileHeader();

  // This function is called by SetUp().  It populates delta_file_ with the
  // concatenated delta file header, delta window header, and delta window
  // body, plus (if UseChecksum() is true) the corresponding checksum.
  // It can be called again by a test that has modified the contents of
  // delta_file_ and needs to restore them to their original state.
  virtual void InitializeDeltaFile();

  // This function adds an Adler32 checksum to the delta window header.
  void AddChecksum(VCDChecksum checksum);

  // This function computes the Adler32 checksum for the expected target
  // and adds it to the delta window header.
  void ComputeAndAddChecksum();

  // Write the maximum expressible positive 32-bit VarintBE
  // (0x7FFFFFFF) at the given offset in the delta window.
  void WriteMaxVarintAtOffset(int offset, int bytes_to_replace);

  // Write a negative 32-bit VarintBE (0x80000000) at the given offset
  // in the delta window.
  void WriteNegativeVarintAtOffset(int offset, int bytes_to_replace);

  // Write a VarintBE that has too many continuation bytes
  // at the given offset in the delta window.
  void WriteInvalidVarintAtOffset(int offset, int bytes_to_replace);

  // This function iterates through a list of fuzzers (bit masks used to corrupt
  // bytes) and through positions in the delta file.  Each time it is called, it
  // attempts to corrupt a different byte in delta_file_ in a different way.  If
  // successful, it returns true. Once it exhausts the list of fuzzers and of
  // byte positions in delta_file_, it returns false.
  bool FuzzOneByteInDeltaFile();

  // Assuming the length of the given string can be expressed as a VarintBE
  // of length N, this function returns the byte at position which_byte, where
  // 0 <= which_byte < N.
  static uint8_t GetByteFromStringLength(const char* s, int which_byte);

  // Assuming the length of the given string can be expressed as a one-byte
  // VarintBE, this function returns that byte value.
  static uint8_t StringLengthAsByte(const char* s) {
    return GetByteFromStringLength(s, 0);
  }

  // Assuming the length of the given string can be expressed as a two-byte
  // VarintBE, this function returns the first byte of its representation.
  static uint8_t FirstByteOfStringLength(const char* s) {
    return GetByteFromStringLength(s, 0);
  }

  // Assuming the length of the given string can be expressed as a two-byte
  // VarintBE, this function returns the second byte of its representation.
  static uint8_t SecondByteOfStringLength(const char* s) {
    return GetByteFromStringLength(s, 1);
  }

  VCDiffStreamingDecoder decoder_;

  // delta_file_ will be populated by InitializeDeltaFile() using the components
  // delta_file_header_, delta_window_header_, and delta_window_body_.
  string delta_file_;

  // This string is not populated during setup, but is used to receive the
  // decoded target file in each test.
  string output_;

  // Test fixtures that inherit from VCDiffDecoderTest can set these strings in
  // their constructors to override their default values (which come from
  // kDictionary, kExpectedTarget, etc.)
  string dictionary_;
  string expected_target_;

  // The components that will be used to construct delta_file_.
  string delta_file_header_;
  string delta_window_header_;
  string delta_window_body_;

 private:
  // These values should only be accessed via UseStandardFileHeader() and
  // UseInterleavedFileHeader().
  static const uint8_t kStandardFileHeader[];
  static const uint8_t kInterleavedFileHeader[];

  // These two counters are used by FuzzOneByteInDeltaFile() to iterate through
  // different ways to corrupt the delta file.
  size_t fuzzer_;
  size_t fuzzed_byte_position_;
};

// The "standard" decoder test, which decodes a delta file that uses the
// standard VCDIFF (RFC 3284) format with no extensions.
class VCDiffStandardDecoderTest : public VCDiffDecoderTest {
 protected:
  VCDiffStandardDecoderTest();
  virtual ~VCDiffStandardDecoderTest() {}

 private:
  static const uint8_t kWindowHeader[];
  static const uint8_t kWindowBody[];
};

class VCDiffInterleavedDecoderTest : public VCDiffDecoderTest {
 protected:
  VCDiffInterleavedDecoderTest();
  virtual ~VCDiffInterleavedDecoderTest() {}

 private:
  static const uint8_t kWindowHeader[];
  static const uint8_t kWindowBody[];
};

class VCDiffSdch3DecoderTest : public VCDiffInterleavedDecoderTest {
 protected:
  VCDiffSdch3DecoderTest();
  virtual ~VCDiffSdch3DecoderTest() {}

  void AddSdch3Window();
  virtual void InitializeDeltaFile() override;

 std::string sdch3dict_;
};

}  // namespace open_vcdiff

#endif  // OPEN_VCDIFF_VCDECODER_TEST_H_
