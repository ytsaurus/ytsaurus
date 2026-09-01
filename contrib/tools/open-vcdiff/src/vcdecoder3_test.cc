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
#include <stdlib.h>  // free, posix_memalign
#include <string.h>  // memcpy
#include <string>
#include "testing.h"
#include "varint_bigendian.h"
#include "vcdecoder_test.h"
#include "vcdiff_defs.h"  // VCD_SOURCE

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

// Test headers, valid and invalid.

TEST_F(VCDiffInterleavedDecoderTest, DecodeHeaderOnly) {
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_header_.data(),
                                   delta_file_header_.size(),
                                   &output_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, PartialHeaderNotEnough) {
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_header_.data(),
                                   delta_file_header_.size() - 2,
                                   &output_));
  EXPECT_FALSE(decoder_.FinishDecoding());
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, BadMagicNumber) {
  delta_file_[1] = '\xD1';  // 'Q' | 0x80;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, BadVersionNumber) {
  delta_file_[3] = 0x01;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, SecondaryCompressionNotSupported) {
  delta_file_[4] = 0x01;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, Decode) {
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_.data(),
                                   delta_file_.size(),
                                   &output_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

TEST_F(VCDiffInterleavedDecoderTest, DecodeWithChecksum) {
  ComputeAndAddChecksum();
  InitializeDeltaFile();
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_.data(),
                                   delta_file_.size(),
                                   &output_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

TEST_F(VCDiffInterleavedDecoderTest, ChecksumDoesNotMatch) {
  AddChecksum(0xBADBAD);
  InitializeDeltaFile();
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, ChecksumIsInvalid64BitVarint) {
  static const uint8_t kInvalidVarint[] = { 0x81, 0x80, 0x80, 0x80, 0x80, 0x80,
                                            0x80, 0x80, 0x80, 0x00 };
  delta_window_header_[0] |= VCD_CHECKSUM;
  delta_window_header_.append(reinterpret_cast<const char *>(kInvalidVarint),
                              sizeof(kInvalidVarint));
  // Adjust delta window size to include size of invalid Varint.
  delta_window_header_[5] += sizeof(kInvalidVarint);
  InitializeDeltaFile();
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

// Remove one byte from the length of the chunk to process, and
// verify that an error is returned for FinishDecoding().
TEST_F(VCDiffInterleavedDecoderTest, FinishAfterDecodingPartialWindow) {
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_.data(),
                                   delta_file_.size() - 1,
                                   &output_));
  EXPECT_FALSE(decoder_.FinishDecoding());
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

TEST_F(VCDiffInterleavedDecoderTest, FinishAfterDecodingPartialWindowHeader) {
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_.data(),
                                   delta_file_header_.size()
                                       + delta_window_header_.size() - 1,
                                   &output_));
  EXPECT_FALSE(decoder_.FinishDecoding());
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

TEST_F(VCDiffInterleavedDecoderTest, TargetMatchesWindowSizeLimit) {
  decoder_.SetMaximumTargetWindowSize(expected_target_.size());
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_.data(),
                                   delta_file_.size(),
                                   &output_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

TEST_F(VCDiffInterleavedDecoderTest, TargetMatchesFileSizeLimit) {
  decoder_.SetMaximumTargetFileSize(expected_target_.size());
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_TRUE(decoder_.DecodeChunk(delta_file_.data(),
                                   delta_file_.size(),
                                   &output_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

TEST_F(VCDiffInterleavedDecoderTest, TargetExceedsWindowSizeLimit) {
  decoder_.SetMaximumTargetWindowSize(expected_target_.size() - 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, TargetExceedsFileSizeLimit) {
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
TEST_F(VCDiffInterleavedDecoderTest, FuzzBits) {
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

// If a checksum is present, then fuzzing any of the bits may produce an error,
// but it should not result in an incorrect target being produced without
// an error.
TEST_F(VCDiffInterleavedDecoderTest, FuzzBitsWithChecksum) {
  ComputeAndAddChecksum();
  InitializeDeltaFile();
  while (FuzzOneByteInDeltaFile()) {
    decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffInterleavedDecoderTest, CopyMoreThanExpectedTarget) {
  delta_file_[delta_file_header_.size() + 0x0D] =
      FirstByteOfStringLength(kExpectedTarget);
  delta_file_[delta_file_header_.size() + 0x0E] =
      SecondByteOfStringLength(kExpectedTarget) + 1;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, CopySizeZero) {
  delta_file_[delta_file_header_.size() + 0x0D] = 0;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, CopySizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 0x0D];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, CopySizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 0x0D];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, CopySizeMaxInt) {
  WriteMaxVarintAtOffset(0x0D, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, CopySizeNegative) {
  WriteNegativeVarintAtOffset(0x0D, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, CopySizeInvalid) {
  WriteInvalidVarintAtOffset(0x0D, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, CopyAddressBeyondHereAddress) {
  delta_file_[delta_file_header_.size() + 0x0E] =
      FirstByteOfStringLength(kDictionary);
  delta_file_[delta_file_header_.size() + 0x0F] =
      SecondByteOfStringLength(kDictionary);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, CopyAddressMaxInt) {
  WriteMaxVarintAtOffset(0x0E, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, CopyAddressNegative) {
  WriteNegativeVarintAtOffset(0x0E, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, CopyAddressInvalid) {
  WriteInvalidVarintAtOffset(0x0E, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, AddMoreThanExpectedTarget) {
  delta_file_[delta_file_header_.size() + 0x10] =
      FirstByteOfStringLength(kExpectedTarget);
  delta_file_[delta_file_header_.size() + 0x11] =
      SecondByteOfStringLength(kExpectedTarget) + 1;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, AddSizeZero) {
  delta_file_[delta_file_header_.size() + 0x10] = 0;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, AddSizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 0x10];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, AddSizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 0x10];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, AddSizeMaxInt) {
  WriteMaxVarintAtOffset(0x10, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, AddSizeNegative) {
  WriteNegativeVarintAtOffset(0x10, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, AddSizeInvalid) {
  WriteInvalidVarintAtOffset(0x10, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, RunMoreThanExpectedTarget) {
  delta_file_[delta_file_header_.size() + 0x60] =
      FirstByteOfStringLength(kExpectedTarget);
  delta_file_[delta_file_header_.size() + 0x61] =
      SecondByteOfStringLength(kExpectedTarget) + 1;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, RunSizeZero) {
  delta_file_[delta_file_header_.size() + 0x60] = 0;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, RunSizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 0x60];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, RunSizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 0x60];
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, RunSizeMaxInt) {
  WriteMaxVarintAtOffset(0x60, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, RunSizeNegative) {
  WriteNegativeVarintAtOffset(0x60, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTest, RunSizeInvalid) {
  WriteInvalidVarintAtOffset(0x60, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_FALSE(decoder_.DecodeChunk(delta_file_.data(),
                                    delta_file_.size(),
                                    &output_));
  EXPECT_EQ("", output_);
}

#if defined(HAVE_MPROTECT) && \
   (defined(HAVE_MEMALIGN) || defined(HAVE_POSIX_MEMALIGN))
TEST_F(VCDiffInterleavedDecoderTest, ShouldNotReadPastEndOfBuffer) {
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

  // Place the delta string at the end of the first page.
  char* delta_with_guard = second_page - delta_file_.size();
  memcpy(delta_with_guard, delta_file_.data(), delta_file_.size());

  // Make the second page unreadable.
  mprotect(second_page, page_size, PROT_NONE);

  // Now perform the decode operation, which will cause a segmentation fault
  // if it reads past the end of the buffer.
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_TRUE(decoder_.DecodeChunk(delta_with_guard,
                                   delta_file_.size(),
                                   &output_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);

  // Undo the mprotect.
  mprotect(second_page, page_size, PROT_READ|PROT_WRITE);
  free(two_pages);
}

TEST_F(VCDiffInterleavedDecoderTest, ShouldNotReadPastBeginningOfBuffer) {
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

  // Place the delta string at the beginning of the second page.
  char* delta_with_guard = second_page;
  memcpy(delta_with_guard, delta_file_.data(), delta_file_.size());

  // Now perform the decode operation, which will cause a segmentation fault
  // if it reads past the beginning of the buffer.
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  EXPECT_TRUE(decoder_.DecodeChunk(delta_with_guard,
                                   delta_file_.size(),
                                   &output_));
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);

  // Undo the mprotect.
  mprotect(first_page, page_size, PROT_READ|PROT_WRITE);
  free(two_pages);
}
#endif  // HAVE_MPROTECT && (HAVE_MEMALIGN || HAVE_POSIX_MEMALIGN)

// These are the same tests as for VCDiffInterleavedDecoderTest, with the added
// complication that instead of calling DecodeChunk() once with the entire data
// set, DecodeChunk() is called once for each byte of input.  This is intended
// to shake out any bugs with rewind and resume while parsing chunked data.

typedef VCDiffInterleavedDecoderTest VCDiffInterleavedDecoderTestByteByByte;

// Test headers, valid and invalid.

TEST_F(VCDiffInterleavedDecoderTestByteByByte, DecodeHeaderOnly) {
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  for (size_t i = 0; i < delta_file_header_.size(); ++i) {
    EXPECT_TRUE(decoder_.DecodeChunk(&delta_file_header_[i], 1, &output_));
  }
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte, PartialHeaderNotEnough) {
  delta_file_.resize(delta_file_header_.size() - 2);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    EXPECT_TRUE(decoder_.DecodeChunk(&delta_file_[i], 1, &output_));
  }
  EXPECT_FALSE(decoder_.FinishDecoding());
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte, BadMagicNumber) {
  delta_file_[1] = '\xD1';  // 'Q' | 0x80;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      // It should fail at the position that was altered
      EXPECT_EQ(1U, i);
      failed = true;
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte, BadVersionNumber) {
  delta_file_[3] = 0x01;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail at the position that was altered
      EXPECT_EQ(3U, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte,
       SecondaryCompressionNotSupported) {
  delta_file_[4] = 0x01;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail at the position that was altered
      EXPECT_EQ(4U, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte, Decode) {
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    EXPECT_TRUE(decoder_.DecodeChunk(&delta_file_[i], 1, &output_));
  }
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte, DecodeWithChecksum) {
  ComputeAndAddChecksum();
  InitializeDeltaFile();
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    EXPECT_TRUE(decoder_.DecodeChunk(&delta_file_[i], 1, &output_));
  }
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte, ChecksumDoesNotMatch) {
  AddChecksum(0xBADBAD);
  InitializeDeltaFile();
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail after decoding the entire delta file
      EXPECT_EQ(delta_file_.size() - 1, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte, ChecksumIsInvalid64BitVarint) {
  static const uint8_t kInvalidVarint[] = { 0x81, 0x80, 0x80, 0x80, 0x80, 0x80,
                                            0x80, 0x80, 0x80, 0x00 };
  delta_window_header_[0] |= VCD_CHECKSUM;
  delta_window_header_.append(reinterpret_cast<const char *>(kInvalidVarint),
                              sizeof(kInvalidVarint));
  // Adjust delta window size to include size of invalid Varint.
  delta_window_header_[5] += sizeof(kInvalidVarint);
  InitializeDeltaFile();
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail while trying to interpret the checksum.
      EXPECT_EQ(delta_file_header_.size() + delta_window_header_.size() - 2, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte, TargetMatchesWindowSizeLimit) {
  decoder_.SetMaximumTargetWindowSize(expected_target_.size());
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    EXPECT_TRUE(decoder_.DecodeChunk(&delta_file_[i], 1, &output_));
  }
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte, TargetMatchesFileSizeLimit) {
  decoder_.SetMaximumTargetFileSize(expected_target_.size());
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    EXPECT_TRUE(decoder_.DecodeChunk(&delta_file_[i], 1, &output_));
  }
  EXPECT_TRUE(decoder_.FinishDecoding());
  EXPECT_EQ(expected_target_.c_str(), output_);
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte, TargetExceedsWindowSizeLimit) {
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

TEST_F(VCDiffInterleavedDecoderTestByteByByte, TargetExceedsFileSizeLimit) {
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
TEST_F(VCDiffInterleavedDecoderTestByteByByte, FuzzBits) {
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
    InitializeDeltaFile();
    output_.clear();
  }
}

// If a checksum is present, then fuzzing any of the bits may produce an error,
// but it should not result in an incorrect target being produced without
// an error.
TEST_F(VCDiffInterleavedDecoderTestByteByByte, FuzzBitsWithChecksum) {
  ComputeAndAddChecksum();
  InitializeDeltaFile();
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

TEST_F(VCDiffInterleavedDecoderTestByteByByte,
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
      // The first COPY instruction should fail.
      EXPECT_EQ(delta_file_header_.size() + delta_window_header_.size() + 2, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  EXPECT_EQ("", output_);
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte, CopyMoreThanExpectedTarget) {
  delta_file_[delta_file_header_.size() + 0x0D] =
      FirstByteOfStringLength(kExpectedTarget);
  delta_file_[delta_file_header_.size() + 0x0E] =
      SecondByteOfStringLength(kExpectedTarget) + 1;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail at the position that was altered
      EXPECT_EQ(delta_file_header_.size() + 0x0E, i);
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
TEST_F(VCDiffInterleavedDecoderTestByteByByte, CopySizeZero) {
  delta_file_[delta_file_header_.size() + 0x0D] = 0;
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

TEST_F(VCDiffInterleavedDecoderTestByteByByte, CopySizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 0x0D];
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

TEST_F(VCDiffInterleavedDecoderTestByteByByte, CopySizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 0x0D];
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

TEST_F(VCDiffInterleavedDecoderTestByteByByte, CopySizeMaxInt) {
  WriteMaxVarintAtOffset(0x0D, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail at the position that was altered
      EXPECT_EQ(delta_file_header_.size() + 0x11, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte, CopySizeNegative) {
  WriteNegativeVarintAtOffset(0x0D, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffInterleavedDecoderTestByteByByte, CopySizeInvalid) {
  WriteInvalidVarintAtOffset(0x0D, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail at the position that was altered
      EXPECT_EQ(delta_file_header_.size() + 0x11, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte, CopyAddressBeyondHereAddress) {
  delta_file_[delta_file_header_.size() + 0x0E] =
      FirstByteOfStringLength(kDictionary);
  delta_file_[delta_file_header_.size() + 0x0F] =
      SecondByteOfStringLength(kDictionary);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
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

TEST_F(VCDiffInterleavedDecoderTestByteByByte, CopyAddressMaxInt) {
  WriteMaxVarintAtOffset(0x0E, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail at the position that was altered
      EXPECT_EQ(delta_file_header_.size() + 0x12, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte, CopyAddressNegative) {
  WriteNegativeVarintAtOffset(0x0E, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail at the position that was altered
      EXPECT_EQ(delta_file_header_.size() + 0x11, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte, CopyAddressInvalid) {
  WriteInvalidVarintAtOffset(0x0E, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail at the position that was altered
      EXPECT_EQ(delta_file_header_.size() + 0x12, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte, AddMoreThanExpectedTarget) {
  delta_file_[delta_file_header_.size() + 0x10] =
      FirstByteOfStringLength(kExpectedTarget);
  delta_file_[delta_file_header_.size() + 0x11] =
      SecondByteOfStringLength(kExpectedTarget) + 1;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail at the position that was altered
      EXPECT_EQ(delta_file_header_.size() + 0x11, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

// An ADD instruction with an explicit size of 0 is not illegal according to the
// standard, although it is inefficient and should not be generated by any
// reasonable encoder.  Changing the size of an ADD instruction to zero will
// cause a failure because the generated target window size will not match the
// expected target size.
TEST_F(VCDiffInterleavedDecoderTestByteByByte, AddSizeZero) {
  delta_file_[delta_file_header_.size() + 0x10] = 0;
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

TEST_F(VCDiffInterleavedDecoderTestByteByByte, AddSizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 0x10];
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

TEST_F(VCDiffInterleavedDecoderTestByteByByte, AddSizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 0x10];
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

TEST_F(VCDiffInterleavedDecoderTestByteByByte, AddSizeMaxInt) {
  WriteMaxVarintAtOffset(0x10, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail at the position that was altered
      EXPECT_EQ(delta_file_header_.size() + 0x14, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte, AddSizeNegative) {
  WriteNegativeVarintAtOffset(0x10, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail at the position that was altered
      EXPECT_EQ(delta_file_header_.size() + 0x13, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte, AddSizeInvalid) {
  WriteInvalidVarintAtOffset(0x10, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail at the position that was altered
      EXPECT_EQ(delta_file_header_.size() + 0x14, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte, RunMoreThanExpectedTarget) {
  delta_file_[delta_file_header_.size() + 0x60] =
      FirstByteOfStringLength(kExpectedTarget);
  delta_file_[delta_file_header_.size() + 0x61] =
      SecondByteOfStringLength(kExpectedTarget) + 1;
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail at the position that was altered
      EXPECT_EQ(delta_file_header_.size() + 0x61, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

// A RUN instruction with an explicit size of 0 is not illegal according to the
// standard, although it is inefficient and should not be generated by any
// reasonable encoder.  Changing the size of a RUN instruction to zero will
// cause a failure because the generated target window size will not match the
// expected target size.
TEST_F(VCDiffInterleavedDecoderTestByteByByte, RunSizeZero) {
  delta_file_[delta_file_header_.size() + 0x60] = 0;
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

TEST_F(VCDiffInterleavedDecoderTestByteByByte, RunSizeTooLargeByOne) {
  ++delta_file_[delta_file_header_.size() + 0x60];
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

TEST_F(VCDiffInterleavedDecoderTestByteByByte, RunSizeTooSmallByOne) {
  --delta_file_[delta_file_header_.size() + 0x60];
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

TEST_F(VCDiffInterleavedDecoderTestByteByByte, RunSizeMaxInt) {
  WriteMaxVarintAtOffset(0x60, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail at the position that was altered
      EXPECT_EQ(delta_file_header_.size() + 0x64, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte, RunSizeNegative) {
  WriteNegativeVarintAtOffset(0x60, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail at the position that was altered
      EXPECT_EQ(delta_file_header_.size() + 0x63, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

TEST_F(VCDiffInterleavedDecoderTestByteByByte, RunSizeInvalid) {
  WriteInvalidVarintAtOffset(0x60, 1);
  decoder_.StartDecoding(dictionary_.data(), dictionary_.size());
  bool failed = false;
  for (size_t i = 0; i < delta_file_.size(); ++i) {
    if (!decoder_.DecodeChunk(&delta_file_[i], 1, &output_)) {
      failed = true;
      // It should fail at the position that was altered
      EXPECT_EQ(delta_file_header_.size() + 0x64, i);
      break;
    }
  }
  EXPECT_TRUE(failed);
  // The decoder should not create more target bytes than were expected.
  EXPECT_GE(expected_target_.size(), output_.size());
}

}  // namespace open_vcdiff
