// Copyright 2007 The open-vcdiff Authors. All Rights Reserved.
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
#include "addrcache.h"
#include <limits.h>  // INT_MAX, INT_MIN
#include <stdint.h>  // uint32_t
#include <stdlib.h>  // rand, srand
#include <iostream>
#include <string>
#include <vector>
#include "testing.h"
#include "varint_bigendian.h"
#include "vcdiff_defs.h"  // RESULT_ERROR

namespace open_vcdiff {
namespace {

// Provides an address_stream_ buffer and functions to manually encode
// values into the buffer, and to manually decode and verify test results
// from the buffer.
//
class VCDiffAddressCacheTest : public testing::Test {
 public:
  typedef std::string string;

  VCDiffAddressCacheTest() : decode_position_(NULL),
                             decode_position_end_(NULL),
                             verify_encode_position_(NULL),
                             last_encode_size_(0),
                             last_decode_position_(NULL) { }

  virtual ~VCDiffAddressCacheTest() { }

  virtual void SetUp() {
    EXPECT_TRUE(cache_.Init());
  }

  // Benchmarks for timing encode/decode operations
  void BM_Setup(int test_size);
  void BM_CacheEncode(int iterations, int test_size);
  void BM_CacheDecode(int iterations, int test_size);

 protected:
  virtual void TestBody() { }  // to allow instantiation of this class

  void BeginDecode() {
    decode_position_ = address_stream_.data();
    EXPECT_TRUE(decode_position_ != NULL);
    last_decode_position_ = decode_position_;
    decode_position_end_ = decode_position_ + address_stream_.size();
  }

  void ExpectEncodedSizeInBytes(int n) {
    EXPECT_EQ(last_encode_size_ + n, address_stream_.size());
    last_encode_size_ = address_stream_.size();
  }

  void ExpectDecodedSizeInBytes(int n) {
    EXPECT_EQ(last_decode_position_ + n, decode_position_);
    last_decode_position_ = decode_position_;
  }

  void ManualEncodeVarint(VCDAddress value) {
    VarintBE<VCDAddress>::AppendToString(value, &address_stream_);
  }

  void ManualEncodeByte(unsigned char byte) {
    address_stream_.push_back(byte);
  }

  void ExpectEncodedVarint(VCDAddress expected_value, int expected_size) {
    if (!verify_encode_position_) {
      verify_encode_position_ = address_stream_.data();
    }
    EXPECT_EQ(expected_size, VarintBE<VCDAddress>::Length(expected_value));
    VCDAddress output_val = VarintBE<VCDAddress>::Parse(
        address_stream_.data() + address_stream_.size(),
        &verify_encode_position_);
    EXPECT_EQ(expected_value, output_val);
  }

  void ExpectEncodedByte(unsigned char expected_value) {
    if (!verify_encode_position_) {
      verify_encode_position_ = address_stream_.data();
    }
    EXPECT_EQ(expected_value, *verify_encode_position_);
    ++verify_encode_position_;
  }

  void TestEncode(VCDAddress address,
                  VCDAddress here_address,
                  unsigned char mode,
                  int size) {
    VCDAddress encoded_addr = 0;
    EXPECT_EQ(mode, cache_.EncodeAddress(address, here_address, &encoded_addr));
    if (cache_.WriteAddressAsVarintForMode(mode)) {
      ManualEncodeVarint(encoded_addr);
    } else {
      EXPECT_GT(256, encoded_addr);
      ManualEncodeByte(static_cast<unsigned char>(encoded_addr));
    }
    ExpectEncodedSizeInBytes(size);
  }

  VCDiffAddressCache cache_;
  string address_stream_;
  const char* decode_position_;
  const char* decode_position_end_;
  string large_address_stream_;
  std::vector<unsigned char> mode_stream_;
  std::vector<VCDAddress> verify_stream_;

 private:
  const char* verify_encode_position_;
  string::size_type last_encode_size_;
  const char*       last_decode_position_;
};

#ifdef GTEST_HAS_DEATH_TEST
// This synonym is needed for the tests that use ASSERT_DEATH
typedef VCDiffAddressCacheTest VCDiffAddressCacheDeathTest;
#endif  // GTEST_HAS_DEATH_TEST

// Having either or both cache size == 0 is acceptable
TEST_F(VCDiffAddressCacheTest, ZeroCacheSizes) {
  VCDiffAddressCache zero_cache(0, 0);
  EXPECT_TRUE(zero_cache.Init());
}

// VCD_MAX_MODES is the maximum number of modes, including SAME and HERE modes.
// So neither the SAME cache nor the HERE cache can be larger than
// (VCD_MAX_MODES - 2).
TEST_F(VCDiffAddressCacheTest, NearCacheSizeIsTooBig) {
  VCDiffAddressCache cache(VCD_MAX_MODES - 1, 0);
  EXPECT_FALSE(cache.Init());
}

TEST_F(VCDiffAddressCacheTest, SameCacheSizeIsTooBig) {
  VCDiffAddressCache cache(0, VCD_MAX_MODES - 1);
  EXPECT_FALSE(cache.Init());
}

TEST_F(VCDiffAddressCacheTest, CombinedSizesAreTooBig) {
  VCDiffAddressCache cache1((VCD_MAX_MODES / 2), (VCD_MAX_MODES / 2) - 1);
  EXPECT_FALSE(cache1.Init());
  VCDiffAddressCache cache2(VCD_MAX_MODES - 1, VCD_MAX_MODES- 1);
  EXPECT_FALSE(cache2.Init());
}

TEST_F(VCDiffAddressCacheTest, MaxLegalNearCacheSize) {
  VCDiffAddressCache cache(VCD_MAX_MODES - 2, 0);
  EXPECT_TRUE(cache.Init());
}

TEST_F(VCDiffAddressCacheTest, MaxLegalSameCacheSize) {
  VCDiffAddressCache cache(0, VCD_MAX_MODES - 2);
  EXPECT_TRUE(cache.Init());
}

TEST_F(VCDiffAddressCacheTest, MaxLegalCombinedSizes) {
  VCDiffAddressCache cache((VCD_MAX_MODES / 2) - 1, (VCD_MAX_MODES / 2) - 1);
  EXPECT_TRUE(cache.Init());
}

TEST_F(VCDiffAddressCacheTest, DestroyWithoutInitialization) {
  VCDiffAddressCache no_init_cache(4, 3);
  // Should be destroyed without crashing
}

TEST_F(VCDiffAddressCacheTest, DestroyDefaultWithoutInitialization) {
  VCDiffAddressCache no_init_cache;
  // Should be destroyed without crashing
}

TEST_F(VCDiffAddressCacheTest, CacheContentsInitiallyZero) {
  VCDAddress test_address = 0;
  // Check that caches are initially set to zero
  for (test_address = 0; test_address < 4; ++test_address) {
    EXPECT_EQ(0, cache_.NearAddress(test_address));
  }
  for (test_address = 0; test_address < 256 * 3; ++test_address) {
    EXPECT_EQ(0, cache_.SameAddress(test_address));
  }
}

// Inserts values 1, 2, ... , 10 into the cache and tests its entire
// contents for consistency.
//
TEST_F(VCDiffAddressCacheTest, InsertFirstTen) {
  VCDAddress test_address = 0;
  for (test_address = 1; test_address <= 10; ++test_address) {
    cache_.UpdateCache(test_address);
  }
  EXPECT_EQ(9, cache_.NearAddress(0));   // slot 0: 1 => 5 => 9
  EXPECT_EQ(10, cache_.NearAddress(1));  // slot 1: 2 => 6 => 10
  EXPECT_EQ(7, cache_.NearAddress(2));   // slot 2: 3 => 7
  EXPECT_EQ(8, cache_.NearAddress(3));   // slot 3: 4 => 8
  EXPECT_EQ(0, cache_.SameAddress(0));
  for (test_address = 1; test_address <= 10; ++test_address) {
    EXPECT_EQ(test_address, cache_.SameAddress(test_address));
  }
  for (test_address = 11; test_address < 256 * 3; ++test_address) {
    EXPECT_EQ(0, cache_.SameAddress(test_address));
  }
}

TEST_F(VCDiffAddressCacheTest, InsertIntMax) {
  cache_.UpdateCache(INT_MAX);
  EXPECT_EQ(INT_MAX, cache_.NearAddress(0));
  EXPECT_EQ(INT_MAX, cache_.SameAddress(INT_MAX % (256 * 3)));
  EXPECT_EQ(0, cache_.SameAddress((INT_MAX - 256) % (256 * 3)));
  EXPECT_EQ(0, cache_.SameAddress((INT_MAX - 512) % (256 * 3)));
}

// Exercises all four addressing mode types by encoding five values
// with EncodeAddress.
// Checks to see that the proper mode was selected in each case,
// and that the encoding is correct.
//
TEST_F(VCDiffAddressCacheTest, EncodeAddressModes) {
  TestEncode(0x0000FFFF, 0x10000000,  VCD_SELF_MODE, 3);
  TestEncode(0x10000000, 0x10000010,  VCD_HERE_MODE, 1);
  TestEncode(0x10000004, 0x10000020,  cache_.FirstNearMode() + 0x01, 1);
  TestEncode(0x0FFFFFFE, 0x10000030,  VCD_HERE_MODE, 1);
  TestEncode(0x10000004, 0x10000040,  cache_.FirstSameMode() + 0x01, 1);
  ExpectEncodedVarint(0xFFFF, 3);  // SELF mode: addr 0x0000FFFF
  ExpectEncodedVarint(0x10, 1);    // HERE mode: here - 0x10 = 0x10000000
  ExpectEncodedVarint(0x04, 1);    // NEAR cache #1:
                                   // last addr + 0x4 = 0x10000004
  ExpectEncodedVarint(0x32, 1);    // HERE mode: here - 0x32 = 0x0FFFFFFE
  ExpectEncodedByte(0x04);         // SAME cache #1: 0x10000004 hits
}

// Exercises all four addressing mode types by manually encoding six values
// and calling DecodeAddress on each one.
//
TEST_F(VCDiffAddressCacheTest, DecodeAddressModes) {
  ManualEncodeVarint(0xCAFE);
  ManualEncodeVarint(0xCAFE);
  ManualEncodeVarint(0x1000);
  ManualEncodeByte(0xFE);   // SAME mode uses a byte, not a Varint
  ManualEncodeVarint(0xFE);
  ManualEncodeVarint(0x1000);
  BeginDecode();
  EXPECT_EQ(0xCAFE,
            cache_.DecodeAddress(0x10000,
                                 VCD_SELF_MODE,
                                 &decode_position_,
                                 decode_position_end_));
  ExpectDecodedSizeInBytes(VarintBE<VCDAddress>::Length(0xCAFE));
  EXPECT_EQ(0x20000 - 0xCAFE,
            cache_.DecodeAddress(0x20000,
                                 VCD_HERE_MODE,
                                 &decode_position_,
                                 decode_position_end_));
  ExpectDecodedSizeInBytes(VarintBE<VCDAddress>::Length(0xCAFE));
  EXPECT_EQ(0xDAFE,
            cache_.DecodeAddress(0x30000,
                                 cache_.FirstNearMode(),
                                 &decode_position_,
                                 decode_position_end_));
  ExpectDecodedSizeInBytes(VarintBE<VCDAddress>::Length(0x1000));
  EXPECT_EQ(0xCAFE,
            cache_.DecodeAddress(0x40000,
                                 cache_.FirstSameMode() + (0xCA % 3),
                                 &decode_position_,
                                 decode_position_end_));
  ExpectDecodedSizeInBytes(sizeof(unsigned char));  // a byte, not a Varint
  EXPECT_EQ(0xFE,
            cache_.DecodeAddress(0x50000,
                                 VCD_SELF_MODE,
                                 &decode_position_,
                                 decode_position_end_));
  ExpectDecodedSizeInBytes(VarintBE<VCDAddress>::Length(0xFE));
  // NEAR mode #0 has been overwritten by fifth computed addr (wrap around)
  EXPECT_EQ(0x10FE,
            cache_.DecodeAddress(0x60000,
                                 cache_.FirstNearMode(),
                                 &decode_position_,
                                 decode_position_end_));
  ExpectDecodedSizeInBytes(VarintBE<VCDAddress>::Length(0x1000));
}

// Test with both cache sizes == 0.  The encoder should not choose
// a SAME or NEAR mode under these conditions.
TEST_F(VCDiffAddressCacheTest, EncodeAddressZeroCacheSizes) {
  VCDAddress encoded_addr = 0;
  VCDiffAddressCache zero_cache(0, 0);
  EXPECT_TRUE(zero_cache.Init());
  EXPECT_EQ(VCD_SELF_MODE,
            zero_cache.EncodeAddress(0x0000FFFF, 0x10000000, &encoded_addr));
  EXPECT_EQ(0xFFFF, encoded_addr);
  EXPECT_EQ(VCD_HERE_MODE,
            zero_cache.EncodeAddress(0x10000000, 0x10000010, &encoded_addr));
  EXPECT_EQ(0x10, encoded_addr);
  EXPECT_EQ(VCD_HERE_MODE,
            zero_cache.EncodeAddress(0x10000004, 0x10000020, &encoded_addr));
  EXPECT_EQ(0x1C, encoded_addr);
  EXPECT_EQ(VCD_HERE_MODE,
            zero_cache.EncodeAddress(0x0FFFFFFE, 0x10000030, &encoded_addr));
  EXPECT_EQ(0x32, encoded_addr);
  EXPECT_EQ(VCD_HERE_MODE,
            zero_cache.EncodeAddress(0x10000004, 0x10000040, &encoded_addr));
  EXPECT_EQ(0x3C, encoded_addr);
}

TEST_F(VCDiffAddressCacheTest, DecodeAddressZeroCacheSizes) {
  VCDiffAddressCache zero_cache(0, 0);
  EXPECT_TRUE(zero_cache.Init());
  ManualEncodeVarint(0xCAFE);
  ManualEncodeVarint(0xCAFE);
  ManualEncodeVarint(0xDAFE);
  BeginDecode();
  EXPECT_EQ(0xCAFE, zero_cache.DecodeAddress(0x10000,
                                             VCD_SELF_MODE,
                                             &decode_position_,
                                             decode_position_end_));
  ExpectDecodedSizeInBytes(VarintBE<VCDAddress>::Length(0xCAFE));
  EXPECT_EQ(0x20000 - 0xCAFE, zero_cache.DecodeAddress(0x20000,
                                                       VCD_HERE_MODE,
                                                       &decode_position_,
                                                       decode_position_end_));
  ExpectDecodedSizeInBytes(VarintBE<VCDAddress>::Length(0xCAFE));
  EXPECT_EQ(0xDAFE, zero_cache.DecodeAddress(0x30000,
                                             VCD_SELF_MODE,
                                             &decode_position_,
                                             decode_position_end_));
  ExpectDecodedSizeInBytes(VarintBE<VCDAddress>::Length(0xDAFE));
}

#ifdef GTEST_HAS_DEATH_TEST
TEST_F(VCDiffAddressCacheDeathTest, EncodeNegativeAddress) {
  VCDAddress dummy_encoded_address = 0;
  EXPECT_DEBUG_DEATH(cache_.EncodeAddress(-1, -1, &dummy_encoded_address),
                     "negative");
}

TEST_F(VCDiffAddressCacheDeathTest, EncodeAddressPastHereAddress) {
  VCDAddress dummy_encoded_address = 0;
  EXPECT_DEBUG_DEATH(cache_.EncodeAddress(0x100, 0x100, &dummy_encoded_address),
                     "address.*<.*here_address");
  EXPECT_DEBUG_DEATH(cache_.EncodeAddress(0x200, 0x100, &dummy_encoded_address),
                     "address.*<.*here_address");
}

TEST_F(VCDiffAddressCacheDeathTest, DecodeInvalidMode) {
  ManualEncodeVarint(0xCAFE);
  BeginDecode();
  EXPECT_DEBUG_DEATH(EXPECT_EQ(RESULT_ERROR,
                               cache_.DecodeAddress(0x10000000,
                                                    cache_.LastMode() + 1,
                                                    &decode_position_,
                                                    decode_position_end_)),
                     "mode");
  EXPECT_DEBUG_DEATH(EXPECT_EQ(RESULT_ERROR,
                               cache_.DecodeAddress(0x10000000,
                                                    0xFF,
                                                    &decode_position_,
                                                    decode_position_end_)),
                     "mode");
  ExpectDecodedSizeInBytes(0);  // Should not modify decode_position_
}

TEST_F(VCDiffAddressCacheDeathTest, DecodeZeroOrNegativeHereAddress) {
  ManualEncodeVarint(0xCAFE);
  ManualEncodeVarint(0xCAFE);
  BeginDecode();
  // Using a Debug build, the check will fail; using a Release build,
  // the check will not occur, and the SELF mode does not depend on
  // the value of here_address, so DecodeAddress() will succeed.
  EXPECT_DEBUG_DEATH(cache_.DecodeAddress(-1,
                                          VCD_SELF_MODE,
                                          &decode_position_,
                                          decode_position_end_),
                     "negative");
  // A zero value for here_address should not kill the decoder,
  // but instead should return an error value.  A delta file may contain
  // a window that has no source segment and that (erroneously)
  // uses a COPY instruction as its first instruction.  This should
  // cause an error to be reported, not a debug check failure.
  EXPECT_EQ(RESULT_ERROR, cache_.DecodeAddress(0,
                                         VCD_SELF_MODE,
                                         &decode_position_,
                                         decode_position_end_));
}
#endif  // GTEST_HAS_DEATH_TEST

TEST_F(VCDiffAddressCacheTest, DecodeAddressPastHereAddress) {
  ManualEncodeVarint(0xCAFE);
  BeginDecode();
  EXPECT_EQ(RESULT_ERROR, cache_.DecodeAddress(0x1000,
                                         VCD_SELF_MODE,
                                         &decode_position_,
                                         decode_position_end_));
  ExpectDecodedSizeInBytes(0);  // Should not modify decode_position_
}

TEST_F(VCDiffAddressCacheTest, HereModeAddressTooLarge) {
  ManualEncodeVarint(0x10001);  // here_address + 1
  BeginDecode();
  EXPECT_EQ(RESULT_ERROR, cache_.DecodeAddress(0x10000,
                                         VCD_HERE_MODE,
                                         &decode_position_,
                                         decode_position_end_));
  ExpectDecodedSizeInBytes(0);  // Should not modify decode_position_
}

TEST_F(VCDiffAddressCacheTest, NearModeAddressOverflow) {
  ManualEncodeVarint(0xCAFE);
  ManualEncodeVarint(0x7FFFFFFF);
  BeginDecode();
  EXPECT_EQ(0xCAFE, cache_.DecodeAddress(0x10000,
                                         VCD_SELF_MODE,
                                         &decode_position_,
                                         decode_position_end_));
  ExpectDecodedSizeInBytes(VarintBE<VCDAddress>::Length(0xCAFE));
  // Now decode a NEAR mode address of base address 0xCAFE
  // (the first decoded address) + offset 0x7FFFFFFF.  This will cause
  // an integer overflow and should signal an error.
  EXPECT_EQ(RESULT_ERROR, cache_.DecodeAddress(0x10000000,
                                         cache_.FirstNearMode(),
                                         &decode_position_,
                                         decode_position_end_));
  ExpectDecodedSizeInBytes(0);  // Should not modify decode_position_
}

// A Varint should contain at most 9 bytes that have their continuation bit
// (the uppermost, or 7 bit) set.  A longer string of bytes that all have
// bit 7 set is not a valid Varint.  Try to parse such a string as a Varint
// and confirm that it does not run off the end of the input buffer and
// it returns an error value (RESULT_ERROR).
//
TEST_F(VCDiffAddressCacheTest, DecodeInvalidVarint) {
  address_stream_.clear();
  // Write 512 0xFE bytes
  address_stream_.append(512, static_cast<char>(0xFE));
  BeginDecode();
  EXPECT_EQ(RESULT_ERROR, cache_.DecodeAddress(0x10000000,
                                         VCD_SELF_MODE,
                                         &decode_position_,
                                         decode_position_end_));
  ExpectDecodedSizeInBytes(0);  // Should not modify decode_position_
}

// If only part of a Varint appears in the data to be decoded,
// then DecodeAddress should return RESULT_END_OF_DATA,
// which means that the Varint *may* be valid if there is more
// data expected to be returned.
//
TEST_F(VCDiffAddressCacheTest, DecodePartialVarint) {
  address_stream_.clear();
  ManualEncodeByte(0xFE);
  ManualEncodeByte(0xFE);
  ManualEncodeByte(0xFE);
  BeginDecode();
  EXPECT_EQ(RESULT_END_OF_DATA,
            cache_.DecodeAddress(0x10000000,
                                 VCD_SELF_MODE,
                                 &decode_position_,
                                 decode_position_end_));
  ExpectDecodedSizeInBytes(0);  // Should not modify decode_position_
  // Now add the missing last byte (supposedly read from a stream of data)
  // and verify that the Varint is now valid.
  ManualEncodeByte(0x01);  // End the Varint with an additional byte
  BeginDecode();  // Reset read position to start of data
  EXPECT_EQ(0xFDFBF01,
            cache_.DecodeAddress(0x10000000,
                                 VCD_SELF_MODE,
                                 &decode_position_,
                                 decode_position_end_));
  ExpectDecodedSizeInBytes(4);  // ManualEncodeByte was called for 4 byte values
}

#ifdef GTEST_HAS_DEATH_TEST
TEST_F(VCDiffAddressCacheDeathTest, DecodeBadMode) {
  ManualEncodeVarint(0xCAFE);
  BeginDecode();
  EXPECT_DEBUG_DEATH(EXPECT_EQ(RESULT_ERROR,
                               cache_.DecodeAddress(0x10000,
                                                    cache_.LastMode() + 1,
                                                    &decode_position_,
                                                    decode_position_end_)),
                     "maximum");
  ExpectDecodedSizeInBytes(0);
}
#endif  // GTEST_HAS_DEATH_TEST

TEST_F(VCDiffAddressCacheTest, DecodeInvalidHereAddress) {
  ManualEncodeVarint(0x10001);  // offset larger than here_address
  BeginDecode();
  EXPECT_EQ(RESULT_ERROR, cache_.DecodeAddress(0x10000,
                                         VCD_HERE_MODE,
                                         &decode_position_,
                                         decode_position_end_));
  ExpectDecodedSizeInBytes(0);
}

TEST_F(VCDiffAddressCacheTest, DecodeInvalidNearAddress) {
  ManualEncodeVarint(0xCAFE);
  ManualEncodeVarint(INT_MAX);  // offset will cause integer overflow
  BeginDecode();
  EXPECT_EQ(0xCAFE,
            cache_.DecodeAddress(0x10000,
                                 VCD_SELF_MODE,
                                 &decode_position_,
                                 decode_position_end_));
  ExpectDecodedSizeInBytes(VarintBE<VCDAddress>::Length(0xCAFE));
  EXPECT_EQ(RESULT_ERROR, cache_.DecodeAddress(0x10000,
                                         cache_.FirstNearMode(),
                                         &decode_position_,
                                         decode_position_end_));
  ExpectDecodedSizeInBytes(0);
}

void VCDiffAddressCacheTest::BM_Setup(int test_size) {
  mode_stream_.resize(test_size);
  verify_stream_.resize(test_size);
  VCDAddress here_address = 1;
  srand(1);
  for (int i = 0; i < test_size; ++i) {
    verify_stream_[i] = PortableRandomInRange(here_address - 1);
    here_address += 4;
  }
  BM_CacheEncode(1, test_size);  // populate large_address_stream_, mode_stream_
}

void VCDiffAddressCacheTest::BM_CacheEncode(int iterations, int test_size) {
  VCDAddress here_address = 1;
  VCDAddress encoded_addr = 0;
  for (int test_iteration = 0; test_iteration < iterations; ++test_iteration) {
    cache_.Init();
    large_address_stream_.clear();
    here_address = 1;
    for (int i = 0; i < test_size; ++i) {
      const unsigned char mode = cache_.EncodeAddress(verify_stream_[i],
                                                      here_address,
                                                      &encoded_addr);
      if (cache_.WriteAddressAsVarintForMode(mode)) {
        VarintBE<VCDAddress>::AppendToString(encoded_addr,
                                             &large_address_stream_);
      } else {
        EXPECT_GT(256, encoded_addr);
        large_address_stream_.push_back(
            static_cast<unsigned char>(encoded_addr));
      }
      mode_stream_[i] = mode;
      here_address += 4;
    }
  }
}

void VCDiffAddressCacheTest::BM_CacheDecode(int iterations, int test_size) {
  VCDAddress here_address = 1;
  for (int test_iteration = 0; test_iteration < iterations; ++test_iteration) {
    cache_.Init();
    const char* large_decode_pointer = large_address_stream_.data();
    const char* const end_of_encoded_data =
        large_decode_pointer + large_address_stream_.size();
    here_address = 1;
    for (int i = 0; i < test_size; ++i) {
      EXPECT_EQ(verify_stream_[i],
                cache_.DecodeAddress(here_address,
                                     mode_stream_[i],
                                     &large_decode_pointer,
                                     end_of_encoded_data));
      here_address += 4;
    }
    EXPECT_EQ(end_of_encoded_data, large_decode_pointer);
  }
}

TEST_F(VCDiffAddressCacheTest, PerformanceTest) {
  const int test_size = 20 * 1024;  // 20K random encode/decode operations
  const int num_iterations = 40;  // run test 40 times and take average
  BM_Setup(test_size);
  {
    CycleTimer encode_timer;
    encode_timer.Start();
    BM_CacheEncode(num_iterations, test_size);
    encode_timer.Stop();
    double encode_time_in_ms =
        static_cast<double>(encode_timer.GetInUsec()) / 1000;
    std::cout << "Time to encode: "
              << (encode_time_in_ms / num_iterations) << " ms" << std::endl;
  }
  {
    CycleTimer decode_timer;
    decode_timer.Start();
    BM_CacheDecode(num_iterations, test_size);
    decode_timer.Stop();
    double decode_time_in_ms =
        static_cast<double>(decode_timer.GetInUsec()) / 1000;
    std::cout << "Time to decode: "
              << (decode_time_in_ms / num_iterations) << " ms" << std::endl;
  }
}

}  // unnamed namespace
}  // namespace open_vcdiff
