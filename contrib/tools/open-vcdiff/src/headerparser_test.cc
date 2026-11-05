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
#include "headerparser.h"
#include <stdlib.h>  // rand, srand
#include <string>
#include <vector>
#include "testing.h"
#include "varint_bigendian.h"

namespace open_vcdiff {
namespace {  // anonymous

using std::vector;

class VCDiffHeaderParserTest : public testing::Test {
 protected:
  typedef std::string string;

  static const int kTestSize = 1024;

  VCDiffHeaderParserTest() : parser(NULL) { }

  virtual ~VCDiffHeaderParserTest() {
    delete parser;
  }

  virtual void SetUp() {
    srand(1);  // make sure each test uses the same data set
  }

  void StartParsing() {
    parser = new VCDiffHeaderParser(
        encoded_buffer_.data(),
        encoded_buffer_.data() + encoded_buffer_.size());
    EXPECT_EQ(encoded_buffer_.data(), parser->UnparsedData());
  }

  void VerifyByte(unsigned char expected_value) {
    unsigned char decoded_byte = 0;
    const char* prior_position = parser->UnparsedData();
    EXPECT_TRUE(parser->ParseByte(&decoded_byte));
    EXPECT_EQ(expected_value, decoded_byte);
    EXPECT_EQ(RESULT_SUCCESS, parser->GetResult());
    EXPECT_EQ(prior_position + sizeof(unsigned char),
              parser->UnparsedData());
  }

  void VerifyInt32(int32_t expected_value) {
    int32_t decoded_integer = 0;
    const char* prior_position = parser->UnparsedData();
    EXPECT_TRUE(parser->ParseInt32("decoded int32", &decoded_integer));
    EXPECT_EQ(expected_value, decoded_integer);
    EXPECT_EQ(RESULT_SUCCESS, parser->GetResult());
    EXPECT_EQ(prior_position + VarintBE<int32_t>::Length(decoded_integer),
              parser->UnparsedData());
  }

  void VerifyUInt32(uint32_t expected_value) {
    uint32_t decoded_integer = 0;
    const char* prior_position = parser->UnparsedData();
    EXPECT_TRUE(parser->ParseUInt32("decoded uint32", &decoded_integer));
    EXPECT_EQ(expected_value, decoded_integer);
    EXPECT_EQ(RESULT_SUCCESS, parser->GetResult());
    EXPECT_EQ(prior_position + VarintBE<int64_t>::Length(decoded_integer),
              parser->UnparsedData());
  }

  void VerifyChecksum(VCDChecksum expected_value) {
    VCDChecksum decoded_checksum = 0;
    const char* prior_position = parser->UnparsedData();
    EXPECT_TRUE(parser->ParseChecksum("decoded checksum", &decoded_checksum));
    EXPECT_EQ(expected_value, decoded_checksum);
    EXPECT_EQ(RESULT_SUCCESS, parser->GetResult());
    EXPECT_EQ(prior_position + VarintBE<int64_t>::Length(decoded_checksum),
              parser->UnparsedData());
  }

  string encoded_buffer_;
  VCDiffHeaderParser* parser;
};

TEST_F(VCDiffHeaderParserTest, ParseRandomBytes) {
  vector<unsigned char> byte_values;
  for (int i = 0; i < kTestSize; ++i) {
    unsigned char random_byte = PortableRandomInRange<unsigned char>(0xFF);
    encoded_buffer_.push_back(random_byte);
    byte_values.push_back(random_byte);
  }
  StartParsing();
  for (int position = 0; position < kTestSize; ++position) {
    VerifyByte(byte_values[position]);
  }
  unsigned char decoded_byte = 0;
  EXPECT_FALSE(parser->ParseByte(&decoded_byte));
  EXPECT_EQ(RESULT_END_OF_DATA, parser->GetResult());
  EXPECT_EQ(encoded_buffer_.data() + encoded_buffer_.size(),
            parser->UnparsedData());
}

TEST_F(VCDiffHeaderParserTest, ParseRandomInt32) {
  vector<int32_t> integer_values;
  for (int i = 0; i < kTestSize; ++i) {
    int32_t random_integer = PortableRandomInRange<int32_t>(0x7FFFFFFF);
    VarintBE<int32_t>::AppendToString(random_integer, &encoded_buffer_);
    integer_values.push_back(random_integer);
  }
  StartParsing();
  for (int i = 0; i < kTestSize; ++i) {
    VerifyInt32(integer_values[i]);
  }
  int32_t decoded_integer = 0;
  EXPECT_FALSE(parser->ParseInt32("decoded integer", &decoded_integer));
  EXPECT_EQ(RESULT_END_OF_DATA, parser->GetResult());
  EXPECT_EQ(encoded_buffer_.data() + encoded_buffer_.size(),
            parser->UnparsedData());
}

TEST_F(VCDiffHeaderParserTest, ParseRandomUInt32) {
  vector<uint32_t> integer_values;
  for (int i = 0; i < kTestSize; ++i) {
    uint32_t random_integer = PortableRandomInRange<uint32_t>(0xFFFFFFFF);
    VarintBE<int64_t>::AppendToString(random_integer, &encoded_buffer_);
    integer_values.push_back(random_integer);
  }
  StartParsing();
  uint32_t decoded_integer = 0;
  for (int i = 0; i < kTestSize; ++i) {
    VerifyUInt32(integer_values[i]);
  }
  EXPECT_FALSE(parser->ParseUInt32("decoded integer", &decoded_integer));
  EXPECT_EQ(RESULT_END_OF_DATA, parser->GetResult());
  EXPECT_EQ(encoded_buffer_.data() + encoded_buffer_.size(),
            parser->UnparsedData());
}

TEST_F(VCDiffHeaderParserTest, ParseRandomChecksum) {
  vector<VCDChecksum> checksum_values;
  for (int i = 0; i < kTestSize; ++i) {
    VCDChecksum random_checksum =
        PortableRandomInRange<VCDChecksum>(0xFFFFFFFF);
    VarintBE<int64_t>::AppendToString(random_checksum, &encoded_buffer_);
    checksum_values.push_back(random_checksum);
  }
  StartParsing();
  for (int i = 0; i < kTestSize; ++i) {
    VerifyChecksum(checksum_values[i]);
  }
  VCDChecksum decoded_checksum = 0;
  EXPECT_FALSE(parser->ParseChecksum("decoded checksum", &decoded_checksum));
  EXPECT_EQ(RESULT_END_OF_DATA, parser->GetResult());
  EXPECT_EQ(encoded_buffer_.data() + encoded_buffer_.size(),
            parser->UnparsedData());
}

TEST_F(VCDiffHeaderParserTest, ParseMixed) {
  VarintBE<int64_t>::AppendToString(0xCAFECAFE, &encoded_buffer_);
  encoded_buffer_.push_back(0xFF);
  VarintBE<int32_t>::AppendToString(0x02020202, &encoded_buffer_);
  VarintBE<int64_t>::AppendToString(0xCAFECAFE, &encoded_buffer_);
  encoded_buffer_.push_back(0xFF);
  encoded_buffer_.push_back(0xFF);
  StartParsing();
  VerifyUInt32(0xCAFECAFE);
  VerifyByte(0xFF);
  VerifyInt32(0x02020202);
  VerifyChecksum(0xCAFECAFE);
  int32_t incomplete_int32 = 0;
  EXPECT_FALSE(parser->ParseInt32("incomplete Varint", &incomplete_int32));
  EXPECT_EQ(0, incomplete_int32);
  EXPECT_EQ(RESULT_END_OF_DATA, parser->GetResult());
  EXPECT_EQ(encoded_buffer_.data() + encoded_buffer_.size() - 2,
            parser->UnparsedData());
}

TEST_F(VCDiffHeaderParserTest, ParseInvalidVarint) {
  // Start with a byte that has the continuation bit plus a high-order bit set
  encoded_buffer_.append(1, static_cast<char>(0xC0));
  // Add too many bytes with continuation bits
  encoded_buffer_.append(6, static_cast<char>(0x80));
  StartParsing();
  int32_t invalid_int32 = 0;
  EXPECT_FALSE(parser->ParseInt32("invalid Varint", &invalid_int32));
  EXPECT_EQ(0, invalid_int32);
  EXPECT_EQ(RESULT_ERROR, parser->GetResult());
  EXPECT_EQ(encoded_buffer_.data(), parser->UnparsedData());
  // After the parse failure, any other call to Parse... should return an error,
  // even though there is still a byte that could be read as valid.
  unsigned char decoded_byte = 0;
  EXPECT_FALSE(parser->ParseByte(&decoded_byte));
  EXPECT_EQ(0, decoded_byte);
  EXPECT_EQ(RESULT_ERROR, parser->GetResult());
  EXPECT_EQ(encoded_buffer_.data(), parser->UnparsedData());
}

}  //  namespace open_vcdiff
}  //  anonymous namespace
