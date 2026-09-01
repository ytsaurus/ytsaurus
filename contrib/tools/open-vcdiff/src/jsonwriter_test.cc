// Copyright 2009 The open-vcdiff Authors. All Rights Reserved.
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
//
// Unit tests for the class JSONCodeTableWriter, found in jsonwriter.h.

#include <config.h>
#include "testing.h"
#include "vcdiff_defs.h"
#include "google/jsonwriter.h"
#include "google/output_string.h"

namespace open_vcdiff {
namespace {

class JSONWriterTest : public testing::Test {
 protected:
  typedef std::string string;

  JSONWriterTest()
      : output_string_(&out_) {
    EXPECT_TRUE(coder_.Init(NULL, 0));
    coder_.WriteHeader(&output_string_, 0);
  }

  virtual ~JSONWriterTest() { }

  string out_;
  OutputString<string> output_string_;
  JSONCodeTableWriter coder_;
};

TEST_F(JSONWriterTest, Null) {
  coder_.FinishEncoding(&output_string_);
  EXPECT_EQ("", out_);
}

TEST_F(JSONWriterTest, Empty) {
  coder_.Output(&output_string_);
  coder_.FinishEncoding(&output_string_);
  EXPECT_EQ("[]", out_);
}

TEST_F(JSONWriterTest, Add) {
  coder_.Add("123", 3);
  coder_.Output(&output_string_);
  coder_.FinishEncoding(&output_string_);
  EXPECT_EQ("[\"123\"]", out_);
}

TEST_F(JSONWriterTest, Copy) {
  coder_.Copy(3, 5);
  coder_.Output(&output_string_);
  coder_.FinishEncoding(&output_string_);
  EXPECT_EQ("[3,5]", out_);
}

TEST_F(JSONWriterTest, Run) {
  coder_.Run(3, 'a');
  coder_.Output(&output_string_);
  coder_.FinishEncoding(&output_string_);
  EXPECT_EQ("[\"aaa\"]", out_);
}

TEST_F(JSONWriterTest, AddEscaped) {
  coder_.Add("\n\b\r", 3);
  coder_.Output(&output_string_);
  coder_.FinishEncoding(&output_string_);
  EXPECT_EQ("[\"\\n\\b\\r\"]", out_);
}

TEST_F(JSONWriterTest, AddCopyAdd) {
  coder_.Add("abc", 3);
  coder_.Copy(3, 5);
  coder_.Add("defghij", 7);
  coder_.Output(&output_string_);
  coder_.FinishEncoding(&output_string_);
  EXPECT_EQ("[\"abc\",3,5,\"defghij\"]", out_);
}

TEST_F(JSONWriterTest, AddOutputAddOutputToSameString) {
  coder_.Add("abc", 3);
  coder_.Output(&output_string_);
  EXPECT_EQ("[\"abc\"", out_);
  coder_.Add("def", 3);
  coder_.Output(&output_string_);
  coder_.FinishEncoding(&output_string_);
  EXPECT_EQ("[\"abc\",\"def\"]", out_);
}

TEST_F(JSONWriterTest, AddOutputAddOutputToDifferentString) {
  coder_.Add("abc", 3);
  coder_.Output(&output_string_);
  coder_.FinishEncoding(&output_string_);
  EXPECT_EQ("[\"abc\"]", out_);
  string out2;
  OutputString<string> output_string2(&out2);
  coder_.Init(NULL, 0);
  coder_.Add("def", 3);
  coder_.Output(&output_string2);
  coder_.FinishEncoding(&output_string2);
  EXPECT_EQ("[\"def\"]", out2);
}

}  // unnamed namespace
}  // namespace open_vcdiff
