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
#include <string>
#include "google/output_string.h"
#include "testing.h"

#ifdef HAVE_EXT_ROPE
#error #include <ext/rope>
#include "output_string_crope.h"
#endif  // HAVE_EXT_ROPE

namespace open_vcdiff {
namespace {

class OutputStringTest : public testing::Test {
 public:
  typedef std::string string;

  OutputStringTest() : string_("ab"), output_string_(&string_) { }

  virtual ~OutputStringTest() { }

 protected:
  string string_;
  OutputString<string> output_string_;
};

TEST_F(OutputStringTest, Append) {
  output_string_.append("cdef", 2);
  EXPECT_EQ("abcd", string_);
}

TEST_F(OutputStringTest, Clear) {
  output_string_.clear();
  EXPECT_EQ("", string_);
}

TEST_F(OutputStringTest, PushBack) {
  output_string_.push_back('c');
  EXPECT_EQ("abc", string_);
}

TEST_F(OutputStringTest, Reserve) {
  const size_t initial_capacity = string_.capacity();
  string_.resize(string_.capacity());
  EXPECT_EQ(initial_capacity, string_.capacity());
  output_string_.ReserveAdditionalBytes(1);
  EXPECT_LE(initial_capacity + 1, string_.capacity());
}

TEST_F(OutputStringTest, Size) {
  EXPECT_EQ(string_.size(), output_string_.size());
  string_.push_back('c');
  EXPECT_EQ(string_.size(), output_string_.size());
  string_.clear();
  EXPECT_EQ(string_.size(), output_string_.size());
}

#ifdef HAVE_EXT_ROPE
class OutputCRopeTest : public testing::Test {
 public:
  typedef __gnu_cxx::crope crope;

  OutputCRopeTest() : crope_("ab"), output_crope_(&crope_) { }

  virtual ~OutputCRopeTest() { }

 protected:
  crope crope_;
  OutputCrope output_crope_;
};

TEST_F(OutputCRopeTest, Append) {
  output_crope_.append("cdef", 2);
  crope expected_abcd("abcd");
  EXPECT_EQ(expected_abcd, crope_);
}

TEST_F(OutputCRopeTest, Clear) {
  output_crope_.clear();
  crope expected_empty;
  EXPECT_EQ(expected_empty, crope_);
}

TEST_F(OutputCRopeTest, PushBack) {
  output_crope_.push_back('c');
  crope expected_abc("abc");
  EXPECT_EQ(expected_abc, crope_);
}

TEST_F(OutputCRopeTest, Size) {
  EXPECT_EQ(crope_.size(), output_crope_.size());
  crope_.push_back('c');
  EXPECT_EQ(crope_.size(), output_crope_.size());
  crope_.clear();
  EXPECT_EQ(crope_.size(), output_crope_.size());
}
#endif  // HAVE_EXT_ROPE

}  // anonymous namespace
}  // namespace open_vcdiff
