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
//
// Unit tests for the class VCDiffCodeTableWriter, found in encodetable.h.

#include <config.h>
#include <string.h>  // strlen
#include <algorithm>
#include <string>
#include "addrcache.h"  // VCDiffAddressCache::kDefaultNearCacheSize
#include "checksum.h"
#include "codetable.h"
#include "google/output_string.h"
#include "google/encodetable.h"
#include "testing.h"
#include "vcdiff_defs.h"

namespace open_vcdiff {
namespace {

class CodeTableWriterTest : public testing::Test {
 protected:
  typedef std::string string;

  CodeTableWriterTest()
      : standard_writer(false, false),
        interleaved_writer(true, false),
        exercise_writer(true, false,
                        VCDiffAddressCache::kDefaultNearCacheSize,
                        VCDiffAddressCache::kDefaultSameCacheSize,
                        *g_exercise_code_table_, kLastExerciseMode),
        output_string(&out),
        out_index(0) { }

  virtual ~CodeTableWriterTest() { }

  static void AddExerciseOpcode(unsigned char inst1,
                                unsigned char mode1,
                                unsigned char size1,
                                unsigned char inst2,
                                unsigned char mode2,
                                unsigned char size2,
                                int opcode) {
    g_exercise_code_table_->inst1[opcode] = inst1;
    g_exercise_code_table_->mode1[opcode] = mode1;
    g_exercise_code_table_->size1[opcode] = (inst1 == VCD_NOOP) ? 0 : size1;
    g_exercise_code_table_->inst2[opcode] = inst2;
    g_exercise_code_table_->mode2[opcode] = mode2;
    g_exercise_code_table_->size2[opcode] = (inst2 == VCD_NOOP) ? 0 : size2;
  }

  static void SetUpTestCase() {
    g_exercise_code_table_ = new VCDiffCodeTableData;
    int opcode = 0;
    for (unsigned char inst_mode1 = 0;
         inst_mode1 <= VCD_LAST_INSTRUCTION_TYPE + kLastExerciseMode;
         ++inst_mode1) {
      unsigned char inst1 = inst_mode1;
      unsigned char mode1 = 0;
      if (inst_mode1 > VCD_COPY) {
        inst1 = VCD_COPY;
        mode1 = inst_mode1 - VCD_COPY;
      }
      for (unsigned char inst_mode2 = 0;
           inst_mode2 <= VCD_LAST_INSTRUCTION_TYPE + kLastExerciseMode;
           ++inst_mode2) {
        unsigned char inst2 = inst_mode2;
        unsigned char mode2 = 0;
        if (inst_mode2 > VCD_COPY) {
          inst2 = VCD_COPY;
          mode2 = inst_mode2 - VCD_COPY;
        }
        AddExerciseOpcode(inst1, mode1, 0, inst2, mode2, 0, opcode++);
        AddExerciseOpcode(inst1, mode1, 0, inst2, mode2, 255, opcode++);
        AddExerciseOpcode(inst1, mode1, 255, inst2, mode2, 0, opcode++);
        AddExerciseOpcode(inst1, mode1, 255, inst2, mode2, 255, opcode++);
      }
    }
    // This is a CHECK rather than an EXPECT because it validates only
    // the logic of the test, not of the code being tested.
    CHECK_EQ(VCDiffCodeTableData::kCodeTableSize, opcode);

    EXPECT_TRUE(g_exercise_code_table_->Validate(kLastExerciseMode));
  }

  static void TearDownTestCase() {
    delete g_exercise_code_table_;
  }

  void ExpectByte(unsigned char b) {
    EXPECT_EQ(b, static_cast<unsigned char>(out[out_index]));
    ++out_index;
  }

  void ExpectString(const char* s) {
    const size_t size = strlen(s);  // don't include terminating NULL char
    EXPECT_EQ(s, string(out.data() + out_index, size));
    out_index += size;
  }

  void ExpectNoMoreBytes() {
    EXPECT_EQ(out_index, out.size());
  }

  // This value is designed so that the total number of inst values and modes
  // will equal 8 (VCD_NOOP, VCD_ADD, VCD_RUN, VCD_COPY modes 0 - 4).
  // Eight combinations of inst and mode, times two possible size values,
  // squared (because there are two instructions per opcode), makes
  // exactly 256 possible instruction combinations, which fits kCodeTableSize
  // (the number of opcodes in the table.)
  static const int kLastExerciseMode = 4;

  // A code table that exercises as many combinations as possible:
  // 2 instructions, each is a NOOP, ADD, RUN, or one of 5 copy modes
  // (== 8 total combinations of inst and mode), and each has
  // size == 0 or 255 (2 possibilities.)
  static VCDiffCodeTableData* g_exercise_code_table_;

  // The code table writer for standard encoding, default code table.
  VCDiffCodeTableWriter standard_writer;

  // The code table writer for interleaved encoding, default code table.
  VCDiffCodeTableWriter interleaved_writer;

  // The code table writer corresponding to g_exercise_code_table_
  // (interleaved encoding).
  VCDiffCodeTableWriter exercise_writer;

  // Destination for VCDiffCodeTableWriter::Output()
  string out;
  OutputString<string> output_string;
  size_t out_index;
};

VCDiffCodeTableData* CodeTableWriterTest::g_exercise_code_table_;

#ifdef GTEST_HAS_DEATH_TEST
typedef CodeTableWriterTest CodeTableWriterDeathTest;
#endif  // GTEST_HAS_DEATH_TEST

#ifdef GTEST_HAS_DEATH_TEST
TEST_F(CodeTableWriterDeathTest, WriterAddWithoutInit) {
#ifndef NDEBUG
  // This condition is only checked in the debug build.
  EXPECT_DEBUG_DEATH(standard_writer.Add("Hello", 5),
                     "Init");
#endif  // !NDEBUG
}

TEST_F(CodeTableWriterDeathTest, WriterRunWithoutInit) {
#ifndef NDEBUG
  // This condition is only checked in the debug build.
  EXPECT_DEBUG_DEATH(standard_writer.Run(3, 'a'),
                     "Init");
#endif  // !NDEBUG
}

TEST_F(CodeTableWriterDeathTest, WriterCopyWithoutInit) {
#ifndef NDEBUG
  // This condition is only checked in the debug build.
  EXPECT_DEBUG_DEATH(standard_writer.Copy(6, 5),
                     "Init");
#endif  // !NDEBUG
}
#endif  // GTEST_HAS_DEATH_TEST

// Output() without Init() is harmless, but will produce no output.
TEST_F(CodeTableWriterTest, WriterOutputWithoutInit) {
  standard_writer.Output(&output_string);
  EXPECT_TRUE(out.empty());
}

TEST_F(CodeTableWriterTest, WriterEncodeNothing) {
  EXPECT_TRUE(standard_writer.Init("", 0));
  standard_writer.Output(&output_string);
  // The writer should know not to append a delta file window
  // if nothing was encoded.
  EXPECT_TRUE(out.empty());

  out.clear();
  // we can pass nullptr as dictionary as long as sdch3dm is not used
  EXPECT_TRUE(interleaved_writer.Init(NULL, 0x10));
  interleaved_writer.Output(&output_string);
  EXPECT_TRUE(out.empty());

  out.clear();
  EXPECT_TRUE(exercise_writer.Init(NULL, 0x20));
  exercise_writer.Output(&output_string);
  EXPECT_TRUE(out.empty());
}

TEST_F(CodeTableWriterTest, StandardWriterEncodeAdd) {
  EXPECT_TRUE(standard_writer.Init(NULL, 0x11));
  standard_writer.Add("foo", 3);
  standard_writer.Output(&output_string);
  ExpectByte(VCD_SOURCE);  // Win_Indicator: VCD_SOURCE (dictionary)
  ExpectByte(0x11);  // Source segment size: dictionary length
  ExpectByte(0x00);  // Source segment position: start of dictionary
  ExpectByte(0x09);  // Length of the delta encoding
  ExpectByte(0x03);  // Size of the target window
  ExpectByte(0x00);  // Delta_indicator (no compression)
  ExpectByte(0x03);  // length of data for ADDs and RUNs
  ExpectByte(0x01);  // length of instructions section
  ExpectByte(0x00);  // length of addresses for COPYs
  ExpectString("foo");
  ExpectByte(0x04);  // ADD(3) opcode
  ExpectNoMoreBytes();
}

TEST_F(CodeTableWriterTest, ExerciseWriterEncodeAdd) {
  EXPECT_TRUE(exercise_writer.Init(NULL, 0x11));
  exercise_writer.Add("foo", 3);
  exercise_writer.Output(&output_string);
  ExpectByte(VCD_SOURCE);  // Win_Indicator: VCD_SOURCE (dictionary)
  ExpectByte(0x11);  // Source segment size: dictionary length
  ExpectByte(0x00);  // Source segment position: start of dictionary
  ExpectByte(0x0A);  // Length of the delta encoding
  ExpectByte(0x03);  // Size of the target window
  ExpectByte(0x00);  // Delta_indicator (no compression)
  ExpectByte(0x00);  // length of data for ADDs and RUNs
  ExpectByte(0x05);  // length of instructions section
  ExpectByte(0x00);  // length of addresses for COPYs
  ExpectByte(0x04);  // Opcode: NOOP + ADD(0)
  ExpectByte(0x03);  // Size of ADD (3)
  ExpectString("foo");
}

TEST_F(CodeTableWriterTest, StandardWriterEncodeRun) {
  EXPECT_TRUE(standard_writer.Init(NULL, 0x11));
  standard_writer.Run(3, 'a');
  standard_writer.Output(&output_string);
  ExpectByte(VCD_SOURCE);  // Win_Indicator: VCD_SOURCE (dictionary)
  ExpectByte(0x11);  // Source segment size: dictionary length
  ExpectByte(0x00);  // Source segment position: start of dictionary
  ExpectByte(0x08);  // Length of the delta encoding
  ExpectByte(0x03);  // Size of the target window
  ExpectByte(0x00);  // Delta_indicator (no compression)
  ExpectByte(0x01);  // length of data for ADDs and RUNs
  ExpectByte(0x02);  // length of instructions section
  ExpectByte(0x00);  // length of addresses for COPYs
  ExpectByte('a');
  ExpectByte(0x00);  // RUN(0) opcode
  ExpectByte(0x03);  // Size of RUN (3)
  ExpectNoMoreBytes();
}

TEST_F(CodeTableWriterTest, ExerciseWriterEncodeRun) {
  EXPECT_TRUE(exercise_writer.Init(NULL, 0x11));
  exercise_writer.Run(3, 'a');
  exercise_writer.Output(&output_string);
  ExpectByte(VCD_SOURCE);  // Win_Indicator: VCD_SOURCE (dictionary)
  ExpectByte(0x11);  // Source segment size: dictionary length
  ExpectByte(0x00);  // Source segment position: start of dictionary
  ExpectByte(0x08);  // Length of the delta encoding
  ExpectByte(0x03);  // Size of the target window
  ExpectByte(0x00);  // Delta_indicator (no compression)
  ExpectByte(0x00);  // length of data for ADDs and RUNs
  ExpectByte(0x03);  // length of instructions section
  ExpectByte(0x00);  // length of addresses for COPYs
  ExpectByte(0x08);  // Opcode: NOOP + RUN(0)
  ExpectByte(0x03);  // Size of RUN (3)
  ExpectByte('a');
  ExpectNoMoreBytes();
}

TEST_F(CodeTableWriterTest, StandardWriterEncodeCopy) {
  EXPECT_TRUE(standard_writer.Init(NULL, 0x11));
  standard_writer.Copy(2, 8);
  standard_writer.Copy(2, 8);
  standard_writer.Output(&output_string);
  ExpectByte(VCD_SOURCE);  // Win_Indicator: VCD_SOURCE (dictionary)
  ExpectByte(0x11);  // Source segment size: dictionary length
  ExpectByte(0x00);  // Source segment position: start of dictionary
  ExpectByte(0x09);  // Length of the delta encoding
  ExpectByte(0x10);  // Size of the target window
  ExpectByte(0x00);  // Delta_indicator (no compression)
  ExpectByte(0x00);  // length of data for ADDs and RUNs
  ExpectByte(0x02);  // length of instructions section
  ExpectByte(0x02);  // length of addresses for COPYs
  ExpectByte(0x18);  // COPY mode SELF, size 8
  ExpectByte(0x78);  // COPY mode SAME(0), size 8
  ExpectByte(0x02);  // COPY address (2)
  ExpectByte(0x02);  // COPY address (2)
  ExpectNoMoreBytes();
}

// The exercise code table can't be used to test how the code table
// writer encodes COPY instructions because the code table writer
// always uses the default cache sizes, which exceed the maximum mode
// used in the exercise table.
TEST_F(CodeTableWriterTest, InterleavedWriterEncodeCopy) {
  EXPECT_TRUE(interleaved_writer.Init(NULL, 0x11));
  interleaved_writer.Copy(2, 8);
  interleaved_writer.Copy(2, 8);
  interleaved_writer.Output(&output_string);
  ExpectByte(VCD_SOURCE);  // Win_Indicator: VCD_SOURCE (dictionary)
  ExpectByte(0x11);  // Source segment size: dictionary length
  ExpectByte(0x00);  // Source segment position: start of dictionary
  ExpectByte(0x09);  // Length of the delta encoding
  ExpectByte(0x10);  // Size of the target window
  ExpectByte(0x00);  // Delta_indicator (no compression)
  ExpectByte(0x00);  // length of data for ADDs and RUNs
  ExpectByte(0x04);  // length of instructions section
  ExpectByte(0x00);  // length of addresses for COPYs
  ExpectByte(0x18);  // COPY mode SELF, size 8
  ExpectByte(0x02);  // COPY address (2)
  ExpectByte(0x78);  // COPY mode SAME(0), size 8
  ExpectByte(0x02);  // COPY address (2)
  ExpectNoMoreBytes();
}

TEST_F(CodeTableWriterTest, StandardWriterEncodeCombo) {
  EXPECT_TRUE(standard_writer.Init(NULL, 0x11));
  standard_writer.Add("rayo", 4);
  standard_writer.Copy(2, 5);
  standard_writer.Copy(0, 4);
  standard_writer.Add("X", 1);
  standard_writer.Output(&output_string);
  ExpectByte(VCD_SOURCE);  // Win_Indicator: VCD_SOURCE (dictionary)
  ExpectByte(0x11);  // Source segment size: dictionary length
  ExpectByte(0x00);  // Source segment position: start of dictionary
  ExpectByte(0x0E);  // Length of the delta encoding
  ExpectByte(0x0E);  // Size of the target window
  ExpectByte(0x00);  // Delta_indicator (no compression)
  ExpectByte(0x05);  // length of data for ADDs and RUNs
  ExpectByte(0x02);  // length of instructions section
  ExpectByte(0x02);  // length of addresses for COPYs
  ExpectString("rayoX");
  ExpectByte(0xAD);  // Combo: Add size 4 + COPY mode SELF, size 5
  ExpectByte(0xFD);  // Combo: COPY mode SAME(0), size 4 + Add size 1
  ExpectByte(0x02);  // COPY address (2)
  ExpectByte(0x00);  // COPY address (0)
  ExpectNoMoreBytes();
}

TEST_F(CodeTableWriterTest, InterleavedWriterEncodeCombo) {
  EXPECT_TRUE(interleaved_writer.Init(NULL, 0x11));
  interleaved_writer.Add("rayo", 4);
  interleaved_writer.Copy(2, 5);
  interleaved_writer.Copy(0, 4);
  interleaved_writer.Add("X", 1);
  interleaved_writer.Output(&output_string);
  ExpectByte(VCD_SOURCE);  // Win_Indicator: VCD_SOURCE (dictionary)
  ExpectByte(0x11);  // Source segment size: dictionary length
  ExpectByte(0x00);  // Source segment position: start of dictionary
  ExpectByte(0x0E);  // Length of the delta encoding
  ExpectByte(0x0E);  // Size of the target window
  ExpectByte(0x00);  // Delta_indicator (no compression)
  ExpectByte(0x00);  // length of data for ADDs and RUNs
  ExpectByte(0x09);  // length of instructions section
  ExpectByte(0x00);  // length of addresses for COPYs
  ExpectByte(0xAD);  // Combo: Add size 4 + COPY mode SELF, size 5
  ExpectString("rayo");
  ExpectByte(0x02);  // COPY address (2)
  ExpectByte(0xFD);  // Combo: COPY mode SAME(0), size 4 + Add size 1
  ExpectByte(0x00);  // COPY address (0)
  ExpectByte('X');
  ExpectNoMoreBytes();
}

TEST_F(CodeTableWriterTest, InterleavedWriterEncodeComboWithChecksum) {
  EXPECT_TRUE(interleaved_writer.Init(NULL, 0x11));
  const VCDChecksum checksum = 0xFFFFFFFF;  // would be negative if signed
  interleaved_writer.AddChecksum(checksum);
  interleaved_writer.Add("rayo", 4);
  interleaved_writer.Copy(2, 5);
  interleaved_writer.Copy(0, 4);
  interleaved_writer.Add("X", 1);
  interleaved_writer.Output(&output_string);
  ExpectByte(VCD_SOURCE | VCD_CHECKSUM);  // Win_Indicator
  ExpectByte(0x11);  // Source segment size: dictionary length
  ExpectByte(0x00);  // Source segment position: start of dictionary
  ExpectByte(0x13);  // Length of the delta encoding
  ExpectByte(0x0E);  // Size of the target window
  ExpectByte(0x00);  // Delta_indicator (no compression)
  ExpectByte(0x00);  // length of data for ADDs and RUNs
  ExpectByte(0x09);  // length of instructions section
  ExpectByte(0x00);  // length of addresses for COPYs
  ExpectByte(0x8F);  // checksum byte 1
  ExpectByte(0xFF);  // checksum byte 2
  ExpectByte(0xFF);  // checksum byte 3
  ExpectByte(0xFF);  // checksum byte 4
  ExpectByte(0x7F);  // checksum byte 5
  ExpectByte(0xAD);  // Combo: Add size 4 + COPY mode SELF, size 5
  ExpectString("rayo");
  ExpectByte(0x02);  // COPY address (2)
  ExpectByte(0xFD);  // Combo: COPY mode SAME(0), size 4 + Add size 1
  ExpectByte(0x00);  // COPY address (0)
  ExpectByte('X');
  ExpectNoMoreBytes();
}

TEST_F(CodeTableWriterTest, ReallyBigDictionary) {
  EXPECT_TRUE(interleaved_writer.Init(NULL, 0x3FFFFFFF));
  interleaved_writer.Copy(2, 8);
  interleaved_writer.Copy(0x3FFFFFFE, 8);
  interleaved_writer.Output(&output_string);
  ExpectByte(VCD_SOURCE);  // Win_Indicator: VCD_SOURCE (dictionary)
  ExpectByte(0x83);  // Source segment size: dictionary length (1)
  ExpectByte(0xFF);  // Source segment size: dictionary length (2)
  ExpectByte(0xFF);  // Source segment size: dictionary length (3)
  ExpectByte(0xFF);  // Source segment size: dictionary length (4)
  ExpectByte(0x7F);  // Source segment size: dictionary length (5)
  ExpectByte(0x00);  // Source segment position: start of dictionary
  ExpectByte(0x09);  // Length of the delta encoding
  ExpectByte(0x10);  // Size of the target window
  ExpectByte(0x00);  // Delta_indicator (no compression)
  ExpectByte(0x00);  // length of data for ADDs and RUNs
  ExpectByte(0x04);  // length of instructions section
  ExpectByte(0x00);  // length of addresses for COPYs
  ExpectByte(0x18);  // COPY mode SELF, size 8
  ExpectByte(0x02);  // COPY address (2)
  ExpectByte(0x28);  // COPY mode HERE, size 8
  ExpectByte(0x09);  // COPY address (9)
  ExpectNoMoreBytes();
}

#ifdef GTEST_HAS_DEATH_TEST
TEST_F(CodeTableWriterDeathTest, DictionaryTooBig) {
  EXPECT_TRUE(interleaved_writer.Init(NULL, 0x7FFFFFFF));
  interleaved_writer.Copy(2, 8);
  EXPECT_DEBUG_DEATH(interleaved_writer.Copy(0x7FFFFFFE, 8),
                     "address.*<.*here_address");
}
#endif  // GTEST_HAS_DEATH_TEST

}  // unnamed namespace
}  // namespace open_vcdiff
