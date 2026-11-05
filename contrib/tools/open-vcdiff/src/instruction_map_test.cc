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
// Unit tests for the class VCDiffInstructionMap, found in instruction_map.h.

#include <config.h>
#include "instruction_map.h"
#include "codetable.h"
#include "testing.h"
#include "vcdiff_defs.h"

namespace open_vcdiff {
namespace {

class InstructionMapTest : public testing::Test {
 protected:
  static void SetUpTestCase();
  static void TearDownTestCase();

  static void AddExerciseOpcode(unsigned char inst1,
                                unsigned char mode1,
                                unsigned char size1,
                                unsigned char inst2,
                                unsigned char mode2,
                                unsigned char size2,
                                int opcode);

  void VerifyExerciseFirstInstruction(unsigned char expected_opcode,
                                      unsigned char inst,
                                      unsigned char size,
                                      unsigned char mode);

  void VerifyExerciseSecondInstruction(unsigned char expected_opcode,
                                       unsigned char inst1,
                                       unsigned char size1,
                                       unsigned char mode1,
                                       unsigned char inst2,
                                       unsigned char size2,
                                       unsigned char mode2);

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

  // The instruction map corresponding to kDefaultCodeTableData.
  static const VCDiffInstructionMap* default_map;

  // The instruction map corresponding to g_exercise_code_table_.
  static const VCDiffInstructionMap* exercise_map;

  size_t out_index;
};

VCDiffCodeTableData* InstructionMapTest::g_exercise_code_table_ = NULL;
const VCDiffInstructionMap* InstructionMapTest::default_map = NULL;
const VCDiffInstructionMap* InstructionMapTest::exercise_map = NULL;

void InstructionMapTest::SetUpTestCase() {
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

  EXPECT_TRUE(VCDiffCodeTableData::kDefaultCodeTableData.Validate());
  EXPECT_TRUE(g_exercise_code_table_->Validate(kLastExerciseMode));
  default_map = VCDiffInstructionMap::GetDefaultInstructionMap();
  exercise_map = new VCDiffInstructionMap(*g_exercise_code_table_,
                                          kLastExerciseMode);
}

void InstructionMapTest::TearDownTestCase() {
  delete exercise_map;
  delete g_exercise_code_table_;
}

void InstructionMapTest::AddExerciseOpcode(unsigned char inst1,
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

void InstructionMapTest::VerifyExerciseFirstInstruction(
    unsigned char expected_opcode,
    unsigned char inst,
    unsigned char size,
    unsigned char mode) {
  int found_opcode = exercise_map->LookupFirstOpcode(inst, size, mode);
  if (g_exercise_code_table_->inst1[found_opcode] == VCD_NOOP) {
    // The opcode is backwards: (VCD_NOOP, [instruction])
    EXPECT_GE(expected_opcode, found_opcode);
    EXPECT_EQ(inst, g_exercise_code_table_->inst2[found_opcode]);
    EXPECT_EQ(size, g_exercise_code_table_->size2[found_opcode]);
    EXPECT_EQ(mode, g_exercise_code_table_->mode2[found_opcode]);
    EXPECT_EQ(VCD_NOOP, g_exercise_code_table_->inst1[found_opcode]);
    EXPECT_EQ(0, g_exercise_code_table_->size1[found_opcode]);
    EXPECT_EQ(0, g_exercise_code_table_->mode1[found_opcode]);
  } else {
    EXPECT_EQ(expected_opcode, found_opcode);
    EXPECT_EQ(inst, g_exercise_code_table_->inst1[found_opcode]);
    EXPECT_EQ(size, g_exercise_code_table_->size1[found_opcode]);
    EXPECT_EQ(mode, g_exercise_code_table_->mode1[found_opcode]);
    EXPECT_EQ(VCD_NOOP, g_exercise_code_table_->inst2[found_opcode]);
    EXPECT_EQ(0, g_exercise_code_table_->size2[found_opcode]);
    EXPECT_EQ(0, g_exercise_code_table_->mode2[found_opcode]);
  }
}

void InstructionMapTest::VerifyExerciseSecondInstruction(
    unsigned char expected_opcode,
    unsigned char inst1,
    unsigned char size1,
    unsigned char mode1,
    unsigned char inst2,
    unsigned char size2,
    unsigned char mode2) {
  int first_opcode = exercise_map->LookupFirstOpcode(inst1, size1, mode1);
  EXPECT_NE(kNoOpcode, first_opcode);
  EXPECT_EQ(expected_opcode,
            exercise_map->LookupSecondOpcode(first_opcode,
                                             inst2,
                                             size2,
                                             mode2));
}

TEST_F(InstructionMapTest, DefaultMapLookupFirstNoop) {
  EXPECT_EQ(kNoOpcode, default_map->LookupFirstOpcode(VCD_NOOP, 0, 0));
  EXPECT_EQ(kNoOpcode, default_map->LookupFirstOpcode(VCD_NOOP, 0, 255));
  EXPECT_EQ(kNoOpcode, default_map->LookupFirstOpcode(VCD_NOOP, 255, 0));
  EXPECT_EQ(kNoOpcode, default_map->LookupFirstOpcode(VCD_NOOP, 255, 255));
}

TEST_F(InstructionMapTest, DefaultMapLookupFirstAdd) {
  EXPECT_EQ(2, default_map->LookupFirstOpcode(VCD_ADD, 1, 0));
  EXPECT_EQ(3, default_map->LookupFirstOpcode(VCD_ADD, 2, 0));
  EXPECT_EQ(4, default_map->LookupFirstOpcode(VCD_ADD, 3, 0));
  EXPECT_EQ(5, default_map->LookupFirstOpcode(VCD_ADD, 4, 0));
  EXPECT_EQ(6, default_map->LookupFirstOpcode(VCD_ADD, 5, 0));
  EXPECT_EQ(7, default_map->LookupFirstOpcode(VCD_ADD, 6, 0));
  EXPECT_EQ(8, default_map->LookupFirstOpcode(VCD_ADD, 7, 0));
  EXPECT_EQ(9, default_map->LookupFirstOpcode(VCD_ADD, 8, 0));
  EXPECT_EQ(10, default_map->LookupFirstOpcode(VCD_ADD, 9, 0));
  EXPECT_EQ(11, default_map->LookupFirstOpcode(VCD_ADD, 10, 0));
  EXPECT_EQ(12, default_map->LookupFirstOpcode(VCD_ADD, 11, 0));
  EXPECT_EQ(13, default_map->LookupFirstOpcode(VCD_ADD, 12, 0));
  EXPECT_EQ(14, default_map->LookupFirstOpcode(VCD_ADD, 13, 0));
  EXPECT_EQ(15, default_map->LookupFirstOpcode(VCD_ADD, 14, 0));
  EXPECT_EQ(16, default_map->LookupFirstOpcode(VCD_ADD, 15, 0));
  EXPECT_EQ(17, default_map->LookupFirstOpcode(VCD_ADD, 16, 0));
  EXPECT_EQ(18, default_map->LookupFirstOpcode(VCD_ADD, 17, 0));
  EXPECT_EQ(kNoOpcode, default_map->LookupFirstOpcode(VCD_ADD, 100, 0));
  EXPECT_EQ(kNoOpcode, default_map->LookupFirstOpcode(VCD_ADD, 255, 0));
  EXPECT_EQ(1, default_map->LookupFirstOpcode(VCD_ADD, 0, 0));
  // Value of "mode" should not matter
  EXPECT_EQ(2, default_map->LookupFirstOpcode(VCD_ADD, 1, 2));
  EXPECT_EQ(2, default_map->LookupFirstOpcode(VCD_ADD, 1, 255));
}

TEST_F(InstructionMapTest, DefaultMapLookupFirstRun) {
  EXPECT_EQ(0, default_map->LookupFirstOpcode(VCD_RUN, 0, 0));
  EXPECT_EQ(kNoOpcode, default_map->LookupFirstOpcode(VCD_RUN, 1, 0));
  EXPECT_EQ(kNoOpcode, default_map->LookupFirstOpcode(VCD_RUN, 255, 0));
  // Value of "mode" should not matter
  EXPECT_EQ(0, default_map->LookupFirstOpcode(VCD_RUN, 0, 2));
}

TEST_F(InstructionMapTest, DefaultMapLookupFirstCopyMode0) {
  EXPECT_EQ(19, default_map->LookupFirstOpcode(VCD_COPY, 0, 0));
  EXPECT_EQ(20, default_map->LookupFirstOpcode(VCD_COPY, 4, 0));
  EXPECT_EQ(21, default_map->LookupFirstOpcode(VCD_COPY, 5, 0));
  EXPECT_EQ(22, default_map->LookupFirstOpcode(VCD_COPY, 6, 0));
  EXPECT_EQ(23, default_map->LookupFirstOpcode(VCD_COPY, 7, 0));
  EXPECT_EQ(24, default_map->LookupFirstOpcode(VCD_COPY, 8, 0));
  EXPECT_EQ(25, default_map->LookupFirstOpcode(VCD_COPY, 9, 0));
  EXPECT_EQ(26, default_map->LookupFirstOpcode(VCD_COPY, 10, 0));
  EXPECT_EQ(27, default_map->LookupFirstOpcode(VCD_COPY, 11, 0));
  EXPECT_EQ(28, default_map->LookupFirstOpcode(VCD_COPY, 12, 0));
  EXPECT_EQ(29, default_map->LookupFirstOpcode(VCD_COPY, 13, 0));
  EXPECT_EQ(30, default_map->LookupFirstOpcode(VCD_COPY, 14, 0));
  EXPECT_EQ(31, default_map->LookupFirstOpcode(VCD_COPY, 15, 0));
  EXPECT_EQ(32, default_map->LookupFirstOpcode(VCD_COPY, 16, 0));
  EXPECT_EQ(33, default_map->LookupFirstOpcode(VCD_COPY, 17, 0));
  EXPECT_EQ(34, default_map->LookupFirstOpcode(VCD_COPY, 18, 0));
}

TEST_F(InstructionMapTest, DefaultMapLookupFirstCopyMode1) {
  EXPECT_EQ(35, default_map->LookupFirstOpcode(VCD_COPY, 0, 1));
  EXPECT_EQ(36, default_map->LookupFirstOpcode(VCD_COPY, 4, 1));
  EXPECT_EQ(37, default_map->LookupFirstOpcode(VCD_COPY, 5, 1));
  EXPECT_EQ(38, default_map->LookupFirstOpcode(VCD_COPY, 6, 1));
  EXPECT_EQ(39, default_map->LookupFirstOpcode(VCD_COPY, 7, 1));
  EXPECT_EQ(40, default_map->LookupFirstOpcode(VCD_COPY, 8, 1));
  EXPECT_EQ(41, default_map->LookupFirstOpcode(VCD_COPY, 9, 1));
  EXPECT_EQ(42, default_map->LookupFirstOpcode(VCD_COPY, 10, 1));
  EXPECT_EQ(43, default_map->LookupFirstOpcode(VCD_COPY, 11, 1));
  EXPECT_EQ(44, default_map->LookupFirstOpcode(VCD_COPY, 12, 1));
  EXPECT_EQ(45, default_map->LookupFirstOpcode(VCD_COPY, 13, 1));
  EXPECT_EQ(46, default_map->LookupFirstOpcode(VCD_COPY, 14, 1));
  EXPECT_EQ(47, default_map->LookupFirstOpcode(VCD_COPY, 15, 1));
  EXPECT_EQ(48, default_map->LookupFirstOpcode(VCD_COPY, 16, 1));
  EXPECT_EQ(49, default_map->LookupFirstOpcode(VCD_COPY, 17, 1));
  EXPECT_EQ(50, default_map->LookupFirstOpcode(VCD_COPY, 18, 1));
}

TEST_F(InstructionMapTest, DefaultMapLookupFirstCopyMode2) {
  EXPECT_EQ(51, default_map->LookupFirstOpcode(VCD_COPY, 0, 2));
  EXPECT_EQ(52, default_map->LookupFirstOpcode(VCD_COPY, 4, 2));
  EXPECT_EQ(53, default_map->LookupFirstOpcode(VCD_COPY, 5, 2));
  EXPECT_EQ(54, default_map->LookupFirstOpcode(VCD_COPY, 6, 2));
  EXPECT_EQ(55, default_map->LookupFirstOpcode(VCD_COPY, 7, 2));
  EXPECT_EQ(56, default_map->LookupFirstOpcode(VCD_COPY, 8, 2));
  EXPECT_EQ(57, default_map->LookupFirstOpcode(VCD_COPY, 9, 2));
  EXPECT_EQ(58, default_map->LookupFirstOpcode(VCD_COPY, 10, 2));
  EXPECT_EQ(59, default_map->LookupFirstOpcode(VCD_COPY, 11, 2));
  EXPECT_EQ(60, default_map->LookupFirstOpcode(VCD_COPY, 12, 2));
  EXPECT_EQ(61, default_map->LookupFirstOpcode(VCD_COPY, 13, 2));
  EXPECT_EQ(62, default_map->LookupFirstOpcode(VCD_COPY, 14, 2));
  EXPECT_EQ(63, default_map->LookupFirstOpcode(VCD_COPY, 15, 2));
  EXPECT_EQ(64, default_map->LookupFirstOpcode(VCD_COPY, 16, 2));
  EXPECT_EQ(65, default_map->LookupFirstOpcode(VCD_COPY, 17, 2));
  EXPECT_EQ(66, default_map->LookupFirstOpcode(VCD_COPY, 18, 2));
}

TEST_F(InstructionMapTest, DefaultMapLookupFirstCopyMode3) {
  EXPECT_EQ(67, default_map->LookupFirstOpcode(VCD_COPY, 0, 3));
  EXPECT_EQ(68, default_map->LookupFirstOpcode(VCD_COPY, 4, 3));
  EXPECT_EQ(69, default_map->LookupFirstOpcode(VCD_COPY, 5, 3));
  EXPECT_EQ(70, default_map->LookupFirstOpcode(VCD_COPY, 6, 3));
  EXPECT_EQ(71, default_map->LookupFirstOpcode(VCD_COPY, 7, 3));
  EXPECT_EQ(72, default_map->LookupFirstOpcode(VCD_COPY, 8, 3));
  EXPECT_EQ(73, default_map->LookupFirstOpcode(VCD_COPY, 9, 3));
  EXPECT_EQ(74, default_map->LookupFirstOpcode(VCD_COPY, 10, 3));
  EXPECT_EQ(75, default_map->LookupFirstOpcode(VCD_COPY, 11, 3));
  EXPECT_EQ(76, default_map->LookupFirstOpcode(VCD_COPY, 12, 3));
  EXPECT_EQ(77, default_map->LookupFirstOpcode(VCD_COPY, 13, 3));
  EXPECT_EQ(78, default_map->LookupFirstOpcode(VCD_COPY, 14, 3));
  EXPECT_EQ(79, default_map->LookupFirstOpcode(VCD_COPY, 15, 3));
  EXPECT_EQ(80, default_map->LookupFirstOpcode(VCD_COPY, 16, 3));
  EXPECT_EQ(81, default_map->LookupFirstOpcode(VCD_COPY, 17, 3));
  EXPECT_EQ(82, default_map->LookupFirstOpcode(VCD_COPY, 18, 3));
}

TEST_F(InstructionMapTest, DefaultMapLookupFirstCopyMode4) {
  EXPECT_EQ(83, default_map->LookupFirstOpcode(VCD_COPY, 0, 4));
  EXPECT_EQ(84, default_map->LookupFirstOpcode(VCD_COPY, 4, 4));
  EXPECT_EQ(85, default_map->LookupFirstOpcode(VCD_COPY, 5, 4));
  EXPECT_EQ(86, default_map->LookupFirstOpcode(VCD_COPY, 6, 4));
  EXPECT_EQ(87, default_map->LookupFirstOpcode(VCD_COPY, 7, 4));
  EXPECT_EQ(88, default_map->LookupFirstOpcode(VCD_COPY, 8, 4));
  EXPECT_EQ(89, default_map->LookupFirstOpcode(VCD_COPY, 9, 4));
  EXPECT_EQ(90, default_map->LookupFirstOpcode(VCD_COPY, 10, 4));
  EXPECT_EQ(91, default_map->LookupFirstOpcode(VCD_COPY, 11, 4));
  EXPECT_EQ(92, default_map->LookupFirstOpcode(VCD_COPY, 12, 4));
  EXPECT_EQ(93, default_map->LookupFirstOpcode(VCD_COPY, 13, 4));
  EXPECT_EQ(94, default_map->LookupFirstOpcode(VCD_COPY, 14, 4));
  EXPECT_EQ(95, default_map->LookupFirstOpcode(VCD_COPY, 15, 4));
  EXPECT_EQ(96, default_map->LookupFirstOpcode(VCD_COPY, 16, 4));
  EXPECT_EQ(97, default_map->LookupFirstOpcode(VCD_COPY, 17, 4));
  EXPECT_EQ(98, default_map->LookupFirstOpcode(VCD_COPY, 18, 4));
}

TEST_F(InstructionMapTest, DefaultMapLookupFirstCopyMode5) {
  EXPECT_EQ(99, default_map->LookupFirstOpcode(VCD_COPY, 0, 5));
  EXPECT_EQ(100, default_map->LookupFirstOpcode(VCD_COPY, 4, 5));
  EXPECT_EQ(101, default_map->LookupFirstOpcode(VCD_COPY, 5, 5));
  EXPECT_EQ(102, default_map->LookupFirstOpcode(VCD_COPY, 6, 5));
  EXPECT_EQ(103, default_map->LookupFirstOpcode(VCD_COPY, 7, 5));
  EXPECT_EQ(104, default_map->LookupFirstOpcode(VCD_COPY, 8, 5));
  EXPECT_EQ(105, default_map->LookupFirstOpcode(VCD_COPY, 9, 5));
  EXPECT_EQ(106, default_map->LookupFirstOpcode(VCD_COPY, 10, 5));
  EXPECT_EQ(107, default_map->LookupFirstOpcode(VCD_COPY, 11, 5));
  EXPECT_EQ(108, default_map->LookupFirstOpcode(VCD_COPY, 12, 5));
  EXPECT_EQ(109, default_map->LookupFirstOpcode(VCD_COPY, 13, 5));
  EXPECT_EQ(110, default_map->LookupFirstOpcode(VCD_COPY, 14, 5));
  EXPECT_EQ(111, default_map->LookupFirstOpcode(VCD_COPY, 15, 5));
  EXPECT_EQ(112, default_map->LookupFirstOpcode(VCD_COPY, 16, 5));
  EXPECT_EQ(113, default_map->LookupFirstOpcode(VCD_COPY, 17, 5));
  EXPECT_EQ(114, default_map->LookupFirstOpcode(VCD_COPY, 18, 5));
}

TEST_F(InstructionMapTest, DefaultMapLookupFirstCopyMode6) {
  EXPECT_EQ(115, default_map->LookupFirstOpcode(VCD_COPY, 0, 6));
  EXPECT_EQ(116, default_map->LookupFirstOpcode(VCD_COPY, 4, 6));
  EXPECT_EQ(117, default_map->LookupFirstOpcode(VCD_COPY, 5, 6));
  EXPECT_EQ(118, default_map->LookupFirstOpcode(VCD_COPY, 6, 6));
  EXPECT_EQ(119, default_map->LookupFirstOpcode(VCD_COPY, 7, 6));
  EXPECT_EQ(120, default_map->LookupFirstOpcode(VCD_COPY, 8, 6));
  EXPECT_EQ(121, default_map->LookupFirstOpcode(VCD_COPY, 9, 6));
  EXPECT_EQ(122, default_map->LookupFirstOpcode(VCD_COPY, 10, 6));
  EXPECT_EQ(123, default_map->LookupFirstOpcode(VCD_COPY, 11, 6));
  EXPECT_EQ(124, default_map->LookupFirstOpcode(VCD_COPY, 12, 6));
  EXPECT_EQ(125, default_map->LookupFirstOpcode(VCD_COPY, 13, 6));
  EXPECT_EQ(126, default_map->LookupFirstOpcode(VCD_COPY, 14, 6));
  EXPECT_EQ(127, default_map->LookupFirstOpcode(VCD_COPY, 15, 6));
  EXPECT_EQ(128, default_map->LookupFirstOpcode(VCD_COPY, 16, 6));
  EXPECT_EQ(129, default_map->LookupFirstOpcode(VCD_COPY, 17, 6));
  EXPECT_EQ(130, default_map->LookupFirstOpcode(VCD_COPY, 18, 6));
}

TEST_F(InstructionMapTest, DefaultMapLookupFirstCopyMode7) {
  EXPECT_EQ(131, default_map->LookupFirstOpcode(VCD_COPY, 0, 7));
  EXPECT_EQ(132, default_map->LookupFirstOpcode(VCD_COPY, 4, 7));
  EXPECT_EQ(133, default_map->LookupFirstOpcode(VCD_COPY, 5, 7));
  EXPECT_EQ(134, default_map->LookupFirstOpcode(VCD_COPY, 6, 7));
  EXPECT_EQ(135, default_map->LookupFirstOpcode(VCD_COPY, 7, 7));
  EXPECT_EQ(136, default_map->LookupFirstOpcode(VCD_COPY, 8, 7));
  EXPECT_EQ(137, default_map->LookupFirstOpcode(VCD_COPY, 9, 7));
  EXPECT_EQ(138, default_map->LookupFirstOpcode(VCD_COPY, 10, 7));
  EXPECT_EQ(139, default_map->LookupFirstOpcode(VCD_COPY, 11, 7));
  EXPECT_EQ(140, default_map->LookupFirstOpcode(VCD_COPY, 12, 7));
  EXPECT_EQ(141, default_map->LookupFirstOpcode(VCD_COPY, 13, 7));
  EXPECT_EQ(142, default_map->LookupFirstOpcode(VCD_COPY, 14, 7));
  EXPECT_EQ(143, default_map->LookupFirstOpcode(VCD_COPY, 15, 7));
  EXPECT_EQ(144, default_map->LookupFirstOpcode(VCD_COPY, 16, 7));
  EXPECT_EQ(145, default_map->LookupFirstOpcode(VCD_COPY, 17, 7));
  EXPECT_EQ(146, default_map->LookupFirstOpcode(VCD_COPY, 18, 7));
}

TEST_F(InstructionMapTest, DefaultMapLookupFirstCopyMode8) {
  EXPECT_EQ(147, default_map->LookupFirstOpcode(VCD_COPY, 0, 8));
  EXPECT_EQ(148, default_map->LookupFirstOpcode(VCD_COPY, 4, 8));
  EXPECT_EQ(149, default_map->LookupFirstOpcode(VCD_COPY, 5, 8));
  EXPECT_EQ(150, default_map->LookupFirstOpcode(VCD_COPY, 6, 8));
  EXPECT_EQ(151, default_map->LookupFirstOpcode(VCD_COPY, 7, 8));
  EXPECT_EQ(152, default_map->LookupFirstOpcode(VCD_COPY, 8, 8));
  EXPECT_EQ(153, default_map->LookupFirstOpcode(VCD_COPY, 9, 8));
  EXPECT_EQ(154, default_map->LookupFirstOpcode(VCD_COPY, 10, 8));
  EXPECT_EQ(155, default_map->LookupFirstOpcode(VCD_COPY, 11, 8));
  EXPECT_EQ(156, default_map->LookupFirstOpcode(VCD_COPY, 12, 8));
  EXPECT_EQ(157, default_map->LookupFirstOpcode(VCD_COPY, 13, 8));
  EXPECT_EQ(158, default_map->LookupFirstOpcode(VCD_COPY, 14, 8));
  EXPECT_EQ(159, default_map->LookupFirstOpcode(VCD_COPY, 15, 8));
  EXPECT_EQ(160, default_map->LookupFirstOpcode(VCD_COPY, 16, 8));
  EXPECT_EQ(161, default_map->LookupFirstOpcode(VCD_COPY, 17, 8));
  EXPECT_EQ(162, default_map->LookupFirstOpcode(VCD_COPY, 18, 8));
}

TEST_F(InstructionMapTest, DefaultMapLookupFirstCopyInvalid) {
  EXPECT_EQ(kNoOpcode, default_map->LookupFirstOpcode(VCD_COPY, 3, 0));
  EXPECT_EQ(kNoOpcode, default_map->LookupFirstOpcode(VCD_COPY, 3, 3));
  EXPECT_EQ(kNoOpcode, default_map->LookupFirstOpcode(VCD_COPY, 255, 0));
}

TEST_F(InstructionMapTest, DefaultMapLookupSecondNoop) {
  // The second opcode table does not store entries for NOOP instructions.
  // Just make sure that a NOOP does not crash the lookup code.
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(20, VCD_NOOP, 0, 0));
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(20, VCD_NOOP, 0, 255));
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(20, VCD_NOOP, 255, 0));
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(20, VCD_NOOP, 255, 255));
}

TEST_F(InstructionMapTest, DefaultMapLookupSecondAdd) {
  EXPECT_EQ(247, default_map->LookupSecondOpcode(20, VCD_ADD, 1, 0));
  EXPECT_EQ(248, default_map->LookupSecondOpcode(36, VCD_ADD, 1, 0));
  EXPECT_EQ(249, default_map->LookupSecondOpcode(52, VCD_ADD, 1, 0));
  EXPECT_EQ(250, default_map->LookupSecondOpcode(68, VCD_ADD, 1, 0));
  EXPECT_EQ(251, default_map->LookupSecondOpcode(84, VCD_ADD, 1, 0));
  EXPECT_EQ(252, default_map->LookupSecondOpcode(100, VCD_ADD, 1, 0));
  EXPECT_EQ(253, default_map->LookupSecondOpcode(116, VCD_ADD, 1, 0));
  EXPECT_EQ(254, default_map->LookupSecondOpcode(132, VCD_ADD, 1, 0));
  EXPECT_EQ(255, default_map->LookupSecondOpcode(148, VCD_ADD, 1, 0));
  // Value of "mode" should not matter
  EXPECT_EQ(247, default_map->LookupSecondOpcode(20, VCD_ADD, 1, 2));
  EXPECT_EQ(247, default_map->LookupSecondOpcode(20, VCD_ADD, 1, 255));
  // Only valid 2nd ADD opcode has size 1
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(20, VCD_ADD, 0, 0));
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(20, VCD_ADD, 0, 255));
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(20, VCD_ADD, 255, 0));
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(0, VCD_ADD, 1, 0));
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(1, VCD_ADD, 1, 0));
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(247, VCD_ADD, 1, 0));
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(255, VCD_ADD, 1, 0));
}

TEST_F(InstructionMapTest, DefaultMapLookupSecondRun) {
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(0, VCD_RUN, 0, 0));
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(20, VCD_RUN, 0, 0));
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(20, VCD_RUN, 0, 255));
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(20, VCD_RUN, 255, 0));
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(20, VCD_RUN, 255, 255));
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(255, VCD_RUN, 0, 0));
}

TEST_F(InstructionMapTest, DefaultMapLookupSecondCopyMode0) {
  EXPECT_EQ(163, default_map->LookupSecondOpcode(2, VCD_COPY, 4, 0));
  EXPECT_EQ(164, default_map->LookupSecondOpcode(2, VCD_COPY, 5, 0));
  EXPECT_EQ(165, default_map->LookupSecondOpcode(2, VCD_COPY, 6, 0));
  EXPECT_EQ(166, default_map->LookupSecondOpcode(3, VCD_COPY, 4, 0));
  EXPECT_EQ(167, default_map->LookupSecondOpcode(3, VCD_COPY, 5, 0));
  EXPECT_EQ(168, default_map->LookupSecondOpcode(3, VCD_COPY, 6, 0));
  EXPECT_EQ(169, default_map->LookupSecondOpcode(4, VCD_COPY, 4, 0));
  EXPECT_EQ(170, default_map->LookupSecondOpcode(4, VCD_COPY, 5, 0));
  EXPECT_EQ(171, default_map->LookupSecondOpcode(4, VCD_COPY, 6, 0));
  EXPECT_EQ(172, default_map->LookupSecondOpcode(5, VCD_COPY, 4, 0));
  EXPECT_EQ(173, default_map->LookupSecondOpcode(5, VCD_COPY, 5, 0));
  EXPECT_EQ(174, default_map->LookupSecondOpcode(5, VCD_COPY, 6, 0));
}

TEST_F(InstructionMapTest, DefaultMapLookupSecondCopyMode1) {
  EXPECT_EQ(175, default_map->LookupSecondOpcode(2, VCD_COPY, 4, 1));
  EXPECT_EQ(176, default_map->LookupSecondOpcode(2, VCD_COPY, 5, 1));
  EXPECT_EQ(177, default_map->LookupSecondOpcode(2, VCD_COPY, 6, 1));
  EXPECT_EQ(178, default_map->LookupSecondOpcode(3, VCD_COPY, 4, 1));
  EXPECT_EQ(179, default_map->LookupSecondOpcode(3, VCD_COPY, 5, 1));
  EXPECT_EQ(180, default_map->LookupSecondOpcode(3, VCD_COPY, 6, 1));
  EXPECT_EQ(181, default_map->LookupSecondOpcode(4, VCD_COPY, 4, 1));
  EXPECT_EQ(182, default_map->LookupSecondOpcode(4, VCD_COPY, 5, 1));
  EXPECT_EQ(183, default_map->LookupSecondOpcode(4, VCD_COPY, 6, 1));
  EXPECT_EQ(184, default_map->LookupSecondOpcode(5, VCD_COPY, 4, 1));
  EXPECT_EQ(185, default_map->LookupSecondOpcode(5, VCD_COPY, 5, 1));
  EXPECT_EQ(186, default_map->LookupSecondOpcode(5, VCD_COPY, 6, 1));
}

TEST_F(InstructionMapTest, DefaultMapLookupSecondCopyMode2) {
  EXPECT_EQ(187, default_map->LookupSecondOpcode(2, VCD_COPY, 4, 2));
  EXPECT_EQ(188, default_map->LookupSecondOpcode(2, VCD_COPY, 5, 2));
  EXPECT_EQ(189, default_map->LookupSecondOpcode(2, VCD_COPY, 6, 2));
  EXPECT_EQ(190, default_map->LookupSecondOpcode(3, VCD_COPY, 4, 2));
  EXPECT_EQ(191, default_map->LookupSecondOpcode(3, VCD_COPY, 5, 2));
  EXPECT_EQ(192, default_map->LookupSecondOpcode(3, VCD_COPY, 6, 2));
  EXPECT_EQ(193, default_map->LookupSecondOpcode(4, VCD_COPY, 4, 2));
  EXPECT_EQ(194, default_map->LookupSecondOpcode(4, VCD_COPY, 5, 2));
  EXPECT_EQ(195, default_map->LookupSecondOpcode(4, VCD_COPY, 6, 2));
  EXPECT_EQ(196, default_map->LookupSecondOpcode(5, VCD_COPY, 4, 2));
  EXPECT_EQ(197, default_map->LookupSecondOpcode(5, VCD_COPY, 5, 2));
  EXPECT_EQ(198, default_map->LookupSecondOpcode(5, VCD_COPY, 6, 2));
}

TEST_F(InstructionMapTest, DefaultMapLookupSecondCopyMode3) {
  EXPECT_EQ(199, default_map->LookupSecondOpcode(2, VCD_COPY, 4, 3));
  EXPECT_EQ(200, default_map->LookupSecondOpcode(2, VCD_COPY, 5, 3));
  EXPECT_EQ(201, default_map->LookupSecondOpcode(2, VCD_COPY, 6, 3));
  EXPECT_EQ(202, default_map->LookupSecondOpcode(3, VCD_COPY, 4, 3));
  EXPECT_EQ(203, default_map->LookupSecondOpcode(3, VCD_COPY, 5, 3));
  EXPECT_EQ(204, default_map->LookupSecondOpcode(3, VCD_COPY, 6, 3));
  EXPECT_EQ(205, default_map->LookupSecondOpcode(4, VCD_COPY, 4, 3));
  EXPECT_EQ(206, default_map->LookupSecondOpcode(4, VCD_COPY, 5, 3));
  EXPECT_EQ(207, default_map->LookupSecondOpcode(4, VCD_COPY, 6, 3));
  EXPECT_EQ(208, default_map->LookupSecondOpcode(5, VCD_COPY, 4, 3));
  EXPECT_EQ(209, default_map->LookupSecondOpcode(5, VCD_COPY, 5, 3));
  EXPECT_EQ(210, default_map->LookupSecondOpcode(5, VCD_COPY, 6, 3));
}

TEST_F(InstructionMapTest, DefaultMapLookupSecondCopyMode4) {
  EXPECT_EQ(211, default_map->LookupSecondOpcode(2, VCD_COPY, 4, 4));
  EXPECT_EQ(212, default_map->LookupSecondOpcode(2, VCD_COPY, 5, 4));
  EXPECT_EQ(213, default_map->LookupSecondOpcode(2, VCD_COPY, 6, 4));
  EXPECT_EQ(214, default_map->LookupSecondOpcode(3, VCD_COPY, 4, 4));
  EXPECT_EQ(215, default_map->LookupSecondOpcode(3, VCD_COPY, 5, 4));
  EXPECT_EQ(216, default_map->LookupSecondOpcode(3, VCD_COPY, 6, 4));
  EXPECT_EQ(217, default_map->LookupSecondOpcode(4, VCD_COPY, 4, 4));
  EXPECT_EQ(218, default_map->LookupSecondOpcode(4, VCD_COPY, 5, 4));
  EXPECT_EQ(219, default_map->LookupSecondOpcode(4, VCD_COPY, 6, 4));
  EXPECT_EQ(220, default_map->LookupSecondOpcode(5, VCD_COPY, 4, 4));
  EXPECT_EQ(221, default_map->LookupSecondOpcode(5, VCD_COPY, 5, 4));
  EXPECT_EQ(222, default_map->LookupSecondOpcode(5, VCD_COPY, 6, 4));
}

TEST_F(InstructionMapTest, DefaultMapLookupSecondCopyMode5) {
  EXPECT_EQ(223, default_map->LookupSecondOpcode(2, VCD_COPY, 4, 5));
  EXPECT_EQ(224, default_map->LookupSecondOpcode(2, VCD_COPY, 5, 5));
  EXPECT_EQ(225, default_map->LookupSecondOpcode(2, VCD_COPY, 6, 5));
  EXPECT_EQ(226, default_map->LookupSecondOpcode(3, VCD_COPY, 4, 5));
  EXPECT_EQ(227, default_map->LookupSecondOpcode(3, VCD_COPY, 5, 5));
  EXPECT_EQ(228, default_map->LookupSecondOpcode(3, VCD_COPY, 6, 5));
  EXPECT_EQ(229, default_map->LookupSecondOpcode(4, VCD_COPY, 4, 5));
  EXPECT_EQ(230, default_map->LookupSecondOpcode(4, VCD_COPY, 5, 5));
  EXPECT_EQ(231, default_map->LookupSecondOpcode(4, VCD_COPY, 6, 5));
  EXPECT_EQ(232, default_map->LookupSecondOpcode(5, VCD_COPY, 4, 5));
  EXPECT_EQ(233, default_map->LookupSecondOpcode(5, VCD_COPY, 5, 5));
  EXPECT_EQ(234, default_map->LookupSecondOpcode(5, VCD_COPY, 6, 5));
}

TEST_F(InstructionMapTest, DefaultMapLookupSecondCopyMode6) {
  EXPECT_EQ(235, default_map->LookupSecondOpcode(2, VCD_COPY, 4, 6));
  EXPECT_EQ(236, default_map->LookupSecondOpcode(3, VCD_COPY, 4, 6));
  EXPECT_EQ(237, default_map->LookupSecondOpcode(4, VCD_COPY, 4, 6));
  EXPECT_EQ(238, default_map->LookupSecondOpcode(5, VCD_COPY, 4, 6));
  EXPECT_EQ(239, default_map->LookupSecondOpcode(2, VCD_COPY, 4, 7));
}

TEST_F(InstructionMapTest, DefaultMapLookupSecondCopyMode7) {
  EXPECT_EQ(240, default_map->LookupSecondOpcode(3, VCD_COPY, 4, 7));
  EXPECT_EQ(241, default_map->LookupSecondOpcode(4, VCD_COPY, 4, 7));
  EXPECT_EQ(242, default_map->LookupSecondOpcode(5, VCD_COPY, 4, 7));
}

TEST_F(InstructionMapTest, DefaultMapLookupSecondCopyMode8) {
  EXPECT_EQ(243, default_map->LookupSecondOpcode(2, VCD_COPY, 4, 8));
  EXPECT_EQ(244, default_map->LookupSecondOpcode(3, VCD_COPY, 4, 8));
  EXPECT_EQ(245, default_map->LookupSecondOpcode(4, VCD_COPY, 4, 8));
  EXPECT_EQ(246, default_map->LookupSecondOpcode(5, VCD_COPY, 4, 8));
}

TEST_F(InstructionMapTest, DefaultMapLookupSecondCopyInvalid) {
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(2, VCD_COPY, 0, 0));
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(2, VCD_COPY, 255, 0));
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(2, VCD_COPY, 255, 255));
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(0, VCD_COPY, 4, 0));
  EXPECT_EQ(kNoOpcode, default_map->LookupSecondOpcode(255, VCD_COPY, 4, 0));
}

TEST_F(InstructionMapTest, ExerciseTableLookup) {
  int opcode = 0;
  // This loop has the same bounds as the one in SetUpTestCase.
  // Look up each instruction type and make sure it returns
  // the proper opcode.
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
      if (inst2 == VCD_NOOP) {
        VerifyExerciseFirstInstruction(opcode, inst1, 0, mode1);
        VerifyExerciseFirstInstruction(opcode + 2,
                                       inst1,
                                       ((inst1 == VCD_NOOP) ? 0 : 255),
                                       mode1);
      } else if (inst1 != VCD_NOOP) {
        VerifyExerciseSecondInstruction(opcode,
                                        inst1,
                                        0,
                                        mode1,
                                        inst2,
                                        0,
                                        mode2);
        VerifyExerciseSecondInstruction(opcode + 1,
                                        inst1,
                                        0,
                                        mode1,
                                        inst2,
                                        255,
                                        mode2);
        VerifyExerciseSecondInstruction(opcode + 2,
                                        inst1,
                                        255,
                                        mode1,
                                        inst2,
                                        0,
                                        mode2);
        VerifyExerciseSecondInstruction(opcode + 3,
                                        inst1,
                                        255,
                                        mode1,
                                        inst2,
                                        255,
                                        mode2);
      }
      opcode += 4;
    }
  }
  // This is a CHECK rather than an EXPECT because it validates only
  // the logic of the test, not of the code being tested.
  CHECK_EQ(VCDiffCodeTableData::kCodeTableSize, opcode);
}

}  // unnamed namespace
}  // namespace open_vcdiff
