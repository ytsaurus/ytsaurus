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
// Unit tests for struct VCDiffCodeTableData, found in codetable.h.

#include <config.h>
#include "codetable.h"
#include "addrcache.h"
#include "testing.h"

namespace open_vcdiff {
namespace {

class CodeTableTest : public testing::Test {
 protected:
  CodeTableTest()
  : code_table_data_(VCDiffCodeTableData::kDefaultCodeTableData) { }

  virtual ~CodeTableTest() { }

  virtual void SetUp() {
    // Default code table must pass
    EXPECT_TRUE(ValidateCodeTable());
  }

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

    EXPECT_TRUE(VCDiffCodeTableData::kDefaultCodeTableData.Validate());
    EXPECT_TRUE(g_exercise_code_table_->Validate(kLastExerciseMode));
  }

  static void TearDownTestCase() {
    delete g_exercise_code_table_;
  }

  void VerifyInstruction(unsigned char opcode,
                         unsigned char inst,
                         unsigned char size,
                         unsigned char mode) {
    EXPECT_EQ(inst, code_table_data_.inst1[opcode]);
    EXPECT_EQ(size, code_table_data_.size1[opcode]);
    EXPECT_EQ(mode, code_table_data_.mode1[opcode]);
    EXPECT_EQ(VCD_NOOP, code_table_data_.inst2[opcode]);
    EXPECT_EQ(0, code_table_data_.size2[opcode]);
    EXPECT_EQ(0, code_table_data_.mode2[opcode]);
  }

  bool ValidateCodeTable() {
    return code_table_data_.Validate();
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

  // The code table used by the current test.
  VCDiffCodeTableData code_table_data_;
};

VCDiffCodeTableData* CodeTableTest::g_exercise_code_table_ = NULL;

// These tests make sure that ValidateCodeTable() catches particular
// error conditions in a custom code table.

// All possible combinations of inst and mode should have an opcode with size 0.
TEST_F(CodeTableTest, MissingCopyMode) {
  VerifyInstruction(/* opcode */ 131, VCD_COPY, /* size */ 0, /* mode */ 7);
  code_table_data_.size1[131] = 0xFF;
  // Now there is no opcode expressing COPY with mode 7 and size 0.
  EXPECT_FALSE(ValidateCodeTable());
}

TEST_F(CodeTableTest, MissingAdd) {
  VerifyInstruction(/* opcode */ 1, VCD_ADD, /* size */ 0, /* mode */ 0);
  code_table_data_.size1[1] = 0xFF;  // Add size 0 => size 255
  // Now there is no opcode expressing ADD with size 0.
  EXPECT_FALSE(ValidateCodeTable());
}

TEST_F(CodeTableTest, MissingRun) {
  VerifyInstruction(/* opcode */ 0, VCD_RUN, /* size */ 0, /* mode */ 0);
  code_table_data_.size1[0] = 0xFF;  // Run size 0 => size 255
  // Now there is no opcode expressing RUN with size 0.
  EXPECT_FALSE(ValidateCodeTable());
}

TEST_F(CodeTableTest, BadOpcode) {
  VerifyInstruction(/* opcode */ 0, VCD_RUN, /* size */ 0, /* mode */ 0);
  code_table_data_.inst1[0] = VCD_LAST_INSTRUCTION_TYPE + 1;
  EXPECT_FALSE(ValidateCodeTable());
  code_table_data_.inst1[0] = 0xFF;
  EXPECT_FALSE(ValidateCodeTable());
}

TEST_F(CodeTableTest, BadMode) {
  VerifyInstruction(/* opcode */ 131, VCD_COPY, /* size */ 0, /* mode */ 7);
  code_table_data_.mode1[131] = VCDiffAddressCache::DefaultLastMode() + 1;
  EXPECT_FALSE(ValidateCodeTable());
  code_table_data_.mode1[131] = 0xFF;
  EXPECT_FALSE(ValidateCodeTable());
}

TEST_F(CodeTableTest, AddWithNonzeroMode) {
  VerifyInstruction(/* opcode */ 1, VCD_ADD, /* size */ 0, /* mode */ 0);
  code_table_data_.mode1[1] = 1;
  EXPECT_FALSE(ValidateCodeTable());
}

TEST_F(CodeTableTest, RunWithNonzeroMode) {
  VerifyInstruction(/* opcode */ 0, VCD_RUN, /* size */ 0, /* mode */ 0);
  code_table_data_.mode1[0] = 1;
  EXPECT_FALSE(ValidateCodeTable());
}

TEST_F(CodeTableTest, NoOpWithNonzeroMode) {
  VerifyInstruction(/* opcode */ 20, VCD_COPY, /* size */ 4, /* mode */ 0);
  code_table_data_.inst1[20] = VCD_NOOP;
  code_table_data_.mode1[20] = 0;
  code_table_data_.size1[20] = 0;
  EXPECT_TRUE(ValidateCodeTable());
  code_table_data_.mode1[20] = 1;
  EXPECT_FALSE(ValidateCodeTable());
}

TEST_F(CodeTableTest, NoOpWithNonzeroSize) {
  VerifyInstruction(/* opcode */ 20, VCD_COPY, /* size */ 4, /* mode */ 0);
  code_table_data_.inst1[20] = VCD_NOOP;
  code_table_data_.mode1[20] = 0;
  code_table_data_.size1[20] = 0;
  EXPECT_TRUE(ValidateCodeTable());
  code_table_data_.size1[20] = 1;
  EXPECT_FALSE(ValidateCodeTable());
}

TEST_F(CodeTableTest, BadSecondOpcode) {
  VerifyInstruction(/* opcode */ 20, VCD_COPY, /* size */ 4, /* mode */ 0);
  code_table_data_.inst2[20] = VCD_LAST_INSTRUCTION_TYPE + 1;
  EXPECT_FALSE(ValidateCodeTable());
  code_table_data_.inst2[20] = 0xFF;
  EXPECT_FALSE(ValidateCodeTable());
}

TEST_F(CodeTableTest, BadSecondMode) {
  VerifyInstruction(/* opcode */ 20, VCD_COPY, /* size */ 4, /* mode */ 0);
  code_table_data_.inst2[20] = VCD_COPY;
  EXPECT_TRUE(ValidateCodeTable());
  code_table_data_.mode2[20] = VCDiffAddressCache::DefaultLastMode() + 1;
  EXPECT_FALSE(ValidateCodeTable());
  code_table_data_.mode2[20] = 0xFF;
  EXPECT_FALSE(ValidateCodeTable());
}

TEST_F(CodeTableTest, AddSecondWithNonzeroMode) {
  VerifyInstruction(/* opcode */ 20, VCD_COPY, /* size */ 4, /* mode */ 0);
  code_table_data_.inst2[20] = VCD_ADD;
  EXPECT_TRUE(ValidateCodeTable());
  code_table_data_.mode2[20] = 1;
  EXPECT_FALSE(ValidateCodeTable());
}

TEST_F(CodeTableTest, RunSecondWithNonzeroMode) {
  VerifyInstruction(/* opcode */ 20, VCD_COPY, /* size */ 4, /* mode */ 0);
  code_table_data_.inst2[20] = VCD_RUN;
  EXPECT_TRUE(ValidateCodeTable());
  code_table_data_.mode2[20] = 1;
  EXPECT_FALSE(ValidateCodeTable());
}

TEST_F(CodeTableTest, SecondNoOpWithNonzeroMode) {
  VerifyInstruction(/* opcode */ 20, VCD_COPY, /* size */ 4, /* mode */ 0);
  EXPECT_EQ(VCD_NOOP, code_table_data_.inst2[20]);
  code_table_data_.mode2[20] = 1;
  EXPECT_FALSE(ValidateCodeTable());
}

TEST_F(CodeTableTest, SecondNoOpWithNonzeroSize) {
  VerifyInstruction(/* opcode */ 20, VCD_COPY, /* size */ 4, /* mode */ 0);
  EXPECT_EQ(VCD_NOOP, code_table_data_.inst2[20]);
  code_table_data_.size2[20] = 1;
  EXPECT_FALSE(ValidateCodeTable());
}

TEST_F(CodeTableTest, ValidateExerciseCodeTable) {
  EXPECT_TRUE(g_exercise_code_table_->Validate(kLastExerciseMode));
}

}  // unnamed namespace
}  // namespace open_vcdiff
