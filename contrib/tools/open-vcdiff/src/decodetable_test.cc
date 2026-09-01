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
// Unit tests for the class VCDiffCodeReader, found in decodetable.h.

#include <config.h>
#include "decodetable.h"
#include <stdint.h>  // int32_t
#include <vector>
#include "addrcache.h"
#include "codetable.h"
#include "testing.h"
#include "varint_bigendian.h"

namespace open_vcdiff {
namespace {

class DecodeTableTest : public testing::Test {
 protected:
  DecodeTableTest()
  : instructions_and_sizes_(instruction_buffer_size),
    found_size_(0),
    found_mode_(0) {
    reader_.Init(&instructions_and_sizes_[0], instruction_buffer_size);
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
    CHECK_EQ(VCDiffCodeTableData::kCodeTableSize, opcode);
    EXPECT_TRUE(VCDiffCodeTableData::kDefaultCodeTableData.Validate());
    EXPECT_TRUE(g_exercise_code_table_->Validate(kLastExerciseMode));
  }

  static void TearDownTestCase() {
    delete g_exercise_code_table_;
  }

  void VerifyInstModeSize(unsigned char inst,
                          unsigned char mode,
                          unsigned char size,
                          unsigned char opcode) {
    if (inst == VCD_NOOP) return;  // GetNextInstruction skips NOOPs
    int32_t found_size = 0;
    unsigned char found_mode = 0;
    unsigned char found_inst = reader_.GetNextInstruction(&found_size,
                                                          &found_mode);
    EXPECT_EQ(inst, found_inst);
    EXPECT_EQ(mode, found_mode);
    if (size == 0) {
      EXPECT_EQ(1000 + opcode, found_size);
    } else {
      EXPECT_EQ(size, found_size);
    }
  }

  void VerifyInstModeSize1(unsigned char inst,
                           unsigned char mode,
                           unsigned char size,
                           unsigned char opcode) {
    if (inst == VCD_NOOP) size = 0;
    EXPECT_EQ(g_exercise_code_table_->inst1[opcode], inst);
    EXPECT_EQ(g_exercise_code_table_->mode1[opcode], mode);
    EXPECT_EQ(g_exercise_code_table_->size1[opcode], size);
    VerifyInstModeSize(inst, mode, size, opcode);
  }

  void VerifyInstModeSize2(unsigned char inst,
                           unsigned char mode,
                           unsigned char size,
                           unsigned char opcode) {
    if (inst == VCD_NOOP) size = 0;
    EXPECT_EQ(g_exercise_code_table_->inst2[opcode], inst);
    EXPECT_EQ(g_exercise_code_table_->mode2[opcode], mode);
    EXPECT_EQ(g_exercise_code_table_->size2[opcode], size);
    VerifyInstModeSize(inst, mode, size, opcode);
  }

  // This value is designed so that the total number of inst values and modes
  // will equal 8 (VCD_NOOP, VCD_ADD, VCD_RUN, VCD_COPY modes 0 - 4).
  // Eight combinations of inst and mode, times two possible size values,
  // squared (because there are two instructions per opcode), makes
  // exactly 256 possible instruction combinations, which fits kCodeTableSize
  // (the number of opcodes in the table.)
  static const int kLastExerciseMode = 4;

  // The buffer size (in bytes) needed to store kCodeTableSize opcodes plus
  // up to kCodeTableSize VarintBE-encoded size values.
  static const int instruction_buffer_size;

  // A code table that exercises as many combinations as possible:
  // 2 instructions, each is a NOOP, ADD, RUN, or one of 5 copy modes
  // (== 8 total combinations of inst and mode), and each has
  // size == 0 or 255 (2 possibilities.)
  static VCDiffCodeTableData* g_exercise_code_table_;

  VCDiffCodeReader reader_;

  // A buffer to which instructions and sizes will be added manually
  // in order to exercise VCDiffCodeReader.
  std::vector<char> instructions_and_sizes_;

  // The size and mode returned by GetNextInstruction().
  int32_t found_size_;
  unsigned char found_mode_;
};

VCDiffCodeTableData* DecodeTableTest::g_exercise_code_table_ = NULL;

const int DecodeTableTest::instruction_buffer_size =
    VCDiffCodeTableData::kCodeTableSize *
        (1 + (VarintBE<VCDAddress>::kMaxBytes));

TEST_F(DecodeTableTest, ReadAdd) {
  instructions_and_sizes_[0] = 1;
  VarintBE<VCDAddress>::Encode(257, &instructions_and_sizes_[1]);
  unsigned char found_inst = reader_.GetNextInstruction(&found_size_,
                                                        &found_mode_);
  EXPECT_EQ(VCD_ADD, found_inst);
  EXPECT_EQ(257, found_size_);
  EXPECT_EQ(0, found_mode_);
}

TEST_F(DecodeTableTest, ReadRun) {
  instructions_and_sizes_[0] = 0;
  VarintBE<VCDAddress>::Encode(111, &instructions_and_sizes_[1]);
  unsigned char found_inst = reader_.GetNextInstruction(&found_size_,
                                                        &found_mode_);
  EXPECT_EQ(VCD_RUN, found_inst);
  EXPECT_EQ(111, found_size_);
  EXPECT_EQ(0, found_mode_);
}

TEST_F(DecodeTableTest, ReadCopy) {
  instructions_and_sizes_[0] = 58;
  instructions_and_sizes_[1] = 0;
  unsigned char found_inst = reader_.GetNextInstruction(&found_size_,
                                                        &found_mode_);
  EXPECT_EQ(VCD_COPY, found_inst);
  EXPECT_EQ(10, found_size_);
  EXPECT_EQ(2, found_mode_);
}

TEST_F(DecodeTableTest, ReadAddCopy) {
  instructions_and_sizes_[0] = '\xAF';  // 175
  instructions_and_sizes_[1] = 0;
  unsigned char found_inst = reader_.GetNextInstruction(&found_size_,
                                                        &found_mode_);
  EXPECT_EQ(VCD_ADD, found_inst);
  EXPECT_EQ(1, found_size_);
  EXPECT_EQ(0, found_mode_);
  found_inst = reader_.GetNextInstruction(&found_size_, &found_mode_);
  EXPECT_EQ(VCD_COPY, found_inst);
  EXPECT_EQ(4, found_size_);
  EXPECT_EQ(1, found_mode_);
}

TEST_F(DecodeTableTest, ReadCopyAdd) {
  instructions_and_sizes_[0] = '\xFF';  // 255
  instructions_and_sizes_[1] = 0;
  unsigned char found_inst = reader_.GetNextInstruction(&found_size_,
                                                        &found_mode_);
  EXPECT_EQ(VCD_COPY, found_inst);
  EXPECT_EQ(4, found_size_);
  EXPECT_EQ(8, found_mode_);
  found_mode_ = 0;
  found_inst = reader_.GetNextInstruction(&found_size_, &found_mode_);
  EXPECT_EQ(VCD_ADD, found_inst);
  EXPECT_EQ(1, found_size_);
  EXPECT_EQ(0, found_mode_);
}

TEST_F(DecodeTableTest, SaveRestore) {
  instructions_and_sizes_[0] = '\xFF';
  instructions_and_sizes_[1] = 0;
  auto saved = reader_.Save();
  unsigned char found_inst = reader_.GetNextInstruction(&found_size_,
                                                        &found_mode_);
  EXPECT_EQ(VCD_COPY, found_inst);
  EXPECT_EQ(4, found_size_);
  EXPECT_EQ(8, found_mode_);
  reader_.Restore(saved);
  found_mode_ = 0;
  found_inst = reader_.GetNextInstruction(&found_size_, &found_mode_);
  EXPECT_EQ(VCD_COPY, found_inst);
  EXPECT_EQ(4, found_size_);
  EXPECT_EQ(8, found_mode_);
  found_mode_ = 0;
  found_inst = reader_.GetNextInstruction(&found_size_, &found_mode_);
  EXPECT_EQ(VCD_ADD, found_inst);
  EXPECT_EQ(1, found_size_);
  EXPECT_EQ(0, found_mode_);
}

TEST_F(DecodeTableTest, ReReadIncomplete) {
  instructions_and_sizes_[0] = '\xAF';  // Add(1) + Copy1(4)
  instructions_and_sizes_[1] = 1;       // Add(0)
  instructions_and_sizes_[2] = 111;     // with size 111
  instructions_and_sizes_[3] = '\xFF';  // Copy8(4) + Add(1)

  reader_.Init(&instructions_and_sizes_[0], 0);  // 0 bytes available
  // We must call Restore() when VCD_INSTRUCTION_END_OF_DATA is returned.
  // For that to work, we call Save() before GetNextInstruction().
  // For simplicity, we don't call Save() before instructions that are
  // expected to succeed.
  auto saved = reader_.Save();
  EXPECT_EQ(VCD_INSTRUCTION_END_OF_DATA,
            reader_.GetNextInstruction(&found_size_, &found_mode_));
  reader_.Restore(saved);
  EXPECT_EQ(&instructions_and_sizes_[0], reader_.UnparsedData());

  reader_.Init(reader_.UnparsedData(), 1);  // 1 more byte available
  EXPECT_EQ(VCD_ADD, reader_.GetNextInstruction(&found_size_, &found_mode_));
  EXPECT_EQ(1, found_size_);
  EXPECT_EQ(0, found_mode_);
  EXPECT_EQ(VCD_COPY, reader_.GetNextInstruction(&found_size_, &found_mode_));
  EXPECT_EQ(4, found_size_);
  EXPECT_EQ(1, found_mode_);
  saved = reader_.Save();
  EXPECT_EQ(VCD_INSTRUCTION_END_OF_DATA,
            reader_.GetNextInstruction(&found_size_, &found_mode_));
  reader_.Restore(saved);
  EXPECT_EQ(&instructions_and_sizes_[1], reader_.UnparsedData());

  reader_.Init(reader_.UnparsedData(), 1);  // 1 more byte available
  saved = reader_.Save();
  // The opcode is available, but the separately encoded size is not
  EXPECT_EQ(VCD_INSTRUCTION_END_OF_DATA,
            reader_.GetNextInstruction(&found_size_, &found_mode_));
  reader_.Restore(saved);
  EXPECT_EQ(&instructions_and_sizes_[1], reader_.UnparsedData());

  reader_.Init(reader_.UnparsedData(), 2);  // 2 more bytes available
  EXPECT_EQ(VCD_ADD, reader_.GetNextInstruction(&found_size_, &found_mode_));
  EXPECT_EQ(111, found_size_);
  EXPECT_EQ(0, found_mode_);
  saved = reader_.Save();
  EXPECT_EQ(VCD_INSTRUCTION_END_OF_DATA,
            reader_.GetNextInstruction(&found_size_, &found_mode_));
  reader_.Restore(saved);
  EXPECT_EQ(&instructions_and_sizes_[3], reader_.UnparsedData());

  reader_.Init(reader_.UnparsedData(), 1);  // 1 more byte available
  EXPECT_EQ(VCD_COPY, reader_.GetNextInstruction(&found_size_, &found_mode_));
  EXPECT_EQ(4, found_size_);
  EXPECT_EQ(8, found_mode_);
  EXPECT_EQ(VCD_ADD, reader_.GetNextInstruction(&found_size_, &found_mode_));
  EXPECT_EQ(1, found_size_);
  EXPECT_EQ(0, found_mode_);
  saved = reader_.Save();
  EXPECT_EQ(VCD_INSTRUCTION_END_OF_DATA,
            reader_.GetNextInstruction(&found_size_, &found_mode_));
  reader_.Restore(saved);
  EXPECT_EQ(&instructions_and_sizes_[4], reader_.UnparsedData());
}

TEST_F(DecodeTableTest, ExerciseCodeTableReader) {
  char* instruction_ptr = &instructions_and_sizes_[0];
  for (int opcode = 0; opcode < VCDiffCodeTableData::kCodeTableSize; ++opcode) {
    *instruction_ptr = opcode;
    ++instruction_ptr;
    if ((g_exercise_code_table_->inst1[opcode] != VCD_NOOP) &&
        (g_exercise_code_table_->size1[opcode] == 0)) {
      // A separately-encoded size value
      int encoded_size = VarintBE<VCDAddress>::Encode(1000 + opcode,
                                                      instruction_ptr);
      EXPECT_LT(0, encoded_size);
      instruction_ptr += encoded_size;
    }
    if ((g_exercise_code_table_->inst2[opcode] != VCD_NOOP) &&
        (g_exercise_code_table_->size2[opcode] == 0)) {
      int encoded_size = VarintBE<VCDAddress>::Encode(1000 + opcode,
                                                      instruction_ptr);
      EXPECT_LT(0, encoded_size);
      instruction_ptr += encoded_size;
    }
  }
  EXPECT_TRUE(reader_.UseCodeTable(*g_exercise_code_table_, kLastExerciseMode));
  int opcode = 0;
  // This loop has the same bounds as the one in SetUpTestCase.
  // Iterate over the instruction types and make sure that the opcodes,
  // interpreted in order, return exactly those instruction types.
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
      VerifyInstModeSize1(inst1, mode1, 0, opcode);
      VerifyInstModeSize2(inst2, mode2, 0, opcode);
      ++opcode;
      VerifyInstModeSize1(inst1, mode1, 0, opcode);
      VerifyInstModeSize2(inst2, mode2, 255, opcode);
      ++opcode;
      VerifyInstModeSize1(inst1, mode1, 255, opcode);
      VerifyInstModeSize2(inst2, mode2, 0, opcode);
      ++opcode;
      VerifyInstModeSize1(inst1, mode1, 255, opcode);
      VerifyInstModeSize2(inst2, mode2, 255, opcode);
      ++opcode;
    }
  }
  CHECK_EQ(VCDiffCodeTableData::kCodeTableSize, opcode);
}

}  // unnamed namespace
}  // namespace open_vcdiff
