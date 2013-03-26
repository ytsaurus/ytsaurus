#include "lrc.h"
#include "helpers.h"
#include "jerasure.h"

#include <ytlib/misc/foreach.h>

#include <contrib/libs/jerasure/jerasure.h>
#include <contrib/libs/jerasure/cauchy.h>

#include <algorithm>

namespace NYT {
namespace NErasure {

////////////////////////////////////////////////////////////////////////////////

namespace {

TSharedRef Xor(const std::vector<TSharedRef>& refs)
{
    // TODO(ignat): optimize it using SSE
    size_t size = refs.front().Size();
    TBlob result(size); // this also fills it with zeros
    FOREACH (const auto& ref, refs) {
        const char* data = ref.Begin();
        for (size_t pos = 0; pos < size; ++pos) {
            result[pos] ^= data[pos];
        }
    }
    return TSharedRef::FromBlob(std::move(result));
}

TBlockIndexList UniqueSortedIndices(const TBlockIndexList& indices)
{
    auto copy = indices;
    std::sort(copy.begin(), copy.end());
    copy.erase(std::unique(copy.begin(), copy.end()), copy.end());
    return copy;
}

TBlockIndexList ExtractRows(const TBlockIndexList& matrix, int width, const TBlockIndexList& rows)
{
    YCHECK(matrix.size() % width == 0);
    TBlockIndexList result(width * rows.size());
    for (int i = 0; i < rows.size(); ++i) {
        auto start = matrix.begin() + rows[i] * width;
        std::copy(start, start + width, result.begin() + i * width);
    }
    return result;
}

} // anonymous namespace

////////////////////////////////////////////////////////////////////////////////

TLrc::TLrc(int blockCount)
    : BlockCount_(blockCount)
    , ParityCount_(4)
    , WordSize_(8)
{
    // Block count must be even
    YCHECK(blockCount % 2 == 0);
    // Block count should be enough small to construct proper matrix
    YCHECK(1 + blockCount / 2 < (1 << (WordSize_ / 2)));

    Matrix_.resize(ParityCount_ * BlockCount_);
    for (int row = 0; row < ParityCount_; ++row) {
        for (int column = 0; column < BlockCount_; ++column) {
            int index = row * BlockCount_ + column;

            bool isFirstHalf = column < BlockCount_ / 2;
            if (row == 0) Matrix_[index] = isFirstHalf ? 1 : 0;
            if (row == 1) Matrix_[index] = isFirstHalf ? 0 : 1;

            // Let alpha_i be coefficient of first half and beta_i of the second half.
            // Then matrix is non-singular iff:
            //   a) alpha_i, beta_j != 0
            //   b) alpha_i != beta_j
            //   c) alpha_i + alpha_k != beta_j + beta_l
            // for any i, j, k, l.
            if (row == 2) {
                int shift = isFirstHalf ? 1 : (1 << (WordSize_ / 2));
                int relativeColumn = isFirstHalf ? column : (column - (BlockCount_ / 2));
                Matrix_[index] = shift * (1 + relativeColumn);
            }

            // The last row is the square of the row before last.
            if (row == 3) {
                int prev = Matrix_[index - BlockCount_];
                Matrix_[index] = galois_single_multiply(prev, prev, WordSize_);
            }
        }
    }

    BitMatrix_ = TMatrix(jerasure_matrix_to_bitmatrix(BlockCount_, ParityCount_, WordSize_, Matrix_.data()));
    Schedule_ = TSchedule(jerasure_dumb_bitmatrix_to_schedule(BlockCount_, ParityCount_, WordSize_, BitMatrix_.Get()));



    Groups_[0] = MakeSegment(0, BlockCount_ / 2);
    Groups_[0].push_back(BlockCount_);

    Groups_[1] = MakeSegment(BlockCount_ / 2, BlockCount_);
    Groups_[1].push_back(BlockCount_ + 1);
}

std::vector<TSharedRef> TLrc::Encode(const std::vector<TSharedRef>& blocks)
{
    return ScheduleEncode(BlockCount_, ParityCount_, WordSize_, Schedule_, blocks);
}

std::vector<TSharedRef> TLrc::Decode(
    const std::vector<TSharedRef>& blocks,
    const TBlockIndexList& erasedIndices)
{
    if (erasedIndices.empty()) {
        return std::vector<TSharedRef>();
    }

    i64 blockLength = blocks.front().Size();
    for (int i = 1; i < blocks.size(); ++i) {
        YCHECK(blocks[i].Size() == blockLength);
    }

    auto indices = UniqueSortedIndices(erasedIndices);

    // We can restore one block by xor.
    if (indices.size() == 1) {
        int index = erasedIndices.front();
        for (int i = 0; i < 2; ++i) {
            if (Contains(Groups_[i], index)) {
                return std::vector<TSharedRef>(1, Xor(blocks));
            }
        }
    }

    auto recoveryIndices = *GetRepairIndices(indices);
    // We can restore two blocks from different groups using xor.
    if (indices.size() == 2 &&
        indices.back() < BlockCount_ + 2 &&
        recoveryIndices.back() < BlockCount_ + 2)
    {
        std::vector<TSharedRef> result;
        FOREACH (int index, indices) {
            for (int groupIndex = 0; groupIndex < 2; ++groupIndex) {
                if (!Contains(Groups_[groupIndex], index)) continue;

                std::vector<TSharedRef> correspondingBlocks;
                FOREACH (int pos, Groups_[groupIndex]) {
                    for (int i = 0; i < blocks.size(); ++i) {
                        if (!(recoveryIndices[i] == pos)) continue;
                        correspondingBlocks.push_back(blocks[i]);
                    }
                }

                result.push_back(Xor(correspondingBlocks));
            }
        }
        return result;
    }

    // Choose subset of matrix rows, corresponding for erased and recovery indices.
    int parityCount = blocks.size() + indices.size() - BlockCount_;

    TBlockIndexList rows;
    for (int i = 0; i < recoveryIndices.size(); ++i) {
        if (recoveryIndices[i] >= BlockCount_) {
            rows.push_back(recoveryIndices[i] - BlockCount_);
        }
    }

    for (int i = 0; i < indices.size(); ++i) {
        if (indices[i] >= BlockCount_) {
            rows.push_back(indices[i] - BlockCount_);
            indices[i] = BlockCount_ + rows.size() - 1;
        }
    }

    auto matrix = ExtractRows(Matrix_, BlockCount_, rows);
    auto bitMatrix = TMatrix(jerasure_matrix_to_bitmatrix(BlockCount_, parityCount, WordSize_, matrix.data()));

    return BitMatrixDecode(BlockCount_, parityCount, WordSize_, bitMatrix, blocks, indices);
}

bool TLrc::CanRepair(const TBlockIndexList& erasedIndices)
{
    // TODO(ignat): it is not so fast. Does overlying layer guarantee that erasedIndices sorted and unique?
    TBlockIndexList indices = UniqueSortedIndices(erasedIndices);
    if (indices.size() > ParityCount_) {
        return false;
    }

    if (indices.size() == 1) {
        int index = indices.front();
        for (int i = 0; i < 2; ++i) {
            if (Contains(Groups_[i], index)) {
                return true;
            }
        }
    }

    if (indices.size() == ParityCount_) {
        for (int i = 0; i < 2; ++i) {
            if (Intersection(indices, Groups_[i]).empty()) {
                return false;
            }
        }
    }

    return true;
}

TNullable<TBlockIndexList> TLrc::GetRepairIndices(const TBlockIndexList& erasedIndices)
{
    if (erasedIndices.empty()) {
        return TBlockIndexList();
    }

    TBlockIndexList indices = UniqueSortedIndices(erasedIndices);

    if (indices.size() > ParityCount_) {
        return Null;
    }

    // One erasure from data or xor blocks.
    if (indices.size() == 1) {
        int index = indices.front();
        for (int i = 0; i < 2; ++i) {
            if (Contains(Groups_[i], index)) {
                return Difference(Groups_[i], index);
            }
        }
    }

    // Null if we have 4 erasures in one group.
    if (indices.size() == ParityCount_) {
        bool intersectsAny = true;
        for (int i = 0; i < 2; ++i) {
            if (Intersection(indices, Groups_[i]).empty()) {
                intersectsAny = false;
            }
        }
        if (!intersectsAny) {
            return Null;
        }
    }

    // Calculate coverage of each group.
    int groupCoverage[2] = {0};
    FOREACH (int index, indices) {
        for (int i = 0; i < 2; ++i) {
            if (Contains(Groups_[i], index)) {
                groupCoverage[i] += 1;
            }
        }
    }

    // Two erasures, one in each group.
    if (indices.size() == 2 && groupCoverage[0] == 1 && groupCoverage[1] == 1) {
        return Difference(Union(Groups_[0], Groups_[1]), indices);
    }

    // Erasures in only parity blocks.
    if (indices.front() >= BlockCount_) {
        return MakeSegment(0, BlockCount_);
    }

    // Remove unnecessary xor parities.
    auto result = Difference(0, BlockCount_ + ParityCount_, indices);
    for (int i = 0; i < 2; ++i) {
        if (groupCoverage[i] == 0 && indices.size() <= 3) {
            result = Difference(result, BlockCount_ + i);
        }
    }

    return result;
}

int TLrc::GetDataBlockCount()
{
    return BlockCount_;
}

int TLrc::GetParityBlockCount()
{
    return ParityCount_;
}

int TLrc::GetWordSize()
{
    return WordSize_ * 8;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT

