#include "lrc.h"
#include "helpers.h"
#include "jerasure.h"

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
    for (const auto& ref : refs) {
        const char* data = ref.Begin();
        for (size_t pos = 0; pos < size; ++pos) {
            result[pos] ^= data[pos];
        }
    }
    return TSharedRef::FromBlob(std::move(result));
}

TPartIndexList UniqueSortedIndices(const TPartIndexList& indices)
{
    auto copy = indices;
    std::sort(copy.begin(), copy.end());
    copy.erase(std::unique(copy.begin(), copy.end()), copy.end());
    return copy;
}

TPartIndexList ExtractRows(const TPartIndexList& matrix, int width, const TPartIndexList& rows)
{
    YCHECK(matrix.size() % width == 0);
    TPartIndexList result(width * rows.size());
    for (int i = 0; i < rows.size(); ++i) {
        auto start = matrix.begin() + rows[i] * width;
        std::copy(start, start + width, result.begin() + i * width);
    }
    return result;
}

} // namespace

const int TLrc::BitmaskOptimizationThreshold = 22;

TLrc::TLrc(int dataPartCount)
    : DataPartCount_(dataPartCount)
    , ParityPartCount_(4)
    , WordSize_(8)
{
    // Data part count must be even
    YCHECK(dataPartCount % 2 == 0);
    // Data part count should be enough small to construct proper matrix
    YCHECK(1 + dataPartCount / 2 < (1 << (WordSize_ / 2)));

    Matrix_.resize(ParityPartCount_ * DataPartCount_);
    for (int row = 0; row < ParityPartCount_; ++row) {
        for (int column = 0; column < DataPartCount_; ++column) {
            int index = row * DataPartCount_ + column;

            bool isFirstHalf = column < DataPartCount_ / 2;
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
                int relativeColumn = isFirstHalf ? column : (column - (DataPartCount_ / 2));
                Matrix_[index] = shift * (1 + relativeColumn);
            }

            // The last row is the square of the row before last.
            if (row == 3) {
                int prev = Matrix_[index - DataPartCount_];
                Matrix_[index] = galois_single_multiply(prev, prev, WordSize_);
            }
        }
    }

    BitMatrix_ = TMatrix(jerasure_matrix_to_bitmatrix(DataPartCount_, ParityPartCount_, WordSize_, Matrix_.data()));
    Schedule_ = TSchedule(jerasure_dumb_bitmatrix_to_schedule(DataPartCount_, ParityPartCount_, WordSize_, BitMatrix_.Get()));



    Groups_[0] = MakeSegment(0, DataPartCount_ / 2);
    Groups_[0].push_back(DataPartCount_);

    Groups_[1] = MakeSegment(DataPartCount_ / 2, DataPartCount_);
    Groups_[1].push_back(DataPartCount_ + 1);

    auto totalPartCount = DataPartCount_ + ParityPartCount_;
    if (totalPartCount <= BitmaskOptimizationThreshold) {
        CanRepair_.resize(1 << totalPartCount);
        for (int mask = 0; mask < (1 << totalPartCount); ++mask) {
            TPartIndexList erasedIndices;
            for (int i = 0; i < totalPartCount; ++i) {
                if ((mask & (1 << i)) == 0) {
                    erasedIndices.push_back(i);
                }
            }
            CanRepair_[mask] = CalculateCanRepair(erasedIndices);
        }
    }
}

std::vector<TSharedRef> TLrc::Encode(const std::vector<TSharedRef>& blocks) const
{
    return ScheduleEncode(DataPartCount_, ParityPartCount_, WordSize_, Schedule_, blocks);
}

std::vector<TSharedRef> TLrc::Decode(
    const std::vector<TSharedRef>& blocks,
    const TPartIndexList& erasedIndices) const
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
        indices.back() < DataPartCount_ + 2 &&
        recoveryIndices.back() < DataPartCount_ + 2)
    {
        std::vector<TSharedRef> result;
        for (int index : indices) {
            for (int groupIndex = 0; groupIndex < 2; ++groupIndex) {
                if (!Contains(Groups_[groupIndex], index)) continue;

                std::vector<TSharedRef> correspondingBlocks;
                for (int pos : Groups_[groupIndex]) {
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
    int parityCount = blocks.size() + indices.size() - DataPartCount_;

    TPartIndexList rows;
    for (int i = 0; i < recoveryIndices.size(); ++i) {
        if (recoveryIndices[i] >= DataPartCount_) {
            rows.push_back(recoveryIndices[i] - DataPartCount_);
        }
    }

    for (int i = 0; i < indices.size(); ++i) {
        if (indices[i] >= DataPartCount_) {
            rows.push_back(indices[i] - DataPartCount_);
            indices[i] = DataPartCount_ + rows.size() - 1;
        }
    }

    auto matrix = ExtractRows(Matrix_, DataPartCount_, rows);
    auto bitMatrix = TMatrix(jerasure_matrix_to_bitmatrix(DataPartCount_, parityCount, WordSize_, matrix.data()));

    return BitMatrixDecode(DataPartCount_, parityCount, WordSize_, bitMatrix, blocks, indices);
}

bool TLrc::CanRepair(const TPartIndexList& erasedIndices) const
{
    auto totalPartCount = DataPartCount_ + ParityPartCount_;
    if (totalPartCount <= BitmaskOptimizationThreshold) {
        int mask = (1 << (totalPartCount)) - 1;
        for (int index : erasedIndices) {
            mask -= (1 << index);
        }
        return CanRepair_[mask];
    } else {
        return CalculateCanRepair(erasedIndices);
    }
}

bool TLrc::CanRepair(const TPartIndexSet& erasedIndicesMask) const
{
    auto totalPartCount = DataPartCount_ + ParityPartCount_;
    if (totalPartCount <= BitmaskOptimizationThreshold) {
        auto mask = erasedIndicesMask;
        return CanRepair_[mask.flip().to_ulong()];
    } else {
        TPartIndexList erasedIndices;
        for (int i = 0; i < erasedIndicesMask.size(); ++i) {
            if (erasedIndicesMask[i]) {
                erasedIndices.push_back(i);
            }
        }
        return CalculateCanRepair(erasedIndices);
    }
}

bool TLrc::CalculateCanRepair(const TPartIndexList& erasedIndices) const
{
    TPartIndexList indices = UniqueSortedIndices(erasedIndices);
    if (indices.size() > ParityPartCount_) {
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

    if (indices.size() == ParityPartCount_) {
        for (int i = 0; i < 2; ++i) {
            if (Intersection(indices, Groups_[i]).empty()) {
                return false;
            }
        }
    }

    return true;
}

TNullable<TPartIndexList> TLrc::GetRepairIndices(const TPartIndexList& erasedIndices) const
{
    if (erasedIndices.empty()) {
        return TPartIndexList();
    }

    auto indices = UniqueSortedIndices(erasedIndices);

    if (indices.size() > ParityPartCount_) {
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
    if (indices.size() == ParityPartCount_) {
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
    for (int index : indices) {
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
    if (indices.front() >= DataPartCount_) {
        return MakeSegment(0, DataPartCount_);
    }

    // Remove unnecessary xor parities.
    auto result = Difference(0, DataPartCount_ + ParityPartCount_, indices);
    for (int i = 0; i < 2; ++i) {
        if (groupCoverage[i] == 0 && indices.size() <= 3) {
            result = Difference(result, DataPartCount_ + i);
        }
    }

    return result;
}

int TLrc::GetDataPartCount() const
{
    return DataPartCount_;
}

int TLrc::GetParityPartCount() const
{
    return ParityPartCount_;
}

int TLrc::GetWordSize() const
{
    return WordSize_ * 8;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasure
} // namespace NYT

