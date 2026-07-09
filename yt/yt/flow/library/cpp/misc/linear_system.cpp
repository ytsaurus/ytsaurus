#include <yt/yt/flow/library/cpp/misc/linear_system.h>

#include <contrib/libs/eigen/Eigen/SVD>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

void TLinearSystem::AddRow(
    std::vector<std::pair<ssize_t, double>> a,
    double b)
{
    // Determine the required number of columns after this row.
    Eigen::Index newCols = A_.cols();
    for (const auto& [index, coeff] : a) {
        if (index + 1 > newCols) {
            newCols = index + 1;
        }
    }

    const Eigen::Index oldCols = A_.cols();
    const Eigen::Index newRow = A_.rows();

    // Expand columns if needed; zero-initialise the new columns in existing rows.
    if (newCols > oldCols) {
        A_.conservativeResize(Eigen::NoChange, newCols);
        A_.rightCols(newCols - oldCols).setZero();
    }

    // Append a new zero row and right-hand side entry.
    A_.conservativeResize(newRow + 1, A_.cols());
    A_.row(newRow).setZero();
    B_.conservativeResize(newRow + 1);

    // Fill in the non-zero coefficients.
    for (const auto& [index, coeff] : a) {
        A_(newRow, index) = coeff;
    }
    B_(newRow) = b;
}

std::vector<double> TLinearSystem::Solve() const
{
    if (A_.rows() == 0 || A_.cols() == 0) {
        return {};
    }

    // JacobiSVD finds the minimum-norm least-squares solution to A·x = b.
    // ComputeThinU | ComputeThinV is sufficient for solve() and avoids
    // computing the full (potentially large) unitary matrices.
    Eigen::VectorXd x = A_.jacobiSvd(Eigen::ComputeThinU | Eigen::ComputeThinV).solve(B_);

    return std::vector<double>(x.data(), x.data() + x.size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
