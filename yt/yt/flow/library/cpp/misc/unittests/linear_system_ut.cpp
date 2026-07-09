#include <yt/yt/flow/library/cpp/misc/linear_system.h>

#include <yt/yt/core/test_framework/framework.h>

#include <cmath>

namespace NYT::NFlow {
namespace {

////////////////////////////////////////////////////////////////////////////////
// AddRow tests
////////////////////////////////////////////////////////////////////////////////

TEST(TLinearSystemTest, EmptySystemHasZeroDimensions)
{
    // An empty system produces an empty solution.
    TLinearSystem sys;
    EXPECT_TRUE(sys.Solve().empty());
}

TEST(TLinearSystemTest, AddRowIncrementsCounters)
{
    // After one row with one variable the solution has size 1;
    // after a second row introducing a second variable it has size 2.
    TLinearSystem sys;
    sys.AddRow({{0, 1.0}}, 1.0);
    EXPECT_EQ(static_cast<ssize_t>(sys.Solve().size()), 1);

    sys.AddRow({{0, 2.0}, {1, 3.0}}, 5.0);
    EXPECT_EQ(static_cast<ssize_t>(sys.Solve().size()), 2);
}

TEST(TLinearSystemTest, AddRowExpandsColumnsForExistingRows)
{
    // First row references only column 0; second row references column 2.
    // The solution must have 3 entries; the implicit zero in column 1 of the
    // first row must not corrupt the result.
    // System: x0 = 1, x2 = 2/5  (x1 unconstrained → 0 minimum-norm).
    TLinearSystem sys;
    sys.AddRow({{0, 1.0}}, 1.0);
    sys.AddRow({{2, 5.0}}, 2.0);

    auto x = sys.Solve();
    ASSERT_EQ(static_cast<ssize_t>(x.size()), 3);
    EXPECT_NEAR(x[0], 1.0, 1e-4);
    EXPECT_NEAR(x[1], 0.0, 1e-4); // unconstrained — minimum-norm gives 0
    EXPECT_NEAR(x[2], 0.4, 1e-4);
}

TEST(TLinearSystemTest, AddRowSparseCoefficientsStoredCorrectly)
{
    // Sparse row: only columns 1 and 3 are non-zero.
    // 2.5·x1 − x3 = 7  (x0, x2 unconstrained → 0).
    // Minimum-norm solution on the line 2.5·x1 − x3 = 7:
    //   x1 = 2.5·7/(2.5²+1²), x3 = -1·7/(2.5²+1²).
    TLinearSystem sys;
    sys.AddRow({{1, 2.5}, {3, -1.0}}, 7.0);

    auto x = sys.Solve();
    ASSERT_EQ(static_cast<ssize_t>(x.size()), 4);
    EXPECT_NEAR(x[0], 0.0, 1e-4);
    EXPECT_NEAR(x[1], 2.5 * 7.0 / (2.5 * 2.5 + 1.0), 1e-4);
    EXPECT_NEAR(x[2], 0.0, 1e-4);
    EXPECT_NEAR(x[3], -1.0 * 7.0 / (2.5 * 2.5 + 1.0), 1e-4);
}

TEST(TLinearSystemTest, AddRowEmptySparseRow)
{
    // An empty sparse row adds a zero row and does not change the column count.
    // x0 = 1 (from first row); second row is 0·x0 = 0 — consistent.
    TLinearSystem sys;
    sys.AddRow({{0, 1.0}}, 1.0);
    sys.AddRow({}, 0.0);

    auto x = sys.Solve();
    ASSERT_EQ(static_cast<ssize_t>(x.size()), 1);
    EXPECT_NEAR(x[0], 1.0, 1e-4);
}

////////////////////////////////////////////////////////////////////////////////
// Solve — degenerate cases
////////////////////////////////////////////////////////////////////////////////

TEST(TLinearSystemTest, SolveNoColumnsReturnsEmpty)
{
    // A system with rows but no columns (all-empty sparse rows).
    TLinearSystem sys;
    sys.AddRow({}, 1.0);
    EXPECT_TRUE(sys.Solve().empty());
}

////////////////////////////////////////////////////////////////////////////////
// Solve — exactly determined systems
////////////////////////////////////////////////////////////////////////////////

TEST(TLinearSystemTest, SolveOneEquationOneVariable)
{
    // 3·x = 9  →  x = 3
    TLinearSystem sys;
    sys.AddRow({{0, 3.0}}, 9.0);

    auto x = sys.Solve();
    ASSERT_EQ(static_cast<ssize_t>(x.size()), 1);
    EXPECT_NEAR(x[0], 3.0, 1e-4);
}

TEST(TLinearSystemTest, SolveTwoEquationsTwoVariables)
{
    // x0 + 2·x1 = 5
    // 3·x0 + 4·x1 = 11
    // Solution: x0 = 1, x1 = 2
    TLinearSystem sys;
    sys.AddRow({{0, 1.0}, {1, 2.0}}, 5.0);
    sys.AddRow({{0, 3.0}, {1, 4.0}}, 11.0);

    auto x = sys.Solve();
    ASSERT_EQ(static_cast<ssize_t>(x.size()), 2);
    EXPECT_NEAR(x[0], 1.0, 1e-4);
    EXPECT_NEAR(x[1], 2.0, 1e-4);
}

TEST(TLinearSystemTest, SolveIdentitySystem)
{
    // x0 = 4, x1 = 7, x2 = -3
    TLinearSystem sys;
    sys.AddRow({{0, 1.0}}, 4.0);
    sys.AddRow({{1, 1.0}}, 7.0);
    sys.AddRow({{2, 1.0}}, -3.0);

    auto x = sys.Solve();
    ASSERT_EQ(static_cast<ssize_t>(x.size()), 3);
    EXPECT_NEAR(x[0], 4.0, 1e-4);
    EXPECT_NEAR(x[1], 7.0, 1e-4);
    EXPECT_NEAR(x[2], -3.0, 1e-4);
}

TEST(TLinearSystemTest, SolveDiagonalScaledSystem)
{
    // 2·x0 = 6  →  x0 = 3
    // 5·x1 = 20 →  x1 = 4
    TLinearSystem sys;
    sys.AddRow({{0, 2.0}}, 6.0);
    sys.AddRow({{1, 5.0}}, 20.0);

    auto x = sys.Solve();
    ASSERT_EQ(static_cast<ssize_t>(x.size()), 2);
    EXPECT_NEAR(x[0], 3.0, 1e-4);
    EXPECT_NEAR(x[1], 4.0, 1e-4);
}

TEST(TLinearSystemTest, SolveZeroRhsGivesZeroSolution)
{
    // A·x = 0 with full-rank A → x = 0
    TLinearSystem sys;
    sys.AddRow({{0, 1.0}, {1, 2.0}}, 0.0);
    sys.AddRow({{0, 3.0}, {1, 4.0}}, 0.0);

    auto x = sys.Solve();
    ASSERT_EQ(static_cast<ssize_t>(x.size()), 2);
    EXPECT_NEAR(x[0], 0.0, 1e-4);
    EXPECT_NEAR(x[1], 0.0, 1e-4);
}

////////////////////////////////////////////////////////////////////////////////
// Solve — overdetermined (least-squares) systems
////////////////////////////////////////////////////////////////////////////////

TEST(TLinearSystemTest, SolveOverdeterminedConsistentSystem)
{
    // Three consistent equations for two unknowns.
    // x0 + x1 = 3
    // x0 - x1 = 1
    // 2·x0    = 4
    // Exact solution: x0 = 2, x1 = 1
    TLinearSystem sys;
    sys.AddRow({{0, 1.0}, {1, 1.0}}, 3.0);
    sys.AddRow({{0, 1.0}, {1, -1.0}}, 1.0);
    sys.AddRow({{0, 2.0}}, 4.0);

    auto x = sys.Solve();
    ASSERT_EQ(static_cast<ssize_t>(x.size()), 2);
    EXPECT_NEAR(x[0], 2.0, 1e-4);
    EXPECT_NEAR(x[1], 1.0, 1e-4);
}

TEST(TLinearSystemTest, SolveOverdeterminedLeastSquares)
{
    // Classic least-squares fit: fit a constant c to noisy observations.
    // c = 1, c = 2, c = 3  →  least-squares solution: c = 2
    TLinearSystem sys;
    sys.AddRow({{0, 1.0}}, 1.0);
    sys.AddRow({{0, 1.0}}, 2.0);
    sys.AddRow({{0, 1.0}}, 3.0);

    auto x = sys.Solve();
    ASSERT_EQ(static_cast<ssize_t>(x.size()), 1);
    EXPECT_NEAR(x[0], 2.0, 1e-4);
}

TEST(TLinearSystemTest, SolveOverdeterminedLinearFit)
{
    // Fit y = a·x + b to four points on the line y = 2x + 1:
    //   (0,1), (1,3), (2,5), (3,7)
    // Exact solution: a = 2, b = 1
    TLinearSystem sys;
    sys.AddRow({{0, 0.0}, {1, 1.0}}, 1.0);
    sys.AddRow({{0, 1.0}, {1, 1.0}}, 3.0);
    sys.AddRow({{0, 2.0}, {1, 1.0}}, 5.0);
    sys.AddRow({{0, 3.0}, {1, 1.0}}, 7.0);

    auto x = sys.Solve();
    ASSERT_EQ(static_cast<ssize_t>(x.size()), 2);
    EXPECT_NEAR(x[0], 2.0, 1e-4); // slope
    EXPECT_NEAR(x[1], 1.0, 1e-4); // intercept
}

TEST(TLinearSystemTest, SolveResidualIsMinimised)
{
    // For an inconsistent overdetermined system the solver must minimise
    // ‖A·x − b‖².  Verify by checking that the residual of the returned
    // solution is no larger than the residual of a nearby perturbed point.
    TLinearSystem sys;
    sys.AddRow({{0, 1.0}}, 1.0);
    sys.AddRow({{0, 1.0}}, 2.0);
    sys.AddRow({{0, 1.0}}, 4.0);

    auto x = sys.Solve();
    ASSERT_EQ(static_cast<ssize_t>(x.size()), 1);

    // Residual at the returned solution.
    double res = 0.0;
    for (double b : {1.0, 2.0, 4.0}) {
        double r = x[0] - b;
        res += r * r;
    }

    // Residual at a slightly perturbed point.
    double xPerturbed = x[0] + 0.1;
    double resPerturbed = 0.0;
    for (double b : {1.0, 2.0, 4.0}) {
        double r = xPerturbed - b;
        resPerturbed += r * r;
    }

    EXPECT_LE(res, resPerturbed);
}

////////////////////////////////////////////////////////////////////////////////
// Solve — sparse column indexing
////////////////////////////////////////////////////////////////////////////////

TEST(TLinearSystemTest, SolveSparseColumnIndices)
{
    // Columns 0 and 2 are used; column 1 is implicitly zero.
    // 1·x0 + 0·x1 + 2·x2 = 5
    // 3·x0 + 0·x1 + 4·x2 = 11
    // Least-squares solution for x0 and x2 (x1 is unconstrained → 0).
    TLinearSystem sys;
    sys.AddRow({{0, 1.0}, {2, 2.0}}, 5.0);
    sys.AddRow({{0, 3.0}, {2, 4.0}}, 11.0);

    auto x = sys.Solve();
    ASSERT_EQ(static_cast<ssize_t>(x.size()), 3);
    EXPECT_NEAR(x[0], 1.0, 1e-4);
    EXPECT_NEAR(x[1], 0.0, 1e-4); // unconstrained — stays at zero
    EXPECT_NEAR(x[2], 2.0, 1e-4);
}

TEST(TLinearSystemTest, SolveColumnsAddedIncrementally)
{
    // First row uses column 0 only; second row introduces column 1.
    // x0 = 3
    // x0 + x1 = 5  →  x1 = 2
    TLinearSystem sys;
    sys.AddRow({{0, 1.0}}, 3.0);
    sys.AddRow({{0, 1.0}, {1, 1.0}}, 5.0);

    auto x = sys.Solve();
    ASSERT_EQ(static_cast<ssize_t>(x.size()), 2);
    EXPECT_NEAR(x[0], 3.0, 1e-4);
    EXPECT_NEAR(x[1], 2.0, 1e-4);
}

////////////////////////////////////////////////////////////////////////////////
// Solve — solution satisfies normal equations
////////////////////////////////////////////////////////////////////////////////

TEST(TLinearSystemTest, SolveSatisfiesNormalEquations)
{
    // For any least-squares solution x*, the normal equations Aᵀ·(A·x* − b) = 0
    // must hold to within tolerance.
    // Keep a local copy of A and b so we can verify the residual without
    // accessing private fields.
    //   row 0: x0        = 2
    //   row 1:      x1   = 3
    //   row 2: x0 + x1   = 6
    const std::vector<std::array<double, 2>> A = {
        {1.0, 0.0},
        {0.0, 1.0},
        {1.0, 1.0},
    };
    const std::vector<double> B = {2.0, 3.0, 6.0};

    TLinearSystem sys;
    sys.AddRow({{0, A[0][0]}, {1, A[0][1]}}, B[0]);
    sys.AddRow({{0, A[1][0]}, {1, A[1][1]}}, B[1]);
    sys.AddRow({{0, A[2][0]}, {1, A[2][1]}}, B[2]);

    auto x = sys.Solve();
    ASSERT_EQ(static_cast<ssize_t>(x.size()), 2);

    const ssize_t numRows = static_cast<ssize_t>(A.size());
    const ssize_t numCols = 2;

    // Compute residual r = A·x − b.
    std::vector<double> r(numRows, 0.0);
    for (ssize_t k = 0; k < numRows; ++k) {
        for (ssize_t j = 0; j < numCols; ++j) {
            r[k] += A[k][j] * x[j];
        }
        r[k] -= B[k];
    }

    // Compute Aᵀ·r and check it is near zero.
    for (ssize_t j = 0; j < numCols; ++j) {
        double atr = 0.0;
        for (ssize_t k = 0; k < numRows; ++k) {
            atr += A[k][j] * r[k];
        }
        EXPECT_NEAR(atr, 0.0, 1e-4) << "Normal equation violated for column " << j;
    }
}

////////////////////////////////////////////////////////////////////////////////
// Solve — underdetermined systems
////////////////////////////////////////////////////////////////////////////////

TEST(TLinearSystemTest, SolveUnderdeterminedOneEquationTwoVariables)
{
    // x0 + x1 = 4  — one equation, two unknowns.
    // The solution space is the line x0 + x1 = 4.
    // JacobiSVD finds the minimum-norm solution: x0 = x1 = 2.
    TLinearSystem sys;
    sys.AddRow({{0, 1.0}, {1, 1.0}}, 4.0);

    auto x = sys.Solve();
    ASSERT_EQ(static_cast<ssize_t>(x.size()), 2);

    // The returned point must satisfy the equation.
    EXPECT_NEAR(x[0] + x[1], 4.0, 1e-4);

    // Minimum-norm solution: x0 = x1 = 2.
    EXPECT_NEAR(x[0], 2.0, 1e-4);
    EXPECT_NEAR(x[1], 2.0, 1e-4);
}

TEST(TLinearSystemTest, SolveUnderdeterminedTwoEquationsThreeVariables)
{
    // x0 + x1      = 3
    //      x1 + x2 = 5
    // Two equations, three unknowns — infinitely many solutions.
    // JacobiSVD finds the minimum-norm solution: x0 = 1/3, x1 = 8/3, x2 = 7/3.
    TLinearSystem sys;
    sys.AddRow({{0, 1.0}, {1, 1.0}}, 3.0);
    sys.AddRow({{1, 1.0}, {2, 1.0}}, 5.0);

    auto x = sys.Solve();
    ASSERT_EQ(static_cast<ssize_t>(x.size()), 3);

    // Both equations must be satisfied.
    EXPECT_NEAR(x[0] + x[1], 3.0, 1e-4);
    EXPECT_NEAR(x[1] + x[2], 5.0, 1e-4);

    // Minimum-norm coordinates.
    EXPECT_NEAR(x[0], 1.0 / 3.0, 1e-4);
    EXPECT_NEAR(x[1], 8.0 / 3.0, 1e-4);
    EXPECT_NEAR(x[2], 7.0 / 3.0, 1e-4);
}

////////////////////////////////////////////////////////////////////////////////
// TKeyedLinearSystem tests
////////////////////////////////////////////////////////////////////////////////

TEST(TKeyedLinearSystemTest, SolveTwoEquationsTwoKeys)
{
    // Same system as TLinearSystemTest::SolveTwoEquationsTwoVariables, but
    // variables are identified by string keys instead of integer indices.
    //   "x" + 2·"y" = 5
    //   3·"x" + 4·"y" = 11
    // Exact solution: x = 1, y = 2.
    TKeyedLinearSystem<TString> sys;
    sys.AddRow({{"x", 1.0}, {"y", 2.0}}, 5.0);
    sys.AddRow({{"x", 3.0}, {"y", 4.0}}, 11.0);

    auto result = sys.Solve();
    ASSERT_TRUE(result.contains("x"));
    ASSERT_TRUE(result.contains("y"));
    EXPECT_NEAR(result.at("x"), 1.0, 1e-4);
    EXPECT_NEAR(result.at("y"), 2.0, 1e-4);
}

TEST(TKeyedLinearSystemTest, SolveKeysIntroducedInDifferentRows)
{
    // Keys are introduced across different AddRow calls; the system must
    // assign consistent indices regardless of insertion order.
    //   "a" = 4
    //   "a" + "b" = 7  →  "b" = 3
    TKeyedLinearSystem<TString> sys;
    sys.AddRow({{"a", 1.0}}, 4.0);
    sys.AddRow({{"a", 1.0}, {"b", 1.0}}, 7.0);

    auto result = sys.Solve();
    ASSERT_TRUE(result.contains("a"));
    ASSERT_TRUE(result.contains("b"));
    EXPECT_NEAR(result.at("a"), 4.0, 1e-4);
    EXPECT_NEAR(result.at("b"), 3.0, 1e-4);
}

TEST(TKeyedLinearSystemTest, SolveResultContainsExactlyKnownKeys)
{
    // The result map must contain exactly the keys that were mentioned in
    // AddRow calls — no more, no less.
    TKeyedLinearSystem<int> sys;
    sys.AddRow({{10, 1.0}}, 5.0);
    sys.AddRow({{20, 2.0}}, 8.0);

    auto result = sys.Solve();
    EXPECT_EQ(result.size(), 2u);
    ASSERT_TRUE(result.contains(10));
    ASSERT_TRUE(result.contains(20));
    EXPECT_NEAR(result.at(10), 5.0, 1e-4);
    EXPECT_NEAR(result.at(20), 4.0, 1e-4);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
