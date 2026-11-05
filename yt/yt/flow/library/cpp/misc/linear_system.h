#pragma once

#include <yt/yt/core/misc/public.h>

#include <contrib/libs/eigen/Eigen/Core>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

//! Represents an overdetermined, underdetermined or exactly determined linear system
//! A·x = b and solves it in the least-squares sense via Jacobi SVD
//! (Eigen::JacobiSVD with thin U and V).
//!
//! Rows are added one at a time with AddRow(); the system is solved by calling
//! Solve().
//!
//! Usage example:
//!   TLinearSystem sys;
//!   sys.AddRow({{0, 1.0}, {1, 2.0}}, 5.0);   // x0 + 2·x1 = 5
//!   sys.AddRow({{0, 3.0}, {1, 4.0}}, 11.0);  // 3·x0 + 4·x1 = 11
//!   auto x = sys.Solve();                     // x ≈ {1, 2}
class TLinearSystem
{
public:
    //! Appends a new equation to the system.
    //!
    //! \param a  Sparse representation of the new row: a list of
    //!           (variable_index, coefficient) pairs.  Indices need not be
    //!           contiguous; any gap is treated as a zero coefficient.
    //!           If an index exceeds the current column count, all existing
    //!           rows are extended with zeros automatically.
    //! \param b  Right-hand side value for this equation.
    void AddRow(
        std::vector<std::pair<ssize_t /*variable index*/, double /*coefficient*/>> a,
        double b);

    //! Solves the system in the least-squares sense and returns the solution
    //! vector x of length A_.cols().
    std::vector<double> Solve() const;

private:
    //! Dense coefficient matrix A (rows × cols), grown via conservativeResize.
    Eigen::MatrixXd A_;

    //! Right-hand side vector b (length rows).
    Eigen::VectorXd B_;
};

////////////////////////////////////////////////////////////////////////////////

//! A thin wrapper around TLinearSystem that identifies variables by arbitrary
//! keys of type T instead of integer column indices.
//!
//! Keys are assigned sequential indices on first encounter and are mapped back
//! to their original values in the result.  Any hashable, equality-comparable
//! type can be used as a key (e.g. TString, int, a custom struct with a hash).
//!
//! The underlying solver is identical to TLinearSystem: steepest descent on
//! the normal equations (Aᵀ·A)·x = Aᵀ·b, which finds the least-squares
//! (minimum-norm for underdetermined systems) solution.
//!
//! Usage example:
//!   TKeyedLinearSystem<TString> sys;
//!   sys.AddRow({{"cpu", 1.0}, {"mem", 2.0}}, 5.0);
//!   sys.AddRow({{"cpu", 3.0}, {"mem", 4.0}}, 11.0);
//!   auto x = sys.Solve();  // x["cpu"] ≈ 1, x["mem"] ≈ 2
template <class T>
class TKeyedLinearSystem
{
public:
    //! Appends a new equation to the system.
    //!
    //! \param a  Sparse representation of the new row: a list of
    //!           (key, coefficient) pairs.  Each key is automatically
    //!           assigned a unique column index on first encounter.
    //!           The same key in different rows always maps to the same column.
    //! \param b  Right-hand side value for this equation.
    void AddRow(
        std::vector<std::pair<T /*variable key*/, double /*coefficient*/>> a,
        double b);

    //! Solves the system in the least-squares sense and returns a map from
    //! each key that appeared in at least one AddRow call to its solution value.
    //!
    //! Returns an empty map when no rows have been added.
    THashMap<T, double> Solve() const;

private:
    THashMap<T, ssize_t> KeyToIndex_;
    std::vector<typename THashMap<T, ssize_t>::iterator> IndexToKey_;
    TLinearSystem System_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow

#define LINEAR_SYSTEM_INL_H_
#include "linear_system-inl.h"
#undef LINEAR_SYSTEM_INL_H_
