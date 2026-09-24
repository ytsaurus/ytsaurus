#pragma once

#include "public.h"

#include <yt/yt/flow/library/cpp/common/public.h>

namespace NYT::NFlow::NBalancer {

////////////////////////////////////////////////////////////////////////////////

struct TWorkerCoefEstimatorConfig
{
    //! An edge that receives a new observation first forgets its old ones with this half-life.
    TDuration HalfLife = TDuration::Hours(24);
    //! Edges of a worker absent for this long are dropped.
    TDuration Retention = TDuration::Days(7);
    //! Weight of the prior "the worker's coefficient is 1", in observation weight units.
    double PriorWeight = 0.05;
    //! Coefficients are clamped to [1 / MaxRatio, MaxRatio].
    double MaxRatio = 4.;
    int MaxEdgesPerWorker = 16;
    //! An observation whose message rates differ more than this is not comparable.
    double MaxRateRatio = 10.;
    double MinCpuUsage = 0.01;
};

//! Estimates the relative speed of the workers of one group from partitions that moved between them.
//!
//! A partition moved from worker A to B with steady-state CPU and message rates measured on both
//! gives an observation of log(coef(B) / coef(A)), where coef is the cost of a unit of work on the
//! worker (1 = the group's average, larger = slower). Observations of one pair merge into one edge;
//! the coefficients are the least-squares solution over all edges with a weak prior towards 1, so
//! every observation moves every worker connected to it, and the result does not depend on the
//! order the observations arrived in.
class TWorkerCoefEstimator
{
public:
    TWorkerCoefEstimator(TBalancerGroupStatePtr state, TWorkerCoefEstimatorConfig config);

    //! Observation of a partition moved from A to B, or nothing when the measurements are not comparable.
    std::optional<double> MakeObservation(double cpuA, double rpsA, double cpuB, double rpsB) const;

    void AddObservation(const std::string& from, const std::string& to, double obs, double weight, TInstant now);

    //! Forgets observed workers that have been absent longer than the retention; |present| are the
    //! workers of the group now. Only workers with edges are tracked.
    void Prune(const THashSet<std::string>& present, TInstant now);

    //! Solves the coefficients of every worker that has edges; others stay implicit (1).
    void Solve();

    //! The clamped coefficient from the last solution; 1 for an unknown worker.
    double GetCoef(const std::string& worker) const;

    int GetEdgeCount() const;

private:
    const TBalancerGroupStatePtr State_;
    const TWorkerCoefEstimatorConfig Config_;

    void EnforceEdgeLimit(const std::string& worker);
    void EraseEdge(const std::string& from, const std::string& to);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NBalancer
