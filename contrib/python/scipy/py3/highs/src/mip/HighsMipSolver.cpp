/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
/*                                                                       */
/*    This file is part of the HiGHS linear optimization suite           */
/*                                                                       */
/*    Written and engineered 2008-2024 by Julian Hall, Ivet Galabova,    */
/*    Leona Gottwald and Michael Feldmeier                               */
/*                                                                       */
/*    Available as open-source under the MIT License                     */
/*                                                                       */
/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
#include "mip/HighsMipSolver.h"

#include "lp_data/HighsLpUtils.h"
#include "lp_data/HighsModelUtils.h"
#include "mip/HighsCliqueTable.h"
#include "mip/HighsCutPool.h"
#include "mip/HighsDomain.h"
#include "mip/HighsImplications.h"
#include "mip/HighsLpRelaxation.h"
#include "mip/HighsMipSolverData.h"
#include "mip/HighsPseudocost.h"
#include "mip/HighsSearch.h"
#include "mip/HighsSeparation.h"
#include "mip/MipTimer.h"
#include "presolve/HPresolve.h"
#include "presolve/HighsPostsolveStack.h"
#include "presolve/PresolveComponent.h"
#include "util/HighsCDouble.h"
#include "util/HighsIntegers.h"

using std::fabs;

HighsMipSolver::HighsMipSolver(HighsCallback& callback,
                               const HighsOptions& options, const HighsLp& lp,
                               const HighsSolution& solution, bool submip,
                               HighsInt submip_level)
    : callback_(&callback),
      options_mip_(&options),
      model_(&lp),
      orig_model_(&lp),
      solution_objective_(kHighsInf),
      submip(submip),
      submip_level(submip_level),
      rootbasis(nullptr),
      pscostinit(nullptr),
      clqtableinit(nullptr),
      implicinit(nullptr) {
  assert(!submip || submip_level > 0);
  max_submip_level = 0;
  if (solution.value_valid) {
    // MIP solver doesn't check row residuals, but they should be OK
    // so validate using assert
#ifndef NDEBUG
    bool valid, integral, feasible;
    assessLpPrimalSolution("For debugging: ", options, lp, solution, valid,
                           integral, feasible);
    assert(valid);
#endif
    bound_violation_ = 0;
    row_violation_ = 0;
    integrality_violation_ = 0;

    HighsCDouble obj = orig_model_->offset_;
    assert((HighsInt)solution.col_value.size() == orig_model_->num_col_);
    for (HighsInt i = 0; i != orig_model_->num_col_; ++i) {
      const double value = solution.col_value[i];
      obj += orig_model_->col_cost_[i] * value;

      if (orig_model_->integrality_[i] == HighsVarType::kInteger) {
        integrality_violation_ =
            std::max(fractionality(value), integrality_violation_);
      }

      const double lower = orig_model_->col_lower_[i];
      const double upper = orig_model_->col_upper_[i];
      double primal_infeasibility;
      if (value < lower - options_mip_->mip_feasibility_tolerance) {
        primal_infeasibility = lower - value;
      } else if (value > upper + options_mip_->mip_feasibility_tolerance) {
        primal_infeasibility = value - upper;
      } else
        continue;

      bound_violation_ = std::max(bound_violation_, primal_infeasibility);
    }

    for (HighsInt i = 0; i != orig_model_->num_row_; ++i) {
      const double value = solution.row_value[i];
      const double lower = orig_model_->row_lower_[i];
      const double upper = orig_model_->row_upper_[i];

      double primal_infeasibility;
      if (value < lower - options_mip_->mip_feasibility_tolerance) {
        primal_infeasibility = lower - value;
      } else if (value > upper + options_mip_->mip_feasibility_tolerance) {
        primal_infeasibility = value - upper;
      } else
        continue;

      row_violation_ = std::max(row_violation_, primal_infeasibility);
    }

    solution_objective_ = double(obj);
    solution_ = solution.col_value;
  }
}

HighsMipSolver::~HighsMipSolver() = default;

void HighsMipSolver::run() {
  modelstatus_ = HighsModelStatus::kNotset;

  if (submip) {
    analysis_.analyse_mip_time = false;
  } else {
    analysis_.timer_ = &this->timer_;
    analysis_.setup(*orig_model_, *options_mip_);
  }
  // Start the total_clock for the timer that is local to the HighsMipSolver
  // instance
  timer_.start(timer_.total_clock);

  improving_solution_file_ = nullptr;
  if (!submip && options_mip_->mip_improving_solution_file != "")
    improving_solution_file_ =
        fopen(options_mip_->mip_improving_solution_file.c_str(), "w");

  mipdata_ = decltype(mipdata_)(new HighsMipSolverData(*this));
  analysis_.mipTimerStart(kMipClockPresolve);
  analysis_.mipTimerStart(kMipClockInit);
  mipdata_->init();
  analysis_.mipTimerStop(kMipClockInit);
  analysis_.mipTimerStart(kMipClockRunPresolve);
  mipdata_->runPresolve(options_mip_->presolve_reduction_limit);
  analysis_.mipTimerStop(kMipClockRunPresolve);
  analysis_.mipTimerStop(kMipClockPresolve);
  if (analysis_.analyse_mip_time & !submip)
    highsLogUser(options_mip_->log_options, HighsLogType::kInfo,
                 "MIP-Timing: %11.2g - completed presolve\n", timer_.read());
  // Identify whether time limit has been reached (in presolve)
  if (modelstatus_ == HighsModelStatus::kNotset &&
      timer_.read(timer_.total_clock) >= options_mip_->time_limit)
    modelstatus_ = HighsModelStatus::kTimeLimit;

  if (modelstatus_ != HighsModelStatus::kNotset) {
    highsLogUser(options_mip_->log_options, HighsLogType::kInfo,
                 "Presolve: %s\n",
                 utilModelStatusToString(modelstatus_).c_str());
    if (modelstatus_ == HighsModelStatus::kOptimal) {
      mipdata_->lower_bound = 0;
      mipdata_->upper_bound = 0;
      mipdata_->transformNewIntegerFeasibleSolution(std::vector<double>());
      mipdata_->saveReportMipSolution();
    }
    cleanupSolve();
    return;
  }

  analysis_.mipTimerStart(kMipClockSolve);

  if (analysis_.analyse_mip_time & !submip)
    highsLogUser(options_mip_->log_options, HighsLogType::kInfo,
                 "MIP-Timing: %11.2g - starting  setup\n", timer_.read());
  analysis_.mipTimerStart(kMipClockRunSetup);
  mipdata_->runSetup();
  analysis_.mipTimerStop(kMipClockRunSetup);
  if (analysis_.analyse_mip_time & !submip)
    highsLogUser(options_mip_->log_options, HighsLogType::kInfo,
                 "MIP-Timing: %11.2g - completed setup\n",
                 timer_.read(timer_.total_clock));
restart:
  if (modelstatus_ == HighsModelStatus::kNotset) {
    // Check limits have not been reached before evaluating root node
    if (mipdata_->checkLimits()) {
      cleanupSolve();
      return;
    }
    // Apply the trivial heuristics
    analysis_.mipTimerStart(kMipClockTrivialHeuristics);
    HighsModelStatus model_status = mipdata_->trivialHeuristics();
    if (modelstatus_ == HighsModelStatus::kNotset &&
        model_status == HighsModelStatus::kInfeasible) {
      // trivialHeuristics can spot trivial infeasibility, so act on it
      modelstatus_ = model_status;
      cleanupSolve();
      return;
    }
    analysis_.mipTimerStop(kMipClockTrivialHeuristics);
    if (analysis_.analyse_mip_time & !submip)
      highsLogUser(options_mip_->log_options, HighsLogType::kInfo,
                   "MIP-Timing: %11.2g - starting evaluate root node\n",
                   timer_.read(timer_.total_clock));
    analysis_.mipTimerStart(kMipClockEvaluateRootNode);
    mipdata_->evaluateRootNode();
    analysis_.mipTimerStop(kMipClockEvaluateRootNode);
    // Sometimes the analytic centre calculation is not completed when
    // evaluateRootNode returns, so stop its clock if it's running
    if (analysis_.analyse_mip_time &&
        analysis_.mipTimerRunning(kMipClockIpmSolveLp))
      analysis_.mipTimerStop(kMipClockIpmSolveLp);
    if (analysis_.analyse_mip_time & !submip)
      highsLogUser(options_mip_->log_options, HighsLogType::kInfo,
                   "MIP-Timing: %11.2g - completed evaluate root node\n",
                   timer_.read(timer_.total_clock));
    // age 5 times to remove stored but never violated cuts after root
    // separation
    analysis_.mipTimerStart(kMipClockPerformAging0);
    mipdata_->cutpool.performAging();
    mipdata_->cutpool.performAging();
    mipdata_->cutpool.performAging();
    mipdata_->cutpool.performAging();
    mipdata_->cutpool.performAging();
    analysis_.mipTimerStop(kMipClockPerformAging0);
  }
  if (mipdata_->nodequeue.empty() || mipdata_->checkLimits()) {
    cleanupSolve();
    return;
  }

  std::shared_ptr<const HighsBasis> basis;
  HighsSearch search{*this, mipdata_->pseudocost};
  mipdata_->debugSolution.registerDomain(search.getLocalDomain());
  HighsSeparation sepa(*this);

  search.setLpRelaxation(&mipdata_->lp);
  sepa.setLpRelaxation(&mipdata_->lp);

  double prev_lower_bound = mipdata_->lower_bound;

  mipdata_->lower_bound = mipdata_->nodequeue.getBestLowerBound();

  bool bound_change = mipdata_->lower_bound != prev_lower_bound;
  if (!submip && bound_change)
    mipdata_->updatePrimalDualIntegral(prev_lower_bound, mipdata_->lower_bound,
                                       mipdata_->upper_bound,
                                       mipdata_->upper_bound);

  mipdata_->printDisplayLine();
  search.installNode(mipdata_->nodequeue.popBestBoundNode());
  int64_t numStallNodes = 0;
  int64_t lastLbLeave = 0;
  int64_t numQueueLeaves = 0;
  HighsInt numHugeTreeEstim = 0;
  int64_t numNodesLastCheck = mipdata_->num_nodes;
  int64_t nextCheck = mipdata_->num_nodes;
  double treeweightLastCheck = 0.0;
  double upperLimLastCheck = mipdata_->upper_limit;
  double lowerBoundLastCheck = mipdata_->lower_bound;
  analysis_.mipTimerStart(kMipClockSearch);
  while (search.hasNode()) {
    analysis_.mipTimerStart(kMipClockPerformAging1);
    mipdata_->conflictPool.performAging();
    analysis_.mipTimerStop(kMipClockPerformAging1);
    // set iteration limit for each lp solve during the dive to 10 times the
    // average nodes

    HighsInt iterlimit = 10 * std::max(mipdata_->lp.getAvgSolveIters(),
                                       mipdata_->avgrootlpiters);
    iterlimit = std::max({HighsInt{10000}, iterlimit,
                          HighsInt((3 * mipdata_->firstrootlpiters) / 2)});

    mipdata_->lp.setIterationLimit(iterlimit);

    // perform the dive and put the open nodes to the queue
    size_t plungestart = mipdata_->num_nodes;
    bool limit_reached = false;
    bool considerHeuristics = true;
    analysis_.mipTimerStart(kMipClockDive);
    while (true) {
      // Possibly apply primal heuristics
      if (considerHeuristics && mipdata_->moreHeuristicsAllowed()) {
        analysis_.mipTimerStart(kMipClockEvaluateNode);
        const HighsSearch::NodeResult evaluate_node_result =
            search.evaluateNode();
        analysis_.mipTimerStop(kMipClockEvaluateNode);

        if (evaluate_node_result == HighsSearch::NodeResult::kSubOptimal) break;

        if (search.currentNodePruned()) {
          ++mipdata_->num_leaves;
          search.flushStatistics();
        } else {
          analysis_.mipTimerStart(kMipClockPrimalHeuristics);
          if (mipdata_->incumbent.empty()) {
            analysis_.mipTimerStart(kMipClockRandomizedRounding0);
            mipdata_->heuristics.randomizedRounding(
                mipdata_->lp.getLpSolver().getSolution().col_value);
            analysis_.mipTimerStop(kMipClockRandomizedRounding0);
          }

          if (mipdata_->incumbent.empty()) {
            analysis_.mipTimerStart(kMipClockRens);
            mipdata_->heuristics.RENS(
                mipdata_->lp.getLpSolver().getSolution().col_value);
            analysis_.mipTimerStop(kMipClockRens);
          } else {
            analysis_.mipTimerStart(kMipClockRins);
            mipdata_->heuristics.RINS(
                mipdata_->lp.getLpSolver().getSolution().col_value);
            analysis_.mipTimerStop(kMipClockRins);
          }

          mipdata_->heuristics.flushStatistics();
          analysis_.mipTimerStop(kMipClockPrimalHeuristics);
        }
      }

      considerHeuristics = false;

      if (mipdata_->domain.infeasible()) break;

      if (!search.currentNodePruned()) {
        analysis_.mipTimerStart(kMipClockTheDive);
        const HighsSearch::NodeResult search_dive_result = search.dive();
        analysis_.mipTimerStop(kMipClockTheDive);

        if (search_dive_result == HighsSearch::NodeResult::kSubOptimal) break;

        ++mipdata_->num_leaves;

        search.flushStatistics();
      }

      if (mipdata_->checkLimits()) {
        limit_reached = true;
        break;
      }

      HighsInt numPlungeNodes = mipdata_->num_nodes - plungestart;
      if (numPlungeNodes >= 100) break;

      analysis_.mipTimerStart(kMipClockBacktrackPlunge);
      const bool backtrack_plunge = search.backtrackPlunge(mipdata_->nodequeue);
      analysis_.mipTimerStop(kMipClockBacktrackPlunge);
      if (!backtrack_plunge) break;

      assert(search.hasNode());

      if (mipdata_->conflictPool.getNumConflicts() >
          options_mip_->mip_pool_soft_limit) {
        analysis_.mipTimerStart(kMipClockPerformAging2);
        mipdata_->conflictPool.performAging();
        analysis_.mipTimerStop(kMipClockPerformAging2);
      }

      search.flushStatistics();
      mipdata_->printDisplayLine();
      // printf("continue plunging due to good estimate\n");
    }  // while (true)
    analysis_.mipTimerStop(kMipClockDive);

    analysis_.mipTimerStart(kMipClockOpenNodesToQueue);
    search.openNodesToQueue(mipdata_->nodequeue);
    analysis_.mipTimerStop(kMipClockOpenNodesToQueue);

    search.flushStatistics();

    if (limit_reached) {
      double prev_lower_bound = mipdata_->lower_bound;

      mipdata_->lower_bound = std::min(mipdata_->upper_bound,
                                       mipdata_->nodequeue.getBestLowerBound());

      bool bound_change = mipdata_->lower_bound != prev_lower_bound;
      if (!submip && bound_change)
        mipdata_->updatePrimalDualIntegral(
            prev_lower_bound, mipdata_->lower_bound, mipdata_->upper_bound,
            mipdata_->upper_bound);
      mipdata_->printDisplayLine();
      break;
    }

    // the search datastructure should have no installed node now
    assert(!search.hasNode());

    // propagate the global domain
    analysis_.mipTimerStart(kMipClockDomainPropgate);
    mipdata_->domain.propagate();
    analysis_.mipTimerStop(kMipClockDomainPropgate);

    analysis_.mipTimerStart(kMipClockPruneInfeasibleNodes);
    mipdata_->pruned_treeweight += mipdata_->nodequeue.pruneInfeasibleNodes(
        mipdata_->domain, mipdata_->feastol);
    analysis_.mipTimerStop(kMipClockPruneInfeasibleNodes);

    // if global propagation detected infeasibility, stop here
    if (mipdata_->domain.infeasible()) {
      mipdata_->nodequeue.clear();
      mipdata_->pruned_treeweight = 1.0;

      double prev_lower_bound = mipdata_->lower_bound;

      mipdata_->lower_bound = std::min(kHighsInf, mipdata_->upper_bound);

      bool bound_change = mipdata_->lower_bound != prev_lower_bound;
      if (!submip && bound_change)
        mipdata_->updatePrimalDualIntegral(
            prev_lower_bound, mipdata_->lower_bound, mipdata_->upper_bound,
            mipdata_->upper_bound);
      mipdata_->printDisplayLine();
      break;
    }

    double prev_lower_bound = mipdata_->lower_bound;

    mipdata_->lower_bound = std::min(mipdata_->upper_bound,
                                     mipdata_->nodequeue.getBestLowerBound());
    bool bound_change = mipdata_->lower_bound != prev_lower_bound;
    if (!submip && bound_change)
      mipdata_->updatePrimalDualIntegral(
          prev_lower_bound, mipdata_->lower_bound, mipdata_->upper_bound,
          mipdata_->upper_bound);
    mipdata_->printDisplayLine();
    if (mipdata_->nodequeue.empty()) break;

    // if global propagation found bound changes, we update the local domain
    if (!mipdata_->domain.getChangedCols().empty()) {
      analysis_.mipTimerStart(kMipClockUpdateLocalDomain);
      highsLogDev(options_mip_->log_options, HighsLogType::kInfo,
                  "added %" HIGHSINT_FORMAT " global bound changes\n",
                  (HighsInt)mipdata_->domain.getChangedCols().size());
      mipdata_->cliquetable.cleanupFixed(mipdata_->domain);
      for (HighsInt col : mipdata_->domain.getChangedCols())
        mipdata_->implications.cleanupVarbounds(col);

      mipdata_->domain.setDomainChangeStack(std::vector<HighsDomainChange>());
      search.resetLocalDomain();

      mipdata_->domain.clearChangedCols();
      mipdata_->removeFixedIndices();
      analysis_.mipTimerStop(kMipClockUpdateLocalDomain);
    }

    if (!submip && mipdata_->num_nodes >= nextCheck) {
      auto nTreeRestarts = mipdata_->numRestarts - mipdata_->numRestartsRoot;
      double currNodeEstim =
          numNodesLastCheck - mipdata_->num_nodes_before_run +
          (mipdata_->num_nodes - numNodesLastCheck) *
              double(1.0 - mipdata_->pruned_treeweight) /
              std::max(
                  double(mipdata_->pruned_treeweight - treeweightLastCheck),
                  mipdata_->epsilon);
      // printf(
      //     "nTreeRestarts: %d, numNodesThisRun: %ld, numNodesLastCheck: %ld,
      //     " "currNodeEstim: %g, " "prunedTreeWeightDelta: %g,
      //     numHugeTreeEstim: %d, numLeavesThisRun:
      //     "
      //     "%ld\n",
      //     nTreeRestarts, mipdata_->num_nodes -
      //     mipdata_->num_nodes_before_run, numNodesLastCheck -
      //     mipdata_->num_nodes_before_run, currNodeEstim, 100.0 *
      //     double(mipdata_->pruned_treeweight - treeweightLastCheck),
      //     numHugeTreeEstim,
      //     mipdata_->num_leaves - mipdata_->num_leaves_before_run);

      bool doRestart = false;

      double activeIntegerRatio =
          1.0 - mipdata_->percentageInactiveIntegers() / 100.0;
      activeIntegerRatio *= activeIntegerRatio;

      if (!doRestart) {
        double gapReduction = 1.0;
        if (mipdata_->upper_limit != kHighsInf) {
          double oldGap = upperLimLastCheck - lowerBoundLastCheck;
          double newGap = mipdata_->upper_limit - mipdata_->lower_bound;
          gapReduction = oldGap / newGap;
        }

        if (gapReduction < 1.0 + (0.05 / activeIntegerRatio) &&
            currNodeEstim >=
                activeIntegerRatio * 20 *
                    (mipdata_->num_nodes - mipdata_->num_nodes_before_run)) {
          nextCheck = mipdata_->num_nodes + 100;
          ++numHugeTreeEstim;
        } else {
          numHugeTreeEstim = 0;
          treeweightLastCheck = double(mipdata_->pruned_treeweight);
          numNodesLastCheck = mipdata_->num_nodes;
          upperLimLastCheck = mipdata_->upper_limit;
          lowerBoundLastCheck = mipdata_->lower_bound;
        }

        // Possibly prevent restart - necessary for debugging presolve
        // errors: see #1553
        if (options_mip_->mip_allow_restart) {
          int64_t minHugeTreeOffset =
              (mipdata_->num_leaves - mipdata_->num_leaves_before_run) / 1000;
          int64_t minHugeTreeEstim = HighsIntegers::nearestInteger(
              activeIntegerRatio * (10 + minHugeTreeOffset) *
              std::pow(1.5, nTreeRestarts));

          doRestart = numHugeTreeEstim >= minHugeTreeEstim;
        } else {
          doRestart = false;
        }
      } else {
        // count restart due to many fixings within the first 1000 nodes as
        // root restart
        ++mipdata_->numRestartsRoot;
      }

      if (doRestart) {
        highsLogUser(options_mip_->log_options, HighsLogType::kInfo,
                     "\nRestarting search from the root node\n");
        mipdata_->performRestart();
        analysis_.mipTimerStop(kMipClockSearch);
        goto restart;
      }
    }  // if (!submip && mipdata_->num_nodes >= nextCheck))

    // remove the iteration limit when installing a new node
    // mipdata_->lp.setIterationLimit();

    // loop to install the next node for the search
    analysis_.mipTimerStart(kMipClockNodeSearch);

    while (!mipdata_->nodequeue.empty()) {
      // printf("popping node from nodequeue (length = %" HIGHSINT_FORMAT ")\n",
      // (HighsInt)nodequeue.size());
      assert(!search.hasNode());

      if (numQueueLeaves - lastLbLeave >= 10) {
        search.installNode(mipdata_->nodequeue.popBestBoundNode());
        lastLbLeave = numQueueLeaves;
      } else {
        HighsInt bestBoundNodeStackSize =
            mipdata_->nodequeue.getBestBoundDomchgStackSize();
        double bestBoundNodeLb = mipdata_->nodequeue.getBestLowerBound();
        HighsNodeQueue::OpenNode nextNode(mipdata_->nodequeue.popBestNode());
        if (nextNode.lower_bound == bestBoundNodeLb &&
            (HighsInt)nextNode.domchgstack.size() == bestBoundNodeStackSize)
          lastLbLeave = numQueueLeaves;
        search.installNode(std::move(nextNode));
      }

      ++numQueueLeaves;

      if (search.getCurrentEstimate() >= mipdata_->upper_limit) {
        ++numStallNodes;
        if (options_mip_->mip_max_stall_nodes != kHighsIInf &&
            numStallNodes >= options_mip_->mip_max_stall_nodes) {
          limit_reached = true;
          modelstatus_ = HighsModelStatus::kSolutionLimit;
          break;
        }
      } else
        numStallNodes = 0;

      assert(search.hasNode());

      // we evaluate the node directly here instead of performing a dive
      // because we first want to check if the node is not fathomed due to
      // new global information before we perform separation rounds for the node
      if (search.evaluateNode() == HighsSearch::NodeResult::kSubOptimal)
        search.currentNodeToQueue(mipdata_->nodequeue);

      // if the node was pruned we remove it from the search and install the
      // next node from the queue
      if (search.currentNodePruned()) {
        search.backtrack();
        ++mipdata_->num_leaves;
        ++mipdata_->num_nodes;
        search.flushStatistics();

        mipdata_->domain.propagate();
        mipdata_->pruned_treeweight += mipdata_->nodequeue.pruneInfeasibleNodes(
            mipdata_->domain, mipdata_->feastol);

        if (mipdata_->domain.infeasible()) {
          mipdata_->nodequeue.clear();
          mipdata_->pruned_treeweight = 1.0;

          double prev_lower_bound = mipdata_->lower_bound;

          mipdata_->lower_bound = std::min(kHighsInf, mipdata_->upper_bound);

          bool bound_change = mipdata_->lower_bound != prev_lower_bound;
          if (!submip && bound_change)
            mipdata_->updatePrimalDualIntegral(
                prev_lower_bound, mipdata_->lower_bound, mipdata_->upper_bound,
                mipdata_->upper_bound);
          break;
        }

        if (mipdata_->checkLimits()) {
          limit_reached = true;
          break;
        }

        double prev_lower_bound = mipdata_->lower_bound;

        mipdata_->lower_bound = std::min(
            mipdata_->upper_bound, mipdata_->nodequeue.getBestLowerBound());

        bool bound_change = mipdata_->lower_bound != prev_lower_bound;
        if (!submip && bound_change)
          mipdata_->updatePrimalDualIntegral(
              prev_lower_bound, mipdata_->lower_bound, mipdata_->upper_bound,
              mipdata_->upper_bound);
        mipdata_->printDisplayLine();

        if (!mipdata_->domain.getChangedCols().empty()) {
          highsLogDev(options_mip_->log_options, HighsLogType::kInfo,
                      "added %" HIGHSINT_FORMAT " global bound changes\n",
                      (HighsInt)mipdata_->domain.getChangedCols().size());
          mipdata_->cliquetable.cleanupFixed(mipdata_->domain);
          for (HighsInt col : mipdata_->domain.getChangedCols())
            mipdata_->implications.cleanupVarbounds(col);

          mipdata_->domain.setDomainChangeStack(
              std::vector<HighsDomainChange>());
          search.resetLocalDomain();

          mipdata_->domain.clearChangedCols();
          mipdata_->removeFixedIndices();
        }

        continue;
      }

      // the node is still not fathomed, so perform separation
      sepa.separate(search.getLocalDomain());

      if (mipdata_->domain.infeasible()) {
        search.cutoffNode();
        search.openNodesToQueue(mipdata_->nodequeue);
        mipdata_->nodequeue.clear();
        mipdata_->pruned_treeweight = 1.0;

        double prev_lower_bound = mipdata_->lower_bound;

        mipdata_->lower_bound = std::min(kHighsInf, mipdata_->upper_bound);

        bool bound_change = mipdata_->lower_bound != prev_lower_bound;
        if (!submip && bound_change)
          mipdata_->updatePrimalDualIntegral(
              prev_lower_bound, mipdata_->lower_bound, mipdata_->upper_bound,
              mipdata_->upper_bound);
        break;
      }

      // after separation we store the new basis and proceed with the outer loop
      // to perform a dive from this node
      if (mipdata_->lp.getStatus() != HighsLpRelaxation::Status::kError &&
          mipdata_->lp.getStatus() != HighsLpRelaxation::Status::kNotSet)
        mipdata_->lp.storeBasis();

      basis = mipdata_->lp.getStoredBasis();
      if (!basis || !isBasisConsistent(mipdata_->lp.getLp(), *basis)) {
        HighsBasis b = mipdata_->firstrootbasis;
        b.row_status.resize(mipdata_->lp.numRows(), HighsBasisStatus::kBasic);
        basis = std::make_shared<const HighsBasis>(std::move(b));
        mipdata_->lp.setStoredBasis(basis);
      }

      break;
    }  // while(!mipdata_->nodequeue.empty())
    analysis_.mipTimerStop(kMipClockNodeSearch);

    if (limit_reached) break;
  }  // while(search.hasNode())
  analysis_.mipTimerStop(kMipClockSearch);

  cleanupSolve();
}

void HighsMipSolver::cleanupSolve() {
  // Force a final logging line
  mipdata_->printDisplayLine(kSolutionSourceCleanup);
  // Stop the solve clock - which won't be running if presolve
  // determines the model status
  if (analysis_.mipTimerRunning(kMipClockSolve))
    analysis_.mipTimerStop(kMipClockSolve);

  // Need to complete the calculation of P-D integral, checking for NO
  // gap change
  mipdata_->updatePrimalDualIntegral(
      mipdata_->lower_bound, mipdata_->lower_bound, mipdata_->upper_bound,
      mipdata_->upper_bound, false);
  analysis_.mipTimerStart(kMipClockPostsolve);

  bool havesolution = solution_objective_ != kHighsInf;
  bool feasible;
  if (havesolution)
    feasible =
        bound_violation_ <= options_mip_->mip_feasibility_tolerance &&
        integrality_violation_ <= options_mip_->mip_feasibility_tolerance &&
        row_violation_ <= options_mip_->mip_feasibility_tolerance;
  else
    feasible = false;

  dual_bound_ = mipdata_->lower_bound;
  if (mipdata_->objectiveFunction.isIntegral()) {
    double rounded_lower_bound =
        std::ceil(mipdata_->lower_bound *
                      mipdata_->objectiveFunction.integralScale() -
                  mipdata_->feastol) /
        mipdata_->objectiveFunction.integralScale();
    dual_bound_ = std::max(dual_bound_, rounded_lower_bound);
  }
  dual_bound_ += model_->offset_;
  primal_bound_ = mipdata_->upper_bound + model_->offset_;
  node_count_ = mipdata_->num_nodes;
  total_lp_iterations_ = mipdata_->total_lp_iterations;
  dual_bound_ = std::min(dual_bound_, primal_bound_);
  primal_dual_integral_ = mipdata_->primal_dual_integral.value;

  // adjust objective sense in case of maximization problem
  if (orig_model_->sense_ == ObjSense::kMaximize) {
    dual_bound_ = -dual_bound_;
    primal_bound_ = -primal_bound_;
  }

  if (modelstatus_ == HighsModelStatus::kNotset ||
      modelstatus_ == HighsModelStatus::kInfeasible) {
    if (feasible && havesolution)
      modelstatus_ = HighsModelStatus::kOptimal;
    else
      modelstatus_ = HighsModelStatus::kInfeasible;
  }

  analysis_.mipTimerStop(kMipClockPostsolve);
  timer_.stop(timer_.total_clock);

  std::string solutionstatus = "-";

  if (havesolution) {
    bool feasible =
        bound_violation_ <= options_mip_->mip_feasibility_tolerance &&
        integrality_violation_ <= options_mip_->mip_feasibility_tolerance &&
        row_violation_ <= options_mip_->mip_feasibility_tolerance;
    solutionstatus = feasible ? "feasible" : "infeasible";
  }

  gap_ = fabs(primal_bound_ - dual_bound_);
  if (primal_bound_ == 0.0)
    gap_ = dual_bound_ == 0.0 ? 0.0 : kHighsInf;
  else if (primal_bound_ != kHighsInf)
    gap_ = fabs(primal_bound_ - dual_bound_) / fabs(primal_bound_);
  else
    gap_ = kHighsInf;

  std::array<char, 128> gapString = {};

  if (gap_ == kHighsInf)
    std::strcpy(gapString.data(), "inf");
  else {
    double printTol = std::max(std::min(1e-2, 1e-1 * gap_), 1e-6);
    auto gapValString = highsDoubleToString(100.0 * gap_, printTol);
    double gapTol = options_mip_->mip_rel_gap;

    if (options_mip_->mip_abs_gap > options_mip_->mip_feasibility_tolerance) {
      gapTol = primal_bound_ == 0.0
                   ? kHighsInf
                   : std::max(gapTol,
                              options_mip_->mip_abs_gap / fabs(primal_bound_));
    }

    if (gapTol == 0.0)
      std::snprintf(gapString.data(), gapString.size(), "%s%%",
                    gapValString.data());
    else if (gapTol != kHighsInf) {
      printTol = std::max(std::min(1e-2, 1e-1 * gapTol), 1e-6);
      auto gapTolString = highsDoubleToString(100.0 * gapTol, printTol);
      std::snprintf(gapString.data(), gapString.size(),
                    "%s%% (tolerance: %s%%)", gapValString.data(),
                    gapTolString.data());
    } else
      std::snprintf(gapString.data(), gapString.size(), "%s%% (tolerance: inf)",
                    gapValString.data());
  }

  highsLogUser(options_mip_->log_options, HighsLogType::kInfo,
               "\nSolving report\n");
  if (this->orig_model_->model_name_.length())
    highsLogUser(options_mip_->log_options, HighsLogType::kInfo,
                 "  Model             %s\n",
                 this->orig_model_->model_name_.c_str());
  highsLogUser(options_mip_->log_options, HighsLogType::kInfo,
               "  Status            %s\n"
               "  Primal bound      %.12g\n"
               "  Dual bound        %.12g\n"
               "  Gap               %s\n"
               "  P-D integral      %.12g\n"
               "  Solution status   %s\n",
               utilModelStatusToString(modelstatus_).c_str(), primal_bound_,
               dual_bound_, gapString.data(),
               mipdata_->primal_dual_integral.value, solutionstatus.c_str());
  if (solutionstatus != "-")
    highsLogUser(options_mip_->log_options, HighsLogType::kInfo,
                 "                    %.12g (objective)\n"
                 "                    %.12g (bound viol.)\n"
                 "                    %.12g (int. viol.)\n"
                 "                    %.12g (row viol.)\n",
                 solution_objective_, bound_violation_, integrality_violation_,
                 row_violation_);
  highsLogUser(options_mip_->log_options, HighsLogType::kInfo,
               "  Timing            %.2f (total)\n"
               "                    %.2f (presolve)\n"
               "                    %.2f (solve)\n"
               "                    %.2f (postsolve)\n"
               "  Max sub-MIP depth %d\n"
               "  Nodes             %llu\n"
               "  Repair LPs        %llu (%llu feasible; %llu iterations)\n"
               "  LP iterations     %llu (total)\n"
               "                    %llu (strong br.)\n"
               "                    %llu (separation)\n"
               "                    %llu (heuristics)\n",
               timer_.read(timer_.total_clock),
               analysis_.mipTimerRead(kMipClockPresolve),
               analysis_.mipTimerRead(kMipClockSolve),
               analysis_.mipTimerRead(kMipClockPostsolve),
               int(max_submip_level), (long long unsigned)mipdata_->num_nodes,
               (long long unsigned)mipdata_->total_repair_lp,
               (long long unsigned)mipdata_->total_repair_lp_feasible,
               (long long unsigned)mipdata_->total_repair_lp_iterations,
               (long long unsigned)mipdata_->total_lp_iterations,
               (long long unsigned)mipdata_->sb_lp_iterations,
               (long long unsigned)mipdata_->sepa_lp_iterations,
               (long long unsigned)mipdata_->heuristic_lp_iterations);

  analysis_.reportMipTimer();

  assert(modelstatus_ != HighsModelStatus::kNotset);
}

// Only called in Highs::runPresolve
void HighsMipSolver::runPresolve(const HighsInt presolve_reduction_limit) {
  mipdata_ = decltype(mipdata_)(new HighsMipSolverData(*this));
  mipdata_->init();
  mipdata_->runPresolve(presolve_reduction_limit);
}

const HighsLp& HighsMipSolver::getPresolvedModel() const {
  return mipdata_->presolvedModel;
}

HighsPresolveStatus HighsMipSolver::getPresolveStatus() const {
  return mipdata_->presolve_status;
}

presolve::HighsPostsolveStack HighsMipSolver::getPostsolveStack() const {
  return mipdata_->postSolveStack;
}

void HighsMipSolver::callbackGetCutPool() const {
  assert(callback_->user_callback);
  assert(callback_->callbackActive(kCallbackMipGetCutPool));
  HighsCallbackDataOut& data_out = callback_->data_out;

  std::vector<double> cut_lower;
  std::vector<double> cut_upper;
  HighsSparseMatrix cut_matrix;

  mipdata_->lp.getCutPool(data_out.cutpool_num_col, data_out.cutpool_num_cut,
                          cut_lower, cut_upper, cut_matrix);

  data_out.cutpool_num_nz = cut_matrix.numNz();
  data_out.cutpool_start = cut_matrix.start_.data();
  data_out.cutpool_index = cut_matrix.index_.data();
  data_out.cutpool_value = cut_matrix.value_.data();
  data_out.cutpool_lower = cut_lower.data();
  data_out.cutpool_upper = cut_upper.data();
  callback_->user_callback(kCallbackMipGetCutPool, "MIP cut pool",
                           &callback_->data_out, &callback_->data_in,
                           callback_->user_callback_data);
}
