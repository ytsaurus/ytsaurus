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
/**@file mip/MipTimer.h
 * @brief Indices of mip iClocks
 */
#ifndef MIP_MIPTIMER_H_
#define MIP_MIPTIMER_H_

// Clocks for profiling the MIP dual mip solver
enum iClockMip {
  kMipClockTotal = 0,
  kMipClockPresolve,
  kMipClockSolve,
  kMipClockPostsolve,
  // Level 1
  kMipClockInit,
  kMipClockRunPresolve,
  kMipClockRunSetup,
  kMipClockTrivialHeuristics,
  kMipClockEvaluateRootNode,
  kMipClockPerformAging0,
  kMipClockSearch,
  // Search
  kMipClockProbingPresolve,
  kMipClockPerformAging1,
  kMipClockDive,
  kMipClockOpenNodesToQueue,
  kMipClockDomainPropgate,
  kMipClockPruneInfeasibleNodes,
  kMipClockUpdateLocalDomain,
  kMipClockNodeSearch,
  //  kMipClock@,
  // Dive
  kMipClockEvaluateNode,
  kMipClockPrimalHeuristics,
  kMipClockTheDive,
  kMipClockBacktrackPlunge,
  kMipClockPerformAging2,
  // Primal heuristics
  kMipClockRandomizedRounding0,
  kMipClockRens,
  kMipClockRins,
  // Evaluate root node
  kMipClockStartSymmetryDetection,
  kMipClockStartAnalyticCentreComputation,
  kMipClockEvaluateRootLp,
  kMipClockSeparateLpCuts,
  kMipClockRandomizedRounding1,
  kMipClockPerformRestart,
  kMipClockSeparation,
  kMipClockFinishAnalyticCentreComputation,
  kMipClockCentralRounding,
  kMipClockRootSeparationRound,
  kMipClockSolveSubMipRootReducedCost,

  // Separation
  kMipClockSeparationRootSeparationRound,
  kMipClockSeparationFinishAnalyticCentreComputation,
  kMipClockSeparationCentralRounding,
  kMipClockSeparationEvaluateRootLp,

  kMipClockSimplexBasisSolveLp,
  kMipClockSimplexNoBasisSolveLp,
  kMipClockIpmSolveLp,

  kMipClockSolveSubMipRENS,
  kMipClockSolveSubMipRINS,

  kMipClockProbingImplications,

  kNumMipClock  //!< Number of MIP clocks
};

const double tolerance_percent_report = 0.1;

class MipTimer {
 public:
  void initialiseMipClocks(HighsTimerClock& mip_timer_clock) {
    HighsTimer* timer_pointer = mip_timer_clock.timer_pointer_;
    std::vector<HighsInt>& clock = mip_timer_clock.clock_;
    clock.resize(kNumMipClock);
    clock[kMipClockTotal] = timer_pointer->total_clock;
    clock[kMipClockPresolve] = timer_pointer->clock_def("MIP presolve");
    clock[kMipClockSolve] = timer_pointer->clock_def("MIP solve");
    clock[kMipClockPostsolve] = timer_pointer->clock_def("MIP postsolve");

    // Level 1 - Should correspond to kMipClockTotal
    clock[kMipClockInit] = timer_pointer->clock_def("Initialise");
    clock[kMipClockRunPresolve] = timer_pointer->clock_def("Run presolve");
    clock[kMipClockRunSetup] = timer_pointer->clock_def("Run setup");
    clock[kMipClockTrivialHeuristics] =
        timer_pointer->clock_def("Trivial heuristics");
    clock[kMipClockEvaluateRootNode] =
        timer_pointer->clock_def("Evaluate root node");
    clock[kMipClockPerformAging0] = timer_pointer->clock_def("Perform aging 0");
    clock[kMipClockSearch] = timer_pointer->clock_def("Search");
    // kMipClockPostsolve

    // Presolve - Should correspond to kMipClockRunPresolve
    clock[kMipClockProbingPresolve] =
        timer_pointer->clock_def("Probing - presolve");

    // Search - Should correspond to kMipClockSearch
    clock[kMipClockPerformAging1] = timer_pointer->clock_def("Perform aging 1");
    clock[kMipClockDive] = timer_pointer->clock_def("Dive");
    clock[kMipClockOpenNodesToQueue] =
        timer_pointer->clock_def("Open nodes to queue");
    clock[kMipClockDomainPropgate] =
        timer_pointer->clock_def("Domain propagate");
    clock[kMipClockPruneInfeasibleNodes] =
        timer_pointer->clock_def("Prune infeasible nodes");
    clock[kMipClockUpdateLocalDomain] =
        timer_pointer->clock_def("Update local domain");
    clock[kMipClockNodeSearch] = timer_pointer->clock_def("Node search");
    //    clock[kMipClock@] = timer_pointer->clock_def("@");

    // Dive - Should correspond to kMipClockDive
    clock[kMipClockEvaluateNode] = timer_pointer->clock_def("Evaluate node");
    clock[kMipClockPrimalHeuristics] =
        timer_pointer->clock_def("Primal heuristics");
    clock[kMipClockTheDive] = timer_pointer->clock_def("The dive");
    clock[kMipClockBacktrackPlunge] =
        timer_pointer->clock_def("Backtrack plunge");
    clock[kMipClockPerformAging2] = timer_pointer->clock_def("Perform aging 2");

    // Primal heuristics - Should correspond to kMipClockPrimalHeuristics
    clock[kMipClockRandomizedRounding0] =
        timer_pointer->clock_def("Randomized rounding 0");
    clock[kMipClockRens] = timer_pointer->clock_def("RENS");
    clock[kMipClockRins] = timer_pointer->clock_def("RINS");

    // Evaluate root node
    clock[kMipClockStartSymmetryDetection] =
        timer_pointer->clock_def("Start symmetry detection");
    clock[kMipClockStartAnalyticCentreComputation] =
        timer_pointer->clock_def("A-centre - start");
    clock[kMipClockEvaluateRootLp] =
        timer_pointer->clock_def("Evaluate root LP");
    clock[kMipClockSeparateLpCuts] =
        timer_pointer->clock_def("Separate LP cuts");
    clock[kMipClockRandomizedRounding1] =
        timer_pointer->clock_def("Randomized rounding 1");
    clock[kMipClockPerformRestart] =
        timer_pointer->clock_def("Perform restart");
    clock[kMipClockSeparation] = timer_pointer->clock_def("Separation");
    clock[kMipClockFinishAnalyticCentreComputation] =
        timer_pointer->clock_def("A-centre - finish");
    clock[kMipClockCentralRounding] =
        timer_pointer->clock_def("Central rounding");
    clock[kMipClockRootSeparationRound] =
        timer_pointer->clock_def("Root separation round");
    clock[kMipClockSolveSubMipRootReducedCost] =
        timer_pointer->clock_def("Solve sub-MIP: root reduced cost");

    // Separation
    clock[kMipClockSeparationRootSeparationRound] =
        timer_pointer->clock_def("Root separation round - s.");
    clock[kMipClockSeparationFinishAnalyticCentreComputation] =
        timer_pointer->clock_def("A-centre - finish - s.");
    clock[kMipClockSeparationCentralRounding] =
        timer_pointer->clock_def("Central rounding - s.");
    clock[kMipClockSeparationEvaluateRootLp] =
        timer_pointer->clock_def("Evaluate root LP - s.");

    // Evaluate LPs
    clock[kMipClockSimplexBasisSolveLp] =
        timer_pointer->clock_def("Solve LP - simplex basis");
    clock[kMipClockSimplexNoBasisSolveLp] =
        timer_pointer->clock_def("Solve LP - simplex no basis");
    clock[kMipClockIpmSolveLp] = timer_pointer->clock_def("Solve LP: IPM");

    // Primal heuristic sub-MIP clocks
    clock[kMipClockSolveSubMipRENS] =
        timer_pointer->clock_def("Solve sub-MIP - RENS");
    clock[kMipClockSolveSubMipRINS] =
        timer_pointer->clock_def("Solve sub-MIP - RINS");

    clock[kMipClockProbingImplications] =
        timer_pointer->clock_def("Probing - implications");
    //    clock[] = timer_pointer->clock_def("");
    //    clock[] = timer_pointer->clock_def("");
  }

  bool reportMipClockList(const char* grepStamp,
                          const std::vector<HighsInt> mip_clock_list,
                          const HighsTimerClock& mip_timer_clock,
                          const HighsInt kMipClockIdeal = kMipClockTotal,
                          const double tolerance_percent_report_ = -1) {
    HighsTimer* timer_pointer = mip_timer_clock.timer_pointer_;
    const std::vector<HighsInt>& clock = mip_timer_clock.clock_;
    HighsInt mip_clock_list_size = mip_clock_list.size();
    std::vector<HighsInt> clockList;
    clockList.resize(mip_clock_list_size);
    for (HighsInt en = 0; en < mip_clock_list_size; en++) {
      clockList[en] = clock[mip_clock_list[en]];
    }
    const double ideal_sum_time =
        timer_pointer->clock_time[clock[kMipClockIdeal]];
    const double tolerance_percent_report =
        tolerance_percent_report_ >= 0 ? tolerance_percent_report_ : 1e-8;
    return timer_pointer->reportOnTolerance(
        grepStamp, clockList, ideal_sum_time, tolerance_percent_report);
  };

  void csvMipClockList(const std::string model_name,
                       const std::vector<HighsInt> mip_clock_list,
                       const HighsTimerClock& mip_timer_clock,
                       const HighsInt kMipClockIdeal, const bool header,
                       const bool end_line) {
    HighsTimer* timer_pointer = mip_timer_clock.timer_pointer_;
    const std::vector<HighsInt>& clock = mip_timer_clock.clock_;
    const double ideal_sum_time =
        timer_pointer->clock_time[clock[kMipClockIdeal]];
    if (ideal_sum_time < 1e-2) return;
    const HighsInt num_clock = mip_clock_list.size();
    if (header) {
      printf("grep_csvMIP,model,ideal");
      for (HighsInt iX = 0; iX < num_clock; iX++) {
        HighsInt iclock = clock[mip_clock_list[iX]];
        printf(",%s", timer_pointer->clock_names[iclock].c_str());
      }
      printf(",Unaccounted");
      if (end_line) printf("\n");
      return;
    }
    double sum_time = 0;
    printf("grep_csvMIP,%s,%11.4g", model_name.c_str(), ideal_sum_time);
    for (HighsInt iX = 0; iX < num_clock; iX++) {
      HighsInt iclock = clock[mip_clock_list[iX]];
      double time = timer_pointer->read(iclock);
      sum_time += time;
      printf(",%11.4g", time);
    }
    printf(",%11.4g", ideal_sum_time - sum_time);
    if (end_line) printf("\n");
  }

  void reportMipCoreClock(const HighsTimerClock& mip_timer_clock) {
    //    const std::vector<HighsInt>& clock = mip_timer_clock.clock_;
    const std::vector<HighsInt> mip_clock_list{
        kMipClockPresolve, kMipClockSolve, kMipClockPostsolve};
    reportMipClockList("MipCore_", mip_clock_list, mip_timer_clock,
                       kMipClockTotal);
  };

  void reportMipLevel1Clock(const HighsTimerClock& mip_timer_clock) {
    const std::vector<HighsInt> mip_clock_list{kMipClockInit,
                                               kMipClockRunPresolve,
                                               kMipClockRunSetup,
                                               kMipClockTrivialHeuristics,
                                               kMipClockEvaluateRootNode,
                                               kMipClockPerformAging0,
                                               kMipClockSearch,
                                               kMipClockPostsolve};
    reportMipClockList("MipLevl1", mip_clock_list, mip_timer_clock,
                       kMipClockTotal, tolerance_percent_report);
  };

  void reportMipSolveLpClock(const HighsTimerClock& mip_timer_clock) {
    const std::vector<HighsInt> mip_clock_list{kMipClockSimplexBasisSolveLp,
                                               kMipClockSimplexNoBasisSolveLp,
                                               kMipClockIpmSolveLp};
    reportMipClockList("MipSlvLp", mip_clock_list, mip_timer_clock,
                       kMipClockTotal);  //, tolerance_percent_report);
  };

  void reportMipPresolveClock(const HighsTimerClock& mip_timer_clock) {
    const std::vector<HighsInt> mip_clock_list{kMipClockProbingPresolve};
    reportMipClockList("MipPrslv", mip_clock_list, mip_timer_clock,
                       kMipClockRunPresolve, tolerance_percent_report);
  };

  void reportMipSearchClock(const HighsTimerClock& mip_timer_clock) {
    const std::vector<HighsInt> mip_clock_list{
        kMipClockPerformAging1,        kMipClockDive,
        kMipClockOpenNodesToQueue,     kMipClockDomainPropgate,
        kMipClockPruneInfeasibleNodes, kMipClockUpdateLocalDomain,
        kMipClockNodeSearch,
        //	kMipClock@
    };
    reportMipClockList("MipSerch", mip_clock_list, mip_timer_clock,
                       kMipClockSearch, tolerance_percent_report);
  };

  void reportMipDiveClock(const HighsTimerClock& mip_timer_clock) {
    const std::vector<HighsInt> mip_clock_list{
        kMipClockEvaluateNode, kMipClockPrimalHeuristics, kMipClockTheDive,
        kMipClockBacktrackPlunge, kMipClockPerformAging2};
    reportMipClockList("MipDive_", mip_clock_list, mip_timer_clock,
                       kMipClockDive, tolerance_percent_report);
  };

  void reportMipPrimalHeuristicsClock(const HighsTimerClock& mip_timer_clock) {
    const std::vector<HighsInt> mip_clock_list{kMipClockRandomizedRounding0,
                                               kMipClockRens, kMipClockRins};
    reportMipClockList("MipPrimalHeuristics", mip_clock_list, mip_timer_clock,
                       kMipClockPrimalHeuristics, tolerance_percent_report);
  };

  void reportMipEvaluateRootNodeClock(const HighsTimerClock& mip_timer_clock) {
    const std::vector<HighsInt> mip_clock_list{
        kMipClockStartSymmetryDetection,
        kMipClockStartAnalyticCentreComputation,
        kMipClockEvaluateRootLp,
        kMipClockSeparateLpCuts,
        kMipClockRandomizedRounding1,
        kMipClockPerformRestart,
        kMipClockSeparation,
        kMipClockFinishAnalyticCentreComputation,
        kMipClockCentralRounding,
        kMipClockRootSeparationRound,
        kMipClockSolveSubMipRootReducedCost};
    reportMipClockList(
        "MipEvaluateRootNode", mip_clock_list, mip_timer_clock,
        kMipClockEvaluateRootNode);  //, tolerance_percent_report);
  };

  void reportMipSeparationClock(const HighsTimerClock& mip_timer_clock) {
    const std::vector<HighsInt> mip_clock_list{
        kMipClockSeparationRootSeparationRound,
        kMipClockSeparationFinishAnalyticCentreComputation,
        kMipClockSeparationCentralRounding, kMipClockSeparationEvaluateRootLp};
    reportMipClockList("MipSeparation", mip_clock_list, mip_timer_clock,
                       kMipClockSeparation);  //, tolerance_percent_report);
  };

  void csvMipClock(const std::string model_name,
                   const HighsTimerClock& mip_timer_clock, const bool header,
                   const bool end_line) {
    const std::vector<HighsInt> mip_clock_list{
        kMipClockRunPresolve, kMipClockEvaluateRootNode,
        kMipClockPrimalHeuristics, kMipClockTheDive};
    csvMipClockList(model_name, mip_clock_list, mip_timer_clock, kMipClockTotal,
                    header, end_line);
  };
};

#endif /* MIP_MIPTIMER_H_ */
