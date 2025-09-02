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
/**@file simplex/HighsMipAnalysis.cpp
 * @brief
 */
#include "mip/HighsMipAnalysis.h"

#include <cmath>

#include "mip/MipTimer.h"

const HighsInt check_mip_clock = -4;

void HighsMipAnalysis::setup(const HighsLp& lp, const HighsOptions& options) {
  model_name = lp.model_name_;
  setupMipTime(options);
}

void HighsMipAnalysis::setupMipTime(const HighsOptions& options) {
  analyse_mip_time = kHighsAnalysisLevelMipTime & options.highs_analysis_level;
  if (analyse_mip_time) {
    HighsTimerClock clock;
    clock.timer_pointer_ = timer_;
    MipTimer mip_timer;
    mip_timer.initialiseMipClocks(clock);
    mip_clocks = clock;
  }
}

void HighsMipAnalysis::mipTimerStart(const HighsInt mip_clock
                                     // , const HighsInt thread_id
) const {
  if (!analyse_mip_time) return;
  HighsInt highs_timer_clock = mip_clocks.clock_[mip_clock];
  if (highs_timer_clock == check_mip_clock) {
    std::string clock_name =
        mip_clocks.timer_pointer_->clock_names[check_mip_clock];
    printf("MipTimer: starting clock %d: %s\n", int(check_mip_clock),
           clock_name.c_str());
  }
  mip_clocks.timer_pointer_->start(highs_timer_clock);
}

void HighsMipAnalysis::mipTimerStop(const HighsInt mip_clock
                                    // , const HighsInt thread_id
) const {
  if (!analyse_mip_time) return;
  HighsInt highs_timer_clock = mip_clocks.clock_[mip_clock];
  if (highs_timer_clock == check_mip_clock) {
    std::string clock_name =
        mip_clocks.timer_pointer_->clock_names[check_mip_clock];
    printf("MipTimer: stopping clock %d: %s\n", int(check_mip_clock),
           clock_name.c_str());
  }
  mip_clocks.timer_pointer_->stop(highs_timer_clock);
}

bool HighsMipAnalysis::mipTimerRunning(const HighsInt mip_clock
                                       // , const HighsInt thread_id
) const {
  if (!analyse_mip_time) return false;
  HighsInt highs_timer_clock = mip_clocks.clock_[mip_clock];
  return mip_clocks.timer_pointer_->running(highs_timer_clock);
}

double HighsMipAnalysis::mipTimerRead(const HighsInt mip_clock
                                      // , const HighsInt thread_id
) const {
  if (!analyse_mip_time) return 0;
  HighsInt highs_timer_clock = mip_clocks.clock_[mip_clock];
  return mip_clocks.timer_pointer_->read(highs_timer_clock);
}

void HighsMipAnalysis::reportMipSolveLpClock(const bool header) {
  if (header) {
    printf(
        ",simplex time,IPM time,#simplex,#IPM,simplex/total time,IPM/total "
        "time,#No basis solve,simplex/#Basis solve,simplex/#No basis solve\n");
    return;
  }
  if (!analyse_mip_time) return;
  double total_time = mip_clocks.timer_pointer_->read(0);
  if (total_time < 0.01) return;
  HighsInt simplex_basis_solve_iclock =
      mip_clocks.clock_[kMipClockSimplexBasisSolveLp];
  HighsInt simplex_no_basis_solve_iclock =
      mip_clocks.clock_[kMipClockSimplexNoBasisSolveLp];
  HighsInt ipm_solve_iclock = mip_clocks.clock_[kMipClockIpmSolveLp];
  //  HighsInt num_no_basis_solve =
  //  mip_clocks.timer_pointer_->clock_num_call[no_basis_solve_iclock]; HighsInt
  //  num_basis_solve =
  //  mip_clocks.timer_pointer_->clock_num_call[basis_solve_iclock];
  HighsInt num_simplex_basis_solve =
      mip_clocks.timer_pointer_->clock_num_call[simplex_basis_solve_iclock];
  HighsInt num_simplex_no_basis_solve =
      mip_clocks.timer_pointer_->clock_num_call[simplex_no_basis_solve_iclock];
  HighsInt num_ipm_solve =
      mip_clocks.timer_pointer_->clock_num_call[ipm_solve_iclock];
  HighsInt num_simplex_solve =
      num_simplex_basis_solve + num_simplex_no_basis_solve;
  //  assert(num_no_basis_solve+num_basis_solve == num_simplex_solve);
  double simplex_basis_solve_time =
      mip_clocks.timer_pointer_->read(simplex_basis_solve_iclock);
  double simplex_no_basis_solve_time =
      mip_clocks.timer_pointer_->read(simplex_no_basis_solve_iclock);
  double simplex_solve_time =
      simplex_basis_solve_time + simplex_no_basis_solve_time;
  double ipm_solve_time = mip_clocks.timer_pointer_->read(ipm_solve_iclock);
  double frac_simplex_solve_time = simplex_solve_time / total_time;
  double frac_ipm_solve_time = ipm_solve_time / total_time;
  double average_simplex_basis_solve_time =
      num_simplex_basis_solve > 0
          ? simplex_basis_solve_time / int(num_simplex_basis_solve)
          : 0.0;
  double average_simplex_no_basis_solve_time =
      num_simplex_no_basis_solve > 0
          ? simplex_no_basis_solve_time / int(num_simplex_no_basis_solve)
          : 0.0;
  printf(",%11.2g,%11.2g,%d,%d,%11.2g,%11.2g,%d,%11.2g,%11.2g\n",
         simplex_solve_time, ipm_solve_time, int(num_simplex_solve),
         int(num_ipm_solve), frac_simplex_solve_time, frac_ipm_solve_time,
         int(num_simplex_no_basis_solve), average_simplex_basis_solve_time,
         average_simplex_no_basis_solve_time);
  printf(
      "LP solver analysis: %d LP with %d simplex (%11.2g CPU), %d IPM (%11.2g "
      "CPU) and %d solved without basis; average simplex solve time "
      "(basis/no_basis) = (%11.2g, %11.2g)\n",
      int(num_simplex_solve + num_ipm_solve), int(num_simplex_solve),
      simplex_solve_time, int(num_ipm_solve), ipm_solve_time,
      int(num_simplex_no_basis_solve), average_simplex_basis_solve_time,
      average_simplex_no_basis_solve_time);
};

void HighsMipAnalysis::reportMipTimer() {
  if (!analyse_mip_time) return;
  //  assert(analyse_mip_time);
  MipTimer mip_timer;
  mip_timer.reportMipCoreClock(mip_clocks);
  mip_timer.reportMipLevel1Clock(mip_clocks);
  mip_timer.reportMipSolveLpClock(mip_clocks);
  mip_timer.reportMipPresolveClock(mip_clocks);
  mip_timer.reportMipSearchClock(mip_clocks);
  mip_timer.reportMipDiveClock(mip_clocks);
  mip_timer.reportMipPrimalHeuristicsClock(mip_clocks);
  mip_timer.reportMipEvaluateRootNodeClock(mip_clocks);
  mip_timer.reportMipSeparationClock(mip_clocks);
  mip_timer.csvMipClock(this->model_name, mip_clocks, true, false);
  reportMipSolveLpClock(true);
  mip_timer.csvMipClock(this->model_name, mip_clocks, false, false);
  reportMipSolveLpClock(false);
}
