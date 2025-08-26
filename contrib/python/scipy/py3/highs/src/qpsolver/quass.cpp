#include "qpsolver/quass.hpp"

#include <fenv.h>

#include <algorithm>
#include <map>

#include "Highs.h"
#include "lp_data/HighsAnalysis.h"
#include "qpsolver/basis.hpp"
#include "qpsolver/crashsolution.hpp"
#include "qpsolver/dantzigpricing.hpp"
#include "qpsolver/devexharrispricing.hpp"
#include "qpsolver/devexpricing.hpp"
#include "qpsolver/factor.hpp"
#include "qpsolver/gradient.hpp"
#include "qpsolver/instance.hpp"
#include "qpsolver/perturbation.hpp"
#include "qpsolver/ratiotest.hpp"
#include "qpsolver/reducedcosts.hpp"
#include "qpsolver/reducedgradient.hpp"
#include "qpsolver/scaling.hpp"
#include "qpsolver/snippets.hpp"
#include "qpsolver/steepestedgepricing.hpp"

Quass::Quass(Runtime& rt) : runtime(rt) {}

static void loginformation(Runtime& rt, Basis& basis, CholeskyFactor& factor,
                           HighsTimer& timer) {
  rt.statistics.iteration.push_back(rt.statistics.num_iterations);
  rt.statistics.nullspacedimension.push_back(rt.instance.num_var -
                                             basis.getnumactive());
  rt.statistics.objval.push_back(rt.instance.objval(rt.primal));
  rt.statistics.time.push_back(timer.readRunHighsClock());
  SumNum sm =
      rt.instance.sumnumprimalinfeasibilities(rt.primal, rt.rowactivity);
  rt.statistics.sum_primal_infeasibilities.push_back(sm.sum);
  rt.statistics.num_primal_infeasibilities.push_back(sm.num);
  rt.statistics.density_factor.push_back(factor.density());
  rt.statistics.density_nullspace.push_back(0.0);
}

static void tidyup(QpVector& p, QpVector& rowmove, Basis& basis,
                   Runtime& runtime) {
  for (unsigned acon : basis.getactive()) {
    if ((HighsInt)acon >= runtime.instance.num_con) {
      p.value[acon - runtime.instance.num_con] = 0.0;
    } else {
      rowmove.value[acon] = 0.0;
    }
  }
}

static void computerowmove(Runtime& runtime, Basis& basis, QpVector& p,
                           QpVector& rowmove) {
  runtime.instance.A.mat_vec(p, rowmove);
  return;
  // rowmove.reset();
  // Commented out unreachable code
  //  MatrixBase& Atran = runtime.instance.A.t();
  //  Atran.vec_mat(p, rowmove);
  //  return;
  //  for (HighsInt i = 0; i < runtime.instance.num_con; i++) {
  //    if (basis.getstatus(i) == BasisStatus::Default) {
  //      // check with assertions, is it really the same?
  //      double val =
  //          p.dot(&Atran.index[Atran.start[i]], &Atran.value[Atran.start[i]],
  //                Atran.start[i + 1] - Atran.start[i]);
  //      // QpVector col = Atran.extractcol(i);
  //      // val = col * p;
  //
  //      // assert(rowmove.value[i] == val);
  //      rowmove.value[i] = val;
  //    } else {
  //      rowmove.value[i] = 0;
  //    }
  //  }
  //  rowmove.resparsify();
}

// VECTOR
static QpVector& computesearchdirection_minor(Runtime& rt, Basis& bas,
                                              CholeskyFactor& cf,
                                              ReducedGradient& redgrad,
                                              QpVector& p) {
  QpVector g2 = -redgrad.get();  // TODO PERF: buffer QpVector
  g2.sanitize();
  cf.solve(g2);

  g2.sanitize();

  return bas.Zprod(g2, p);
}

// VECTOR
static QpVector& computesearchdirection_major(
    Runtime& runtime, Basis& basis, CholeskyFactor& factor, const QpVector& yp,
    Gradient& gradient, QpVector& gyp, QpVector& l, QpVector& m, QpVector& p) {
  QpVector yyp = yp;  // TODO PERF: buffer QpVector
  // if (gradient.getGradient().dot(yp) > 0.0) {
  //   yyp.scale(-1.0);
  // }
  runtime.instance.Q.mat_vec(yyp, gyp);
  if (basis.getnumactive() < runtime.instance.num_var) {
    basis.Ztprod(gyp, m);
    l = m;
    factor.solveL(l);
    QpVector v = l;  // TODO PERF: buffer QpVector
    factor.solveLT(v);
    basis.Zprod(v, p);
    if (gradient.getGradient().dot(yyp) < 0.0) {
      return p.saxpy(-1.0, 1.0, yyp);
    } else {
      return p.saxpy(-1.0, -1.0, yyp);
    }

  } else {
    return p.repopulate(yp).scale(-gradient.getGradient().dot(yp));
    // return -yp;
  }
}

static double computemaxsteplength(Runtime& runtime, const QpVector& p,
                                   Gradient& gradient, QpVector& buffer_Qp,
                                   bool& zcd) {
  double denominator = p * runtime.instance.Q.mat_vec(p, buffer_Qp);
  if (fabs(denominator) > runtime.settings.pQp_zero_threshold) {
    double numerator = -(p * gradient.getGradient());
    if (numerator < 0.0) {
      return 0.0;
    } else {
      return numerator / denominator;
    }
  } else {
    zcd = true;
    return std::numeric_limits<double>::infinity();
  }
}

static QpSolverStatus reduce(Runtime& rt, Basis& basis,
                             const HighsInt newactivecon, QpVector& buffer_d,
                             HighsInt& maxabsd, HighsInt& constrainttodrop) {
  HighsInt idx = indexof(basis.getinactive(), newactivecon);
  if (idx != -1) {
    maxabsd = idx;
    constrainttodrop = newactivecon;
    QpVector::unit(basis.getinactive().size(), idx, buffer_d);
    return QpSolverStatus::OK;
    // return NullspaceReductionResult(true);
  }

  // TODO: this operation is inefficient.
  QpVector aq =
      rt.instance.A.t().extractcol(newactivecon);  // TODO PERF: buffer QpVector
  basis.Ztprod(aq, buffer_d, true, newactivecon);

  maxabsd = 0;
  for (HighsInt i = 0; i < buffer_d.num_nz; i++) {
    if (fabs(buffer_d.value[buffer_d.index[i]]) >
        fabs(buffer_d.value[maxabsd])) {
      maxabsd = buffer_d.index[i];
    }
  }
  constrainttodrop = basis.getinactive()[maxabsd];
  if (fabs(buffer_d.value[maxabsd]) < rt.settings.d_zero_threshold) {
    printf(
        "degeneracy? not possible to find non-active constraint to "
        "leave basis. max: log(d[%" HIGHSINT_FORMAT "]) = %lf\n",
        maxabsd, log10(fabs(buffer_d.value[maxabsd])));
    return QpSolverStatus::DEGENERATE;
  }
  return QpSolverStatus::OK;
  // return NullspaceReductionResult(idx != -1);
}

static std::unique_ptr<Pricing> getPricing(Runtime& runtime, Basis& basis,
                                           ReducedCosts& redcosts) {
  switch (runtime.settings.pricing) {
    case PricingStrategy::SteepestEdge:
      return std::unique_ptr<Pricing>(
          new SteepestEdgePricing(runtime, basis, redcosts));
    case PricingStrategy::Devex:
      return std::unique_ptr<Pricing>(
          new DevexPricing(runtime, basis, redcosts));
    case PricingStrategy::DantzigWolfe:
      return std::unique_ptr<Pricing>(
          new DantzigPricing(runtime, basis, redcosts));
  }
  return nullptr;
}

static void regularize(Runtime& rt) {
  if (!rt.settings.hessianregularization) {
    return;
  }
  // add small diagonal to hessian
  for (HighsInt i = 0; i < rt.instance.num_var; i++) {
    for (HighsInt index = rt.instance.Q.mat.start[i];
         index < rt.instance.Q.mat.start[i + 1]; index++) {
      if (rt.instance.Q.mat.index[index] == i) {
        rt.instance.Q.mat.value[index] +=
            rt.settings.hessianregularizationfactor;
      }
    }
  }
}

#if 0
static void compute_actual_duals(Runtime& rt, Basis& basis, QpVector& lambda, QpVector& dual_con, QpVector& dual_var) {
  for (auto e : basis.getactive()) {
    HighsInt indexinbasis = basis.getindexinfactor()[e];
    BasisStatus status = basis.getstatus(e);
    if (e >= rt.instance.num_con) {
      // active variable bound
      HighsInt var = e - rt.instance.num_con;

      if (status == BasisStatus::kActiveAtUpper) {
        dual_var.value[var] = -lambda.value[indexinbasis];
      } else if (status == BasisStatus::kActiveAtLower) {
        dual_var.value[var] = lambda.value[indexinbasis];
      } else {
        assert(lambda.value[indexinbasis] == 0);
        (void) dual_var.value[var];
      }
        
    } else {
      if (status == BasisStatus::kActiveAtUpper) {
        dual_con.value[e] = -lambda.value[indexinbasis];
      } else if (status == BasisStatus::kActiveAtLower) {
        dual_con.value[e] = lambda.value[indexinbasis];
      } else {
        assert(lambda.value[indexinbasis] == 0);
        dual_con.value[e] = 0;
      }
    }
  }
  dual_con.resparsify();
  dual_var.resparsify();
}

static double compute_primal_violation(Runtime& rt) {
  double maxviolation = 0.0;
  QpVector rowact = rt.instance.A.mat_vec(rt.primal);
  for (HighsInt i = 0; i < rt.instance.num_con; i++) {
    double violation = rt.instance.con_lo[i] - rowact.value[i];
    maxviolation = max(violation, maxviolation);
    violation = rowact.value[i] - rt.instance.con_up[i];
    maxviolation = max(violation, maxviolation);
  }
  for (HighsInt i = 0; i < rt.instance.num_var; i++) {
    double violation = rt.instance.var_lo[i] - rt.primal.value[i];
    maxviolation = max(violation, maxviolation);
    violation = rt.primal.value[i] - rt.instance.var_up[i];
    maxviolation = max(violation, maxviolation);
  }
  return maxviolation;
}

static double compute_dual_violation(Instance& instance, QpVector& primal, QpVector& dual_con, QpVector& dual_var) {
  double maxviolation = 0.0;

  QpVector residuals = instance.Q.mat_vec(primal) + instance.c + instance.A.t().mat_vec(dual_con) + dual_var;

  for (HighsInt i = 0; i < instance.num_var; i++) {
    double violation = residuals.value[i];
    maxviolation = max(violation, maxviolation);
    violation = -residuals.value[i];
    maxviolation = max(violation, maxviolation);
  }
  return maxviolation;
}
#endif

static bool check_reinvert_due(Basis& basis) {
  // reinvert can be triggered by basis
  return basis.getreinversionhint();
}

static void reinvert(Basis& basis, CholeskyFactor& factor, Gradient& grad,
                     ReducedCosts& rc, ReducedGradient& rg,
                     std::unique_ptr<Pricing>& pricing) {
  basis.rebuild();
  factor.recompute();
  grad.recompute();
  rc.recompute();
  rg.recompute();
  // pricing->recompute();
}

void Quass::solve(const QpVector& x0, const QpVector& ra, Basis& b0,
                  HighsTimer& timer) {
  // feenableexcept(FE_ALL_EXCEPT & ~FE_INEXACT & ~FE_UNDERFLOW);

  runtime.statistics.time_start = std::chrono::high_resolution_clock::now();
  Basis& basis = b0;
  runtime.primal = x0;

  // TODO: remove redundant equations before starting
  // HOWTO: from crash start, check all (near-)equality constraints (not
  // bounds). if the residual is 0 (or near-zero?), remove constraint

  Gradient gradient(runtime);
  ReducedCosts redcosts(runtime, basis, gradient);
  ReducedGradient redgrad(runtime, basis, gradient);
  CholeskyFactor factor(runtime, basis);
  runtime.instance.A.mat_vec(runtime.primal, runtime.rowactivity);
  std::unique_ptr<Pricing> pricing = getPricing(runtime, basis, redcosts);

  QpVector p(runtime.instance.num_var);
  QpVector rowmove(runtime.instance.num_con);

  QpVector buffer_yp(runtime.instance.num_var);
  QpVector buffer_gyp(runtime.instance.num_var);
  QpVector buffer_l(runtime.instance.num_var);
  QpVector buffer_m(runtime.instance.num_var);

  QpVector buffer_Qp(runtime.instance.num_var);

  // buffers for reduction
  QpVector buffer_d(runtime.instance.num_var);

  regularize(runtime);

  runtime.relaxed_for_ratiotest = ratiotest_relax_instance(runtime);

  HighsInt last_logging_iteration = runtime.statistics.num_iterations - 1;
  double last_logging_time = 0;
  double logging_time_interval = 10;

  const HighsInt current_num_active = basis.getnumactive();
  bool atfsep = current_num_active == runtime.instance.num_var;
  while (true) {
    // check iteration limit
    if (runtime.statistics.num_iterations >= runtime.settings.iteration_limit) {
      runtime.status = QpModelStatus::kIterationLimit;
      break;
    }

    // check time limit
    if (timer.readRunHighsClock() >= runtime.settings.time_limit) {
      runtime.status = QpModelStatus::kTimeLimit;
      break;
    }

    if (basis.getnuminactive() > runtime.settings.nullspace_limit) {
      runtime.settings.nullspace_limit_log.fire(
          runtime.settings.nullspace_limit);
      runtime.status = QpModelStatus::kLargeNullspace;
      return;
    }

    // LOGGING
    double run_time = timer.readRunHighsClock();
    if ((runtime.statistics.num_iterations %
                 runtime.settings.reportingfequency ==
             0 ||
         run_time - last_logging_time > logging_time_interval) &&
        runtime.statistics.num_iterations > last_logging_iteration) {
      bool log_report = true;
      if (runtime.statistics.num_iterations >
          10 * runtime.settings.reportingfequency) {
        runtime.settings.reportingfequency *= 10;
        log_report = false;
      }
      if (run_time > 10 * logging_time_interval) logging_time_interval *= 2.0;
      if (log_report) {
        last_logging_time = run_time;
        last_logging_iteration = runtime.statistics.num_iterations;
        loginformation(runtime, basis, factor, timer);
        runtime.settings.iteration_log.fire(runtime.statistics);
      }
    }

    // REINVERSION
    if (check_reinvert_due(basis)) {
      reinvert(basis, factor, gradient, redcosts, redgrad, pricing);
    }

    QpSolverStatus status;

    bool zero_curvature_direction = false;
    double maxsteplength = 1.0;
    if (atfsep) {
      // Determine a variable to relax from being active. If there is
      // none, then basis is optimal
      HighsInt minidx = pricing->price(runtime.primal, gradient.getGradient());
      if (minidx == -1) {
        runtime.status = QpModelStatus::kOptimal;
        break;
      }
      // Now perform a real iteration
      runtime.statistics.num_iterations++;

      HighsInt unit = basis.getindexinfactor()[minidx];
      QpVector::unit(runtime.instance.num_var, unit, buffer_yp);
      basis.btran(buffer_yp, buffer_yp, true, minidx);

      buffer_l.dim = basis.getnuminactive();
      buffer_m.dim = basis.getnuminactive();
      computesearchdirection_major(runtime, basis, factor, buffer_yp, gradient,
                                   buffer_gyp, buffer_l, buffer_m, p);
      basis.deactivate(minidx);
      computerowmove(runtime, basis, p, rowmove);
      tidyup(p, rowmove, basis, runtime);
      maxsteplength = std::numeric_limits<double>::infinity();
      // if (runtime.instance.Q.mat.value.size() > 0) {
      maxsteplength = computemaxsteplength(runtime, p, gradient, buffer_Qp,
                                           zero_curvature_direction);
      if (!zero_curvature_direction) {
        status = factor.expand(buffer_yp, buffer_gyp, buffer_l, buffer_m);
        if (status != QpSolverStatus::OK) {
          runtime.status = QpModelStatus::kUndetermined;
          return;
        }
      }
      redgrad.expand(buffer_yp);
    } else {
      // Compute a search direction - which may be zero, in which case
      // atfsep is set true and the loop repeats with this
      // condition. In particular, this happens when the current basis
      // is optimal
      computesearchdirection_minor(runtime, basis, factor, redgrad, p);
      computerowmove(runtime, basis, p, rowmove);
      tidyup(p, rowmove, basis, runtime);
      runtime.instance.Q.mat_vec(p, buffer_Qp);
    }
    if (p.norm2() < runtime.settings.pnorm_zero_threshold ||
        maxsteplength == 0.0 ||
        (false && fabs(gradient.getGradient().dot(p)) <
                      runtime.settings.improvement_zero_threshold)) {
      atfsep = true;
    } else {
      // Now perform a real iteration
      runtime.statistics.num_iterations++;

      RatiotestResult stepres = ratiotest(runtime, p, rowmove, maxsteplength);
      if (stepres.limitingconstraint != -1) {
        HighsInt constrainttodrop;
        HighsInt maxabsd;
        status = reduce(runtime, basis, stepres.limitingconstraint, buffer_d,
                        maxabsd, constrainttodrop);
        if (status != QpSolverStatus::OK) {
          runtime.status = QpModelStatus::kUndetermined;
          return;
        }
        if (!zero_curvature_direction) {
          factor.reduce(
              buffer_d, maxabsd,
              indexof(basis.getinactive(), stepres.limitingconstraint) != -1);
        }
        redgrad.reduce(buffer_d, maxabsd);
        redgrad.update(stepres.alpha, false);

        status = basis.activate(runtime.settings, stepres.limitingconstraint,
                                stepres.nowactiveatlower
                                    ? BasisStatus::kActiveAtLower
                                    : BasisStatus::kActiveAtUpper,
                                constrainttodrop, pricing.get());
        if (status != QpSolverStatus::OK) {
          runtime.status = QpModelStatus::kUndetermined;
          return;
        }
        if (basis.getnumactive() != runtime.instance.num_var) {
          atfsep = false;
        }
      } else {
        if (stepres.alpha == std::numeric_limits<double>::infinity()) {
          // unbounded
          runtime.status = QpModelStatus::kUnbounded;
          return;
        }
        atfsep = false;
        redgrad.update(stepres.alpha, false);
      }

      runtime.primal.saxpy(stepres.alpha, p);
      runtime.rowactivity.saxpy(stepres.alpha, rowmove);

      gradient.update(buffer_Qp, stepres.alpha);
      redcosts.update();
    }
  }

  loginformation(runtime, basis, factor, timer);
  runtime.settings.iteration_log.fire(runtime.statistics);
  //  basis.report();

  runtime.instance.sumnumprimalinfeasibilities(
      runtime.primal, runtime.instance.A.mat_vec(runtime.primal));

  QpVector& lambda = redcosts.getReducedCosts();
  for (auto e : basis.getactive()) {
    HighsInt indexinbasis = basis.getindexinfactor()[e];
    if (e >= runtime.instance.num_con) {
      // active variable bound
      HighsInt var = e - runtime.instance.num_con;
      runtime.dualvar.value[var] = lambda.value[indexinbasis];
    } else {
      runtime.dualcon.value[e] = lambda.value[indexinbasis];
    }
  }
  runtime.dualcon.resparsify();
  runtime.dualvar.resparsify();

  // QpVector actual_dual_var(runtime.instance.num_var);
  // QpVector actual_dual_con(runtime.instance.num_con);
  // compute_actual_duals(runtime, basis, redcosts.getReducedCosts(),
  // actual_dual_con, actual_dual_var); printf("max primal violation =
  // %.20lf\n", compute_primal_violation(runtime)); printf("max dual   violation
  // = %.20lf\n", compute_dual_violation(runtime.instance, runtime.primal,
  // actual_dual_con, actual_dual_var));

  // extract basis status
  for (HighsInt i = 0; i < runtime.instance.num_var; i++) {
    runtime.status_var[i] = basis.getstatus(runtime.instance.num_con + i);
  }

  for (HighsInt i = 0; i < runtime.instance.num_con; i++) {
    runtime.status_con[i] = basis.getstatus(i);
  }

  if (basis.getnumactive() == runtime.instance.num_var) {
    runtime.primal = basis.recomputex(runtime.instance);
  }
  // x.report("x");
  runtime.statistics.time_end = std::chrono::high_resolution_clock::now();
}
