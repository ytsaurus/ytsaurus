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
#include "mip/HighsImplications.h"

#include "../extern/pdqsort/pdqsort.h"
#include "mip/HighsCliqueTable.h"
#include "mip/HighsMipSolverData.h"
#include "mip/MipTimer.h"

bool HighsImplications::computeImplications(HighsInt col, bool val) {
  HighsDomain& globaldomain = mipsolver.mipdata_->domain;
  HighsCliqueTable& cliquetable = mipsolver.mipdata_->cliquetable;
  globaldomain.propagate();
  if (globaldomain.infeasible() || globaldomain.isFixed(col)) return true;
  const auto& domchgstack = globaldomain.getDomainChangeStack();
  const auto& domchgreason = globaldomain.getDomainChangeReason();
  HighsInt changedend = globaldomain.getChangedCols().size();

  HighsInt stackimplicstart = domchgstack.size() + 1;
  HighsInt numImplications = -stackimplicstart;
  if (val)
    globaldomain.changeBound(HighsBoundType::kLower, col, 1);
  else
    globaldomain.changeBound(HighsBoundType::kUpper, col, 0);

  if (globaldomain.infeasible()) {
    globaldomain.backtrack();
    globaldomain.clearChangedCols(changedend);
    cliquetable.vertexInfeasible(globaldomain, col, val);

    return true;
  }

  globaldomain.propagate();

  if (globaldomain.infeasible()) {
    globaldomain.backtrack();
    globaldomain.clearChangedCols(changedend);

    cliquetable.vertexInfeasible(globaldomain, col, val);

    return true;
  }

  HighsInt stackimplicend = domchgstack.size();
  numImplications += stackimplicend;
  mipsolver.mipdata_->pseudocost.addInferenceObservation(col, numImplications,
                                                         val);

  std::vector<HighsDomainChange> implics;
  implics.reserve(numImplications);

  HighsInt numEntries = mipsolver.mipdata_->cliquetable.getNumEntries();
  HighsInt maxEntries = 100000 + mipsolver.numNonzero();

  for (HighsInt i = stackimplicstart; i < stackimplicend; ++i) {
    if (domchgreason[i].type == HighsDomain::Reason::kCliqueTable &&
        ((domchgreason[i].index >> 1) == col || numEntries >= maxEntries))
      continue;

    implics.push_back(domchgstack[i]);
  }

  globaldomain.backtrack();
  globaldomain.clearChangedCols(changedend);

  // add the implications of binary variables to the clique table
  auto binstart = std::partition(implics.begin(), implics.end(),
                                 [&](const HighsDomainChange& a) {
                                   return !globaldomain.isBinary(a.column);
                                 });

  pdqsort(implics.begin(), binstart);

  std::array<HighsCliqueTable::CliqueVar, 2> clique;
  clique[0] = HighsCliqueTable::CliqueVar(col, val);

  for (auto i = binstart; i != implics.end(); ++i) {
    if (i->boundtype == HighsBoundType::kLower)
      clique[1] = HighsCliqueTable::CliqueVar(i->column, 0);
    else
      clique[1] = HighsCliqueTable::CliqueVar(i->column, 1);

    cliquetable.addClique(mipsolver, clique.data(), 2);
    if (globaldomain.infeasible() || globaldomain.isFixed(col)) return true;
  }

  // store variable bounds derived from implications
  for (auto i = implics.begin(); i != binstart; ++i) {
    if (i->boundtype == HighsBoundType::kLower) {
      if (val == 1) {
        if (globaldomain.col_lower_[i->column] != -kHighsInf)
          addVLB(i->column, col,
                 i->boundval - globaldomain.col_lower_[i->column],
                 globaldomain.col_lower_[i->column]);
      } else
        addVLB(i->column,
               col,  // in case the lower bound is infinite the varbound can
                     // still be tightened as soon as a finite upper bound is
                     // known because the offset is finite
               globaldomain.col_lower_[i->column] - i->boundval, i->boundval);
    } else {
      if (val == 1) {
        if (globaldomain.col_upper_[i->column] != kHighsInf)
          addVUB(i->column, col,
                 i->boundval - globaldomain.col_upper_[i->column],
                 globaldomain.col_upper_[i->column]);
      } else
        addVUB(i->column,
               col,  // in case the upper bound is infinite the varbound can
                     // still be tightened as soon as a finite upper bound is
                     // known because the offset is finite
               globaldomain.col_upper_[i->column] - i->boundval, i->boundval);
    }
  }

  HighsInt loc = 2 * col + val;
  implications[loc].computed = true;
  implics.erase(binstart, implics.end());
  if (!implics.empty()) {
    implications[loc].implics = std::move(implics);
    this->numImplications += implications[loc].implics.size();
  }

  return false;
}

static constexpr bool kSkipBadVbds = true;
static constexpr bool kUseDualsForBreakingTies = true;

std::pair<HighsInt, HighsImplications::VarBound> HighsImplications::getBestVub(
    HighsInt col, const HighsSolution& lpSolution, double& bestUb) const {
  std::pair<HighsInt, VarBound> bestVub =
      std::make_pair(-1, VarBound{0.0, kHighsInf});
  double minbestUb = bestUb;
  double bestUbDist = kHighsInf;
  int64_t bestvubnodes = 0;

  auto isVubBetter = [&](double ubDist, int64_t vubNodes, double minVubVal,
                         HighsInt vubCol, const VarBound& vub) {
    if (ubDist < bestUbDist - mipsolver.mipdata_->feastol) return true;
    if (vubNodes > bestvubnodes) return true;
    if (vubNodes < bestvubnodes) return false;
    if (minVubVal < minbestUb - mipsolver.mipdata_->feastol) return true;
    if (kUseDualsForBreakingTies) {
      if (minVubVal > minbestUb + mipsolver.mipdata_->feastol) return false;
      if (lpSolution.col_dual[vubCol] / vub.coef -
              lpSolution.col_dual[bestVub.first] / bestVub.second.coef >
          mipsolver.mipdata_->feastol)
        return true;
    }

    return false;
  };

  double scale = mipsolver.mipdata_->domain.col_upper_[col] -
                 mipsolver.mipdata_->domain.col_lower_[col];
  if (scale == kHighsInf)
    scale = 1.0;
  else
    scale = 1.0 / scale;

  vubs[col].for_each([&](HighsInt vubCol, const VarBound& vub) {
    if (vub.coef == kHighsInf) return;
    if (mipsolver.mipdata_->domain.isFixed(vubCol)) return;
    assert(mipsolver.mipdata_->domain.isBinary(vubCol));
    double vubval = lpSolution.col_value[vubCol] * vub.coef + vub.constant;
    double ubDist = std::max(0.0, vubval - lpSolution.col_value[col]);

    double yDist = mipsolver.mipdata_->feastol +
                   (vub.coef > 0 ? 1 - lpSolution.col_value[vubCol]
                                 : lpSolution.col_value[vubCol]);
    // skip variable bound if the distance towards the variable bound constraint
    // is larger than the distance to the point where the binary column is
    // relaxed to it's weakest bound, i.e. 1 if its coefficient is positive.
    // The variable bound constraint has the form x <= ay + b with y binary and
    // hence the norm is sqrt(1 + a^2) and the distance of the variable bound
    // constraint is ay + b - x evaluated at the solution values of x and y
    // divided by the norm.
    double norm2 = 1.0 + vub.coef * vub.coef;
    if (kSkipBadVbds && ubDist * ubDist > yDist * yDist * norm2) return;

    assert(vubCol >= 0 && vubCol < mipsolver.numCol());
    ubDist *= scale;
    if (ubDist <= bestUbDist + mipsolver.mipdata_->feastol) {
      double minvubval = vub.minValue();
      int64_t vubnodes =
          vub.coef > 0 ? mipsolver.mipdata_->nodequeue.numNodesDown(vubCol)
                       : mipsolver.mipdata_->nodequeue.numNodesUp(vubCol);

      if (isVubBetter(ubDist, vubnodes, minvubval, vubCol, vub)) {
        bestUb = vubval;
        minbestUb = minvubval;
        bestVub = std::make_pair(vubCol, vub);
        bestvubnodes = vubnodes;
        bestUbDist = ubDist;
      }
    }
  });

  return bestVub;
}

std::pair<HighsInt, HighsImplications::VarBound> HighsImplications::getBestVlb(
    HighsInt col, const HighsSolution& lpSolution, double& bestLb) const {
  std::pair<HighsInt, VarBound> bestVlb =
      std::make_pair(-1, VarBound{0.0, -kHighsInf});
  double maxbestlb = bestLb;
  double bestLbDist = kHighsInf;
  int64_t bestvlbnodes = 0;

  auto isVlbBetter = [&](double lbDist, int64_t vlbNodes, double maxVlbVal,
                         HighsInt vlbCol, const VarBound& vlb) {
    if (lbDist < bestLbDist - mipsolver.mipdata_->feastol) return true;
    if (vlbNodes > bestvlbnodes) return true;
    if (vlbNodes < bestvlbnodes) return false;
    if (maxVlbVal > maxbestlb + mipsolver.mipdata_->feastol) return true;
    if (kUseDualsForBreakingTies) {
      if (maxVlbVal < maxbestlb - mipsolver.mipdata_->feastol) return false;
      if (lpSolution.col_dual[vlbCol] / vlb.coef -
              lpSolution.col_dual[bestVlb.first] / bestVlb.second.coef <
          -mipsolver.mipdata_->feastol)
        return true;
    }

    return false;
  };

  double scale = mipsolver.mipdata_->domain.col_upper_[col] -
                 mipsolver.mipdata_->domain.col_lower_[col];
  if (scale == kHighsInf)
    scale = 1.0;
  else
    scale = 1.0 / scale;

  vlbs[col].for_each([&](HighsInt vlbCol, const VarBound& vlb) {
    if (vlb.coef == -kHighsInf) return;
    if (mipsolver.mipdata_->domain.isFixed(vlbCol)) return;
    assert(mipsolver.mipdata_->domain.isBinary(vlbCol));
    assert(vlbCol >= 0 && vlbCol < mipsolver.numCol());
    double vlbval = lpSolution.col_value[vlbCol] * vlb.coef + vlb.constant;
    double lbDist = std::max(0.0, lpSolution.col_value[col] - vlbval);

    double yDist = mipsolver.mipdata_->feastol +
                   (vlb.coef > 0 ? lpSolution.col_value[vlbCol]
                                 : 1 - lpSolution.col_value[vlbCol]);

    double norm2 = 1.0 + vlb.coef * vlb.coef;
    if (kSkipBadVbds && lbDist * lbDist > yDist * yDist * norm2) return;

    // scale the distance as if the bounded column was scaled to have ub-lb=1
    lbDist *= scale;
    if (lbDist <= bestLbDist + mipsolver.mipdata_->feastol) {
      double maxvlbval = vlb.maxValue();
      int64_t vlbnodes =
          vlb.coef > 0 ? mipsolver.mipdata_->nodequeue.numNodesUp(vlbCol)
                       : mipsolver.mipdata_->nodequeue.numNodesDown(vlbCol);

      if (isVlbBetter(lbDist, vlbnodes, maxvlbval, vlbCol, vlb)) {
        bestLb = vlbval;
        maxbestlb = maxvlbval;
        bestVlb = std::make_pair(vlbCol, vlb);
        bestvlbnodes = vlbnodes;
        bestLbDist = lbDist;
      }
    }
  });

  return bestVlb;
}

bool HighsImplications::runProbing(HighsInt col, HighsInt& numReductions) {
  HighsDomain& globaldomain = mipsolver.mipdata_->domain;
  if (globaldomain.isBinary(col) && !implicationsCached(col, 1) &&
      !implicationsCached(col, 0) &&
      mipsolver.mipdata_->cliquetable.getSubstitution(col) == nullptr) {
    bool infeasible;

    infeasible = computeImplications(col, 1);
    if (globaldomain.infeasible()) return true;
    if (infeasible) return true;
    if (mipsolver.mipdata_->cliquetable.getSubstitution(col) != nullptr)
      return true;

    infeasible = computeImplications(col, 0);
    if (globaldomain.infeasible()) return true;
    if (infeasible) return true;
    if (mipsolver.mipdata_->cliquetable.getSubstitution(col) != nullptr)
      return true;

    // analyze implications
    const std::vector<HighsDomainChange>& implicsdown =
        getImplications(col, 0, infeasible);
    const std::vector<HighsDomainChange>& implicsup =
        getImplications(col, 1, infeasible);
    HighsInt nimplicsdown = implicsdown.size();
    HighsInt nimplicsup = implicsup.size();
    HighsInt u = 0;
    HighsInt d = 0;

    while (u < nimplicsup && d < nimplicsdown) {
      if (implicsup[u].column < implicsdown[d].column)
        ++u;
      else if (implicsdown[d].column < implicsup[u].column)
        ++d;
      else {
        assert(implicsup[u].column == implicsdown[d].column);
        HighsInt implcol = implicsup[u].column;
        double lbDown = globaldomain.col_lower_[implcol];
        double ubDown = globaldomain.col_upper_[implcol];
        double lbUp = lbDown;
        double ubUp = ubDown;

        do {
          if (implicsdown[d].boundtype == HighsBoundType::kLower)
            lbDown = std::max(lbDown, implicsdown[d].boundval);
          else
            ubDown = std::min(ubDown, implicsdown[d].boundval);
          ++d;
        } while (d < nimplicsdown && implicsdown[d].column == implcol);

        do {
          if (implicsup[u].boundtype == HighsBoundType::kLower)
            lbUp = std::max(lbUp, implicsup[u].boundval);
          else
            ubUp = std::min(ubUp, implicsup[u].boundval);
          ++u;
        } while (u < nimplicsup && implicsup[u].column == implcol);

        if (colsubstituted[implcol] || globaldomain.isFixed(implcol)) continue;

        if (lbDown == ubDown && lbUp == ubUp &&
            std::abs(lbDown - lbUp) > mipsolver.mipdata_->feastol) {
          HighsSubstitution substitution;
          substitution.substcol = implcol;
          substitution.staycol = col;
          substitution.offset = lbDown;
          substitution.scale = lbUp - lbDown;
          substitutions.push_back(substitution);
          colsubstituted[implcol] = true;
          ++numReductions;
        } else {
          double lb = std::min(lbDown, lbUp);
          double ub = std::max(ubDown, ubUp);

          if (lb > globaldomain.col_lower_[implcol]) {
            globaldomain.changeBound(HighsBoundType::kLower, implcol, lb,
                                     HighsDomain::Reason::unspecified());
            ++numReductions;
          }

          if (ub < globaldomain.col_upper_[implcol]) {
            globaldomain.changeBound(HighsBoundType::kUpper, implcol, ub,
                                     HighsDomain::Reason::unspecified());
            ++numReductions;
          }
        }
      }
    }

    return true;
  }

  return false;
}

void HighsImplications::addVUB(HighsInt col, HighsInt vubcol, double vubcoef,
                               double vubconstant) {
  // assume that VUBs do not have infinite coefficients and infinite constant
  // terms since such VUBs effectively evaluate to NaN.
  assert(std::abs(vubcoef) != kHighsInf || std::abs(vubconstant) != kHighsInf);

  VarBound vub{vubcoef, vubconstant};

  mipsolver.mipdata_->debugSolution.checkVub(col, vubcol, vubcoef, vubconstant);

  double minBound = vub.minValue();
  if (minBound >=
      mipsolver.mipdata_->domain.col_upper_[col] - mipsolver.mipdata_->feastol)
    return;

  auto insertresult = vubs[col].insert_or_get(vubcol, vub);

  if (!insertresult.second) {
    VarBound& currentvub = *insertresult.first;
    double currentMinBound = currentvub.minValue();
    if (minBound < currentMinBound - mipsolver.mipdata_->feastol) {
      currentvub.coef = vubcoef;
      currentvub.constant = vubconstant;
    }
  }
}

void HighsImplications::addVLB(HighsInt col, HighsInt vlbcol, double vlbcoef,
                               double vlbconstant) {
  // assume that VLBs do not have infinite coefficients and infinite constant
  // terms since such VLBs effectively evaluate to NaN.
  assert(std::abs(vlbcoef) != kHighsInf || std::abs(vlbconstant) != kHighsInf);

  VarBound vlb{vlbcoef, vlbconstant};

  mipsolver.mipdata_->debugSolution.checkVlb(col, vlbcol, vlbcoef, vlbconstant);

  double maxBound = vlb.maxValue();
  if (vlb.maxValue() <=
      mipsolver.mipdata_->domain.col_lower_[col] + mipsolver.mipdata_->feastol)
    return;

  auto insertresult = vlbs[col].insert_or_get(vlbcol, vlb);

  if (!insertresult.second) {
    VarBound& currentvlb = *insertresult.first;

    double currentMaxNound = currentvlb.maxValue();
    if (maxBound > currentMaxNound + mipsolver.mipdata_->feastol) {
      currentvlb.coef = vlbcoef;
      currentvlb.constant = vlbconstant;
    }
  }
}

void HighsImplications::rebuild(HighsInt ncols,
                                const std::vector<HighsInt>& orig2reducedcol,
                                const std::vector<HighsInt>& orig2reducedrow) {
  std::vector<HighsHashTree<HighsInt, VarBound>> oldvubs;
  std::vector<HighsHashTree<HighsInt, VarBound>> oldvlbs;

  oldvlbs.swap(vlbs);
  oldvubs.swap(vubs);

  colsubstituted.clear();
  colsubstituted.shrink_to_fit();
  implications.clear();
  implications.shrink_to_fit();

  implications.resize(2 * ncols);
  colsubstituted.resize(ncols);
  substitutions.clear();
  vubs.clear();
  vubs.shrink_to_fit();
  vubs.resize(ncols);
  vlbs.clear();
  vlbs.shrink_to_fit();
  vlbs.resize(ncols);
  numImplications = 0;
  HighsInt oldncols = oldvubs.size();

  nextCleanupCall = mipsolver.numNonzero();

  for (HighsInt i = 0; i != oldncols; ++i) {
    HighsInt newi = orig2reducedcol[i];

    if (newi == -1 ||
        !mipsolver.mipdata_->postSolveStack.isColLinearlyTransformable(newi))
      continue;

    oldvubs[i].for_each([&](HighsInt vubCol, VarBound vub) {
      HighsInt newVubCol = orig2reducedcol[vubCol];
      if (newVubCol == -1) return;

      if (!mipsolver.mipdata_->domain.isBinary(newVubCol) ||
          !mipsolver.mipdata_->postSolveStack.isColLinearlyTransformable(
              newVubCol))
        return;

      addVUB(newi, newVubCol, vub.coef, vub.constant);
    });

    oldvlbs[i].for_each([&](HighsInt vlbCol, VarBound vlb) {
      HighsInt newVlbCol = orig2reducedcol[vlbCol];
      if (newVlbCol == -1) return;

      if (!mipsolver.mipdata_->domain.isBinary(newVlbCol) ||
          !mipsolver.mipdata_->postSolveStack.isColLinearlyTransformable(
              newVlbCol))
        return;

      addVLB(newi, newVlbCol, vlb.coef, vlb.constant);
    });

    // todo also add old implications once implications can be added
    // incrementally for now we discard the old implications as they might be
    // weaker then newly computed ones and adding them would block computation
    // of new implications
  }
}

void HighsImplications::buildFrom(const HighsImplications& init) {
  // todo check if this should be done
  HighsInt numcol = mipsolver.numCol();

  for (HighsInt i = 0; i != numcol; ++i) {
    init.vubs[i].for_each([&](HighsInt vubCol, VarBound vub) {
      if (!mipsolver.mipdata_->domain.isBinary(vubCol)) return;
      addVUB(i, vubCol, vub.coef, vub.constant);
    });

    init.vlbs[i].for_each([&](HighsInt vlbCol, VarBound vlb) {
      if (!mipsolver.mipdata_->domain.isBinary(vlbCol)) return;
      addVLB(i, vlbCol, vlb.coef, vlb.constant);
    });

    // todo also add old implications once implications can be added
    // incrementally for now we discard the old implications as they might be
    // weaker then newly computed ones and adding them would block computation
    // of new implications
  }
}

void HighsImplications::separateImpliedBounds(
    const HighsLpRelaxation& lpRelaxation, const std::vector<double>& sol,
    HighsCutPool& cutpool, double feastol) {
  HighsDomain& globaldomain = mipsolver.mipdata_->domain;

  std::array<HighsInt, 2> inds;
  std::array<double, 2> vals;
  double rhs;

  HighsInt numboundchgs = 0;

  // first do probing on all candidates that have not been probed yet
  if (!mipsolver.mipdata_->cliquetable.isFull()) {
    auto oldNumQueries =
        mipsolver.mipdata_->cliquetable.numNeighbourhoodQueries;
    HighsInt oldNumEntries = mipsolver.mipdata_->cliquetable.getNumEntries();

    for (std::pair<HighsInt, double> fracint :
         lpRelaxation.getFractionalIntegers()) {
      HighsInt col = fracint.first;
      if (globaldomain.col_lower_[col] != 0.0 ||
          globaldomain.col_upper_[col] != 1.0 ||
          (implicationsCached(col, 0) && implicationsCached(col, 1)))
        continue;

      mipsolver.analysis_.mipTimerStart(kMipClockProbingImplications);
      const bool probing_result = runProbing(col, numboundchgs);
      mipsolver.analysis_.mipTimerStop(kMipClockProbingImplications);
      if (probing_result) {
        if (globaldomain.infeasible()) return;
      }

      if (mipsolver.mipdata_->cliquetable.isFull()) break;
    }

    // if (!mipsolver.submip)
    //   printf("numEntries: %d, beforeProbing: %d\n",
    //          mipsolver.mipdata_->cliquetable.getNumEntries(), oldNumEntries);
    HighsInt numNewEntries =
        mipsolver.mipdata_->cliquetable.getNumEntries() - oldNumEntries;

    nextCleanupCall -= std::max(HighsInt{0}, numNewEntries);

    if (nextCleanupCall < 0) {
      // HighsInt oldNumEntries =
      // mipsolver.mipdata_->cliquetable.getNumEntries();
      mipsolver.mipdata_->cliquetable.runCliqueMerging(globaldomain);
      // printf("numEntries: %d, beforeMerging: %d\n",
      //        mipsolver.mipdata_->cliquetable.getNumEntries(), oldNumEntries);
      nextCleanupCall =
          std::min(mipsolver.mipdata_->numCliqueEntriesAfterFirstPresolve,
                   mipsolver.mipdata_->cliquetable.getNumEntries());
      // printf("nextCleanupCall: %d\n", nextCleanupCall);
    }

    mipsolver.mipdata_->cliquetable.numNeighbourhoodQueries = oldNumQueries;
  }

  for (std::pair<HighsInt, double> fracint :
       lpRelaxation.getFractionalIntegers()) {
    HighsInt col = fracint.first;
    // skip non binary variables
    if (globaldomain.col_lower_[col] != 0.0 ||
        globaldomain.col_upper_[col] != 1.0)
      continue;

    bool infeas;
    if (implicationsCached(col, 1)) {
      const std::vector<HighsDomainChange>& implics =
          getImplications(col, 1, infeas);
      if (globaldomain.infeasible()) return;

      if (infeas) {
        vals[0] = 1.0;
        inds[0] = col;
        cutpool.addCut(mipsolver, inds.data(), vals.data(), 1, 0.0, false, true,
                       false);
        continue;
      }

      HighsInt nimplics = implics.size();
      for (HighsInt i = 0; i < nimplics; ++i) {
        if (implics[i].boundtype == HighsBoundType::kUpper) {
          if (implics[i].boundval + feastol >=
              globaldomain.col_upper_[implics[i].column])
            continue;

          vals[0] = 1.0;
          inds[0] = implics[i].column;
          vals[1] =
              globaldomain.col_upper_[implics[i].column] - implics[i].boundval;
          inds[1] = col;
          rhs = globaldomain.col_upper_[implics[i].column];

        } else {
          if (implics[i].boundval - feastol <=
              globaldomain.col_lower_[implics[i].column])
            continue;

          vals[0] = -1.0;
          inds[0] = implics[i].column;
          vals[1] =
              globaldomain.col_lower_[implics[i].column] - implics[i].boundval;
          inds[1] = col;
          rhs = -globaldomain.col_lower_[implics[i].column];
        }

        double viol = sol[inds[0]] * vals[0] + sol[inds[1]] * vals[1] - rhs;

        if (viol > feastol) {
          // printf("added implied bound cut to pool\n");
          cutpool.addCut(mipsolver, inds.data(), vals.data(), 2, rhs,
                         mipsolver.variableType(implics[i].column) !=
                             HighsVarType::kContinuous,
                         false, false, false);
        }
      }
    }

    if (implicationsCached(col, 0)) {
      const std::vector<HighsDomainChange>& implics =
          getImplications(col, 0, infeas);
      if (globaldomain.infeasible()) return;

      if (infeas) {
        vals[0] = -1.0;
        inds[0] = col;
        cutpool.addCut(mipsolver, inds.data(), vals.data(), 1, -1.0, false,
                       true, false);
        continue;
      }

      HighsInt nimplics = implics.size();
      for (HighsInt i = 0; i < nimplics; ++i) {
        if (implics[i].boundtype == HighsBoundType::kUpper) {
          if (implics[i].boundval + feastol >=
              globaldomain.col_upper_[implics[i].column])
            continue;

          vals[0] = 1.0;
          inds[0] = implics[i].column;
          vals[1] =
              implics[i].boundval - globaldomain.col_upper_[implics[i].column];
          inds[1] = col;
          rhs = implics[i].boundval;
        } else {
          if (implics[i].boundval - feastol <=
              globaldomain.col_lower_[implics[i].column])
            continue;

          vals[0] = -1.0;
          inds[0] = implics[i].column;
          vals[1] =
              globaldomain.col_lower_[implics[i].column] - implics[i].boundval;
          inds[1] = col;
          rhs = -implics[i].boundval;
        }

        double viol = sol[inds[0]] * vals[0] + sol[inds[1]] * vals[1] - rhs;

        if (viol > feastol) {
          // printf("added implied bound cut to pool\n");
          cutpool.addCut(mipsolver, inds.data(), vals.data(), 2, rhs,
                         mipsolver.variableType(implics[i].column) !=
                             HighsVarType::kContinuous,
                         false, false, false);
        }
      }
    }
  }
}

void HighsImplications::cleanupVarbounds(HighsInt col) {
  double ub = mipsolver.mipdata_->domain.col_upper_[col];
  double lb = mipsolver.mipdata_->domain.col_lower_[col];

  if (ub == lb) {
    vlbs[col].clear();
    vubs[col].clear();
    return;
  }

  std::vector<HighsInt> delVbds;

  vubs[col].for_each([&](HighsInt vubCol, VarBound& vub) {
    mipsolver.mipdata_->debugSolution.checkVub(col, vubCol, vub.coef,
                                               vub.constant);

    if (vub.coef > 0) {
      double minub = vub.constant;
      double maxub = vub.constant + vub.coef;
      if (minub >= ub - mipsolver.mipdata_->feastol)
        delVbds.push_back(vubCol);  // variable bound is redundant
      else if (maxub > ub + mipsolver.mipdata_->epsilon) {
        vub.coef = ub - vub.constant;  // coefficient can be tightened
        mipsolver.mipdata_->debugSolution.checkVub(col, vubCol, vub.coef,
                                                   vub.constant);
      } else if (maxub < ub - mipsolver.mipdata_->epsilon) {
        mipsolver.mipdata_->domain.changeBound(
            HighsBoundType::kUpper, col, maxub,
            HighsDomain::Reason::unspecified());
        if (mipsolver.mipdata_->domain.infeasible()) return;
      }
    } else {
      HighsCDouble minub = HighsCDouble(vub.constant) + vub.coef;
      double maxub = vub.constant;
      if (minub >= ub - mipsolver.mipdata_->feastol)
        delVbds.push_back(vubCol);  // variable bound is redundant
      else if (maxub > ub + mipsolver.mipdata_->epsilon) {
        // variable bound can be tightened
        vub.constant = ub;
        vub.coef = double(minub - ub);
        mipsolver.mipdata_->debugSolution.checkVub(col, vubCol, vub.coef,
                                                   vub.constant);
      } else if (maxub < ub - mipsolver.mipdata_->epsilon) {
        mipsolver.mipdata_->domain.changeBound(
            HighsBoundType::kUpper, col, maxub,
            HighsDomain::Reason::unspecified());
        if (mipsolver.mipdata_->domain.infeasible()) return;
      }
    }
  });

  if (!delVbds.empty()) {
    for (HighsInt vubCol : delVbds) vubs[col].erase(vubCol);
    delVbds.clear();
  }

  vlbs[col].for_each([&](HighsInt vlbCol, VarBound& vlb) {
    mipsolver.mipdata_->debugSolution.checkVlb(col, vlbCol, vlb.coef,
                                               vlb.constant);

    if (vlb.coef > 0) {
      HighsCDouble maxlb = HighsCDouble(vlb.constant) + vlb.coef;
      double minlb = vlb.constant;
      if (maxlb <= lb + mipsolver.mipdata_->feastol)
        delVbds.push_back(vlbCol);  // variable bound is redundant
      else if (minlb < lb - mipsolver.mipdata_->epsilon) {
        // variable bound can be tightened
        vlb.constant = lb;
        vlb.coef = double(maxlb - lb);
        mipsolver.mipdata_->debugSolution.checkVlb(col, vlbCol, vlb.coef,
                                                   vlb.constant);
      } else if (minlb > lb + mipsolver.mipdata_->epsilon) {
        mipsolver.mipdata_->domain.changeBound(
            HighsBoundType::kLower, col, minlb,
            HighsDomain::Reason::unspecified());
        if (mipsolver.mipdata_->domain.infeasible()) return;
      }

    } else {
      double maxlb = vlb.constant;
      double minlb = vlb.constant + vlb.coef;
      if (maxlb <= lb + mipsolver.mipdata_->feastol)
        delVbds.push_back(vlbCol);  // variable bound is redundant
      else if (minlb < lb - mipsolver.mipdata_->epsilon) {
        vlb.coef = lb - vlb.constant;  // variable bound can be tightened
        mipsolver.mipdata_->debugSolution.checkVlb(col, vlbCol, vlb.coef,
                                                   vlb.constant);
      } else if (minlb > lb + mipsolver.mipdata_->epsilon) {
        mipsolver.mipdata_->domain.changeBound(
            HighsBoundType::kLower, col, minlb,
            HighsDomain::Reason::unspecified());
        if (mipsolver.mipdata_->domain.infeasible()) return;
      }
    }
  });

  for (HighsInt vlbCol : delVbds) vlbs[col].erase(vlbCol);
}
