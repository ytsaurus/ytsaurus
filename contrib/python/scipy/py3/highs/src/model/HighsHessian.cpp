/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
/*                                                                       */
/*    This file is part of the HiGHS linear optimization suite           */
/*                                                                       */
/*    Written and engineered 2008-2021 at the University of Edinburgh    */
/*                                                                       */
/*    Available as open-source under the MIT License                     */
/*                                                                       */
/*    Authors: Julian Hall, Ivet Galabova, Qi Huangfu, Leona Gottwald    */
/*    and Michael Feldmeier                                              */
/*                                                                       */
/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */
/**@file lp_data/HighsHessian.cpp
 * @brief
 */
#include "model/HighsHessian.h"

#include <cassert>
#include <cstdio>

void HighsHessian::clear() {
  this->dim_ = 0;
  this->start_.clear();
  this->index_.clear();
  this->value_.clear();
  this->format_ = HessianFormat::kTriangular;
  this->start_.assign(1, 0);
}

void HighsHessian::exactResize() {
  if (this->dim_) {
    this->start_.resize(this->dim_ + 1);
    HighsInt num_nz = this->start_[this->dim_];
    this->index_.resize(num_nz);
    this->value_.resize(num_nz);
  } else {
    this->start_.clear();
    this->index_.clear();
    this->value_.clear();
  }
}

void HighsHessian::deleteCols(const HighsIndexCollection& index_collection) {
  if (this->dim_ == 0) return;
  // Can't handle non-triangular matrices yet
  assert(this->format_ == HessianFormat::kTriangular);
  assert(ok(index_collection));
  HighsInt from_k;
  HighsInt to_k;
  limits(index_collection, from_k, to_k);
  if (from_k > to_k) return;
  HighsInt delete_from_col;
  HighsInt delete_to_col;
  HighsInt keep_from_col;
  HighsInt keep_to_col = -1;
  HighsInt current_set_entry = 0;

  // Initial pass creates a look-up to for the new index of columns
  // being retained, and -1 for columns being deleted
  std::vector<HighsInt> new_index;
  new_index.assign(this->dim_, -1);
  HighsInt new_dim = 0;
  for (HighsInt k = from_k; k <= to_k; k++) {
    updateOutInIndex(index_collection, delete_from_col, delete_to_col,
                     keep_from_col, keep_to_col, current_set_entry);
    if (k == from_k) {
      // Account for the initial columns being kept
      for (HighsInt iCol = 0; iCol < delete_from_col; iCol++)
        new_index[iCol] = new_dim++;
    }
    for (HighsInt iCol = keep_from_col; iCol <= keep_to_col; iCol++)
      new_index[iCol] = new_dim++;
    // When using a mask, to_k = this->dim_, but consecutive
    // keep/delete entries are accumulated, so may not need all passes
    if (keep_to_col >= this->dim_ - 1) break;
  }
  assert(new_dim < this->dim_);
  // Now perform the pass that deletes rows/columns
  keep_to_col = -1;
  current_set_entry = 0;
  // Have to accumulate new number of entries from the outset, as
  // entries may be lost from columns being kept before any are
  // deleted. Also keep a count of the number of nonzeros, in case the new
  HighsInt check_new_dim = new_dim;
  new_dim = 0;
  HighsInt new_num_nz = 0;
  HighsInt new_num_entries = 0;
  std::vector<HighsInt> save_start = this->start_;
  for (HighsInt k = from_k; k <= to_k; k++) {
    updateOutInIndex(index_collection, delete_from_col, delete_to_col,
                     keep_from_col, keep_to_col, current_set_entry);
    if (k == from_k) {
      // Account for the initial columns being kept
      for (HighsInt iCol = 0; iCol < delete_from_col; iCol++) {
        assert(new_index[iCol] >= 0);
        for (HighsInt iEl = save_start[iCol]; iEl < save_start[iCol + 1];
             iEl++) {
          HighsInt iRow = new_index[this->index_[iEl]];
          if (iRow < 0) continue;
          this->index_[new_num_entries] = iRow;
          this->value_[new_num_entries] = this->value_[iEl];
          if (this->value_[new_num_entries]) new_num_nz++;
          new_num_entries++;
        }
        new_dim++;
        this->start_[new_dim] = new_num_entries;
      }
      assert(new_dim == delete_from_col);
    }
    for (HighsInt iCol = keep_from_col; iCol <= keep_to_col; iCol++) {
      assert(new_index[iCol] >= 0);
      for (HighsInt iEl = save_start[iCol]; iEl < save_start[iCol + 1]; iEl++) {
        HighsInt iRow = new_index[this->index_[iEl]];
        if (iRow < 0) continue;
        this->index_[new_num_entries] = iRow;
        this->value_[new_num_entries] = this->value_[iEl];
        if (this->value_[new_num_entries]) new_num_nz++;
        new_num_entries++;
      }
      new_dim++;
      this->start_[new_dim] = new_num_entries;
    }
    if (keep_to_col >= this->dim_ - 1) break;
  }
  assert(new_dim == check_new_dim);
  this->dim_ = new_dim;
  if (!new_num_nz) {
    this->clear();
  } else {
    this->exactResize();
  }
}

bool HighsHessian::scaleOk(const HighsInt hessian_scale,
                           const double small_matrix_value,
                           const double large_matrix_value) const {
  if (!this->dim_) return true;
  double hessian_scale_value = std::pow(2, hessian_scale);
  for (HighsInt iEl = 0; iEl < this->start_[this->dim_]; iEl++) {
    double abs_new_value = std::abs(this->value_[iEl] * hessian_scale_value);
    if (abs_new_value >= large_matrix_value) return false;
    if (abs_new_value <= small_matrix_value) return false;
  }
  return true;
}

HighsInt HighsHessian::numNz() const {
  assert(this->formatOk());
  assert((HighsInt)this->start_.size() >= this->dim_ + 1);
  return this->start_[this->dim_];
}

void HighsHessian::print() const {
  HighsInt num_nz = this->numNz();
  printf("Hessian of dimension %" HIGHSINT_FORMAT " and %" HIGHSINT_FORMAT
         " entries\n",
         dim_, num_nz);
  printf("Start; Index; Value of sizes %d; %d; %d\n", (int)this->start_.size(),
         (int)this->index_.size(), (int)this->value_.size());
  if (dim_ <= 0) return;
  printf(" Row|");
  for (int iCol = 0; iCol < dim_; iCol++) printf(" %4d", iCol);
  printf("\n");
  printf("-----");
  for (int iCol = 0; iCol < dim_; iCol++) printf("-----");
  printf("\n");
  std::vector<double> col;
  col.assign(dim_, 0);
  for (HighsInt iCol = 0; iCol < dim_; iCol++) {
    for (HighsInt iEl = this->start_[iCol]; iEl < this->start_[iCol + 1]; iEl++)
      col[this->index_[iEl]] = this->value_[iEl];
    printf("%4d|", (int)iCol);
    for (int iRow = 0; iRow < dim_; iRow++) printf(" %4g", col[iRow]);
    printf("\n");
    for (HighsInt iEl = this->start_[iCol]; iEl < this->start_[iCol + 1]; iEl++)
      col[this->index_[iEl]] = 0;
  }
}
bool HighsHessian::operator==(const HighsHessian& hessian) const {
  bool equal = true;
  equal = this->dim_ == hessian.dim_ && equal;
  equal = this->start_ == hessian.start_ && equal;
  equal = this->index_ == hessian.index_ && equal;
  equal = this->value_ == hessian.value_ && equal;
  return equal;
}

void HighsHessian::product(const std::vector<double>& solution,
                           std::vector<double>& product) const {
  if (this->dim_ <= 0) return;
  product.assign(this->dim_, 0);
  for (HighsInt iCol = 0; iCol < this->dim_; iCol++) {
    for (HighsInt iEl = this->start_[iCol]; iEl < this->start_[iCol + 1];
         iEl++) {
      const HighsInt iRow = this->index_[iEl];
      product[iRow] += this->value_[iEl] * solution[iCol];
    }
  }
}

double HighsHessian::objectiveValue(const std::vector<double>& solution) const {
  double objective_function_value = 0;
  for (HighsInt iCol = 0; iCol < this->dim_; iCol++) {
    HighsInt iEl = this->start_[iCol];
    assert(this->index_[iEl] == iCol);
    objective_function_value +=
        0.5 * solution[iCol] * this->value_[iEl] * solution[iCol];
    for (HighsInt iEl = this->start_[iCol] + 1; iEl < this->start_[iCol + 1];
         iEl++)
      objective_function_value +=
          solution[iCol] * this->value_[iEl] * solution[this->index_[iEl]];
  }
  return objective_function_value;
}

HighsCDouble HighsHessian::objectiveCDoubleValue(
    const std::vector<double>& solution) const {
  HighsCDouble objective_function_value = HighsCDouble(0);
  for (HighsInt iCol = 0; iCol < this->dim_; iCol++) {
    HighsInt iEl = this->start_[iCol];
    assert(this->index_[iEl] == iCol);
    objective_function_value +=
        0.5 * solution[iCol] * this->value_[iEl] * solution[iCol];
    for (HighsInt iEl = this->start_[iCol] + 1; iEl < this->start_[iCol + 1];
         iEl++)
      objective_function_value +=
          solution[iCol] * this->value_[iEl] * solution[this->index_[iEl]];
  }
  return objective_function_value;
}
