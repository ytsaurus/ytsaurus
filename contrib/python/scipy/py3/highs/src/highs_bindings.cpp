#define PYBIND11_DETAILED_ERROR_MESSAGES 1
#include <pybind11/functional.h>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <cassert>

#include "Highs.h"
#include "lp_data/HighsCallback.h"

namespace py = pybind11;
using namespace pybind11::literals;

// arrays are assumed to be contiguous c-style arrays of correct type
// * c_style forces the array to be stored in C-style contiguous order
// * forcecast converts the array to the correct type if needed
template <typename T>
using dense_array_t = py::array_t<T, py::array::c_style | py::array::forcecast>;

// wrapper for raw pointers
template <class T>
class readonly_ptr_wrapper {
 public:
  readonly_ptr_wrapper() : ptr(nullptr) {}
  readonly_ptr_wrapper(T* ptr) : ptr(ptr) {}
  readonly_ptr_wrapper(const readonly_ptr_wrapper& other) : ptr(other.ptr) {}
  T& operator*() const { return *ptr; }
  T* operator->() const { return ptr; }
  T* get() const { return ptr; }
  T& operator[](std::size_t idx) const { return ptr[idx]; }
  bool is_valid() { return ptr != nullptr; }

  py::array_t<T, py::array::c_style> to_array(std::size_t size) {
    return py::array_t<T, py::array::c_style>(py::buffer_info(
        ptr, sizeof(T), py::format_descriptor<T>::format(), 1, {size}, {1}));
  }

 private:
  T* ptr;
};

HighsStatus highs_passModel(Highs* h, HighsModel& model) {
  return h->passModel(model);
}

HighsStatus highs_passModelPointers(
    Highs* h, const HighsInt num_col, const HighsInt num_row,
    const HighsInt num_nz, const HighsInt q_num_nz, const HighsInt a_format,
    const HighsInt q_format, const HighsInt sense, const double offset,
    const dense_array_t<double> col_cost, const dense_array_t<double> col_lower,
    const dense_array_t<double> col_upper,
    const dense_array_t<double> row_lower,
    const dense_array_t<double> row_upper,
    const dense_array_t<HighsInt> a_start,
    const dense_array_t<HighsInt> a_index, const dense_array_t<double> a_value,
    const dense_array_t<HighsInt> q_start,
    const dense_array_t<HighsInt> q_index, const dense_array_t<double> q_value,
    const dense_array_t<HighsInt> integrality) {
  py::buffer_info col_cost_info = col_cost.request();
  py::buffer_info col_lower_info = col_lower.request();
  py::buffer_info col_upper_info = col_upper.request();
  py::buffer_info row_lower_info = row_lower.request();
  py::buffer_info row_upper_info = row_upper.request();
  py::buffer_info a_start_info = a_start.request();
  py::buffer_info a_index_info = a_index.request();
  py::buffer_info a_value_info = a_value.request();
  py::buffer_info q_start_info = q_start.request();
  py::buffer_info q_index_info = q_index.request();
  py::buffer_info q_value_info = q_value.request();
  py::buffer_info integrality_info = integrality.request();

  const double* col_cost_ptr = static_cast<double*>(col_cost_info.ptr);
  const double* col_lower_ptr = static_cast<double*>(col_lower_info.ptr);
  const double* col_upper_ptr = static_cast<double*>(col_upper_info.ptr);
  const double* row_lower_ptr = static_cast<double*>(row_lower_info.ptr);
  const double* row_upper_ptr = static_cast<double*>(row_upper_info.ptr);
  const double* a_value_ptr = static_cast<double*>(a_value_info.ptr);
  const double* q_value_ptr = static_cast<double*>(q_value_info.ptr);
  const HighsInt* a_start_ptr = static_cast<HighsInt*>(a_start_info.ptr);
  const HighsInt* a_index_ptr = static_cast<HighsInt*>(a_index_info.ptr);
  const HighsInt* q_start_ptr = static_cast<HighsInt*>(q_start_info.ptr);
  const HighsInt* q_index_ptr = static_cast<HighsInt*>(q_index_info.ptr);
  const HighsInt* integrality_ptr =
      static_cast<HighsInt*>(integrality_info.ptr);

  return h->passModel(
      static_cast<HighsInt>(num_col), static_cast<HighsInt>(num_row),
      static_cast<HighsInt>(num_nz), static_cast<HighsInt>(q_num_nz),
      static_cast<HighsInt>(a_format), static_cast<HighsInt>(q_format),
      static_cast<HighsInt>(sense), offset, col_cost_ptr, col_lower_ptr,
      col_upper_ptr, row_lower_ptr, row_upper_ptr, a_start_ptr, a_index_ptr,
      a_value_ptr, q_start_ptr, q_index_ptr, q_value_ptr, integrality_ptr);
}

HighsStatus highs_passLp(Highs* h, HighsLp& lp) { return h->passModel(lp); }

HighsStatus highs_passLpPointers(Highs* h, const HighsInt num_col,
                                 const HighsInt num_row, const HighsInt num_nz,
                                 const HighsInt a_format, const HighsInt sense,
                                 const double offset,
                                 const dense_array_t<double> col_cost,
                                 const dense_array_t<double> col_lower,
                                 const dense_array_t<double> col_upper,
                                 const dense_array_t<double> row_lower,
                                 const dense_array_t<double> row_upper,
                                 const dense_array_t<HighsInt> a_start,
                                 const dense_array_t<HighsInt> a_index,
                                 const dense_array_t<double> a_value,
                                 const dense_array_t<HighsInt> integrality) {
  py::buffer_info col_cost_info = col_cost.request();
  py::buffer_info col_lower_info = col_lower.request();
  py::buffer_info col_upper_info = col_upper.request();
  py::buffer_info row_lower_info = row_lower.request();
  py::buffer_info row_upper_info = row_upper.request();
  py::buffer_info a_start_info = a_start.request();
  py::buffer_info a_index_info = a_index.request();
  py::buffer_info a_value_info = a_value.request();
  py::buffer_info integrality_info = integrality.request();

  const double* col_cost_ptr = static_cast<double*>(col_cost_info.ptr);
  const double* col_lower_ptr = static_cast<double*>(col_lower_info.ptr);
  const double* col_upper_ptr = static_cast<double*>(col_upper_info.ptr);
  const double* row_lower_ptr = static_cast<double*>(row_lower_info.ptr);
  const double* row_upper_ptr = static_cast<double*>(row_upper_info.ptr);
  const HighsInt* a_start_ptr = static_cast<HighsInt*>(a_start_info.ptr);
  const HighsInt* a_index_ptr = static_cast<HighsInt*>(a_index_info.ptr);
  const double* a_value_ptr = static_cast<double*>(a_value_info.ptr);
  const HighsInt* integrality_ptr =
      static_cast<HighsInt*>(integrality_info.ptr);

  return h->passModel(
      static_cast<HighsInt>(num_col), static_cast<HighsInt>(num_row),
      static_cast<HighsInt>(num_nz), static_cast<HighsInt>(a_format),
      static_cast<HighsInt>(sense), offset, col_cost_ptr, col_lower_ptr,
      col_upper_ptr, row_lower_ptr, row_upper_ptr, a_start_ptr, a_index_ptr,
      a_value_ptr, integrality_ptr);
}

HighsStatus highs_passHessian(Highs* h, HighsHessian& hessian) {
  return h->passHessian(hessian);
}

HighsStatus highs_passHessianPointers(Highs* h, const HighsInt dim,
                                      const HighsInt num_nz,
                                      const HighsInt format,
                                      const dense_array_t<HighsInt> q_start,
                                      const dense_array_t<HighsInt> q_index,
                                      const dense_array_t<double> q_value) {
  py::buffer_info q_start_info = q_start.request();
  py::buffer_info q_index_info = q_index.request();
  py::buffer_info q_value_info = q_value.request();

  const HighsInt* q_start_ptr = static_cast<HighsInt*>(q_start_info.ptr);
  const HighsInt* q_index_ptr = static_cast<HighsInt*>(q_index_info.ptr);
  const double* q_value_ptr = static_cast<double*>(q_value_info.ptr);

  return h->passHessian(dim, num_nz, format, q_start_ptr, q_index_ptr,
                        q_value_ptr);
}

HighsStatus highs_postsolve(Highs* h, const HighsSolution& solution,
                            const HighsBasis& basis) {
  return h->postsolve(solution, basis);
}

HighsStatus highs_mipPostsolve(Highs* h, const HighsSolution& solution) {
  return h->postsolve(solution);
}

HighsStatus highs_writeSolution(Highs* h, const std::string filename,
                                const HighsInt style) {
  return h->writeSolution(filename, style);
}

// Not needed once getModelStatus(const bool scaled_model) disappears
// from, Highs.h
HighsModelStatus highs_getModelStatus(Highs* h) { return h->getModelStatus(); }

std::tuple<HighsStatus, HighsRanging> highs_getRanging(Highs* h) {
  HighsRanging ranging;
  HighsStatus status = h->getRanging(ranging);
  return std::make_tuple(status, ranging);
}

HighsStatus highs_addRow(Highs* h, double lower, double upper,
                         HighsInt num_new_nz, dense_array_t<HighsInt> indices,
                         dense_array_t<double> values) {
  py::buffer_info indices_info = indices.request();
  py::buffer_info values_info = values.request();

  HighsInt* indices_ptr = reinterpret_cast<HighsInt*>(indices_info.ptr);
  double* values_ptr = static_cast<double*>(values_info.ptr);

  return h->addRow(lower, upper, num_new_nz, indices_ptr, values_ptr);
}

HighsStatus highs_addRows(Highs* h, HighsInt num_row,
                          dense_array_t<double> lower,
                          dense_array_t<double> upper, HighsInt num_new_nz,
                          dense_array_t<HighsInt> starts,
                          dense_array_t<HighsInt> indices,
                          dense_array_t<double> values) {
  py::buffer_info lower_info = lower.request();
  py::buffer_info upper_info = upper.request();
  py::buffer_info starts_info = starts.request();
  py::buffer_info indices_info = indices.request();
  py::buffer_info values_info = values.request();

  double* lower_ptr = static_cast<double*>(lower_info.ptr);
  double* upper_ptr = static_cast<double*>(upper_info.ptr);
  HighsInt* starts_ptr = reinterpret_cast<HighsInt*>(starts_info.ptr);
  HighsInt* indices_ptr = reinterpret_cast<HighsInt*>(indices_info.ptr);
  double* values_ptr = static_cast<double*>(values_info.ptr);

  return h->addRows(num_row, lower_ptr, upper_ptr, num_new_nz, starts_ptr,
                    indices_ptr, values_ptr);
}

HighsStatus highs_addCol(Highs* h, double cost, double lower, double upper,
                         HighsInt num_new_nz, dense_array_t<HighsInt> indices,
                         dense_array_t<double> values) {
  py::buffer_info indices_info = indices.request();
  py::buffer_info values_info = values.request();

  HighsInt* indices_ptr = reinterpret_cast<HighsInt*>(indices_info.ptr);
  double* values_ptr = static_cast<double*>(values_info.ptr);

  return h->addCol(cost, lower, upper, num_new_nz, indices_ptr, values_ptr);
}

HighsStatus highs_addCols(Highs* h, HighsInt num_col,
                          dense_array_t<double> cost,
                          dense_array_t<double> lower,
                          dense_array_t<double> upper, HighsInt num_new_nz,
                          dense_array_t<HighsInt> starts,
                          dense_array_t<HighsInt> indices,
                          dense_array_t<double> values) {
  py::buffer_info cost_info = cost.request();
  py::buffer_info lower_info = lower.request();
  py::buffer_info upper_info = upper.request();
  py::buffer_info starts_info = starts.request();
  py::buffer_info indices_info = indices.request();
  py::buffer_info values_info = values.request();

  double* cost_ptr = static_cast<double*>(cost_info.ptr);
  double* lower_ptr = static_cast<double*>(lower_info.ptr);
  double* upper_ptr = static_cast<double*>(upper_info.ptr);
  HighsInt* starts_ptr = reinterpret_cast<HighsInt*>(starts_info.ptr);
  const HighsInt* indices_ptr = reinterpret_cast<HighsInt*>(indices_info.ptr);
  double* values_ptr = static_cast<double*>(values_info.ptr);

  return h->addCols(num_col, cost_ptr, lower_ptr, upper_ptr, num_new_nz,
                    starts_ptr, indices_ptr, values_ptr);
}

HighsStatus highs_addVar(Highs* h, double lower, double upper) {
  return h->addVar(lower, upper);
}

HighsStatus highs_addVars(Highs* h, HighsInt num_vars,
                          dense_array_t<double> lower,
                          dense_array_t<double> upper) {
  py::buffer_info lower_info = lower.request();
  py::buffer_info upper_info = upper.request();

  double* lower_ptr = static_cast<double*>(lower_info.ptr);
  double* upper_ptr = static_cast<double*>(upper_info.ptr);

  return h->addVars(num_vars, lower_ptr, upper_ptr);
}

HighsStatus highs_changeColsCost(Highs* h, HighsInt num_set_entries,
                                 dense_array_t<HighsInt> indices,
                                 dense_array_t<double> cost) {
  py::buffer_info indices_info = indices.request();
  py::buffer_info cost_info = cost.request();

  HighsInt* indices_ptr = static_cast<HighsInt*>(indices_info.ptr);
  double* cost_ptr = static_cast<double*>(cost_info.ptr);

  return h->changeColsCost(num_set_entries, indices_ptr, cost_ptr);
}

HighsStatus highs_changeColsBounds(Highs* h, HighsInt num_set_entries,
                                   dense_array_t<HighsInt> indices,
                                   dense_array_t<double> lower,
                                   dense_array_t<double> upper) {
  py::buffer_info indices_info = indices.request();
  py::buffer_info lower_info = lower.request();
  py::buffer_info upper_info = upper.request();

  HighsInt* indices_ptr = static_cast<HighsInt*>(indices_info.ptr);
  double* lower_ptr = static_cast<double*>(lower_info.ptr);
  double* upper_ptr = static_cast<double*>(upper_info.ptr);

  return h->changeColsBounds(num_set_entries, indices_ptr, lower_ptr,
                             upper_ptr);
}

HighsStatus highs_changeColsIntegrality(
    Highs* h, HighsInt num_set_entries, dense_array_t<HighsInt> indices,
    dense_array_t<HighsVarType> integrality) {
  py::buffer_info indices_info = indices.request();
  py::buffer_info integrality_info = integrality.request();

  HighsInt* indices_ptr = static_cast<HighsInt*>(indices_info.ptr);
  HighsVarType* integrality_ptr =
      static_cast<HighsVarType*>(integrality_info.ptr);

  return h->changeColsIntegrality(num_set_entries, indices_ptr,
                                  integrality_ptr);
}

// Same as deleteVars
HighsStatus highs_deleteCols(Highs* h, HighsInt num_set_entries,
                             dense_array_t<HighsInt> indices) {
  py::buffer_info index_info = indices.request();
  HighsInt* index_ptr = reinterpret_cast<HighsInt*>(index_info.ptr);
  return h->deleteCols(num_set_entries, index_ptr);
}

HighsStatus highs_deleteRows(Highs* h, HighsInt num_set_entries,
                             dense_array_t<HighsInt> indices) {
  py::buffer_info index_info = indices.request();
  HighsInt* index_ptr = reinterpret_cast<HighsInt*>(index_info.ptr);
  return h->deleteRows(num_set_entries, index_ptr);
}

HighsStatus highs_setSolution(Highs* h, HighsSolution& solution) {
  return h->setSolution(solution);
}

HighsStatus highs_setSparseSolution(Highs* h, HighsInt num_entries,
                                    dense_array_t<HighsInt> index,
                                    dense_array_t<double> value) {
  py::buffer_info index_info = index.request();
  py::buffer_info value_info = value.request();

  HighsInt* index_ptr = reinterpret_cast<HighsInt*>(index_info.ptr);
  double* value_ptr = static_cast<double*>(value_info.ptr);

  return h->setSolution(num_entries, index_ptr, value_ptr);
}

HighsStatus highs_setBasis(Highs* h, HighsBasis& basis) {
  return h->setBasis(basis);
}

HighsStatus highs_setLogicalBasis(Highs* h) { return h->setBasis(); }

std::tuple<HighsStatus, py::object> highs_getOptionValue(
    Highs* h, const std::string& option) {
  HighsOptionType option_type;
  HighsStatus status = h->getOptionType(option, option_type);

  if (status != HighsStatus::kOk) return std::make_tuple(status, py::cast(0));

  if (option_type == HighsOptionType::kBool) {
    bool value;
    status = h->getOptionValue(option, value);
    return std::make_tuple(status, py::cast(value));
  } else if (option_type == HighsOptionType::kInt) {
    HighsInt value;
    status = h->getOptionValue(option, value);
    return std::make_tuple(status, py::cast(value));
  } else if (option_type == HighsOptionType::kDouble) {
    double value;
    status = h->getOptionValue(option, value);
    return std::make_tuple(status, py::cast(value));
  } else if (option_type == HighsOptionType::kString) {
    std::string value;
    status = h->getOptionValue(option, value);
    return std::make_tuple(status, py::cast(value));
  } else
    return std::make_tuple(HighsStatus::kError, py::cast(0));
}

std::tuple<HighsStatus, HighsOptionType> highs_getOptionType(
    Highs* h, const std::string& option) {
  HighsOptionType option_type;
  HighsStatus status = h->getOptionType(option, option_type);
  return std::make_tuple(status, option_type);
}

HighsStatus highs_writeOptions(Highs* h, const std::string& filename) {
  return h->writeOptions(filename);
}

std::tuple<HighsStatus, py::object> highs_getInfoValue(
    Highs* h, const std::string& info) {
  HighsInfoType info_type;
  HighsStatus status = h->getInfoType(info, info_type);

  if (status != HighsStatus::kOk) return std::make_tuple(status, py::cast(0));

  if (info_type == HighsInfoType::kInt64) {
    int64_t value;
    status = h->getInfoValue(info, value);
    return std::make_tuple(status, py::cast(value));
  } else if (info_type == HighsInfoType::kInt) {
    HighsInt value;
    status = h->getInfoValue(info, value);
    return std::make_tuple(status, py::cast(value));
  } else if (info_type == HighsInfoType::kDouble) {
    double value;
    status = h->getInfoValue(info, value);
    return std::make_tuple(status, py::cast(value));
  } else
    return std::make_tuple(HighsStatus::kError, py::cast(0));
}

std::tuple<HighsStatus, HighsInfoType> highs_getInfoType(
    Highs* h, const std::string& info) {
  HighsInfoType info_type;
  HighsStatus status = h->getInfoType(info, info_type);
  return std::make_tuple(status, info_type);
}

std::tuple<HighsStatus, ObjSense> highs_getObjectiveSense(Highs* h) {
  ObjSense obj_sense;
  HighsStatus status = h->getObjectiveSense(obj_sense);
  return std::make_tuple(status, obj_sense);
}

std::tuple<HighsStatus, double> highs_getObjectiveOffset(Highs* h) {
  double obj_offset;
  HighsStatus status = h->getObjectiveOffset(obj_offset);
  return std::make_tuple(status, obj_offset);
}

std::tuple<HighsStatus, double, double, double, HighsInt> highs_getCol(
    Highs* h, HighsInt col) {
  double cost, lower, upper;
  HighsInt get_num_col;
  HighsInt get_num_nz;
  HighsInt col_ = static_cast<HighsInt>(col);
  HighsStatus status = h->getCols(1, &col_, get_num_col, &cost, &lower, &upper,
                                  get_num_nz, nullptr, nullptr, nullptr);
  return std::make_tuple(status, cost, lower, upper, get_num_nz);
}

std::tuple<HighsStatus, dense_array_t<HighsInt>, dense_array_t<double>>
highs_getColEntries(Highs* h, HighsInt col) {
  HighsInt get_num_col;
  HighsInt get_num_nz;
  HighsInt col_ = static_cast<HighsInt>(col);
  h->getCols(1, &col_, get_num_col, nullptr, nullptr, nullptr, get_num_nz,
             nullptr, nullptr, nullptr);
  get_num_nz = get_num_nz > 0 ? get_num_nz : 1;
  HighsInt start;
  std::vector<HighsInt> index(get_num_nz);
  std::vector<double> value(get_num_nz);
  HighsInt* index_ptr = static_cast<HighsInt*>(index.data());
  double* value_ptr = static_cast<double*>(value.data());
  HighsStatus status =
      h->getCols(1, &col_, get_num_col, nullptr, nullptr, nullptr, get_num_nz,
                 &start, index_ptr, value_ptr);
  return std::make_tuple(status, py::cast(index), py::cast(value));
}

std::tuple<HighsStatus, double, double, HighsInt> highs_getRow(Highs* h,
                                                               HighsInt row) {
  double lower, upper;
  HighsInt get_num_row;
  HighsInt get_num_nz;
  HighsInt row_ = static_cast<HighsInt>(row);
  HighsStatus status = h->getRows(1, &row_, get_num_row, &lower, &upper,
                                  get_num_nz, nullptr, nullptr, nullptr);
  return std::make_tuple(status, lower, upper, get_num_nz);
}

std::tuple<HighsStatus, dense_array_t<HighsInt>, dense_array_t<double>>
highs_getRowEntries(Highs* h, HighsInt row) {
  HighsInt get_num_row;
  HighsInt get_num_nz;
  HighsInt row_ = static_cast<HighsInt>(row);
  h->getRows(1, &row_, get_num_row, nullptr, nullptr, get_num_nz, nullptr,
             nullptr, nullptr);
  get_num_nz = get_num_nz > 0 ? get_num_nz : 1;
  HighsInt start;
  std::vector<HighsInt> index(get_num_nz);
  std::vector<double> value(get_num_nz);
  HighsInt* index_ptr = static_cast<HighsInt*>(index.data());
  double* value_ptr = static_cast<double*>(value.data());
  HighsStatus status = h->getRows(1, &row_, get_num_row, nullptr, nullptr,
                                  get_num_nz, &start, index_ptr, value_ptr);
  return std::make_tuple(status, py::cast(index), py::cast(value));
}

std::tuple<HighsStatus, HighsInt, dense_array_t<double>, dense_array_t<double>,
           dense_array_t<double>, HighsInt>
highs_getCols(Highs* h, HighsInt num_set_entries,
              dense_array_t<HighsInt> indices) {
  py::buffer_info indices_info = indices.request();
  HighsInt* indices_ptr = static_cast<HighsInt*>(indices_info.ptr);
  // Make sure that the vectors are not empty
  const HighsInt dim = num_set_entries > 0 ? num_set_entries : 1;
  std::vector<double> cost(dim);
  std::vector<double> lower(dim);
  std::vector<double> upper(dim);
  double* cost_ptr = static_cast<double*>(cost.data());
  double* lower_ptr = static_cast<double*>(lower.data());
  double* upper_ptr = static_cast<double*>(upper.data());
  HighsInt get_num_col;
  HighsInt get_num_nz;
  HighsStatus status =
      h->getCols(num_set_entries, indices_ptr, get_num_col, cost_ptr, lower_ptr,
                 upper_ptr, get_num_nz, nullptr, nullptr, nullptr);
  return std::make_tuple(status, get_num_col, py::cast(cost), py::cast(lower),
                         py::cast(upper), get_num_nz);
}

std::tuple<HighsStatus, dense_array_t<HighsInt>, dense_array_t<HighsInt>,
           dense_array_t<double>>
highs_getColsEntries(Highs* h, HighsInt num_set_entries,
                     dense_array_t<HighsInt> indices) {
  py::buffer_info indices_info = indices.request();
  HighsInt* indices_ptr = static_cast<HighsInt*>(indices_info.ptr);
  // Make sure that the vectors are not empty
  const HighsInt dim = num_set_entries > 0 ? num_set_entries : 1;
  HighsInt get_num_col;
  HighsInt get_num_nz;
  h->getCols(num_set_entries, indices_ptr, get_num_col, nullptr, nullptr,
             nullptr, get_num_nz, nullptr, nullptr, nullptr);
  get_num_nz = get_num_nz > 0 ? get_num_nz : 1;
  std::vector<HighsInt> start(dim);
  std::vector<HighsInt> index(get_num_nz);
  std::vector<double> value(get_num_nz);
  HighsInt* start_ptr = static_cast<HighsInt*>(start.data());
  HighsInt* index_ptr = static_cast<HighsInt*>(index.data());
  double* value_ptr = static_cast<double*>(value.data());
  HighsStatus status =
      h->getCols(num_set_entries, indices_ptr, get_num_col, nullptr, nullptr,
                 nullptr, get_num_nz, start_ptr, index_ptr, value_ptr);
  return std::make_tuple(status, py::cast(start), py::cast(index),
                         py::cast(value));
}

std::tuple<HighsStatus, HighsVarType>
highs_getColIntegrality(Highs* h, HighsInt col) {
  HighsInt col_ = static_cast<HighsInt>(col);
  HighsVarType integrality;
  HighsStatus status = h->getColIntegrality(col_, integrality);
  return std::make_tuple(status, integrality);
}

std::tuple<HighsStatus, HighsInt, dense_array_t<double>, dense_array_t<double>,
           HighsInt>
highs_getRows(Highs* h, HighsInt num_set_entries,
              dense_array_t<HighsInt> indices) {
  py::buffer_info indices_info = indices.request();
  HighsInt* indices_ptr = static_cast<HighsInt*>(indices_info.ptr);
  // Make sure that the vectors are not empty
  const HighsInt dim = num_set_entries > 0 ? num_set_entries : 1;
  std::vector<double> lower(dim);
  std::vector<double> upper(dim);
  double* lower_ptr = static_cast<double*>(lower.data());
  double* upper_ptr = static_cast<double*>(upper.data());
  HighsInt get_num_row;
  HighsInt get_num_nz;
  HighsStatus status =
      h->getRows(num_set_entries, indices_ptr, get_num_row, lower_ptr,
                 upper_ptr, get_num_nz, nullptr, nullptr, nullptr);
  return std::make_tuple(status, get_num_row, py::cast(lower), py::cast(upper),
                         get_num_nz);
}

std::tuple<HighsStatus, dense_array_t<HighsInt>, dense_array_t<HighsInt>,
           dense_array_t<double>>
highs_getRowsEntries(Highs* h, HighsInt num_set_entries,
                     dense_array_t<HighsInt> indices) {
  py::buffer_info indices_info = indices.request();
  HighsInt* indices_ptr = static_cast<HighsInt*>(indices_info.ptr);
  // Make sure that the vectors are not empty
  const HighsInt dim = num_set_entries > 0 ? num_set_entries : 1;
  HighsInt get_num_row;
  HighsInt get_num_nz;
  h->getRows(num_set_entries, indices_ptr, get_num_row, nullptr, nullptr,
             get_num_nz, nullptr, nullptr, nullptr);
  get_num_nz = get_num_nz > 0 ? get_num_nz : 1;
  std::vector<HighsInt> start(dim);
  std::vector<HighsInt> index(get_num_nz);
  std::vector<double> value(get_num_nz);
  HighsInt* start_ptr = static_cast<HighsInt*>(start.data());
  HighsInt* index_ptr = static_cast<HighsInt*>(index.data());
  double* value_ptr = static_cast<double*>(value.data());
  HighsStatus status =
      h->getRows(num_set_entries, indices_ptr, get_num_row, nullptr, nullptr,
                 get_num_nz, start_ptr, index_ptr, value_ptr);
  return std::make_tuple(status, py::cast(start), py::cast(index),
                         py::cast(value));
}

std::tuple<HighsStatus, std::string> highs_getColName(Highs* h,
                                                      const HighsInt col) {
  std::string name;
  HighsStatus status = h->getColName(col, name);
  return std::make_tuple(status, name);
}

std::tuple<HighsStatus, int> highs_getColByName(Highs* h,
                                                const std::string name) {
  HighsInt col;
  HighsStatus status = h->getColByName(name, col);
  return std::make_tuple(status, col);
}

std::tuple<HighsStatus, std::string> highs_getRowName(Highs* h,
                                                      const HighsInt row) {
  std::string name;
  HighsStatus status = h->getRowName(row, name);
  return std::make_tuple(status, name);
}

std::tuple<HighsStatus, int> highs_getRowByName(Highs* h,
                                                const std::string name) {
  HighsInt row;
  HighsStatus status = h->getRowByName(name, row);
  return std::make_tuple(status, row);
}

// Wrap the setCallback function to appropriately handle user data.
// pybind11 automatically ensures GIL is re-acquired when the callback is called.
HighsStatus highs_setCallback(
    Highs* h,
    std::function<void(int, const std::string&, const HighsCallbackDataOut*, HighsCallbackDataIn*, py::handle)> fn,
    py::handle data) {
  if (static_cast<bool>(fn) == false)
    return h->setCallback((HighsCallbackFunctionType) nullptr, nullptr);
  else
    return h->setCallback(
        [fn](int callbackType, const std::string& msg,
                   const HighsCallbackDataOut* dataOut,
                   HighsCallbackDataIn* dataIn, void* d) {
          return fn(callbackType, msg, dataOut, dataIn, py::handle(reinterpret_cast<PyObject*>(d)));
        },
        data.ptr());
}

PYBIND11_MODULE(_core, m, py::mod_gil_not_used()) {
  // To keep a smaller diff, for reviewers, the declarations are not moved, but
  // keep in mind:
  // C++ enum classes :: don't need .export_values()
  // C++ enums, need .export_values()
  // Quoting [1]:
  // "The enum_::export_values() function exports the enum entries into the
  // parent scope, which should be skipped for newer C++11-style strongly typed
  // enums."
  // [1]: https://pybind11.readthedocs.io/en/stable/classes.html
  py::enum_<ObjSense>(m, "ObjSense", py::module_local())
      .value("kMinimize", ObjSense::kMinimize)
      .value("kMaximize", ObjSense::kMaximize);
  py::enum_<MatrixFormat>(m, "MatrixFormat", py::module_local())
      .value("kColwise", MatrixFormat::kColwise)
      .value("kRowwise", MatrixFormat::kRowwise)
      .value("kRowwisePartitioned", MatrixFormat::kRowwisePartitioned);
  py::enum_<HessianFormat>(m, "HessianFormat", py::module_local())
      .value("kTriangular", HessianFormat::kTriangular)
      .value("kSquare", HessianFormat::kSquare);
  py::enum_<SolutionStatus>(m, "SolutionStatus", py::module_local())
      .value("kSolutionStatusNone", SolutionStatus::kSolutionStatusNone)
      .value("kSolutionStatusInfeasible",
             SolutionStatus::kSolutionStatusInfeasible)
      .value("kSolutionStatusFeasible", SolutionStatus::kSolutionStatusFeasible)
      .export_values();
  py::enum_<BasisValidity>(m, "BasisValidity", py::module_local())
      .value("kBasisValidityInvalid", BasisValidity::kBasisValidityInvalid)
      .value("kBasisValidityValid", BasisValidity::kBasisValidityValid)
      .export_values();
  py::enum_<HighsModelStatus>(m, "HighsModelStatus", py::module_local())
      .value("kNotset", HighsModelStatus::kNotset)
      .value("kLoadError", HighsModelStatus::kLoadError)
      .value("kModelError", HighsModelStatus::kModelError)
      .value("kPresolveError", HighsModelStatus::kPresolveError)
      .value("kSolveError", HighsModelStatus::kSolveError)
      .value("kPostsolveError", HighsModelStatus::kPostsolveError)
      .value("kModelEmpty", HighsModelStatus::kModelEmpty)
      .value("kOptimal", HighsModelStatus::kOptimal)
      .value("kInfeasible", HighsModelStatus::kInfeasible)
      .value("kUnboundedOrInfeasible", HighsModelStatus::kUnboundedOrInfeasible)
      .value("kUnbounded", HighsModelStatus::kUnbounded)
      .value("kObjectiveBound", HighsModelStatus::kObjectiveBound)
      .value("kObjectiveTarget", HighsModelStatus::kObjectiveTarget)
      .value("kTimeLimit", HighsModelStatus::kTimeLimit)
      .value("kIterationLimit", HighsModelStatus::kIterationLimit)
      .value("kUnknown", HighsModelStatus::kUnknown)
      .value("kSolutionLimit", HighsModelStatus::kSolutionLimit)
      .value("kInterrupt", HighsModelStatus::kInterrupt)
      .value("kMemoryLimit", HighsModelStatus::kMemoryLimit);
  py::enum_<HighsPresolveStatus>(m, "HighsPresolveStatus", py::module_local())
      .value("kNotPresolved", HighsPresolveStatus::kNotPresolved)
      .value("kNotReduced", HighsPresolveStatus::kNotReduced)
      .value("kInfeasible", HighsPresolveStatus::kInfeasible)
      .value("kUnboundedOrInfeasible",
             HighsPresolveStatus::kUnboundedOrInfeasible)
      .value("kReduced", HighsPresolveStatus::kReduced)
      .value("kReducedToEmpty", HighsPresolveStatus::kReducedToEmpty)
      .value("kTimeout", HighsPresolveStatus::kTimeout)
      .value("kNullError", HighsPresolveStatus::kNullError)
      .value("kOptionsError", HighsPresolveStatus::kOptionsError);
  py::enum_<HighsBasisStatus>(m, "HighsBasisStatus", py::module_local())
      .value("kLower", HighsBasisStatus::kLower)
      .value("kBasic", HighsBasisStatus::kBasic)
      .value("kUpper", HighsBasisStatus::kUpper)
      .value("kZero", HighsBasisStatus::kZero)
      .value("kNonbasic", HighsBasisStatus::kNonbasic);
  py::enum_<HighsVarType>(m, "HighsVarType", py::module_local())
      .value("kContinuous", HighsVarType::kContinuous)
      .value("kInteger", HighsVarType::kInteger)
      .value("kSemiContinuous", HighsVarType::kSemiContinuous)
      .value("kSemiInteger", HighsVarType::kSemiInteger);
  py::enum_<HighsOptionType>(m, "HighsOptionType", py::module_local())
      .value("kBool", HighsOptionType::kBool)
      .value("kInt", HighsOptionType::kInt)
      .value("kDouble", HighsOptionType::kDouble)
      .value("kString", HighsOptionType::kString);
  py::enum_<HighsInfoType>(m, "HighsInfoType", py::module_local())
      .value("kInt64", HighsInfoType::kInt64)
      .value("kInt", HighsInfoType::kInt)
      .value("kDouble", HighsInfoType::kDouble);
  py::enum_<HighsStatus>(m, "HighsStatus", py::module_local())
      .value("kError", HighsStatus::kError)
      .value("kOk", HighsStatus::kOk)
      .value("kWarning", HighsStatus::kWarning);
  py::enum_<HighsLogType>(m, "HighsLogType", py::module_local())
      .value("kInfo", HighsLogType::kInfo)
      .value("kDetailed", HighsLogType::kDetailed)
      .value("kVerbose", HighsLogType::kVerbose)
      .value("kWarning", HighsLogType::kWarning)
      .value("kError", HighsLogType::kError);
  py::enum_<IisStrategy>(m, "IisStrategy", py::module_local())
      .value("kIisStrategyMin", IisStrategy::kIisStrategyMin)
      .value("kIisStrategyFromLpRowPriority",
             IisStrategy::kIisStrategyFromLpRowPriority)
      .value("kIisStrategyFromLpColPriority",
             IisStrategy::kIisStrategyFromLpColPriority)
      .value("kIisStrategyMax", IisStrategy::kIisStrategyMax)
      .export_values();
  py::enum_<IisBoundStatus>(m, "IisBoundStatus", py::module_local())
      .value("kIisBoundStatusDropped", IisBoundStatus::kIisBoundStatusDropped)
      .value("kIisBoundStatusNull", IisBoundStatus::kIisBoundStatusNull)
      .value("kIisBoundStatusFree", IisBoundStatus::kIisBoundStatusFree)
      .value("kIisBoundStatusLower", IisBoundStatus::kIisBoundStatusLower)
      .value("kIisBoundStatusUpper", IisBoundStatus::kIisBoundStatusUpper)
      .value("kIisBoundStatusBoxed", IisBoundStatus::kIisBoundStatusBoxed)
      .export_values();
  py::enum_<HighsDebugLevel>(m, "HighsDebugLevel", py::module_local())
      .value("kHighsDebugLevelNone", HighsDebugLevel::kHighsDebugLevelNone)
      .value("kHighsDebugLevelCheap", HighsDebugLevel::kHighsDebugLevelCheap)
      .value("kHighsDebugLevelCostly", HighsDebugLevel::kHighsDebugLevelCostly)
      .value("kHighsDebugLevelExpensive", HighsDebugLevel::kHighsDebugLevelExpensive)
      .value("kHighsDebugLevelMin", HighsDebugLevel::kHighsDebugLevelMin)
      .value("kHighsDebugLevelMax", HighsDebugLevel::kHighsDebugLevelMax)
      .export_values();
  // Classes
  py::class_<HighsSparseMatrix>(m, "HighsSparseMatrix", py::module_local())
      .def(py::init<>())
      .def_readwrite("format_", &HighsSparseMatrix::format_)
      .def_readwrite("num_col_", &HighsSparseMatrix::num_col_)
      .def_readwrite("num_row_", &HighsSparseMatrix::num_row_)
      .def_readwrite("start_", &HighsSparseMatrix::start_)
      .def_readwrite("p_end_", &HighsSparseMatrix::p_end_)
      .def_readwrite("index_", &HighsSparseMatrix::index_)
      .def_readwrite("value_", &HighsSparseMatrix::value_);
  py::class_<HighsLpMods>(m, "HighsLpMods", py::module_local());
  py::class_<HighsScale>(m, "HighsScale", py::module_local());
  py::class_<HighsLp>(m, "HighsLp", py::module_local())
      .def(py::init<>())
      .def_readwrite("num_col_", &HighsLp::num_col_)
      .def_readwrite("num_row_", &HighsLp::num_row_)
      .def_readwrite("col_cost_", &HighsLp::col_cost_)
      .def_readwrite("col_lower_", &HighsLp::col_lower_)
      .def_readwrite("col_upper_", &HighsLp::col_upper_)
      .def_readwrite("row_lower_", &HighsLp::row_lower_)
      .def_readwrite("row_upper_", &HighsLp::row_upper_)
      .def_readwrite("a_matrix_", &HighsLp::a_matrix_)
      .def_readwrite("sense_", &HighsLp::sense_)
      .def_readwrite("offset_", &HighsLp::offset_)
      .def_readwrite("model_name_", &HighsLp::model_name_)
      .def_readwrite("col_names_", &HighsLp::col_names_)
      .def_readwrite("row_names_", &HighsLp::row_names_)
      .def_readwrite("integrality_", &HighsLp::integrality_)
      .def_readwrite("scale_", &HighsLp::scale_)
      .def_readwrite("is_scaled_", &HighsLp::is_scaled_)
      .def_readwrite("is_moved_", &HighsLp::is_moved_)
      .def_readwrite("mods_", &HighsLp::mods_);
  py::class_<HighsHessian>(m, "HighsHessian", py::module_local())
      .def(py::init<>())
      .def_readwrite("dim_", &HighsHessian::dim_)
      .def_readwrite("format_", &HighsHessian::format_)
      .def_readwrite("start_", &HighsHessian::start_)
      .def_readwrite("index_", &HighsHessian::index_)
      .def_readwrite("value_", &HighsHessian::value_);
  py::class_<HighsModel>(m, "HighsModel", py::module_local())
      .def(py::init<>())
      .def_readwrite("lp_", &HighsModel::lp_)
      .def_readwrite("hessian_", &HighsModel::hessian_);
  py::class_<HighsInfo>(m, "HighsInfo", py::module_local())
      .def(py::init<>())
      .def_readwrite("valid", &HighsInfo::valid)
      .def_readwrite("mip_node_count", &HighsInfo::mip_node_count)
      .def_readwrite("simplex_iteration_count",
                     &HighsInfo::simplex_iteration_count)
      .def_readwrite("ipm_iteration_count", &HighsInfo::ipm_iteration_count)
      .def_readwrite("qp_iteration_count", &HighsInfo::qp_iteration_count)
      .def_readwrite("crossover_iteration_count",
                     &HighsInfo::crossover_iteration_count)
      .def_readwrite("pdlp_iteration_count", &HighsInfo::pdlp_iteration_count)
      .def_readwrite("primal_solution_status",
                     &HighsInfo::primal_solution_status)
      .def_readwrite("dual_solution_status", &HighsInfo::dual_solution_status)
      .def_readwrite("basis_validity", &HighsInfo::basis_validity)
      .def_readwrite("objective_function_value",
                     &HighsInfo::objective_function_value)
      .def_readwrite("mip_dual_bound", &HighsInfo::mip_dual_bound)
      .def_readwrite("mip_gap", &HighsInfo::mip_gap)
      .def_readwrite("max_integrality_violation",
                     &HighsInfo::max_integrality_violation)
      .def_readwrite("num_primal_infeasibilities",
                     &HighsInfo::num_primal_infeasibilities)
      .def_readwrite("max_primal_infeasibility",
                     &HighsInfo::max_primal_infeasibility)
      .def_readwrite("sum_primal_infeasibilities",
                     &HighsInfo::sum_primal_infeasibilities)
      .def_readwrite("num_dual_infeasibilities",
                     &HighsInfo::num_dual_infeasibilities)
      .def_readwrite("max_dual_infeasibility",
                     &HighsInfo::max_dual_infeasibility)
      .def_readwrite("sum_dual_infeasibilities",
                     &HighsInfo::sum_dual_infeasibilities)
      .def_readwrite("max_complementarity_violation",
                     &HighsInfo::max_complementarity_violation)
      .def_readwrite("sum_complementarity_violations",
                     &HighsInfo::sum_complementarity_violations)
      .def_readwrite("primal_dual_integral",
                     &HighsInfo::primal_dual_integral);
  py::class_<HighsOptions>(m, "HighsOptions", py::module_local())
      .def(py::init<>())
      .def_readwrite("presolve", &HighsOptions::presolve)
      .def_readwrite("solver", &HighsOptions::solver)
      .def_readwrite("parallel", &HighsOptions::parallel)
      .def_readwrite("run_crossover", &HighsOptions::run_crossover)
      .def_readwrite("ranging", &HighsOptions::ranging)
      .def_readwrite("time_limit", &HighsOptions::time_limit)
      .def_readwrite("infinite_cost", &HighsOptions::infinite_cost)
      .def_readwrite("infinite_bound", &HighsOptions::infinite_bound)
      .def_readwrite("small_matrix_value", &HighsOptions::small_matrix_value)
      .def_readwrite("large_matrix_value", &HighsOptions::large_matrix_value)
      .def_readwrite("primal_feasibility_tolerance",
                     &HighsOptions::primal_feasibility_tolerance)
      .def_readwrite("dual_feasibility_tolerance",
                     &HighsOptions::dual_feasibility_tolerance)
      .def_readwrite("ipm_optimality_tolerance",
                     &HighsOptions::ipm_optimality_tolerance)
      .def_readwrite("objective_bound", &HighsOptions::objective_bound)
      .def_readwrite("objective_target", &HighsOptions::objective_target)
      .def_readwrite("random_seed", &HighsOptions::random_seed)
      .def_readwrite("threads", &HighsOptions::threads)
      .def_readwrite("highs_debug_level", &HighsOptions::highs_debug_level)
      .def_readwrite("highs_analysis_level",
                     &HighsOptions::highs_analysis_level)
      .def_readwrite("simplex_strategy", &HighsOptions::simplex_strategy)
      .def_readwrite("simplex_scale_strategy",
                     &HighsOptions::simplex_scale_strategy)
      .def_readwrite("simplex_crash_strategy",
                     &HighsOptions::simplex_crash_strategy)
      .def_readwrite("simplex_dual_edge_weight_strategy",
                     &HighsOptions::simplex_dual_edge_weight_strategy)
      .def_readwrite("simplex_primal_edge_weight_strategy",
                     &HighsOptions::simplex_primal_edge_weight_strategy)
      .def_readwrite("simplex_iteration_limit",
                     &HighsOptions::simplex_iteration_limit)
      .def_readwrite("simplex_update_limit",
                     &HighsOptions::simplex_update_limit)
      .def_readwrite("simplex_min_concurrency",
                     &HighsOptions::simplex_min_concurrency)
      .def_readwrite("simplex_max_concurrency",
                     &HighsOptions::simplex_max_concurrency)
      .def_readwrite("ipm_iteration_limit", &HighsOptions::ipm_iteration_limit)
      .def_readwrite("write_model_file", &HighsOptions::write_model_file)
      .def_readwrite("solution_file", &HighsOptions::solution_file)
      .def_readwrite("log_file", &HighsOptions::log_file)
      .def_readwrite("write_model_to_file", &HighsOptions::write_model_to_file)
      .def_readwrite("write_solution_to_file",
                     &HighsOptions::write_solution_to_file)
      .def_readwrite("write_solution_style",
                     &HighsOptions::write_solution_style)
      .def_readwrite("output_flag", &HighsOptions::output_flag)
      .def_readwrite("log_to_console", &HighsOptions::log_to_console)
      .def_readwrite("log_dev_level", &HighsOptions::log_dev_level)
      .def_readwrite("allow_unbounded_or_infeasible",
                     &HighsOptions::allow_unbounded_or_infeasible)
      .def_readwrite("allowed_matrix_scale_factor",
                     &HighsOptions::allowed_matrix_scale_factor)
      .def_readwrite("ipx_dualize_strategy",
                     &HighsOptions::ipx_dualize_strategy)
      .def_readwrite("simplex_dualize_strategy",
                     &HighsOptions::simplex_dualize_strategy)
      .def_readwrite("simplex_permute_strategy",
                     &HighsOptions::simplex_permute_strategy)
      .def_readwrite("simplex_price_strategy",
                     &HighsOptions::simplex_price_strategy)
      .def_readwrite("mip_detect_symmetry", &HighsOptions::mip_detect_symmetry)
      .def_readwrite("mip_max_nodes", &HighsOptions::mip_max_nodes)
      .def_readwrite("mip_max_stall_nodes", &HighsOptions::mip_max_stall_nodes)
      .def_readwrite("mip_max_leaves", &HighsOptions::mip_max_leaves)
      .def_readwrite("mip_max_improving_sols",
                     &HighsOptions::mip_max_improving_sols)
      .def_readwrite("mip_lp_age_limit", &HighsOptions::mip_lp_age_limit)
      .def_readwrite("mip_pool_age_limit", &HighsOptions::mip_pool_age_limit)
      .def_readwrite("mip_pool_soft_limit", &HighsOptions::mip_pool_soft_limit)
      .def_readwrite("mip_pscost_minreliable",
                     &HighsOptions::mip_pscost_minreliable)
      .def_readwrite("mip_min_cliquetable_entries_for_parallelism",
                     &HighsOptions::mip_min_cliquetable_entries_for_parallelism)
      .def_readwrite("mip_report_level", &HighsOptions::mip_report_level)
      .def_readwrite("mip_feasibility_tolerance",
                     &HighsOptions::mip_feasibility_tolerance)
      .def_readwrite("mip_rel_gap", &HighsOptions::mip_rel_gap)
      .def_readwrite("mip_abs_gap", &HighsOptions::mip_abs_gap)
      .def_readwrite("mip_heuristic_effort",
                     &HighsOptions::mip_heuristic_effort)
      .def_readwrite("mip_min_logging_interval",
                     &HighsOptions::mip_min_logging_interval);
  py::class_<Highs>(m, "_Highs", py::module_local())
      .def(py::init<>())
      .def("version", &Highs::version)
      .def("versionMajor", &Highs::versionMajor)
      .def("versionMinor", &Highs::versionMinor)
      .def("versionPatch", &Highs::versionPatch)
      .def("githash", &Highs::githash)
      .def("clear", &Highs::clear)
      .def("clearModel", &Highs::clearModel)
      .def("clearSolver", &Highs::clearSolver)
      .def("passModel", &highs_passModel)
      .def("passModel", &highs_passModelPointers)
      .def("passModel", &highs_passLp)
      .def("passModel", &highs_passLpPointers)
      .def("passHessian", &highs_passHessian)
      .def("passHessian", &highs_passHessianPointers)
      .def("passColName", &Highs::passColName)
      .def("passRowName", &Highs::passRowName)
      .def("readModel", &Highs::readModel)
      .def("readBasis", &Highs::readBasis)
      .def("writeBasis", &Highs::writeBasis)
      .def("postsolve", &highs_postsolve)
      .def("postsolve", &highs_mipPostsolve)
      .def("run", &Highs::run, py::call_guard<py::gil_scoped_release>())
      .def_static("resetGlobalScheduler", &Highs::resetGlobalScheduler)
      .def(
          "feasibilityRelaxation",
          [](Highs& self, double global_lower_penalty,
             double global_upper_penalty, double global_rhs_penalty,
             py::object local_lower_penalty, py::object local_upper_penalty,
             py::object local_rhs_penalty) {
            std::vector<double> llp, lup, lrp;
            const double* llp_ptr = nullptr;
            const double* lup_ptr = nullptr;
            const double* lrp_ptr = nullptr;

            if (!local_lower_penalty.is_none()) {
              llp = local_lower_penalty.cast<std::vector<double>>();
              llp_ptr = llp.data();
            }
            if (!local_upper_penalty.is_none()) {
              lup = local_upper_penalty.cast<std::vector<double>>();
              lup_ptr = lup.data();
            }
            if (!local_rhs_penalty.is_none()) {
              lrp = local_rhs_penalty.cast<std::vector<double>>();
              lrp_ptr = lrp.data();
            }

            return self.feasibilityRelaxation(
                global_lower_penalty, global_upper_penalty, global_rhs_penalty,
                llp_ptr, lup_ptr, lrp_ptr);
          },
          py::arg("global_lower_penalty"), py::arg("global_upper_penalty"),
          py::arg("global_rhs_penalty"),
          py::arg("local_lower_penalty") = py::none(),
          py::arg("local_upper_penalty") = py::none(),
          py::arg("local_rhs_penalty") = py::none())
      .def("getIis", &Highs::getIis)
      .def("presolve", &Highs::presolve, py::call_guard<py::gil_scoped_release>())
      .def("writeSolution", &highs_writeSolution)
      .def("readSolution", &Highs::readSolution)
      .def("setOptionValue",
           static_cast<HighsStatus (Highs::*)(const std::string&, const bool)>(
               &Highs::setOptionValue))
      .def("setOptionValue",
           static_cast<HighsStatus (Highs::*)(const std::string&, const int)>(
               &Highs::setOptionValue))
      .def(
          "setOptionValue",
          static_cast<HighsStatus (Highs::*)(const std::string&, const double)>(
              &Highs::setOptionValue))
      .def("setOptionValue",
           static_cast<HighsStatus (Highs::*)(
               const std::string&, const std::string&)>(&Highs::setOptionValue))
      .def("readOptions", &Highs::readOptions)
      .def("passOptions", &Highs::passOptions)
      .def("getOptions", &Highs::getOptions)
      .def("getOptionValue", &highs_getOptionValue)
      //    .def("getOptionName", &highs_getOptionName)
      .def("getOptionType", &highs_getOptionType)
      .def("resetOptions", &Highs::resetOptions)
      .def("writeOptions", &highs_writeOptions)
      //    .def("getBoolOptionValues", &highs_getBoolOptionValues)
      //    .def("getIntOptionValues", &highs_getIntOptionValues)
      //    .def("getDoubleOptionValues", &highs_getDoubleOptionValues)
      //    .def("getStringOptionValues", &highs_getStringOptionValues)
      .def("getInfo", &Highs::getInfo)
      .def("getInfoValue", &highs_getInfoValue)
      .def("getInfoType", &highs_getInfoType)
      .def("writeInfo", &Highs::writeInfo)
      .def("getInfinity", &Highs::getInfinity)
      .def("getRunTime", &Highs::getRunTime)
      .def("getPresolvedLp", &Highs::getPresolvedLp)
      //    .def("getPresolvedModel", &Highs::getPresolvedModel)
      //    .def("getPresolveLog", &Highs::getPresolveLog)
      .def("getLp", &Highs::getLp)
      .def("getModel", &Highs::getModel)
      .def("getSolution", &Highs::getSolution)
      .def("getSavedMipSolutions", &Highs::getSavedMipSolutions)
      .def("getBasis", &Highs::getBasis)
      // &highs_getModelStatus not needed once getModelStatus(const bool
      // scaled_model) disappears from, Highs.h
      .def("getModelStatus", &highs_getModelStatus)  //&Highs::getModelStatus)
      .def("getModelPresolveStatus", &Highs::getModelPresolveStatus)
      .def("getRanging", &highs_getRanging)
      .def("getObjectiveValue", &Highs::getObjectiveValue)
      .def("getNumCol", &Highs::getNumCol)
      .def("getNumRow", &Highs::getNumRow)
      .def("getNumNz", &Highs::getNumNz)
      .def("getHessianNumNz", &Highs::getHessianNumNz)
      .def("getObjectiveSense", &highs_getObjectiveSense)
      .def("getObjectiveOffset", &highs_getObjectiveOffset)

      .def("getCol", &highs_getCol)
      .def("getColEntries", &highs_getColEntries)
      .def("getColIntegrality", &highs_getColIntegrality)
      .def("getRow", &highs_getRow)
      .def("getRowEntries", &highs_getRowEntries)

      .def("getCols", &highs_getCols)
      .def("getColsEntries", &highs_getColsEntries)

      .def("getRows", &highs_getRows)
      .def("getRowsEntries", &highs_getRowsEntries)

      .def("getColName", &highs_getColName)
      .def("getColByName", &highs_getColByName)
      .def("getRowName", &highs_getRowName)
      .def("getRowByName", &highs_getRowByName)

      .def("writeModel", &Highs::writeModel)
      .def("writePresolvedModel", &Highs::writePresolvedModel)
      .def("crossover", &Highs::crossover)
      .def("changeObjectiveSense", &Highs::changeObjectiveSense)
      .def("changeObjectiveOffset", &Highs::changeObjectiveOffset)
      .def("changeColIntegrality", &Highs::changeColIntegrality)
      .def("changeColCost", &Highs::changeColCost)
      .def("changeColBounds", &Highs::changeColBounds)
      .def("changeRowBounds", &Highs::changeRowBounds)
      .def("changeCoeff", &Highs::changeCoeff)
      .def("addRows", &highs_addRows)
      .def("addRow", &highs_addRow)
      .def("addCol", &highs_addCol)
      .def("addCols", &highs_addCols)
      .def("addVar", &highs_addVar)
      .def("addVars", &highs_addVars)
      .def("changeColsCost", &highs_changeColsCost)
      .def("changeColsBounds", &highs_changeColsBounds)
      .def("changeColsIntegrality", &highs_changeColsIntegrality)
      .def("deleteCols", &highs_deleteCols)
      .def("deleteVars", &highs_deleteCols)  // alias
      .def("deleteRows", &highs_deleteRows)
      .def("setSolution", &highs_setSolution)
      .def("setSolution", &highs_setSparseSolution)
      .def("setBasis", &highs_setBasis)
      .def("setBasis", &highs_setLogicalBasis)
      .def("modelStatusToString", &Highs::modelStatusToString)
      .def("solutionStatusToString", &Highs::solutionStatusToString)
      .def("basisStatusToString", &Highs::basisStatusToString)
      .def("basisValidityToString", &Highs::basisValidityToString)
      .def("setCallback", &highs_setCallback)
      .def("startCallback",
           static_cast<HighsStatus (Highs::*)(const HighsCallbackType)>(
               &Highs::startCallback))
      .def("stopCallback",
           static_cast<HighsStatus (Highs::*)(const HighsCallbackType)>(
               &Highs::stopCallback))
      .def("startCallbackInt", static_cast<HighsStatus (Highs::*)(const int)>(
                                   &Highs::startCallback))
      .def("stopCallbackInt", static_cast<HighsStatus (Highs::*)(const int)>(
                                  &Highs::stopCallback));

  py::class_<HighsIis>(m, "HighsIis", py::module_local())
      .def(py::init<>())
      .def("invalidate", &HighsIis::invalidate)
      .def_readwrite("valid", &HighsIis::valid_)
      .def_readwrite("strategy", &HighsIis::strategy_)
      .def_readwrite("col_index", &HighsIis::col_index_)
      .def_readwrite("row_index", &HighsIis::row_index_)
      .def_readwrite("col_bound", &HighsIis::col_bound_)
      .def_readwrite("row_bound", &HighsIis::row_bound_)
      .def_readwrite("info", &HighsIis::info_);
  // structs
  py::class_<HighsSolution>(m, "HighsSolution", py::module_local())
      .def(py::init<>())
      .def_readwrite("value_valid", &HighsSolution::value_valid)
      .def_readwrite("dual_valid", &HighsSolution::dual_valid)
      .def_readwrite("col_value", &HighsSolution::col_value)
      .def_readwrite("col_dual", &HighsSolution::col_dual)
      .def_readwrite("row_value", &HighsSolution::row_value)
      .def_readwrite("row_dual", &HighsSolution::row_dual);
  py::class_<HighsObjectiveSolution>(m, "HighsObjectiveSolution", py::module_local())
      .def(py::init<>())
      .def_readwrite("objective", &HighsObjectiveSolution::objective)
      .def_readwrite("col_value", &HighsObjectiveSolution::col_value);
  py::class_<HighsBasis>(m, "HighsBasis", py::module_local())
      .def(py::init<>())
      .def_readwrite("valid", &HighsBasis::valid)
      .def_readwrite("alien", &HighsBasis::alien)
      .def_readwrite("was_alien", &HighsBasis::was_alien)
      .def_readwrite("debug_id", &HighsBasis::debug_id)
      .def_readwrite("debug_update_count", &HighsBasis::debug_update_count)
      .def_readwrite("debug_origin_name", &HighsBasis::debug_origin_name)
      .def_readwrite("col_status", &HighsBasis::col_status)
      .def_readwrite("row_status", &HighsBasis::row_status);
  py::class_<HighsRangingRecord>(m, "HighsRangingRecord", py::module_local())
      .def(py::init<>())
      .def_readwrite("value_", &HighsRangingRecord::value_)
      .def_readwrite("objective_", &HighsRangingRecord::objective_)
      .def_readwrite("in_var_", &HighsRangingRecord::in_var_)
      .def_readwrite("ou_var_", &HighsRangingRecord::ou_var_);
  py::class_<HighsRanging>(m, "HighsRanging", py::module_local())
      .def(py::init<>())
      .def_readwrite("valid", &HighsRanging::valid)
      .def_readwrite("col_cost_up", &HighsRanging::col_cost_up)
      .def_readwrite("col_cost_dn", &HighsRanging::col_cost_dn)
      .def_readwrite("col_bound_up", &HighsRanging::col_bound_up)
      .def_readwrite("col_bound_dn", &HighsRanging::col_bound_dn)
      .def_readwrite("row_bound_up", &HighsRanging::row_bound_up)
      .def_readwrite("row_bound_dn", &HighsRanging::row_bound_dn);
  py::class_<HighsIisInfo>(m, "HighsIisInfo", py::module_local())
      .def(py::init<>())
      .def_readwrite("simplex_time", &HighsIisInfo::simplex_time)
      .def_readwrite("simplex_iterations", &HighsIisInfo::simplex_iterations);
  // constants
  m.attr("kHighsInf") = kHighsInf;
  m.attr("kHighsIInf") = kHighsIInf;

  m.attr("HIGHS_VERSION_MAJOR") = HIGHS_VERSION_MAJOR;
  m.attr("HIGHS_VERSION_MINOR") = HIGHS_VERSION_MINOR;
  m.attr("HIGHS_VERSION_PATCH") = HIGHS_VERSION_PATCH;

  // Submodules
  py::module_ simplex_constants =
      m.def_submodule("simplex_constants", "Submodule for simplex constants");

  py::enum_<SimplexStrategy>(simplex_constants, "SimplexStrategy", py::module_local())
      .value("kSimplexStrategyMin", SimplexStrategy::kSimplexStrategyMin)
      .value("kSimplexStrategyChoose", SimplexStrategy::kSimplexStrategyChoose)
      .value("kSimplexStrategyDual", SimplexStrategy::kSimplexStrategyDual)
      .value("kSimplexStrategyDualPlain",
             SimplexStrategy::kSimplexStrategyDualPlain)
      .value("kSimplexStrategyDualTasks",
             SimplexStrategy::kSimplexStrategyDualTasks)
      .value("kSimplexStrategyDualMulti",
             SimplexStrategy::kSimplexStrategyDualMulti)
      .value("kSimplexStrategyPrimal", SimplexStrategy::kSimplexStrategyPrimal)
      .value("kSimplexStrategyMax", SimplexStrategy::kSimplexStrategyMax)
      .value("kSimplexStrategyNum", SimplexStrategy::kSimplexStrategyNum)
      .export_values();
  py::enum_<SimplexUnscaledSolutionStrategy>(simplex_constants,
                                             "SimplexUnscaledSolutionStrategy", py::module_local())
      .value(
          "kSimplexUnscaledSolutionStrategyMin",
          SimplexUnscaledSolutionStrategy::kSimplexUnscaledSolutionStrategyMin)
      .value(
          "kSimplexUnscaledSolutionStrategyNone",
          SimplexUnscaledSolutionStrategy::kSimplexUnscaledSolutionStrategyNone)
      .value("kSimplexUnscaledSolutionStrategyRefine",
             SimplexUnscaledSolutionStrategy::
                 kSimplexUnscaledSolutionStrategyRefine)
      .value("kSimplexUnscaledSolutionStrategyDirect",
             SimplexUnscaledSolutionStrategy::
                 kSimplexUnscaledSolutionStrategyDirect)
      .value(
          "kSimplexUnscaledSolutionStrategyMax",
          SimplexUnscaledSolutionStrategy::kSimplexUnscaledSolutionStrategyMax)
      .value(
          "kSimplexUnscaledSolutionStrategyNum",
          SimplexUnscaledSolutionStrategy::kSimplexUnscaledSolutionStrategyNum)
      .export_values();
  py::enum_<SimplexSolvePhase>(simplex_constants, "SimplexSolvePhase", py::module_local())
      .value("kSolvePhaseMin", SimplexSolvePhase::kSolvePhaseMin)
      .value("kSolvePhaseError", SimplexSolvePhase::kSolvePhaseError)
      .value("kSolvePhaseExit", SimplexSolvePhase::kSolvePhaseExit)
      .value("kSolvePhaseUnknown", SimplexSolvePhase::kSolvePhaseUnknown)
      .value("kSolvePhaseOptimal", SimplexSolvePhase::kSolvePhaseOptimal)
      .value("kSolvePhase1", SimplexSolvePhase::kSolvePhase1)
      .value("kSolvePhase2", SimplexSolvePhase::kSolvePhase2)
      .value("kSolvePhasePrimalInfeasibleCleanup",
             SimplexSolvePhase::kSolvePhasePrimalInfeasibleCleanup)
      .value("kSolvePhaseOptimalCleanup",
             SimplexSolvePhase::kSolvePhaseOptimalCleanup)
      .value("kSolvePhaseTabooBasis", SimplexSolvePhase::kSolvePhaseTabooBasis)
      .value("kSolvePhaseMax", SimplexSolvePhase::kSolvePhaseMax)
      .export_values();
  py::enum_<SimplexEdgeWeightStrategy>(simplex_constants,
                                       "SimplexEdgeWeightStrategy", py::module_local())
      .value("kSimplexEdgeWeightStrategyMin",
             SimplexEdgeWeightStrategy::kSimplexEdgeWeightStrategyMin)
      .value("kSimplexEdgeWeightStrategyChoose",
             SimplexEdgeWeightStrategy::kSimplexEdgeWeightStrategyChoose)
      .value("kSimplexEdgeWeightStrategyDantzig",
             SimplexEdgeWeightStrategy::kSimplexEdgeWeightStrategyDantzig)
      .value("kSimplexEdgeWeightStrategyDevex",
             SimplexEdgeWeightStrategy::kSimplexEdgeWeightStrategyDevex)
      .value("kSimplexEdgeWeightStrategySteepestEdge",
             SimplexEdgeWeightStrategy::kSimplexEdgeWeightStrategySteepestEdge)
      .value("kSimplexEdgeWeightStrategyMax",
             SimplexEdgeWeightStrategy::kSimplexEdgeWeightStrategyMax)
      .export_values();
  py::enum_<SimplexPriceStrategy>(simplex_constants, "SimplexPriceStrategy", py::module_local())
      .value("kSimplexPriceStrategyMin",
             SimplexPriceStrategy::kSimplexPriceStrategyMin)
      .value("kSimplexPriceStrategyCol",
             SimplexPriceStrategy::kSimplexPriceStrategyCol)
      .value("kSimplexPriceStrategyRow",
             SimplexPriceStrategy::kSimplexPriceStrategyRow)
      .value("kSimplexPriceStrategyRowSwitch",
             SimplexPriceStrategy::kSimplexPriceStrategyRowSwitch)
      .value("kSimplexPriceStrategyRowSwitchColSwitch",
             SimplexPriceStrategy::kSimplexPriceStrategyRowSwitchColSwitch)
      .value("kSimplexPriceStrategyMax",
             SimplexPriceStrategy::kSimplexPriceStrategyMax)
      .export_values();
  py::enum_<SimplexPivotalRowRefinementStrategy>(
      simplex_constants, "SimplexPivotalRowRefinementStrategy", py::module_local())
      .value("kSimplexInfeasibilityProofRefinementMin",
             SimplexPivotalRowRefinementStrategy::
                 kSimplexInfeasibilityProofRefinementMin)
      .value("kSimplexInfeasibilityProofRefinementNo",
             SimplexPivotalRowRefinementStrategy::
                 kSimplexInfeasibilityProofRefinementNo)
      .value("kSimplexInfeasibilityProofRefinementUnscaledLp",
             SimplexPivotalRowRefinementStrategy::
                 kSimplexInfeasibilityProofRefinementUnscaledLp)
      .value("kSimplexInfeasibilityProofRefinementAlsoScaledLp",
             SimplexPivotalRowRefinementStrategy::
                 kSimplexInfeasibilityProofRefinementAlsoScaledLp)
      .value("kSimplexInfeasibilityProofRefinementMax",
             SimplexPivotalRowRefinementStrategy::
                 kSimplexInfeasibilityProofRefinementMax)
      .export_values();
  py::enum_<SimplexPrimalCorrectionStrategy>(simplex_constants,
                                             "SimplexPrimalCorrectionStrategy", py::module_local())
      .value(
          "kSimplexPrimalCorrectionStrategyNone",
          SimplexPrimalCorrectionStrategy::kSimplexPrimalCorrectionStrategyNone)
      .value("kSimplexPrimalCorrectionStrategyInRebuild",
             SimplexPrimalCorrectionStrategy::
                 kSimplexPrimalCorrectionStrategyInRebuild)
      .value("kSimplexPrimalCorrectionStrategyAlways",
             SimplexPrimalCorrectionStrategy::
                 kSimplexPrimalCorrectionStrategyAlways)
      .export_values();
  py::enum_<SimplexNlaOperation>(simplex_constants, "SimplexNlaOperation", py::module_local())
      .value("kSimplexNlaNull", SimplexNlaOperation::kSimplexNlaNull)
      .value("kSimplexNlaBtranFull", SimplexNlaOperation::kSimplexNlaBtranFull)
      .value("kSimplexNlaPriceFull", SimplexNlaOperation::kSimplexNlaPriceFull)
      .value("kSimplexNlaBtranBasicFeasibilityChange",
             SimplexNlaOperation::kSimplexNlaBtranBasicFeasibilityChange)
      // .value("kSimplexNlaPriceBasicFeasibilityChange",
      //        /khighsSimplexNlaOperation::kSimplexNlaPriceBasicFeasibilityChange)
      .value("kSimplexNlaBtranEp", SimplexNlaOperation::kSimplexNlaBtranEp)
      .value("kSimplexNlaPriceAp", SimplexNlaOperation::kSimplexNlaPriceAp)
      .value("kSimplexNlaFtran", SimplexNlaOperation::kSimplexNlaFtran)
      .value("kSimplexNlaFtranBfrt", SimplexNlaOperation::kSimplexNlaFtranBfrt)
      .value("kSimplexNlaFtranDse", SimplexNlaOperation::kSimplexNlaFtranDse)
      .value("kSimplexNlaBtranPse", SimplexNlaOperation::kSimplexNlaBtranPse)
      .value("kNumSimplexNlaOperation",
             SimplexNlaOperation::kNumSimplexNlaOperation)
      .export_values();
  py::enum_<EdgeWeightMode>(simplex_constants, "EdgeWeightMode", py::module_local())
      .value("kDantzig", EdgeWeightMode::kDantzig)
      .value("kDevex", EdgeWeightMode::kDevex)
      .value("kSteepestEdge", EdgeWeightMode::kSteepestEdge)
      .value("kCount", EdgeWeightMode::kCount);
  py::module_ callbacks = m.def_submodule("cb", "Callback interface submodule");
  // Types for interface
  py::enum_<HighsCallbackType>(callbacks, "HighsCallbackType", py::module_local())
      .value("kCallbackMin", HighsCallbackType::kCallbackMin)
      .value("kCallbackLogging", HighsCallbackType::kCallbackLogging)
      .value("kCallbackSimplexInterrupt",
             HighsCallbackType::kCallbackSimplexInterrupt)
      .value("kCallbackIpmInterrupt", HighsCallbackType::kCallbackIpmInterrupt)
      .value("kCallbackMipSolution", HighsCallbackType::kCallbackMipSolution)
      .value("kCallbackMipImprovingSolution",
             HighsCallbackType::kCallbackMipImprovingSolution)
      .value("kCallbackMipLogging", HighsCallbackType::kCallbackMipLogging)
      .value("kCallbackMipInterrupt", HighsCallbackType::kCallbackMipInterrupt)
      .value("kCallbackMipGetCutPool",
             HighsCallbackType::kCallbackMipGetCutPool)
      .value("kCallbackMipDefineLazyConstraints",
             HighsCallbackType::kCallbackMipDefineLazyConstraints)
      .value("kCallbackMax", HighsCallbackType::kCallbackMax)
      .value("kNumCallbackType", HighsCallbackType::kNumCallbackType)
      .export_values();
  // Classes
  py::class_<readonly_ptr_wrapper<double>>(m, "readonly_ptr_wrapper_double", py::module_local())
      .def(py::init<double*>())
      .def("__getitem__", &readonly_ptr_wrapper<double>::operator[])
      .def("__bool__", &readonly_ptr_wrapper<double>::is_valid)
      .def("to_array", &readonly_ptr_wrapper<double>::to_array);
  py::class_<HighsCallbackDataOut>(callbacks, "HighsCallbackDataOut", py::module_local())
      .def(py::init<>())
      .def_readwrite("log_type", &HighsCallbackDataOut::log_type)
      .def_readwrite("running_time", &HighsCallbackDataOut::running_time)
      .def_readwrite("simplex_iteration_count",
                     &HighsCallbackDataOut::simplex_iteration_count)
      .def_readwrite("ipm_iteration_count",
                     &HighsCallbackDataOut::ipm_iteration_count)
      .def_readwrite("pdlp_iteration_count",
                     &HighsCallbackDataOut::pdlp_iteration_count)
      .def_readwrite("objective_function_value",
                     &HighsCallbackDataOut::objective_function_value)
      .def_readwrite("mip_node_count", &HighsCallbackDataOut::mip_node_count)
      .def_readwrite("mip_primal_bound",
                     &HighsCallbackDataOut::mip_primal_bound)
      .def_readwrite("mip_dual_bound", &HighsCallbackDataOut::mip_dual_bound)
      .def_readwrite("mip_gap", &HighsCallbackDataOut::mip_gap)
      .def_property_readonly(
          "mip_solution",
          [](const HighsCallbackDataOut& self) -> readonly_ptr_wrapper<double> {
            return readonly_ptr_wrapper<double>(self.mip_solution);
          });
  py::class_<HighsCallbackDataIn>(callbacks, "HighsCallbackDataIn", py::module_local())
      .def(py::init<>())
      .def_readwrite("user_interrupt", &HighsCallbackDataIn::user_interrupt);
}
