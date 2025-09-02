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
/**@file lp_data/HighsOptions.h
 * @brief
 */
#ifndef LP_DATA_HIGHS_OPTIONS_H_
#define LP_DATA_HIGHS_OPTIONS_H_

#include <cstring>  // For strlen
#include <vector>

#include "io/HighsIO.h"
#include "lp_data/HConst.h"
#include "lp_data/HighsStatus.h"
#include "simplex/SimplexConst.h"
#include "util/HFactorConst.h"

using std::string;

enum class OptionStatus { kOk = 0, kUnknownOption, kIllegalValue };

const bool kAdvancedInDocumentation = false;

class OptionRecord {
 public:
  HighsOptionType type;
  std::string name;
  std::string description;
  bool advanced;

  OptionRecord(HighsOptionType Xtype, std::string Xname,
               std::string Xdescription, bool Xadvanced) {
    this->type = Xtype;
    this->name = Xname;
    this->description = Xdescription;
    this->advanced = Xadvanced;
  }

  virtual ~OptionRecord() {}
};

class OptionRecordBool : public OptionRecord {
 public:
  bool* value;
  bool default_value;
  OptionRecordBool(std::string Xname, std::string Xdescription, bool Xadvanced,
                   bool* Xvalue_pointer, bool Xdefault_value)
      : OptionRecord(HighsOptionType::kBool, Xname, Xdescription, Xadvanced) {
    advanced = Xadvanced;
    value = Xvalue_pointer;
    default_value = Xdefault_value;
    *value = default_value;
  }

  void assignvalue(bool Xvalue) { *value = Xvalue; }

  virtual ~OptionRecordBool() {}
};

class OptionRecordInt : public OptionRecord {
 public:
  HighsInt* value;
  HighsInt lower_bound;
  HighsInt default_value;
  HighsInt upper_bound;
  OptionRecordInt(std::string Xname, std::string Xdescription, bool Xadvanced,
                  HighsInt* Xvalue_pointer, HighsInt Xlower_bound,
                  HighsInt Xdefault_value, HighsInt Xupper_bound)
      : OptionRecord(HighsOptionType::kInt, Xname, Xdescription, Xadvanced) {
    value = Xvalue_pointer;
    lower_bound = Xlower_bound;
    default_value = Xdefault_value;
    upper_bound = Xupper_bound;
    *value = default_value;
  }

  void assignvalue(HighsInt Xvalue) { *value = Xvalue; }

  virtual ~OptionRecordInt() {}
};

class OptionRecordDouble : public OptionRecord {
 public:
  double* value;
  double lower_bound;
  double upper_bound;
  double default_value;
  OptionRecordDouble(std::string Xname, std::string Xdescription,
                     bool Xadvanced, double* Xvalue_pointer,
                     double Xlower_bound, double Xdefault_value,
                     double Xupper_bound)
      : OptionRecord(HighsOptionType::kDouble, Xname, Xdescription, Xadvanced) {
    value = Xvalue_pointer;
    lower_bound = Xlower_bound;
    default_value = Xdefault_value;
    upper_bound = Xupper_bound;
    *value = default_value;
  }

  void assignvalue(double Xvalue) { *value = Xvalue; }

  virtual ~OptionRecordDouble() {}
};

class OptionRecordString : public OptionRecord {
 public:
  std::string* value;
  std::string default_value;
  OptionRecordString(std::string Xname, std::string Xdescription,
                     bool Xadvanced, std::string* Xvalue_pointer,
                     std::string Xdefault_value)
      : OptionRecord(HighsOptionType::kString, Xname, Xdescription, Xadvanced) {
    value = Xvalue_pointer;
    default_value = Xdefault_value;
    *value = default_value;
  }

  void assignvalue(std::string Xvalue) { *value = Xvalue; }

  virtual ~OptionRecordString() {}
};

void highsOpenLogFile(HighsLogOptions& log_options,
                      std::vector<OptionRecord*>& option_records,
                      const std::string log_file);

bool commandLineOffChooseOnOk(const HighsLogOptions& report_log_options,
                              const string& name, const string& value);
bool commandLineOffOnOk(const HighsLogOptions& report_log_options,
                        const string& name, const string& value);
bool commandLineSolverOk(const HighsLogOptions& report_log_options,
                         const string& value);

bool boolFromString(std::string value, bool& bool_value);

OptionStatus getOptionIndex(const HighsLogOptions& report_log_options,
                            const std::string& name,
                            const std::vector<OptionRecord*>& option_records,
                            HighsInt& index);

OptionStatus checkOptions(const HighsLogOptions& report_log_options,
                          const std::vector<OptionRecord*>& option_records);
OptionStatus checkOption(const HighsLogOptions& report_log_options,
                         const OptionRecordInt& option);
OptionStatus checkOption(const HighsLogOptions& report_log_options,
                         const OptionRecordDouble& option);

OptionStatus checkOptionValue(const HighsLogOptions& report_log_options,
                              OptionRecordInt& option_records,
                              const HighsInt value);
OptionStatus checkOptionValue(const HighsLogOptions& report_log_options,
                              OptionRecordDouble& option_records,
                              const double value);
OptionStatus checkOptionValue(const HighsLogOptions& report_log_options,
                              OptionRecordString& option_records,
                              const std::string value);

OptionStatus setLocalOptionValue(const HighsLogOptions& report_log_options,
                                 const std::string& name,
                                 std::vector<OptionRecord*>& option_records,
                                 const bool value);

OptionStatus setLocalOptionValue(const HighsLogOptions& report_log_options,
                                 const std::string& name,
                                 std::vector<OptionRecord*>& option_records,
                                 const HighsInt value);
#ifdef HIGHSINT64
inline OptionStatus setLocalOptionValue(
    const HighsLogOptions& report_log_options, const std::string& name,
    std::vector<OptionRecord*>& option_records, const int value) {
  return setLocalOptionValue(report_log_options, name, option_records,
                             HighsInt{value});
}
#endif
OptionStatus setLocalOptionValue(const HighsLogOptions& report_log_options,
                                 const std::string& name,
                                 std::vector<OptionRecord*>& option_records,
                                 const double value);
OptionStatus setLocalOptionValue(const HighsLogOptions& report_log_options,
                                 const std::string& name,
                                 HighsLogOptions& log_options,
                                 std::vector<OptionRecord*>& option_records,
                                 const std::string value);
OptionStatus setLocalOptionValue(const HighsLogOptions& report_log_options,
                                 const std::string& name,
                                 HighsLogOptions& log_options,
                                 std::vector<OptionRecord*>& option_records,
                                 const char* value);

OptionStatus setLocalOptionValue(OptionRecordBool& option, const bool value);
OptionStatus setLocalOptionValue(const HighsLogOptions& report_log_options,
                                 OptionRecordInt& option, const HighsInt value);
OptionStatus setLocalOptionValue(const HighsLogOptions& report_log_options,
                                 OptionRecordDouble& option,
                                 const double value);
OptionStatus setLocalOptionValue(const HighsLogOptions& report_log_options,
                                 OptionRecordString& option,
                                 std::string const value);

OptionStatus passLocalOptions(const HighsLogOptions& report_log_options,
                              const HighsOptions& from_options,
                              HighsOptions& to_options);

OptionStatus getLocalOptionValues(
    const HighsLogOptions& report_log_options, const std::string& name,
    const std::vector<OptionRecord*>& option_records, bool* current_value,
    bool* default_value = nullptr);
OptionStatus getLocalOptionValues(
    const HighsLogOptions& report_log_options, const std::string& name,
    const std::vector<OptionRecord*>& option_records, HighsInt* current_value,
    HighsInt* min_value = nullptr, HighsInt* max_value = nullptr,
    HighsInt* default_value = nullptr);
OptionStatus getLocalOptionValues(
    const HighsLogOptions& report_log_options, const std::string& name,
    const std::vector<OptionRecord*>& option_records, double* current_value,
    double* min_value = nullptr, double* max_value = nullptr,
    double* default_value = nullptr);
OptionStatus getLocalOptionValues(
    const HighsLogOptions& report_log_options, const std::string& name,
    const std::vector<OptionRecord*>& option_records,
    std::string* current_value, std::string* default_value = nullptr);

OptionStatus getLocalOptionType(
    const HighsLogOptions& report_log_options, const std::string& name,
    const std::vector<OptionRecord*>& option_records,
    HighsOptionType* type = nullptr);

void resetLocalOptions(std::vector<OptionRecord*>& option_records);

HighsStatus writeOptionsToFile(
    FILE* file, const std::vector<OptionRecord*>& option_records,
    const bool report_only_deviations = false,
    const HighsFileType file_type = HighsFileType::kFull);
void reportOptions(FILE* file, const std::vector<OptionRecord*>& option_records,
                   const bool report_only_deviations = false,
                   const HighsFileType file_type = HighsFileType::kFull);
void reportOption(FILE* file, const OptionRecordBool& option,
                  const bool report_only_deviations,
                  const HighsFileType file_type);
void reportOption(FILE* file, const OptionRecordInt& option,
                  const bool report_only_deviations,
                  const HighsFileType file_type);
void reportOption(FILE* file, const OptionRecordDouble& option,
                  const bool report_only_deviations,
                  const HighsFileType file_type);
void reportOption(FILE* file, const OptionRecordString& option,
                  const bool report_only_deviations,
                  const HighsFileType file_type);

const string kSimplexString = "simplex";
const string kIpmString = "ipm";
const string kPdlpString = "pdlp";

const HighsInt kKeepNRowsDeleteRows = -1;
const HighsInt kKeepNRowsDeleteEntries = 0;
const HighsInt kKeepNRowsKeepRows = 1;

// Strings for command line options
const string kModelFileString = "model_file";
const string kPresolveString = "presolve";
const string kSolverString = "solver";
const string kParallelString = "parallel";
const string kRunCrossoverString = "run_crossover";
const string kTimeLimitString = "time_limit";
const string kOptionsFileString = "options_file";
const string kRandomSeedString = "random_seed";
const string kSolutionFileString = "solution_file";
const string kRangingString = "ranging";
const string kVersionString = "version";
const string kWriteModelFileString = "write_model_file";
const string kWritePresolvedModelFileString = "write_presolved_model_file";
const string kReadSolutionFileString = "read_solution_file";

// String for HiGHS log file option
const string kLogFileString = "log_file";

struct HighsOptionsStruct {
  // Run-time options read from the command line
  std::string presolve;
  std::string solver;
  std::string parallel;
  std::string run_crossover;
  double time_limit;
  std::string solution_file;
  std::string write_model_file;
  HighsInt random_seed;
  std::string ranging;

  // Options read from the file
  double infinite_cost;
  double infinite_bound;
  double small_matrix_value;
  double large_matrix_value;
  double primal_feasibility_tolerance;
  double dual_feasibility_tolerance;
  double ipm_optimality_tolerance;
  double objective_bound;
  double objective_target;
  HighsInt threads;
  HighsInt user_bound_scale;
  HighsInt user_cost_scale;
  HighsInt highs_debug_level;
  HighsInt highs_analysis_level;
  HighsInt simplex_strategy;
  HighsInt simplex_scale_strategy;
  HighsInt simplex_crash_strategy;
  HighsInt simplex_dual_edge_weight_strategy;
  HighsInt simplex_primal_edge_weight_strategy;
  HighsInt simplex_iteration_limit;
  HighsInt simplex_update_limit;
  HighsInt simplex_min_concurrency;
  HighsInt simplex_max_concurrency;

  std::string log_file;
  bool write_model_to_file;
  bool write_presolved_model_to_file;
  bool write_solution_to_file;
  HighsInt write_solution_style;
  HighsInt glpsol_cost_row_location;
  std::string write_presolved_model_file;

  // Control of HiGHS log
  bool output_flag;
  bool log_to_console;

  // Options for IPM solver
  HighsInt ipm_iteration_limit;

  // Options for PDLP solver
  bool pdlp_native_termination;
  bool pdlp_scaling;
  HighsInt pdlp_iteration_limit;
  HighsInt pdlp_e_restart_method;
  double pdlp_d_gap_tol;

  // Options for QP solver
  HighsInt qp_iteration_limit;
  HighsInt qp_nullspace_limit;

  // Options for IIS calculation
  HighsInt iis_strategy;

  // Advanced options
  HighsInt log_dev_level;
  bool log_githash;
  bool solve_relaxation;
  bool allow_unbounded_or_infeasible;
  bool use_implied_bounds_from_presolve;
  bool lp_presolve_requires_basis_postsolve;
  bool mps_parser_type_free;
  HighsInt keep_n_rows;
  HighsInt cost_scale_factor;
  HighsInt allowed_matrix_scale_factor;
  HighsInt allowed_cost_scale_factor;
  HighsInt ipx_dualize_strategy;
  HighsInt simplex_dualize_strategy;
  HighsInt simplex_permute_strategy;
  HighsInt max_dual_simplex_cleanup_level;
  HighsInt max_dual_simplex_phase1_cleanup_level;
  HighsInt simplex_price_strategy;
  HighsInt simplex_unscaled_solution_strategy;
  HighsInt presolve_reduction_limit;
  HighsInt restart_presolve_reduction_limit;
  HighsInt presolve_substitution_maxfillin;
  HighsInt presolve_rule_off;
  bool presolve_rule_logging;
  bool presolve_remove_slacks;
  bool simplex_initial_condition_check;
  bool no_unnecessary_rebuild_refactor;
  double simplex_initial_condition_tolerance;
  double rebuild_refactor_solution_error_tolerance;
  double dual_steepest_edge_weight_error_tolerance;
  double dual_steepest_edge_weight_log_error_threshold;
  double dual_simplex_cost_perturbation_multiplier;
  double primal_simplex_bound_perturbation_multiplier;
  double dual_simplex_pivot_growth_tolerance;
  double presolve_pivot_threshold;
  double factor_pivot_threshold;
  double factor_pivot_tolerance;
  double start_crossover_tolerance;
  bool less_infeasible_DSE_check;
  bool less_infeasible_DSE_choose_row;
  bool use_original_HFactor_logic;
  bool run_centring;
  double primal_residual_tolerance;
  double dual_residual_tolerance;
  HighsInt max_centring_steps;
  double centring_ratio_tolerance;

  // Options for iCrash
  bool icrash;
  bool icrash_dualize;
  std::string icrash_strategy;
  double icrash_starting_weight;
  HighsInt icrash_iterations;
  HighsInt icrash_approx_iter;
  bool icrash_exact;
  bool icrash_breakpoints;

  // Options for MIP solver
  bool mip_detect_symmetry;
  bool mip_allow_restart;
  HighsInt mip_max_nodes;
  HighsInt mip_max_stall_nodes;
  HighsInt mip_max_start_nodes;
  HighsInt mip_max_leaves;
  HighsInt mip_max_improving_sols;
  HighsInt mip_lp_age_limit;
  HighsInt mip_pool_age_limit;
  HighsInt mip_pool_soft_limit;
  HighsInt mip_pscost_minreliable;
  HighsInt mip_min_cliquetable_entries_for_parallelism;
  HighsInt mip_report_level;
  double mip_feasibility_tolerance;
  double mip_rel_gap;
  double mip_abs_gap;
  double mip_heuristic_effort;
  double mip_min_logging_interval;
#ifdef HIGHS_DEBUGSOL
  std::string mip_debug_solution_file;
#endif
  bool mip_improving_solution_save;
  bool mip_improving_solution_report_sparse;
  std::string mip_improving_solution_file;

  // Logging callback identifiers
  HighsLogOptions log_options;
  virtual ~HighsOptionsStruct() {}

  HighsOptionsStruct()
      : presolve(""),
        solver(""),
        parallel(""),
        run_crossover(""),
        time_limit(0.0),
        solution_file(""),
        write_model_file(""),
        random_seed(0),
        ranging(""),
        infinite_cost(0.0),
        infinite_bound(0.0),
        small_matrix_value(0.0),
        large_matrix_value(0.0),
        primal_feasibility_tolerance(0.0),
        dual_feasibility_tolerance(0.0),
        ipm_optimality_tolerance(0.0),
        objective_bound(0.0),
        objective_target(0.0),
        threads(0),
        user_bound_scale(0),
        user_cost_scale(0),
        highs_debug_level(0),
        highs_analysis_level(0),
        simplex_strategy(0),
        simplex_scale_strategy(0),
        simplex_crash_strategy(0),
        simplex_dual_edge_weight_strategy(0),
        simplex_primal_edge_weight_strategy(0),
        simplex_iteration_limit(0),
        simplex_update_limit(0),
        simplex_min_concurrency(0),
        simplex_max_concurrency(0),
        log_file(""),
        write_model_to_file(false),
        write_presolved_model_to_file(false),
        write_solution_to_file(false),
        write_solution_style(0),
        glpsol_cost_row_location(0),
        write_presolved_model_file(""),
        output_flag(false),
        log_to_console(false),
        ipm_iteration_limit(0),
        pdlp_native_termination(false),
        pdlp_scaling(false),
        pdlp_iteration_limit(0),
        pdlp_e_restart_method(0),
        pdlp_d_gap_tol(0.0),
        qp_iteration_limit(0),
        qp_nullspace_limit(0),
        log_dev_level(0),
        log_githash(false),
        solve_relaxation(false),
        allow_unbounded_or_infeasible(false),
        use_implied_bounds_from_presolve(false),
        lp_presolve_requires_basis_postsolve(false),
        mps_parser_type_free(false),
        keep_n_rows(0),
        cost_scale_factor(0),
        allowed_matrix_scale_factor(0),
        allowed_cost_scale_factor(0),
        ipx_dualize_strategy(0),
        simplex_dualize_strategy(0),
        simplex_permute_strategy(0),
        max_dual_simplex_cleanup_level(0),
        max_dual_simplex_phase1_cleanup_level(0),
        simplex_price_strategy(0),
        simplex_unscaled_solution_strategy(0),
        presolve_reduction_limit(0),
        restart_presolve_reduction_limit(0),
        presolve_substitution_maxfillin(0),
        presolve_rule_off(0),
        presolve_rule_logging(false),
        presolve_remove_slacks(false),
        simplex_initial_condition_check(false),
        no_unnecessary_rebuild_refactor(false),
        simplex_initial_condition_tolerance(0.0),
        rebuild_refactor_solution_error_tolerance(0.0),
        dual_steepest_edge_weight_error_tolerance(0.0),
        dual_steepest_edge_weight_log_error_threshold(0.0),
        dual_simplex_cost_perturbation_multiplier(0.0),
        primal_simplex_bound_perturbation_multiplier(0.0),
        dual_simplex_pivot_growth_tolerance(0.0),
        presolve_pivot_threshold(0.0),
        factor_pivot_threshold(0.0),
        factor_pivot_tolerance(0.0),
        start_crossover_tolerance(0.0),
        less_infeasible_DSE_check(false),
        less_infeasible_DSE_choose_row(false),
        use_original_HFactor_logic(false),
        run_centring(false),
        primal_residual_tolerance(0.0),
        dual_residual_tolerance(0.0),
        max_centring_steps(0),
        centring_ratio_tolerance(0.0),
        icrash(false),
        icrash_dualize(false),
        icrash_strategy(""),
        icrash_starting_weight(0.0),
        icrash_iterations(0),
        icrash_approx_iter(0),
        icrash_exact(false),
        icrash_breakpoints(false),
        mip_detect_symmetry(false),
        mip_allow_restart(false),
        mip_max_nodes(0),
        mip_max_stall_nodes(0),
        mip_max_start_nodes(0),
        mip_max_leaves(0),
        mip_max_improving_sols(0),
        mip_lp_age_limit(0),
        mip_pool_age_limit(0),
        mip_pool_soft_limit(0),
        mip_pscost_minreliable(0),
        mip_min_cliquetable_entries_for_parallelism(0),
        mip_report_level(0),
        mip_feasibility_tolerance(0.0),
        mip_rel_gap(0.0),
        mip_abs_gap(0.0),
        mip_heuristic_effort(0.0),
        mip_min_logging_interval(0.0),
#ifdef HIGHS_DEBUGSOL
        mip_debug_solution_file(""),
#endif
        mip_improving_solution_save(false),
        mip_improving_solution_report_sparse(false),
        // clang-format off
	mip_improving_solution_file("") {};
  // clang-format on
};

// For now, but later change so HiGHS properties are string based so that new
// options (for debug and testing too) can be added easily. The options below
// are just what has been used to parse options from argv.
// todo: when creating the new options don't forget underscores for class
// variables but no underscores for struct
class HighsOptions : public HighsOptionsStruct {
 public:
  HighsOptions() {
    initRecords();
    setLogOptions();
  }

  HighsOptions(const HighsOptions& options) {
    initRecords();
    HighsOptionsStruct::operator=(options);
    setLogOptions();
  }

  HighsOptions(HighsOptions&& options) {
    records = std::move(options.records);
    HighsOptionsStruct::operator=(std::move(options));
    this->log_options.log_stream = options.log_options.log_stream;
    setLogOptions();
  }

  const HighsOptions& operator=(const HighsOptions& other) {
    if (&other != this) {
      if ((HighsInt)records.size() == 0) initRecords();
      HighsOptionsStruct::operator=(other);
      this->log_options.log_stream = other.log_options.log_stream;
      setLogOptions();
    }
    return *this;
  }

  const HighsOptions& operator=(HighsOptions&& other) {
    if (&other != this) {
      if ((HighsInt)records.size() == 0) initRecords();
      HighsOptionsStruct::operator=(other);
      this->log_options.log_stream = other.log_options.log_stream;
      setLogOptions();
    }
    return *this;
  }

  virtual ~HighsOptions() {
    if (records.size() > 0) deleteRecords();
  }

 private:
  void initRecords() {
    OptionRecordBool* record_bool;
    OptionRecordInt* record_int;
    OptionRecordDouble* record_double;
    OptionRecordString* record_string;
    bool advanced = false;
    const bool now_advanced = true;
    // Options read from the command line
    record_string = new OptionRecordString(
        kPresolveString, "Presolve option: \"off\", \"choose\" or \"on\"",
        advanced, &presolve, kHighsChooseString);
    records.push_back(record_string);

    record_string = new OptionRecordString(
        kSolverString,
        "Solver option: \"simplex\", \"choose\", \"ipm\" or \"pdlp\". If "
        "\"simplex\"/\"ipm\"/\"pdlp\" is chosen then, for a MIP (QP) the "
        "integrality "
        "constraint (quadratic term) will be ignored",
        advanced, &solver, kHighsChooseString);
    records.push_back(record_string);

    record_string = new OptionRecordString(
        kParallelString, "Parallel option: \"off\", \"choose\" or \"on\"",
        advanced, &parallel, kHighsChooseString);
    records.push_back(record_string);

    record_string = new OptionRecordString(
        kRunCrossoverString, "Run IPM crossover: \"off\", \"choose\" or \"on\"",
        advanced, &run_crossover, kHighsOnString);
    records.push_back(record_string);

    record_double =
        new OptionRecordDouble(kTimeLimitString, "Time limit (seconds)",
                               advanced, &time_limit, 0, kHighsInf, kHighsInf);
    records.push_back(record_double);

    record_string =
        new OptionRecordString(kRangingString,
                               "Compute cost, bound, RHS and basic solution "
                               "ranging: \"off\" or \"on\"",
                               advanced, &ranging, kHighsOffString);
    records.push_back(record_string);
    //
    // Options read from the file
    record_double = new OptionRecordDouble(
        "infinite_cost",
        "Limit on |cost coefficient|: values greater than or equal to "
        "this will be treated as infinite",
        advanced, &infinite_cost, 1e15, 1e20, kHighsInf);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "infinite_bound",
        "Limit on |constraint bound|: values greater than or equal to "
        "this will be treated as infinite",
        advanced, &infinite_bound, 1e15, 1e20, kHighsInf);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "small_matrix_value",
        "Lower limit on |matrix entries|: values less than or equal to this "
        "will be "
        "treated as zero",
        advanced, &small_matrix_value, 1e-12, 1e-9, kHighsInf);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "large_matrix_value",
        "Upper limit on |matrix entries|: values greater than or equal to "
        "this will be treated as infinite",
        advanced, &large_matrix_value, 1e0, 1e15, kHighsInf);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "primal_feasibility_tolerance", "Primal feasibility tolerance",
        advanced, &primal_feasibility_tolerance, 1e-10, 1e-7, kHighsInf);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "dual_feasibility_tolerance", "Dual feasibility tolerance", advanced,
        &dual_feasibility_tolerance, 1e-10, 1e-7, kHighsInf);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "ipm_optimality_tolerance", "IPM optimality tolerance", advanced,
        &ipm_optimality_tolerance, 1e-12, 1e-8, kHighsInf);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "objective_bound",
        "Objective bound for termination of the dual simplex solver", advanced,
        &objective_bound, -kHighsInf, kHighsInf, kHighsInf);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "objective_target",
        "Objective target for termination of the MIP solver", advanced,
        //"primal simplex and "
        &objective_target, -kHighsInf, -kHighsInf, kHighsInf);
    records.push_back(record_double);

    record_int =
        new OptionRecordInt(kRandomSeedString, "Random seed used in HiGHS",
                            advanced, &random_seed, 0, 0, kHighsIInf);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "threads", "Number of threads used by HiGHS (0: automatic)", advanced,
        &threads, 0, 0, kHighsIInf);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "user_bound_scale", "Exponent of power-of-two bound scaling for model",
        advanced, &user_bound_scale, -kHighsIInf, 0, kHighsIInf);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "user_cost_scale", "Exponent of power-of-two cost scaling for model",
        advanced, &user_cost_scale, -kHighsIInf, 0, kHighsIInf);
    records.push_back(record_int);

    record_int = new OptionRecordInt("highs_debug_level",
                                     "Debugging level in HiGHS", now_advanced,
                                     &highs_debug_level, kHighsDebugLevelMin,
                                     kHighsDebugLevelMin, kHighsDebugLevelMax);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "highs_analysis_level", "Analysis level in HiGHS", now_advanced,
        &highs_analysis_level, kHighsAnalysisLevelMin, kHighsAnalysisLevelMin,
        kHighsAnalysisLevelMax);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "simplex_strategy",
        "Strategy for simplex solver 0 => Choose; 1 => Dual (serial); 2 => "
        "Dual (PAMI); 3 => Dual (SIP); 4 => Primal",
        advanced, &simplex_strategy, kSimplexStrategyMin, kSimplexStrategyDual,
        kSimplexStrategyMax);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "simplex_scale_strategy",
        "Simplex scaling strategy: off / choose / equilibration / forced "
        "equilibration / max value 0 / max value 1 (0/1/2/3/4/5)",
        advanced, &simplex_scale_strategy, kSimplexScaleStrategyMin,
        kSimplexScaleStrategyChoose, kSimplexScaleStrategyMax);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "simplex_crash_strategy",
        "Strategy for simplex crash: off / LTSSF / Bixby (0/1/2)", now_advanced,
        &simplex_crash_strategy, kSimplexCrashStrategyMin,
        kSimplexCrashStrategyOff, kSimplexCrashStrategyMax);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "simplex_dual_edge_weight_strategy",
        "Strategy for simplex dual edge weights: Choose / "
        "Dantzig / Devex / Steepest "
        "Edge (-1/0/1/2)",
        advanced, &simplex_dual_edge_weight_strategy,
        kSimplexEdgeWeightStrategyMin, kSimplexEdgeWeightStrategyChoose,
        kSimplexEdgeWeightStrategyMax);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "simplex_primal_edge_weight_strategy",
        "Strategy for simplex primal edge weights: Choose "
        "/ Dantzig / Devex / Steepest "
        "Edge (-1/0/1/2)",
        advanced, &simplex_primal_edge_weight_strategy,
        kSimplexEdgeWeightStrategyMin, kSimplexEdgeWeightStrategyChoose,
        kSimplexEdgeWeightStrategyMax);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "simplex_iteration_limit",
        "Iteration limit for simplex solver when solving LPs, but not "
        "subproblems in the MIP solver",
        advanced, &simplex_iteration_limit, 0, kHighsIInf, kHighsIInf);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "simplex_update_limit",
        "Limit on the number of simplex UPDATE operations", advanced,
        &simplex_update_limit, 0, 5000, kHighsIInf);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "simplex_min_concurrency",
        "Minimum level of concurrency in parallel simplex", now_advanced,
        &simplex_min_concurrency, 1, 1, kSimplexConcurrencyLimit);
    records.push_back(record_int);

    record_int =
        new OptionRecordInt("simplex_max_concurrency",
                            "Maximum level of concurrency in parallel simplex",
                            advanced, &simplex_max_concurrency, 1,
                            kSimplexConcurrencyLimit, kSimplexConcurrencyLimit);
    records.push_back(record_int);

    record_bool =
        new OptionRecordBool("output_flag", "Enables or disables solver output",
                             advanced, &output_flag, true);
    records.push_back(record_bool);

    record_bool = new OptionRecordBool("log_to_console",
                                       "Enables or disables console logging",
                                       advanced, &log_to_console, true);
    records.push_back(record_bool);

    record_string =
        new OptionRecordString(kSolutionFileString, "Solution file", advanced,
                               &solution_file, kHighsFilenameDefault);
    records.push_back(record_string);

    record_string = new OptionRecordString(kLogFileString, "Log file", advanced,
                                           &log_file, "");
    records.push_back(record_string);

    record_bool =
        new OptionRecordBool("write_solution_to_file",
                             "Write the primal and dual solution to a file",
                             advanced, &write_solution_to_file, false);
    records.push_back(record_bool);

    record_int = new OptionRecordInt(
        "write_solution_style",
        "Style of solution file (raw = computer-readable, "
        "pretty = human-readable): "
        "-1 => HiGHS old raw (deprecated); 0 => HiGHS raw; "
        "1 => HiGHS pretty; 2 => Glpsol raw; 3 => Glpsol pretty; "
        "4 => HiGHS sparse raw",
        advanced, &write_solution_style, kSolutionStyleMin, kSolutionStyleRaw,
        kSolutionStyleMax);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "glpsol_cost_row_location",
        "Location of cost row for Glpsol file: "
        "-2 => Last; -1 => None; 0 => None if empty, otherwise data file "
        "location; 1 <= n <= num_row => Location n; n > "
        "num_row => Last",
        advanced, &glpsol_cost_row_location, kGlpsolCostRowLocationMin, 0,
        kHighsIInf);
    records.push_back(record_int);

    record_bool = new OptionRecordBool("icrash", "Run iCrash", now_advanced,
                                       &icrash, false);
    records.push_back(record_bool);

    record_bool =
        new OptionRecordBool("icrash_dualize", "Dualize strategy for iCrash",
                             now_advanced, &icrash_dualize, false);
    records.push_back(record_bool);

    record_string =
        new OptionRecordString("icrash_strategy", "Strategy for iCrash",
                               now_advanced, &icrash_strategy, "ICA");
    records.push_back(record_string);

    record_double = new OptionRecordDouble(
        "icrash_starting_weight", "iCrash starting weight", now_advanced,
        &icrash_starting_weight, 1e-10, 1e-3, 1e50);
    records.push_back(record_double);

    record_int =
        new OptionRecordInt("icrash_iterations", "iCrash iterations",
                            now_advanced, &icrash_iterations, 0, 30, 200);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "icrash_approx_iter", "iCrash approximate minimization iterations",
        now_advanced, &icrash_approx_iter, 0, 50, 100);
    records.push_back(record_int);

    record_bool = new OptionRecordBool("icrash_exact",
                                       "Exact subproblem solution for iCrash",
                                       now_advanced, &icrash_exact, false);
    records.push_back(record_bool);

    record_bool = new OptionRecordBool(
        "icrash_breakpoints", "Exact subproblem solution for iCrash",
        now_advanced, &icrash_breakpoints, false);
    records.push_back(record_bool);

    record_string = new OptionRecordString(
        kWriteModelFileString, "Write model file", advanced, &write_model_file,
        kHighsFilenameDefault);
    records.push_back(record_string);

    record_bool =
        new OptionRecordBool("write_model_to_file", "Write the model to a file",
                             advanced, &write_model_to_file, false);
    records.push_back(record_bool);

    record_string = new OptionRecordString(
        kWritePresolvedModelFileString, "Write presolved model file", advanced,
        &write_presolved_model_file, kHighsFilenameDefault);
    records.push_back(record_string);

    record_bool = new OptionRecordBool(
        "write_presolved_model_to_file", "Write the presolved model to a file",
        advanced, &write_presolved_model_to_file, false);
    records.push_back(record_bool);

    record_bool = new OptionRecordBool(
        "mip_detect_symmetry", "Whether MIP symmetry should be detected",
        advanced, &mip_detect_symmetry, true);
    records.push_back(record_bool);

    record_bool = new OptionRecordBool("mip_allow_restart",
                                       "Whether MIP restart is permitted",
                                       advanced, &mip_allow_restart, true);
    records.push_back(record_bool);

    record_int = new OptionRecordInt("mip_max_nodes",
                                     "MIP solver max number of nodes", advanced,
                                     &mip_max_nodes, 0, kHighsIInf, kHighsIInf);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "mip_max_stall_nodes",
        "MIP solver max number of nodes where estimate is above cutoff bound",
        advanced, &mip_max_stall_nodes, 0, kHighsIInf, kHighsIInf);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "mip_max_start_nodes",
        "MIP solver max number of nodes when completing a partial MIP start",
        advanced, &mip_max_start_nodes, 0, 500, kHighsIInf);
    records.push_back(record_int);

#ifdef HIGHS_DEBUGSOL
    record_string = new OptionRecordString(
        "mip_debug_solution_file",
        "Solution file for debug solution of the MIP solver", advanced,
        &mip_debug_solution_file, kHighsFilenameDefault);
    records.push_back(record_string);
#endif

    record_bool =
        new OptionRecordBool("mip_improving_solution_save",
                             "Whether improving MIP solutions should be saved",
                             advanced, &mip_improving_solution_save, false);
    records.push_back(record_bool);

    record_bool = new OptionRecordBool(
        "mip_improving_solution_report_sparse",
        "Whether improving MIP solutions should be reported in sparse format",
        advanced, &mip_improving_solution_report_sparse, false);
    records.push_back(record_bool);

    record_string = new OptionRecordString(
        "mip_improving_solution_file",
        "File for reporting improving MIP solutions: not reported for an empty "
        "string \\\"\\\"",
        advanced, &mip_improving_solution_file, kHighsFilenameDefault);
    records.push_back(record_string);

    record_int = new OptionRecordInt(
        "mip_max_leaves", "MIP solver max number of leaf nodes", advanced,
        &mip_max_leaves, 0, kHighsIInf, kHighsIInf);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "mip_max_improving_sols",
        "Limit on the number of improving solutions found to stop the MIP "
        "solver prematurely",
        advanced, &mip_max_improving_sols, 1, kHighsIInf, kHighsIInf);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "mip_lp_age_limit",
        "Maximal age of dynamic LP rows before "
        "they are removed from the LP relaxation in the MIP solver",
        advanced, &mip_lp_age_limit, 0, 10,
        std::numeric_limits<int16_t>::max());
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "mip_pool_age_limit",
        "Maximal age of rows in the MIP solver cutpool before they are deleted",
        advanced, &mip_pool_age_limit, 0, 30, 1000);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "mip_pool_soft_limit",
        "Soft limit on the number of rows in the "
        "MIP solver cutpool for dynamic age adjustment",
        advanced, &mip_pool_soft_limit, 1, 10000, kHighsIInf);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "mip_pscost_minreliable",
        "Minimal number of observations before "
        "MIP solver pseudo costs are considered reliable",
        advanced, &mip_pscost_minreliable, 0, 8, kHighsIInf);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "mip_min_cliquetable_entries_for_parallelism",
        "Minimal number of entries in the MIP solver cliquetable before "
        "neighbourhood "
        "queries of the conflict graph use parallel processing",
        advanced, &mip_min_cliquetable_entries_for_parallelism, 0, 100000,
        kHighsIInf);
    records.push_back(record_int);

    record_int =
        new OptionRecordInt("mip_report_level", "MIP solver reporting level",
                            now_advanced, &mip_report_level, 0, 1, 2);
    records.push_back(record_int);

    record_double = new OptionRecordDouble(
        "mip_feasibility_tolerance", "MIP feasibility tolerance", advanced,
        &mip_feasibility_tolerance, 1e-10, 1e-6, kHighsInf);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "mip_heuristic_effort", "Effort spent for MIP heuristics", advanced,
        &mip_heuristic_effort, 0.0, 0.05, 1.0);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "mip_rel_gap",
        "Tolerance on relative gap, |ub-lb|/|ub|, to determine whether "
        "optimality has been reached for a MIP instance",
        advanced, &mip_rel_gap, 0.0, 1e-4, kHighsInf);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "mip_abs_gap",
        "Tolerance on absolute gap of MIP, |ub-lb|, to determine whether "
        "optimality has been reached for a MIP instance",
        advanced, &mip_abs_gap, 0.0, 1e-6, kHighsInf);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "mip_min_logging_interval", "MIP minimum logging interval", advanced,
        &mip_min_logging_interval, 0, 5, kHighsInf);
    records.push_back(record_double);

    record_int = new OptionRecordInt(
        "ipm_iteration_limit", "Iteration limit for IPM solver", advanced,
        &ipm_iteration_limit, 0, kHighsIInf, kHighsIInf);
    records.push_back(record_int);

    record_bool = new OptionRecordBool(
        "pdlp_native_termination",
        "Use native termination for PDLP solver: Default = false", advanced,
        &pdlp_native_termination, false);
    records.push_back(record_bool);

    record_bool = new OptionRecordBool(
        "pdlp_scaling", "Scaling option for PDLP solver: Default = true",
        advanced, &pdlp_scaling, true);
    records.push_back(record_bool);

    record_int = new OptionRecordInt(
        "pdlp_iteration_limit", "Iteration limit for PDLP solver", advanced,
        &pdlp_iteration_limit, 0, kHighsIInf, kHighsIInf);
    records.push_back(record_int);

    record_int = new OptionRecordInt("pdlp_e_restart_method",
                                     "Restart mode for PDLP solver: 0 => none; "
                                     "1 => GPU (default); 2 => CPU ",
                                     advanced, &pdlp_e_restart_method, 0, 1, 2);
    records.push_back(record_int);

    record_double = new OptionRecordDouble(
        "pdlp_d_gap_tol",
        "Duality gap tolerance for PDLP solver: Default = 1e-4", advanced,
        &pdlp_d_gap_tol, 1e-12, 1e-4, kHighsInf);
    records.push_back(record_double);

    record_int = new OptionRecordInt(
        "qp_iteration_limit", "Iteration limit for QP solver", advanced,
        &qp_iteration_limit, 0, kHighsIInf, kHighsIInf);
    records.push_back(record_int);

    record_int = new OptionRecordInt("qp_nullspace_limit",
                                     "Nullspace limit for QP solver", advanced,
                                     &qp_nullspace_limit, 0, 4000, kHighsIInf);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "iis_strategy",
        "Strategy for IIS calculation: "
        //        "Use LP and p"
        "Prioritise rows (default) / "
        //        "Use LP and p"
        "Prioritise columns"
        //        "Use unbounded dual ray and prioritise low number of rows
        //        (default) / " "Use ray and prioritise low numbers of columns "
        " (0/1"
        //        "/2/3)",
        ")",
        advanced, &iis_strategy, kIisStrategyMin, kIisStrategyFromLpRowPriority,
        kIisStrategyMax);
    records.push_back(record_int);

    // Fix the number of user settable options
    num_user_settable_options_ = static_cast<HighsInt>(records.size());

    // Advanced options
    advanced = true;

    record_int = new OptionRecordInt(
        "log_dev_level",
        "Output development messages: 0 => none; 1 => info; 2 => verbose",
        advanced, &log_dev_level, kHighsLogDevLevelMin, kHighsLogDevLevelNone,
        kHighsLogDevLevelMax);
    records.push_back(record_int);

    record_bool = new OptionRecordBool("log_githash", "Log the githash",
                                       advanced, &log_githash, true);
    records.push_back(record_bool);

    record_bool = new OptionRecordBool(
        "solve_relaxation", "Solve the relaxation of discrete model components",
        advanced, &solve_relaxation, false);
    records.push_back(record_bool);

    record_bool =
        new OptionRecordBool("allow_unbounded_or_infeasible",
                             "Allow ModelStatus::kUnboundedOrInfeasible",
                             advanced, &allow_unbounded_or_infeasible, false);
    records.push_back(record_bool);

    record_bool = new OptionRecordBool(
        "use_implied_bounds_from_presolve",
        "Use relaxed implied bounds from presolve", advanced,
        &use_implied_bounds_from_presolve, false);
    records.push_back(record_bool);

    record_bool = new OptionRecordBool(
        "lp_presolve_requires_basis_postsolve",
        "Prevents LP presolve steps for which postsolve cannot maintain a "
        "basis",
        advanced, &lp_presolve_requires_basis_postsolve, true);
    records.push_back(record_bool);

    record_bool = new OptionRecordBool("mps_parser_type_free",
                                       "Use the free format MPS file reader",
                                       advanced, &mps_parser_type_free, true);
    records.push_back(record_bool);

    record_int =
        new OptionRecordInt("keep_n_rows",
                            "For multiple N-rows in MPS files: delete rows / "
                            "delete entries / keep rows (-1/0/1)",
                            advanced, &keep_n_rows, kKeepNRowsDeleteRows,
                            kKeepNRowsDeleteRows, kKeepNRowsKeepRows);
    records.push_back(record_int);

    record_int =
        new OptionRecordInt("cost_scale_factor", "Scaling factor for costs",
                            advanced, &cost_scale_factor, -20, 0, 20);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "allowed_matrix_scale_factor",
        "Largest power-of-two factor permitted when "
        "scaling the constraint matrix",
        advanced, &allowed_matrix_scale_factor, 0,
        kDefaultAllowedMatrixPow2Scale, kMaxAllowedMatrixPow2Scale);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "allowed_cost_scale_factor",
        "Largest power-of-two factor permitted when scaling the costs",
        advanced, &allowed_cost_scale_factor, 0, 0, 20);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "ipx_dualize_strategy", "Strategy for dualizing before IPX", advanced,
        &ipx_dualize_strategy, kIpxDualizeStrategyMin, kIpxDualizeStrategyLukas,
        kIpxDualizeStrategyMax);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "simplex_dualize_strategy", "Strategy for dualizing before simplex",
        advanced, &simplex_dualize_strategy, kHighsOptionOff, kHighsOptionOff,
        kHighsOptionOn);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "simplex_permute_strategy", "Strategy for permuting before simplex",
        advanced, &simplex_permute_strategy, kHighsOptionOff, kHighsOptionOff,
        kHighsOptionOn);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "max_dual_simplex_cleanup_level", "Max level of dual simplex cleanup",
        advanced, &max_dual_simplex_cleanup_level, 0, 1, kHighsIInf);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "max_dual_simplex_phase1_cleanup_level",
        "Max level of dual simplex phase 1 cleanup", advanced,
        &max_dual_simplex_phase1_cleanup_level, 0, 2, kHighsIInf);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "simplex_price_strategy", "Strategy for PRICE in simplex", advanced,
        &simplex_price_strategy, kSimplexPriceStrategyMin,
        kSimplexPriceStrategyRowSwitchColSwitch, kSimplexPriceStrategyMax);
    records.push_back(record_int);

    record_int =
        new OptionRecordInt("simplex_unscaled_solution_strategy",
                            "Strategy for solving unscaled LP in simplex",
                            advanced, &simplex_unscaled_solution_strategy,
                            kSimplexUnscaledSolutionStrategyMin,
                            kSimplexUnscaledSolutionStrategyRefine,
                            kSimplexUnscaledSolutionStrategyMax);
    records.push_back(record_int);

    record_bool =
        new OptionRecordBool("simplex_initial_condition_check",
                             "Perform initial basis condition check in simplex",
                             advanced, &simplex_initial_condition_check, true);
    records.push_back(record_bool);

    record_bool = new OptionRecordBool(
        "no_unnecessary_rebuild_refactor",
        "No unnecessary refactorization on simplex rebuild", advanced,
        &no_unnecessary_rebuild_refactor, true);
    records.push_back(record_bool);

    record_double = new OptionRecordDouble(
        "simplex_initial_condition_tolerance",
        "Tolerance on initial basis condition in simplex", advanced,
        &simplex_initial_condition_tolerance, 1.0, 1e14, kHighsInf);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "rebuild_refactor_solution_error_tolerance",
        "Tolerance on solution error when considering refactorization on "
        "simplex rebuild",
        advanced, &rebuild_refactor_solution_error_tolerance, -kHighsInf, 1e-8,
        kHighsInf);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "dual_steepest_edge_weight_error_tolerance",
        "Tolerance on dual steepest edge weight errors", advanced,
        &dual_steepest_edge_weight_error_tolerance, 0.0, kHighsInf, kHighsInf);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "dual_steepest_edge_weight_log_error_threshold",
        "Threshold on dual steepest edge weight errors for Devex switch",
        advanced, &dual_steepest_edge_weight_log_error_threshold, 1.0, 1e1,
        kHighsInf);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "dual_simplex_cost_perturbation_multiplier",
        "Dual simplex cost perturbation multiplier: 0 => no perturbation",
        advanced, &dual_simplex_cost_perturbation_multiplier, 0.0, 1.0,
        kHighsInf);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "primal_simplex_bound_perturbation_multiplier",
        "Primal simplex bound perturbation multiplier: 0 => no perturbation",
        advanced, &primal_simplex_bound_perturbation_multiplier, 0.0, 1.0,
        kHighsInf);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "dual_simplex_pivot_growth_tolerance",
        "Dual simplex pivot growth tolerance", advanced,
        &dual_simplex_pivot_growth_tolerance, 1e-12, 1e-9, kHighsInf);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "presolve_pivot_threshold",
        "Matrix factorization pivot threshold for substitutions in presolve",
        advanced, &presolve_pivot_threshold, kMinPivotThreshold, 0.01,
        kMaxPivotThreshold);
    records.push_back(record_double);

    record_int = new OptionRecordInt(
        "presolve_reduction_limit",
        "Limit on number of presolve reductions -1 => no limit", advanced,
        &presolve_reduction_limit, -1, -1, kHighsIInf);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "restart_presolve_reduction_limit",
        "Limit on number of further presolve reductions on restart in MIP "
        "solver -1 => no limit, otherwise, must be positive",
        advanced, &restart_presolve_reduction_limit, -1, -1, kHighsIInf);
    records.push_back(record_int);

    record_int = new OptionRecordInt(
        "presolve_rule_off", "Bit mask of presolve rules that are not allowed",
        advanced, &presolve_rule_off, 0, 0, kHighsIInf);
    records.push_back(record_int);

    record_bool = new OptionRecordBool(
        "presolve_rule_logging", "Log effectiveness of presolve rules for LP",
        advanced, &presolve_rule_logging, false);
    records.push_back(record_bool);

    record_bool = new OptionRecordBool("presolve_remove_slacks",
                                       "Remove slacks after presolve", advanced,
                                       &presolve_remove_slacks, false);
    records.push_back(record_bool);

    record_int = new OptionRecordInt(
        "presolve_substitution_maxfillin",
        "Maximal fillin allowed for substitutions in presolve", advanced,
        &presolve_substitution_maxfillin, 0, 10, kHighsIInf);
    records.push_back(record_int);

    record_double = new OptionRecordDouble(
        "factor_pivot_threshold", "Matrix factorization pivot threshold",
        advanced, &factor_pivot_threshold, kMinPivotThreshold,
        kDefaultPivotThreshold, kMaxPivotThreshold);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "factor_pivot_tolerance", "Matrix factorization pivot tolerance",
        advanced, &factor_pivot_tolerance, kMinPivotTolerance,
        kDefaultPivotTolerance, kMaxPivotTolerance);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "start_crossover_tolerance",
        "Tolerance to be satisfied before IPM crossover will start", advanced,
        &start_crossover_tolerance, 1e-12, 1e-8, kHighsInf);
    records.push_back(record_double);

    record_bool = new OptionRecordBool(
        "use_original_HFactor_logic",
        "Use original HFactor logic for sparse vs hyper-sparse TRANs", advanced,
        &use_original_HFactor_logic, true);
    records.push_back(record_bool);

    record_bool = new OptionRecordBool(
        "less_infeasible_DSE_check", "Check whether LP is candidate for LiDSE",
        advanced, &less_infeasible_DSE_check, true);
    records.push_back(record_bool);

    record_bool =
        new OptionRecordBool("less_infeasible_DSE_choose_row",
                             "Use LiDSE if LP has right properties", advanced,
                             &less_infeasible_DSE_choose_row, true);
    records.push_back(record_bool);

    record_bool =
        new OptionRecordBool("run_centring", "Perform centring steps or not",
                             advanced, &run_centring, false);
    records.push_back(record_bool);

    record_int =
        new OptionRecordInt("max_centring_steps",
                            "Maximum number of steps to use (default = 5) "
                            "when computing the analytic centre",
                            advanced, &max_centring_steps, 0, 5, kHighsIInf);
    records.push_back(record_int);

    record_double = new OptionRecordDouble(
        "centring_ratio_tolerance",
        "Centring stops when the ratio max(x_j*s_j) / min(x_j*s_j) is below "
        "this tolerance (default = 100)",
        advanced, &centring_ratio_tolerance, 0, 100, kHighsInf);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "primal_residual_tolerance", "Primal residual tolerance", advanced,
        &primal_residual_tolerance, 1e-10, 1e-7, kHighsInf);
    records.push_back(record_double);

    record_double = new OptionRecordDouble(
        "dual_residual_tolerance", "Dual residual tolerance", advanced,
        &dual_residual_tolerance, 1e-10, 1e-7, kHighsInf);
    records.push_back(record_double);

    // Set up the log_options aliases
    log_options.clear();
    log_options.log_stream =
        log_file.empty() ? nullptr : fopen(log_file.c_str(), "w");
    log_options.output_flag = &output_flag;
    log_options.log_to_console = &log_to_console;
    log_options.log_dev_level = &log_dev_level;
  }

  void deleteRecords() {
    for (size_t i = 0; i < records.size(); i++) delete records[i];
  }

 public:
  std::vector<OptionRecord*> records;
  HighsInt num_user_settable_options_;
  void setLogOptions();
};

#endif
