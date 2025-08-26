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
/**@file lp_data/HighsOptions.cpp
 * @brief
 */
#include "lp_data/HighsOptions.h"

#include <algorithm>
#include <cassert>
#include <cctype>

#include "util/stringutil.h"

// void setLogOptions();

void highsOpenLogFile(HighsLogOptions& log_options,
                      std::vector<OptionRecord*>& option_records,
                      const std::string log_file) {
  HighsInt index;
  OptionStatus status =
      getOptionIndex(log_options, "log_file", option_records, index);
  assert(status == OptionStatus::kOk);
  if (log_options.log_stream != NULL) {
    // Current log file stream is not null, so flush and close it
    fflush(log_options.log_stream);
    fclose(log_options.log_stream);
  }
  if (log_file.compare("")) {
    // New log file name is not empty, so open it, appending if
    // possible
    log_options.log_stream = fopen(log_file.c_str(), "a");
  } else {
    // New log file name is empty, so set the stream to null
    log_options.log_stream = NULL;
  }
  OptionRecordString& option = *(OptionRecordString*)option_records[index];
  option.assignvalue(log_file);
}

static std::string optionEntryTypeToString(const HighsOptionType type) {
  if (type == HighsOptionType::kBool) {
    return "bool";
  } else if (type == HighsOptionType::kInt) {
    return "HighsInt";
  } else if (type == HighsOptionType::kDouble) {
    return "double";
  } else {
    return "string";
  }
}

bool commandLineOffChooseOnOk(const HighsLogOptions& report_log_options,
                              const string& name, const string& value) {
  if (value == kHighsOffString || value == kHighsChooseString ||
      value == kHighsOnString)
    return true;
  highsLogUser(
      report_log_options, HighsLogType::kWarning,
      "Value \"%s\" for %s option is not one of \"%s\", \"%s\" or \"%s\"\n",
      value.c_str(), name.c_str(), kHighsOffString.c_str(),
      kHighsChooseString.c_str(), kHighsOnString.c_str());
  return false;
}

bool commandLineOffOnOk(const HighsLogOptions& report_log_options,
                        const string& name, const string& value) {
  if (value == kHighsOffString || value == kHighsOnString) return true;
  highsLogUser(report_log_options, HighsLogType::kWarning,
               "Value \"%s\" for %s option is not one of \"%s\" or \"%s\"\n",
               value.c_str(), name.c_str(), kHighsOffString.c_str(),
               kHighsOnString.c_str());
  return false;
}

bool commandLineSolverOk(const HighsLogOptions& report_log_options,
                         const string& value) {
  if (value == kSimplexString || value == kHighsChooseString ||
      value == kIpmString || value == kPdlpString)
    return true;
  highsLogUser(report_log_options, HighsLogType::kWarning,
               "Value \"%s\" for solver option is not one of \"%s\", \"%s\", "
               "\"%s\" or \"%s\"\n",
               value.c_str(), kSimplexString.c_str(),
               kHighsChooseString.c_str(), kIpmString.c_str(),
               kPdlpString.c_str());
  return false;
}

bool boolFromString(std::string value, bool& bool_value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  if (value == "t" || value == "true" || value == "1" || value == "on") {
    bool_value = true;
  } else if (value == "f" || value == "false" || value == "0" ||
             value == "off") {
    bool_value = false;
  } else {
    return false;
  }
  return true;
}

OptionStatus getOptionIndex(const HighsLogOptions& report_log_options,
                            const std::string& name,
                            const std::vector<OptionRecord*>& option_records,
                            HighsInt& index) {
  HighsInt num_options = option_records.size();
  for (index = 0; index < num_options; index++)
    if (option_records[index]->name == name) return OptionStatus::kOk;
  highsLogUser(report_log_options, HighsLogType::kError,
               "getOptionIndex: Option \"%s\" is unknown\n", name.c_str());
  return OptionStatus::kUnknownOption;
}

OptionStatus checkOptions(const HighsLogOptions& report_log_options,
                          const std::vector<OptionRecord*>& option_records) {
  bool error_found = false;
  HighsInt num_options = option_records.size();
  for (HighsInt index = 0; index < num_options; index++) {
    std::string name = option_records[index]->name;
    HighsOptionType type = option_records[index]->type;
    // Check that there are no other options with the same name
    for (HighsInt check_index = 0; check_index < num_options; check_index++) {
      if (check_index == index) continue;
      std::string check_name = option_records[check_index]->name;
      if (check_name == name) {
        highsLogUser(report_log_options, HighsLogType::kError,
                     "checkOptions: Option %" HIGHSINT_FORMAT
                     " (\"%s\") has the same name as "
                     "option %" HIGHSINT_FORMAT " \"%s\"\n",
                     index, name.c_str(), check_index, check_name.c_str());
        error_found = true;
      }
    }
    if (type == HighsOptionType::kBool) {
      // Check bool option
      OptionRecordBool& option = ((OptionRecordBool*)option_records[index])[0];
      // Check that there are no other options with the same value pointers
      bool* value_pointer = option.value;
      for (HighsInt check_index = 0; check_index < num_options; check_index++) {
        if (check_index == index) continue;
        if (option_records[check_index]->type == HighsOptionType::kBool) {
          OptionRecordBool& check_option =
              ((OptionRecordBool*)option_records[check_index])[0];
          if (check_option.value == value_pointer) {
            highsLogUser(report_log_options, HighsLogType::kError,
                         "checkOptions: Option %" HIGHSINT_FORMAT
                         " (\"%s\") has the same "
                         "value pointer as option %" HIGHSINT_FORMAT
                         " (\"%s\")\n",
                         index, option.name.c_str(), check_index,
                         check_option.name.c_str());
            error_found = true;
          }
        }
      }
    } else if (type == HighsOptionType::kInt) {
      // Check HighsInt option
      OptionRecordInt& option = ((OptionRecordInt*)option_records[index])[0];
      if (checkOption(report_log_options, option) != OptionStatus::kOk)
        error_found = true;
      // Check that there are no other options with the same value pointers
      HighsInt* value_pointer = option.value;
      for (HighsInt check_index = 0; check_index < num_options; check_index++) {
        if (check_index == index) continue;
        if (option_records[check_index]->type == HighsOptionType::kInt) {
          OptionRecordInt& check_option =
              ((OptionRecordInt*)option_records[check_index])[0];
          if (check_option.value == value_pointer) {
            highsLogUser(report_log_options, HighsLogType::kError,
                         "checkOptions: Option %" HIGHSINT_FORMAT
                         " (\"%s\") has the same "
                         "value pointer as option %" HIGHSINT_FORMAT
                         " (\"%s\")\n",
                         index, option.name.c_str(), check_index,
                         check_option.name.c_str());
            error_found = true;
          }
        }
      }
    } else if (type == HighsOptionType::kDouble) {
      // Check double option
      OptionRecordDouble& option =
          ((OptionRecordDouble*)option_records[index])[0];
      if (checkOption(report_log_options, option) != OptionStatus::kOk)
        error_found = true;
      // Check that there are no other options with the same value pointers
      double* value_pointer = option.value;
      for (HighsInt check_index = 0; check_index < num_options; check_index++) {
        if (check_index == index) continue;
        if (option_records[check_index]->type == HighsOptionType::kDouble) {
          OptionRecordDouble& check_option =
              ((OptionRecordDouble*)option_records[check_index])[0];
          if (check_option.value == value_pointer) {
            highsLogUser(report_log_options, HighsLogType::kError,
                         "checkOptions: Option %" HIGHSINT_FORMAT
                         " (\"%s\") has the same "
                         "value pointer as option %" HIGHSINT_FORMAT
                         " (\"%s\")\n",
                         index, option.name.c_str(), check_index,
                         check_option.name.c_str());
            error_found = true;
          }
        }
      }
    } else if (type == HighsOptionType::kString) {
      // Check string option
      OptionRecordString& option =
          ((OptionRecordString*)option_records[index])[0];
      // Check that there are no other options with the same value pointers
      std::string* value_pointer = option.value;
      for (HighsInt check_index = 0; check_index < num_options; check_index++) {
        if (check_index == index) continue;
        if (option_records[check_index]->type == HighsOptionType::kString) {
          OptionRecordString& check_option =
              ((OptionRecordString*)option_records[check_index])[0];
          if (check_option.value == value_pointer) {
            highsLogUser(report_log_options, HighsLogType::kError,
                         "checkOptions: Option %" HIGHSINT_FORMAT
                         " (\"%s\") has the same "
                         "value pointer as option %" HIGHSINT_FORMAT
                         " (\"%s\")\n",
                         index, option.name.c_str(), check_index,
                         check_option.name.c_str());
            error_found = true;
          }
        }
      }
    }
  }
  if (error_found) return OptionStatus::kIllegalValue;
  highsLogUser(report_log_options, HighsLogType::kInfo,
               "checkOptions: Options are OK\n");
  return OptionStatus::kOk;
}

OptionStatus checkOption(const HighsLogOptions& report_log_options,
                         const OptionRecordInt& option) {
  if (option.lower_bound > option.upper_bound) {
    highsLogUser(
        report_log_options, HighsLogType::kError,
        "checkOption: Option \"%s\" has inconsistent bounds [%" HIGHSINT_FORMAT
        ", %" HIGHSINT_FORMAT "]\n",
        option.name.c_str(), option.lower_bound, option.upper_bound);
    return OptionStatus::kIllegalValue;
  }
  if (option.default_value < option.lower_bound ||
      option.default_value > option.upper_bound) {
    highsLogUser(
        report_log_options, HighsLogType::kError,
        "checkOption: Option \"%s\" has default value %" HIGHSINT_FORMAT
        " "
        "inconsistent with bounds [%" HIGHSINT_FORMAT ", %" HIGHSINT_FORMAT
        "]\n",
        option.name.c_str(), option.default_value, option.lower_bound,
        option.upper_bound);
    return OptionStatus::kIllegalValue;
  }
  HighsInt value = *option.value;
  if (value < option.lower_bound || value > option.upper_bound) {
    highsLogUser(report_log_options, HighsLogType::kError,
                 "checkOption: Option \"%s\" has value %" HIGHSINT_FORMAT
                 " inconsistent with "
                 "bounds [%" HIGHSINT_FORMAT ", %" HIGHSINT_FORMAT "]\n",
                 option.name.c_str(), value, option.lower_bound,
                 option.upper_bound);
    return OptionStatus::kIllegalValue;
  }
  return OptionStatus::kOk;
}

OptionStatus checkOption(const HighsLogOptions& report_log_options,
                         const OptionRecordDouble& option) {
  if (option.lower_bound > option.upper_bound) {
    highsLogUser(
        report_log_options, HighsLogType::kError,
        "checkOption: Option \"%s\" has inconsistent bounds [%g, %g]\n",
        option.name.c_str(), option.lower_bound, option.upper_bound);
    return OptionStatus::kIllegalValue;
  }
  if (option.default_value < option.lower_bound ||
      option.default_value > option.upper_bound) {
    highsLogUser(report_log_options, HighsLogType::kError,
                 "checkOption: Option \"%s\" has default value %g "
                 "inconsistent with bounds [%g, %g]\n",
                 option.name.c_str(), option.default_value, option.lower_bound,
                 option.upper_bound);
    return OptionStatus::kIllegalValue;
  }
  double value = *option.value;
  if (value < option.lower_bound || value > option.upper_bound) {
    highsLogUser(report_log_options, HighsLogType::kError,
                 "checkOption: Option \"%s\" has value %g inconsistent with "
                 "bounds [%g, %g]\n",
                 option.name.c_str(), value, option.lower_bound,
                 option.upper_bound);
    return OptionStatus::kIllegalValue;
  }
  return OptionStatus::kOk;
}

OptionStatus checkOptionValue(const HighsLogOptions& report_log_options,
                              OptionRecordInt& option, const HighsInt value) {
  if (value < option.lower_bound) {
    highsLogUser(report_log_options, HighsLogType::kWarning,
                 "checkOptionValue: Value %" HIGHSINT_FORMAT
                 " for option \"%s\" is below "
                 "lower bound of %" HIGHSINT_FORMAT "\n",
                 value, option.name.c_str(), option.lower_bound);
    return OptionStatus::kIllegalValue;
  } else if (value > option.upper_bound) {
    highsLogUser(report_log_options, HighsLogType::kWarning,
                 "checkOptionValue: Value %" HIGHSINT_FORMAT
                 " for option \"%s\" is above "
                 "upper bound of %" HIGHSINT_FORMAT "\n",
                 value, option.name.c_str(), option.upper_bound);
    return OptionStatus::kIllegalValue;
  }
  return OptionStatus::kOk;
}

OptionStatus checkOptionValue(const HighsLogOptions& report_log_options,
                              OptionRecordDouble& option, const double value) {
  if (value < option.lower_bound) {
    highsLogUser(report_log_options, HighsLogType::kWarning,
                 "checkOptionValue: Value %g for option \"%s\" is below "
                 "lower bound of %g\n",
                 value, option.name.c_str(), option.lower_bound);
    return OptionStatus::kIllegalValue;
  } else if (value > option.upper_bound) {
    highsLogUser(report_log_options, HighsLogType::kWarning,
                 "checkOptionValue: Value %g for option \"%s\" is above "
                 "upper bound of %g\n",
                 value, option.name.c_str(), option.upper_bound);
    return OptionStatus::kIllegalValue;
  }
  return OptionStatus::kOk;
}

OptionStatus checkOptionValue(const HighsLogOptions& report_log_options,
                              OptionRecordString& option,
                              const std::string value) {
  // Setting a string option. For some options only particular values
  // are permitted, so check them
  if (option.name == kPresolveString) {
    if (!commandLineOffChooseOnOk(report_log_options, option.name, value) &&
        value != "mip")
      return OptionStatus::kIllegalValue;
  } else if (option.name == kSolverString) {
    if (!commandLineSolverOk(report_log_options, value))
      return OptionStatus::kIllegalValue;
  } else if (option.name == kParallelString) {
    if (!commandLineOffChooseOnOk(report_log_options, option.name, value))
      return OptionStatus::kIllegalValue;
  } else if (option.name == kRunCrossoverString) {
    if (!commandLineOffChooseOnOk(report_log_options, option.name, value))
      return OptionStatus::kIllegalValue;
  } else if (option.name == kRangingString) {
    if (!commandLineOffOnOk(report_log_options, option.name, value))
      return OptionStatus::kIllegalValue;
  }
  return OptionStatus::kOk;
}

OptionStatus setLocalOptionValue(const HighsLogOptions& report_log_options,
                                 const std::string& name,
                                 std::vector<OptionRecord*>& option_records,
                                 const bool value) {
  HighsInt index;
  //  printf("setLocalOptionValue: \"%s\" with bool %" HIGHSINT_FORMAT "\n",
  //  name.c_str(), value);
  OptionStatus status =
      getOptionIndex(report_log_options, name, option_records, index);
  if (status != OptionStatus::kOk) return status;
  HighsOptionType type = option_records[index]->type;
  if (type != HighsOptionType::kBool) {
    highsLogUser(
        report_log_options, HighsLogType::kError,
        "setLocalOptionValue: Option \"%s\" cannot be assigned a bool\n",
        name.c_str());
    return OptionStatus::kIllegalValue;
  }
  return setLocalOptionValue(((OptionRecordBool*)option_records[index])[0],
                             value);
}

OptionStatus setLocalOptionValue(const HighsLogOptions& report_log_options,
                                 const std::string& name,
                                 std::vector<OptionRecord*>& option_records,
                                 const HighsInt value) {
  HighsInt index;
  //  printf("setLocalOptionValue: \"%s\" with HighsInt %" HIGHSINT_FORMAT "\n",
  //  name.c_str(), value);
  OptionStatus status =
      getOptionIndex(report_log_options, name, option_records, index);
  if (status != OptionStatus::kOk) return status;
  HighsOptionType type = option_records[index]->type;
  if (type != HighsOptionType::kInt) {
    if (type == HighsOptionType::kDouble) {
      // Interpret integer as double
      double use_value = value;
      return setLocalOptionValue(
          report_log_options, ((OptionRecordDouble*)option_records[index])[0],
          use_value);
    }
    highsLogUser(
        report_log_options, HighsLogType::kError,
        "setLocalOptionValue: Option \"%s\" cannot be assigned an int\n",
        name.c_str());
    return OptionStatus::kIllegalValue;
  }
  return setLocalOptionValue(
      report_log_options, ((OptionRecordInt*)option_records[index])[0], value);
}

OptionStatus setLocalOptionValue(const HighsLogOptions& report_log_options,
                                 const std::string& name,
                                 std::vector<OptionRecord*>& option_records,
                                 const double value) {
  HighsInt index;
  //  printf("setLocalOptionValue: \"%s\" with double %g\n", name.c_str(),
  //  value);
  OptionStatus status =
      getOptionIndex(report_log_options, name, option_records, index);
  if (status != OptionStatus::kOk) return status;
  HighsOptionType type = option_records[index]->type;
  if (type != HighsOptionType::kDouble) {
    highsLogUser(
        report_log_options, HighsLogType::kError,
        "setLocalOptionValue: Option \"%s\" cannot be assigned a double\n",
        name.c_str());
    return OptionStatus::kIllegalValue;
  }
  return setLocalOptionValue(report_log_options,
                             ((OptionRecordDouble*)option_records[index])[0],
                             value);
}

OptionStatus setLocalOptionValue(const HighsLogOptions& report_log_options,
                                 const std::string& name,
                                 HighsLogOptions& log_options,
                                 std::vector<OptionRecord*>& option_records,
                                 const std::string value_passed) {
  // Trim any leading and trailing spaces
  std::string value_trim = value_passed;
  trim(value_trim, " ");
  HighsInt index;
  OptionStatus status =
      getOptionIndex(report_log_options, name, option_records, index);
  if (status != OptionStatus::kOk) return status;
  HighsOptionType type = option_records[index]->type;
  if (type == HighsOptionType::kBool) {
    bool value_bool;
    bool return_status = boolFromString(value_trim, value_bool);
    if (!return_status) {
      highsLogUser(
          report_log_options, HighsLogType::kError,
          "setLocalOptionValue: Value \"%s\" cannot be interpreted as a bool\n",
          value_trim.c_str());
      return OptionStatus::kIllegalValue;
    }
    return setLocalOptionValue(((OptionRecordBool*)option_records[index])[0],
                               value_bool);
  } else if (type == HighsOptionType::kInt) {
    // Check that the string only contains legitimate characters
    if (value_trim.find_first_not_of("+-0123456789eE") != std::string::npos)
      return OptionStatus::kIllegalValue;
    HighsInt value_int;
    int scanned_num_char;
    const char* value_char = value_trim.c_str();
    sscanf(value_char, "%" HIGHSINT_FORMAT "%n", &value_int, &scanned_num_char);
    const int value_num_char = strlen(value_char);
    // Check that all characters in value_char are scanned by the format
    const bool converted_ok = scanned_num_char == value_num_char;
    if (!converted_ok) {
      highsLogDev(report_log_options, HighsLogType::kError,
                  "setLocalOptionValue: Value = \"%s\" converts via sscanf as "
                  "%" HIGHSINT_FORMAT
                  " "
                  "by scanning %" HIGHSINT_FORMAT " of %" HIGHSINT_FORMAT
                  " characters\n",
                  value_trim.c_str(), value_int, scanned_num_char,
                  value_num_char);
      return OptionStatus::kIllegalValue;
    }
    return setLocalOptionValue(report_log_options,
                               ((OptionRecordInt*)option_records[index])[0],
                               value_int);
  } else if (type == HighsOptionType::kDouble) {
    // Check that the string only contains legitimate characters -
    // after handling +/- inf
    double value_double = 0;
    tolower(value_trim);
    if (value_trim == "inf" || value_trim == "+inf") {
      value_double = kHighsInf;
    } else if (value_trim == "-inf") {
      value_double = -kHighsInf;
    } else {
      if (value_trim.find_first_not_of("+-.0123456789eE") != std::string::npos)
        return OptionStatus::kIllegalValue;
      HighsInt value_int = atoi(value_trim.c_str());
      value_double = atof(value_trim.c_str());
      double value_int_double = value_int;
      if (value_double == value_int_double) {
        highsLogDev(report_log_options, HighsLogType::kInfo,
                    "setLocalOptionValue: Value = \"%s\" converts via atoi as "
                    "%" HIGHSINT_FORMAT
                    " "
                    "so is %g as double, and %g via atof\n",
                    value_trim.c_str(), value_int, value_int_double,
                    value_double);
      }
    }
    return setLocalOptionValue(report_log_options,
                               ((OptionRecordDouble*)option_records[index])[0],
                               value_double);
  } else {
    // Setting a string option value
    if (!name.compare(kLogFileString)) {
      OptionRecordString& option = *(OptionRecordString*)option_records[index];
      std::string original_log_file = *(option.value);
      if (value_passed.compare(original_log_file)) {
        // Changing the name of the log file
        highsOpenLogFile(log_options, option_records, value_passed);
      }
    }
    if (!name.compare(kModelFileString)) {
      // Don't allow model filename to be changed - it's only an
      // option so that reading of run-time options works
      highsLogUser(report_log_options, HighsLogType::kError,
                   "setLocalOptionValue: model filename cannot be set\n");
      return OptionStatus::kUnknownOption;
    } else {
      return setLocalOptionValue(
          report_log_options, ((OptionRecordString*)option_records[index])[0],
          value_passed);
    }
  }
}

OptionStatus setLocalOptionValue(const HighsLogOptions& report_log_options,
                                 const std::string& name,
                                 HighsLogOptions& log_options,
                                 std::vector<OptionRecord*>& option_records,
                                 const char* value) {
  // Handles values passed as explicit values in quotes
  std::string value_as_string(value);
  return setLocalOptionValue(report_log_options, name, log_options,
                             option_records, value_as_string);
}

OptionStatus setLocalOptionValue(OptionRecordBool& option, const bool value) {
  option.assignvalue(value);
  return OptionStatus::kOk;
}

OptionStatus setLocalOptionValue(const HighsLogOptions& report_log_options,
                                 OptionRecordInt& option,
                                 const HighsInt value) {
  OptionStatus return_status =
      checkOptionValue(report_log_options, option, value);
  if (return_status != OptionStatus::kOk) return return_status;
  option.assignvalue(value);
  return OptionStatus::kOk;
}

OptionStatus setLocalOptionValue(const HighsLogOptions& report_log_options,
                                 OptionRecordDouble& option,
                                 const double value) {
  OptionStatus return_status =
      checkOptionValue(report_log_options, option, value);
  if (return_status != OptionStatus::kOk) return return_status;
  option.assignvalue(value);
  return OptionStatus::kOk;
}

OptionStatus setLocalOptionValue(const HighsLogOptions& report_log_options,
                                 OptionRecordString& option,
                                 const std::string value) {
  OptionStatus return_status =
      checkOptionValue(report_log_options, option, value);
  if (return_status != OptionStatus::kOk) return return_status;
  option.assignvalue(value);
  return OptionStatus::kOk;
}

OptionStatus passLocalOptions(const HighsLogOptions& report_log_options,
                              const HighsOptions& from_options,
                              HighsOptions& to_options) {
  // (Attempt to) set option value from the HighsOptions passed in
  OptionStatus return_status;
  //  std::string empty_file = "";
  //  std::string from_log_file = from_options.log_file;
  //  std::string original_to_log_file = to_options.log_file;
  //  FILE* original_to_log_stream =
  //  to_options.log_options.log_stream;
  HighsInt num_options = to_options.records.size();
  // Check all the option values before setting any of them - in case
  // to_options are the main Highs options. Checks are only needed for
  // HighsInt, double and string since bool values can't be illegal
  for (HighsInt index = 0; index < num_options; index++) {
    HighsOptionType type = to_options.records[index]->type;
    if (type == HighsOptionType::kInt) {
      HighsInt value =
          *(((OptionRecordInt*)from_options.records[index])[0].value);
      return_status = checkOptionValue(
          report_log_options, ((OptionRecordInt*)to_options.records[index])[0],
          value);
      if (return_status != OptionStatus::kOk) return return_status;
    } else if (type == HighsOptionType::kDouble) {
      double value =
          *(((OptionRecordDouble*)from_options.records[index])[0].value);
      return_status = checkOptionValue(
          report_log_options,
          ((OptionRecordDouble*)to_options.records[index])[0], value);
      if (return_status != OptionStatus::kOk) return return_status;
    } else if (type == HighsOptionType::kString) {
      std::string value =
          *(((OptionRecordString*)from_options.records[index])[0].value);
      return_status = checkOptionValue(
          report_log_options,
          ((OptionRecordString*)to_options.records[index])[0], value);
      if (return_status != OptionStatus::kOk) return return_status;
    }
  }
  // Checked from_options and found it to be OK, so set all the values
  for (HighsInt index = 0; index < num_options; index++) {
    HighsOptionType type = to_options.records[index]->type;
    if (type == HighsOptionType::kBool) {
      bool value = *(((OptionRecordBool*)from_options.records[index])[0].value);
      return_status = setLocalOptionValue(
          ((OptionRecordBool*)to_options.records[index])[0], value);
      if (return_status != OptionStatus::kOk) return return_status;
    } else if (type == HighsOptionType::kInt) {
      HighsInt value =
          *(((OptionRecordInt*)from_options.records[index])[0].value);
      return_status = setLocalOptionValue(
          report_log_options, ((OptionRecordInt*)to_options.records[index])[0],
          value);
      if (return_status != OptionStatus::kOk) return return_status;
    } else if (type == HighsOptionType::kDouble) {
      double value =
          *(((OptionRecordDouble*)from_options.records[index])[0].value);
      return_status = setLocalOptionValue(
          report_log_options,
          ((OptionRecordDouble*)to_options.records[index])[0], value);
      if (return_status != OptionStatus::kOk) return return_status;
    } else {
      std::string value =
          *(((OptionRecordString*)from_options.records[index])[0].value);
      return_status = setLocalOptionValue(
          report_log_options,
          ((OptionRecordString*)to_options.records[index])[0], value);
      if (return_status != OptionStatus::kOk) return return_status;
    }
  }
  /*
  if (from_log_file.compare(original_to_log_file)) {
    // The log file name has changed
    if (from_options.log_options.log_stream &&
        !original_to_log_file.compare(empty_file)) {
      // The stream corresponding to from_log_file is non-null and the
      // original log file name was empty, so to_options inherits the
      // stream, but associated with the (necessarily) non-empty name
      // from_log_file
      //
      // This ensures that the stream to Highs.log opened in
      // RunHighs.cpp is retained unless the log file name is changed.
      assert(from_log_file.compare(empty_file));
      assert(!original_to_log_stream);
      to_options.log_options.log_stream =
          from_options.log_options.log_stream;
    } else {
      highsOpenLogFile(to_options, to_options.log_file);
    }
  }
  */
  return OptionStatus::kOk;
}

OptionStatus getLocalOptionValues(
    const HighsLogOptions& report_log_options, const std::string& option,
    const std::vector<OptionRecord*>& option_records, bool* current_value,
    bool* default_value) {
  HighsInt index;
  OptionStatus status =
      getOptionIndex(report_log_options, option, option_records, index);
  if (status != OptionStatus::kOk) return status;
  HighsOptionType type = option_records[index]->type;
  if (type != HighsOptionType::kBool) {
    highsLogUser(report_log_options, HighsLogType::kError,
                 "getLocalOptionValue: Option \"%s\" requires value of type "
                 "%s, not bool\n",
                 option.c_str(), optionEntryTypeToString(type).c_str());
    return OptionStatus::kIllegalValue;
  }
  OptionRecordBool& option_record =
      ((OptionRecordBool*)option_records[index])[0];
  if (current_value) *current_value = *(option_record.value);
  if (default_value) *default_value = option_record.default_value;
  return OptionStatus::kOk;
}

OptionStatus getLocalOptionValues(
    const HighsLogOptions& report_log_options, const std::string& option,
    const std::vector<OptionRecord*>& option_records, HighsInt* current_value,
    HighsInt* min_value, HighsInt* max_value, HighsInt* default_value) {
  HighsInt index;
  OptionStatus status =
      getOptionIndex(report_log_options, option, option_records, index);
  if (status != OptionStatus::kOk) return status;
  HighsOptionType type = option_records[index]->type;
  if (type != HighsOptionType::kInt) {
    highsLogUser(report_log_options, HighsLogType::kError,
                 "getLocalOptionValue: Option \"%s\" requires value of type "
                 "%s, not HighsInt\n",
                 option.c_str(), optionEntryTypeToString(type).c_str());
    return OptionStatus::kIllegalValue;
  }
  OptionRecordInt& option_record = ((OptionRecordInt*)option_records[index])[0];
  if (current_value) *current_value = *(option_record.value);
  if (min_value) *min_value = option_record.lower_bound;
  if (max_value) *max_value = option_record.upper_bound;
  if (default_value) *default_value = option_record.default_value;
  return OptionStatus::kOk;
}

OptionStatus getLocalOptionValues(
    const HighsLogOptions& report_log_options, const std::string& option,
    const std::vector<OptionRecord*>& option_records, double* current_value,
    double* min_value, double* max_value, double* default_value) {
  HighsInt index;
  OptionStatus status =
      getOptionIndex(report_log_options, option, option_records, index);
  if (status != OptionStatus::kOk) return status;
  HighsOptionType type = option_records[index]->type;
  if (type != HighsOptionType::kDouble) {
    highsLogUser(report_log_options, HighsLogType::kError,
                 "getLocalOptionValue: Option \"%s\" requires value of type "
                 "%s, not double\n",
                 option.c_str(), optionEntryTypeToString(type).c_str());
    return OptionStatus::kIllegalValue;
  }
  OptionRecordDouble& option_record =
      ((OptionRecordDouble*)option_records[index])[0];
  if (current_value) *current_value = *(option_record.value);
  if (min_value) *min_value = option_record.lower_bound;
  if (max_value) *max_value = option_record.upper_bound;
  if (default_value) *default_value = option_record.default_value;
  return OptionStatus::kOk;
}

OptionStatus getLocalOptionValues(
    const HighsLogOptions& report_log_options, const std::string& option,
    const std::vector<OptionRecord*>& option_records,
    std::string* current_value, std::string* default_value) {
  HighsInt index;
  OptionStatus status =
      getOptionIndex(report_log_options, option, option_records, index);
  if (status != OptionStatus::kOk) return status;
  HighsOptionType type = option_records[index]->type;
  if (type != HighsOptionType::kString) {
    highsLogUser(report_log_options, HighsLogType::kError,
                 "getLocalOptionValue: Option \"%s\" requires value of type "
                 "%s, not string\n",
                 option.c_str(), optionEntryTypeToString(type).c_str());
    return OptionStatus::kIllegalValue;
  }
  OptionRecordString& option_record =
      ((OptionRecordString*)option_records[index])[0];
  if (current_value) *current_value = *(option_record.value);
  if (default_value) *default_value = option_record.default_value;
  return OptionStatus::kOk;
}

OptionStatus getLocalOptionType(
    const HighsLogOptions& report_log_options, const std::string& option,
    const std::vector<OptionRecord*>& option_records, HighsOptionType* type) {
  HighsInt index;
  OptionStatus status =
      getOptionIndex(report_log_options, option, option_records, index);
  if (status != OptionStatus::kOk) return status;
  if (type) *type = option_records[index]->type;
  return OptionStatus::kOk;
}

void resetLocalOptions(std::vector<OptionRecord*>& option_records) {
  HighsInt num_options = option_records.size();
  for (HighsInt index = 0; index < num_options; index++) {
    HighsOptionType type = option_records[index]->type;
    if (type == HighsOptionType::kBool) {
      OptionRecordBool& option = ((OptionRecordBool*)option_records[index])[0];
      *(option.value) = option.default_value;
    } else if (type == HighsOptionType::kInt) {
      OptionRecordInt& option = ((OptionRecordInt*)option_records[index])[0];
      *(option.value) = option.default_value;
    } else if (type == HighsOptionType::kDouble) {
      OptionRecordDouble& option =
          ((OptionRecordDouble*)option_records[index])[0];
      *(option.value) = option.default_value;
    } else {
      OptionRecordString& option =
          ((OptionRecordString*)option_records[index])[0];
      *(option.value) = option.default_value;
    }
  }
}

HighsStatus writeOptionsToFile(FILE* file,
                               const std::vector<OptionRecord*>& option_records,
                               const bool report_only_deviations,
                               const HighsFileType file_type) {
  reportOptions(file, option_records, report_only_deviations, file_type);
  return HighsStatus::kOk;
}

void reportOptions(FILE* file, const std::vector<OptionRecord*>& option_records,
                   const bool report_only_deviations,
                   const HighsFileType file_type) {
  HighsInt num_options = option_records.size();
  for (HighsInt index = 0; index < num_options; index++) {
    HighsOptionType type = option_records[index]->type;
    // Only report non-advanced options
    if (option_records[index]->advanced) {
      // Possibly skip the advanced options when creating Md file
      if (!kAdvancedInDocumentation) continue;
    }
    if (type == HighsOptionType::kBool) {
      reportOption(file, ((OptionRecordBool*)option_records[index])[0],
                   report_only_deviations, file_type);
    } else if (type == HighsOptionType::kInt) {
      reportOption(file, ((OptionRecordInt*)option_records[index])[0],
                   report_only_deviations, file_type);
    } else if (type == HighsOptionType::kDouble) {
      reportOption(file, ((OptionRecordDouble*)option_records[index])[0],
                   report_only_deviations, file_type);
    } else {
      reportOption(file, ((OptionRecordString*)option_records[index])[0],
                   report_only_deviations, file_type);
    }
  }
}

void reportOption(FILE* file, const OptionRecordBool& option,
                  const bool report_only_deviations,
                  const HighsFileType file_type) {
  if (!report_only_deviations || option.default_value != *option.value) {
    if (file_type == HighsFileType::kMd) {
      fprintf(file, "## %s\n- %s\n- Type: boolean\n- Default: \"%s\"\n\n",
              highsInsertMdEscapes(option.name).c_str(),
              highsInsertMdEscapes(option.description).c_str(),
              highsBoolToString(option.default_value).c_str());
    } else if (file_type == HighsFileType::kFull) {
      fprintf(file, "\n# %s\n", option.description.c_str());
      fprintf(
          file,
          "# [type: bool, advanced: %s, range: {false, true}, default: %s]\n",
          highsBoolToString(option.advanced).c_str(),
          highsBoolToString(option.default_value).c_str());
      fprintf(file, "%s = %s\n", option.name.c_str(),
              highsBoolToString(*option.value).c_str());
    } else {
      fprintf(file, "%s = %s\n", option.name.c_str(),
              highsBoolToString(*option.value).c_str());
    }
  }
}

void reportOption(FILE* file, const OptionRecordInt& option,
                  const bool report_only_deviations,
                  const HighsFileType file_type) {
  if (!report_only_deviations || option.default_value != *option.value) {
    if (file_type == HighsFileType::kMd) {
      fprintf(file,
              "## %s\n- %s\n- Type: integer\n- Range: {%" HIGHSINT_FORMAT
              ", %" HIGHSINT_FORMAT "}\n- Default: %" HIGHSINT_FORMAT "\n\n",
              highsInsertMdEscapes(option.name).c_str(),
              highsInsertMdEscapes(option.description).c_str(),
              option.lower_bound, option.upper_bound, option.default_value);
    } else if (file_type == HighsFileType::kFull) {
      fprintf(file, "\n# %s\n", option.description.c_str());
      fprintf(file,
              "# [type: integer, advanced: %s, range: {%" HIGHSINT_FORMAT
              ", %" HIGHSINT_FORMAT "}, default: %" HIGHSINT_FORMAT "]\n",
              highsBoolToString(option.advanced).c_str(), option.lower_bound,
              option.upper_bound, option.default_value);
      fprintf(file, "%s = %" HIGHSINT_FORMAT "\n", option.name.c_str(),
              *option.value);
    } else {
      fprintf(file, "%s = %" HIGHSINT_FORMAT "\n", option.name.c_str(),
              *option.value);
    }
  }
}

void reportOption(FILE* file, const OptionRecordDouble& option,
                  const bool report_only_deviations,
                  const HighsFileType file_type) {
  if (!report_only_deviations || option.default_value != *option.value) {
    if (file_type == HighsFileType::kMd) {
      fprintf(
          file,
          "## %s\n- %s\n- Type: double\n- Range: [%g, %g]\n- Default: %g\n\n",
          highsInsertMdEscapes(option.name).c_str(),
          highsInsertMdEscapes(option.description).c_str(), option.lower_bound,
          option.upper_bound, option.default_value);
    } else if (file_type == HighsFileType::kFull) {
      fprintf(file, "\n# %s\n", option.description.c_str());
      fprintf(file,
              "# [type: double, advanced: %s, range: [%g, %g], default: %g]\n",
              highsBoolToString(option.advanced).c_str(), option.lower_bound,
              option.upper_bound, option.default_value);
      fprintf(file, "%s = %g\n", option.name.c_str(), *option.value);
    } else {
      fprintf(file, "%s = %g\n", option.name.c_str(), *option.value);
    }
  }
}

void reportOption(FILE* file, const OptionRecordString& option,
                  const bool report_only_deviations,
                  const HighsFileType file_type) {
  // Don't report for the options file if writing to an options file
  // Don't report options that can only be passed via the command line
  if (option.name == kOptionsFileString) return;
  // ToDo: are there others?

  if (!report_only_deviations || option.default_value != *option.value) {
    if (file_type == HighsFileType::kMd) {
      fprintf(file, "## %s\n- %s\n- Type: string\n- Default: \"%s\"\n\n",
              highsInsertMdEscapes(option.name).c_str(),
              highsInsertMdEscapes(option.description).c_str(),
              option.default_value.c_str());
    } else if (file_type == HighsFileType::kFull) {
      fprintf(file, "\n# %s\n", option.description.c_str());
      fprintf(file, "# [type: string, advanced: %s, default: \"%s\"]\n",
              highsBoolToString(option.advanced).c_str(),
              option.default_value.c_str());
      fprintf(file, "%s = %s\n", option.name.c_str(), (*option.value).c_str());
    } else {
      fprintf(file, "%s = %s\n", option.name.c_str(), (*option.value).c_str());
    }
  }
}

void HighsOptions::setLogOptions() {
  this->log_options.output_flag = &this->output_flag;
  this->log_options.log_to_console = &this->log_to_console;
  this->log_options.log_dev_level = &this->log_dev_level;
}
