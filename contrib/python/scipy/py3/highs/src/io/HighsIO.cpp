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
/**@file io/HighsIO.cpp
 * @brief IO methods for HiGHS - currently just print/log messages
 */
#include "io/HighsIO.h"

#include <cmath>
#include <cstdarg>
#include <cstdio>

#include "lp_data/HighsLp.h"
#include "lp_data/HighsOptions.h"

void highsLogHeader(const HighsLogOptions& log_options,
                    const bool log_githash) {
  const std::string githash_string(HIGHS_GITHASH);
  const std::string githash_text =
      log_githash ? " (git hash: " + githash_string + ")" : "";
  highsLogUser(log_options, HighsLogType::kInfo,
               "Running HiGHS %d.%d.%d%s: %s\n", (int)HIGHS_VERSION_MAJOR,
               (int)HIGHS_VERSION_MINOR, (int)HIGHS_VERSION_PATCH,
               githash_text.c_str(), kHighsCopyrightStatement.c_str());
}

std::array<char, 32> highsDoubleToString(const double val,
                                         const double tolerance) {
  decltype(highsDoubleToString(std::declval<double>(),
                               std::declval<double>())) printString = {};
  double l =
      std::abs(val) == kHighsInf
          ? 1.0
          : (1.0 - tolerance +
             std::log10(std::max(tolerance, std::abs(val)) / (tolerance)));
  switch (int(l)) {
    case 0:
      std::snprintf(printString.data(), printString.size(), "%c", '0');
      break;
    case 1:
      std::snprintf(printString.data(), printString.size(), "%.1g", val);
      break;
    case 2:
      std::snprintf(printString.data(), printString.size(), "%.2g", val);
      break;
    case 3:
      std::snprintf(printString.data(), printString.size(), "%.3g", val);
      break;
    case 4:
      std::snprintf(printString.data(), printString.size(), "%.4g", val);
      break;
    case 5:
      std::snprintf(printString.data(), printString.size(), "%.5g", val);
      break;
    case 6:
      std::snprintf(printString.data(), printString.size(), "%.6g", val);
      break;
    case 7:
      std::snprintf(printString.data(), printString.size(), "%.7g", val);
      break;
    case 8:
      std::snprintf(printString.data(), printString.size(), "%.8g", val);
      break;
    case 9:
      std::snprintf(printString.data(), printString.size(), "%.9g", val);
      break;
    case 10:
      std::snprintf(printString.data(), printString.size(), "%.10g", val);
      break;
    case 11:
      std::snprintf(printString.data(), printString.size(), "%.11g", val);
      break;
    case 12:
      std::snprintf(printString.data(), printString.size(), "%.12g", val);
      break;
    case 13:
      std::snprintf(printString.data(), printString.size(), "%.13g", val);
      break;
    case 14:
      std::snprintf(printString.data(), printString.size(), "%.14g", val);
      break;
    case 15:
      std::snprintf(printString.data(), printString.size(), "%.15g", val);
      break;
    default:
      std::snprintf(printString.data(), printString.size(), "%.16g", val);
  }

  return printString;
}

void highsLogUser(const HighsLogOptions& log_options_, const HighsLogType type,
                  const char* format, ...) {
  if (!*log_options_.output_flag ||
      (log_options_.log_stream == NULL && !*log_options_.log_to_console))
    return;
  // highsLogUser should not be passed HighsLogType::kDetailed or
  // HighsLogType::kVerbose
  assert(type != HighsLogType::kDetailed);
  assert(type != HighsLogType::kVerbose);
  const bool prefix =
      type == HighsLogType::kWarning || type == HighsLogType::kError;
  va_list argptr;
  va_start(argptr, format);
  const bool flush_streams = true;
  const bool use_log_callback =
      log_options_.user_log_callback ||
      (log_options_.user_callback && log_options_.user_callback_active);

  if (!use_log_callback) {
    // Write to log file stream unless it is NULL
    if (log_options_.log_stream) {
      if (prefix)
        fprintf(log_options_.log_stream, "%-9s", HighsLogTypeTag[(int)type]);
      vfprintf(log_options_.log_stream, format, argptr);
      if (flush_streams) fflush(log_options_.log_stream);
      va_end(argptr);
      va_start(argptr, format);
    }
    // Write to stdout unless log file stream is stdout
    if (*log_options_.log_to_console && log_options_.log_stream != stdout) {
      if (prefix) fprintf(stdout, "%-9s", HighsLogTypeTag[(int)type]);
      vfprintf(stdout, format, argptr);
      if (flush_streams) fflush(stdout);
    }
  } else {
    size_t len = 0;
    std::array<char, kIoBufferSize> msgbuffer = {};
    if (prefix) {
      int l = snprintf(msgbuffer.data(), msgbuffer.size(), "%-9s",
                       HighsLogTypeTag[(int)type]);
      // assert that there are no encoding errors
      assert(l >= 0);
      len = static_cast<size_t>(l);
    }
    if (len < msgbuffer.size()) {
      int l = vsnprintf(msgbuffer.data() + len, msgbuffer.size() - len, format,
                        argptr);
      // assert that there are no encoding errors
      assert(l >= 0);
      len += static_cast<size_t>(l);
    }
    if (log_options_.user_log_callback) {
      log_options_.user_log_callback(type, msgbuffer.data(),
                                     log_options_.user_log_callback_data);
    }
    if (log_options_.user_callback_active) {
      assert(log_options_.user_callback);
      HighsCallbackDataOut data_out;
      data_out.log_type = int(type);
      log_options_.user_callback(kCallbackLogging, msgbuffer.data(), &data_out,
                                 nullptr, log_options_.user_callback_data);
    }
  }
  va_end(argptr);
}

void highsLogDev(const HighsLogOptions& log_options_, const HighsLogType type,
                 const char* format, ...) {
  if (!*log_options_.output_flag ||
      (log_options_.log_stream == NULL && !*log_options_.log_to_console) ||
      !*log_options_.log_dev_level)
    return;
  // Always report HighsLogType::kInfo, HighsLogType::kWarning or
  // HighsLogType::kError
  //
  // Report HighsLogType::kDetailed if *log_options_.log_dev_level >=
  // kHighsLogDevLevelDetailed
  //
  // Report HighsLogType::kVerbose if *log_options_.log_dev_level >=
  // kHighsLogDevLevelVerbose
  if (type == HighsLogType::kDetailed &&
      *log_options_.log_dev_level < kHighsLogDevLevelDetailed)
    return;
  if (type == HighsLogType::kVerbose &&
      *log_options_.log_dev_level < kHighsLogDevLevelVerbose)
    return;
  va_list argptr;
  va_start(argptr, format);
  const bool flush_streams = true;
  const bool use_log_callback =
      log_options_.user_log_callback ||
      (log_options_.user_callback && log_options_.user_callback_active);
  if (!use_log_callback) {
    // Write to log file stream unless it is NULL
    if (log_options_.log_stream) {
      // Write to log file stream
      vfprintf(log_options_.log_stream, format, argptr);
      if (flush_streams) fflush(log_options_.log_stream);
      va_end(argptr);
      va_start(argptr, format);
    }
    // Write to stdout unless log file stream is stdout
    if (*log_options_.log_to_console && log_options_.log_stream != stdout) {
      vfprintf(stdout, format, argptr);
      if (flush_streams) fflush(stdout);
    }
  } else {
    std::array<char, kIoBufferSize> msgbuffer = {};
    int len = vsnprintf(msgbuffer.data(), msgbuffer.size(), format, argptr);
    // assert that there are no encoding errors
    assert(len >= 0);
    if (log_options_.user_log_callback) {
      log_options_.user_log_callback(type, msgbuffer.data(),
                                     log_options_.user_log_callback_data);
    } else if (log_options_.user_callback_active) {
      assert(log_options_.user_callback);
      HighsCallbackDataOut data_out;
      data_out.log_type = int(type);
      log_options_.user_callback(kCallbackLogging, msgbuffer.data(), &data_out,
                                 nullptr, log_options_.user_callback_data);
    }
  }
  va_end(argptr);
}

void highsFprintfString(FILE* file, const HighsLogOptions& log_options_,
                        const std::string& s) {
  if (file == nullptr) return;
  if (file == stdout) {
    highsLogUser(log_options_, HighsLogType::kInfo, "%s", s.c_str());
  } else {
    fprintf(file, "%s", s.c_str());
  }
}

void highsReportDevInfo(const HighsLogOptions* log_options,
                        const std::string line) {
  if (log_options) {
    highsLogDev(*log_options, HighsLogType::kInfo, "%s", line.c_str());
  } else {
    printf("%s", line.c_str());
  }
}

void highsOpenLogFile(HighsOptions& options, const std::string log_file) {
  highsOpenLogFile(options.log_options, options.records, log_file);
}

void highsReportLogOptions(const HighsLogOptions& log_options_) {
  printf("\nHighs log options\n");
  if (log_options_.log_stream == NULL) {
    printf("   log_stream = NULL\n");
  } else {
    printf("   log_stream = Not NULL\n");
  }
  printf("   output_flag = %s\n",
         highsBoolToString(*log_options_.output_flag).c_str());
  printf("   log_to_console = %s\n",
         highsBoolToString(*log_options_.log_to_console).c_str());
  printf("   log_dev_level = %" HIGHSINT_FORMAT "\n\n",
         *log_options_.log_dev_level);
}

std::string highsFormatToString(const char* format, ...) {
  va_list argptr;
  va_start(argptr, format);
  std::array<char, kIoBufferSize> msgbuffer = {};
  int len = vsnprintf(msgbuffer.data(), msgbuffer.size(), format, argptr);
  // assert that there are no encoding errors
  assert(len >= 0);
  va_end(argptr);
  return std::string(msgbuffer.data());
}

const std::string highsBoolToString(const bool b, const HighsInt field_width) {
  const HighsInt abs_field_width = std::abs(field_width);
  if (abs_field_width <= 1) return b ? "T" : "F";
  if (abs_field_width <= 2) return b ? "true" : "false";
  if (field_width < 0) return b ? "true " : "false";
  return b ? " true" : "false";
}

const std::string highsInsertMdEscapes(const std::string from_string) {
  std::string to_string = "";
  const char* underscore = "_";
  const char* backslash = "\\";
  HighsInt from_string_length = from_string.length();
  for (HighsInt p = 0; p < from_string_length; p++) {
    const char string_ch = from_string[p];
    if (string_ch == *underscore) {
      to_string += backslash;
    }
    to_string += from_string[p];
  }
  return to_string;
}

void HighsLogOptions::clear() {
  this->log_stream = nullptr;
  this->output_flag = nullptr;
  this->log_to_console = nullptr;
  this->log_dev_level = nullptr;
  this->user_log_callback = nullptr;
  this->user_log_callback_data = nullptr;
  this->user_callback = nullptr;
  this->user_callback_data = nullptr;
  this->user_callback_active = false;
}
