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

#include "io/Filereader.h"

#include <cctype>

#include "io/FilereaderEms.h"
#include "io/FilereaderLp.h"
#include "io/FilereaderMps.h"
#include "io/HighsIO.h"

// convert string to lower-case, modifies string
static inline void tolower(std::string& s) {
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return std::tolower(c); });
}

static const std::string getFilenameExt(const std::string filename) {
  // Extract file name extension
  std::string name = filename;
  std::size_t found = name.find_last_of(".");
  if (found < name.size()) {
    name = name.substr(found + 1);
  } else {
    name = "";
  }
  return name;
}

Filereader* Filereader::getFilereader(const HighsLogOptions& log_options,
                                      const std::string filename) {
  Filereader* reader;
  std::string extension = getFilenameExt(filename);
  if (extension == "gz") {
#ifdef ZLIB_FOUND
    extension = getFilenameExt(filename.substr(0, filename.size() - 3));
#else
    highsLogUser(log_options, HighsLogType::kError,
                 "HiGHS build without zlib support. Cannot read .gz file.\n",
                 filename.c_str());
    reader = NULL;
#endif
    //  } else if (extension == "zip") {
    // #ifdef ZLIB_FOUND
    //    extension = getFilenameExt(filename.substr(0, filename.size() - 4));
    // #else
    //    highsLogUser(log_options, HighsLogType::kError,
    //                 "HiGHS build without zlib support. Cannot read .zip
    //                 file.\n", filename.c_str());
    //    reader = NULL;
    // #endif
  }
  std::string lower_case_extension = extension;
  tolower(lower_case_extension);
  if (lower_case_extension.compare("mps") == 0) {
    reader = new FilereaderMps();
  } else if (lower_case_extension.compare("lp") == 0) {
    reader = new FilereaderLp();
  } else if (lower_case_extension.compare("ems") == 0) {
    reader = new FilereaderEms();
  } else {
    reader = NULL;
  }
  return reader;
}

void interpretFilereaderRetcode(const HighsLogOptions& log_options,
                                const std::string filename,
                                const FilereaderRetcode code) {
  switch (code) {
    case FilereaderRetcode::kOk:
      break;
    case FilereaderRetcode::kFileNotFound:
      highsLogUser(log_options, HighsLogType::kError, "File %s not found\n",
                   filename.c_str());
      break;
    case FilereaderRetcode::kParserError:
      highsLogUser(log_options, HighsLogType::kError,
                   "Parser error reading %s\n", filename.c_str());
      break;
    case FilereaderRetcode::kNotImplemented:
      highsLogUser(log_options, HighsLogType::kError,
                   "Parser not implemented for %s", filename.c_str());
      break;
    case FilereaderRetcode::kTimeout:
      highsLogUser(log_options, HighsLogType::kError,
                   "Parser reached timeout\n", filename.c_str());
      break;
  }
}

std::string extractModelName(const std::string filename) {
  // Extract model name
  std::string name = filename;
  std::size_t found = name.find_last_of("/\\");
  if (found < name.size()) name = name.substr(found + 1);
  found = name.find_last_of(".");
  if (name.substr(found + 1) == "gz"
      //      || name.substr(found + 1) == "zip"
  ) {
    name.erase(found, name.size() - found);
    found = name.find_last_of(".");
  }
  if (found < name.size()) name.erase(found, name.size() - found);
  return name;
}
