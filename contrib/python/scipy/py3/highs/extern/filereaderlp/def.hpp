#ifndef __READERLP_DEF_HPP__
#define __READERLP_DEF_HPP__

#include <stdexcept>
#include <string>

void inline lpassert(bool condition) {
  if (!condition) {
    throw std::invalid_argument("File not existent or illegal file format.");
  }
}

const std::string LP_KEYWORD_INF[] = {"infinity", "inf"};
const std::string LP_KEYWORD_FREE[] = {"free"};

const unsigned int LP_KEYWORD_INF_N = 2;
const unsigned int LP_KEYWORD_FREE_N = 1;

#endif
