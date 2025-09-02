#include "reader.hpp"

#include <algorithm>
#include <array>
#include <cassert>
#include <cctype>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <unordered_map>
#include <vector>

#include "HConfig.h"  // for ZLIB_FOUND
#include "builder.hpp"
#include "def.hpp"
#ifdef ZLIB_FOUND
#include "../extern/zstr/zstr.hpp"
#endif

// Cygwin doesn't come with an implementation for strdup if compiled with
// std=cxx
#ifdef __CYGWIN__
#include <cstdlib>
char* strdup(const char* s) {
  size_t slen = strlen(s);
  char* result = (char*)malloc(slen + 1);
  if (result == NULL) {
    return NULL;
  }

  memcpy(result, s, slen + 1);
  return result;
}
#endif

enum class RawTokenType {
  NONE,
  STR,
  CONS,
  LESS,
  GREATER,
  EQUAL,
  COLON,
  LNEND,
  FLEND,
  BRKOP,
  BRKCL,
  PLUS,
  MINUS,
  HAT,
  SLASH,
  ASTERISK
};

struct RawToken {
  RawTokenType type = RawTokenType::NONE;
  std::string svalue;
  double dvalue = 0.0;

  inline bool istype(RawTokenType t) const { return this->type == t; }

  RawToken& operator=(RawTokenType t) {
    type = t;
    return *this;
  }
  RawToken& operator=(const std::string& v) {
    svalue = v;
    type = RawTokenType::STR;
    return *this;
  }
  RawToken& operator=(const double v) {
    dvalue = v;
    type = RawTokenType::CONS;
    return *this;
  }
};

enum class ProcessedTokenType {
  NONE,
  SECID,
  VARID,
  CONID,
  CONST,
  FREE,
  BRKOP,
  BRKCL,
  COMP,
  LNEND,
  SLASH,
  ASTERISK,
  HAT,
  SOSTYPE
};

enum class LpSectionKeyword {
  NONE,
  OBJMIN,
  OBJMAX,
  CON,
  BOUNDS,
  GEN,
  BIN,
  SEMI,
  SOS,
  END
};

static const std::unordered_map<std::string, LpSectionKeyword>
    sectionkeywordmap{{"minimize", LpSectionKeyword::OBJMIN},
                      {"min", LpSectionKeyword::OBJMIN},
                      {"minimum", LpSectionKeyword::OBJMIN},
                      {"maximize", LpSectionKeyword::OBJMAX},
                      {"max", LpSectionKeyword::OBJMAX},
                      {"maximum", LpSectionKeyword::OBJMAX},
                      {"subject to", LpSectionKeyword::CON},
                      {"such that", LpSectionKeyword::CON},
                      {"st", LpSectionKeyword::CON},
                      {"s.t.", LpSectionKeyword::CON},
                      {"bounds", LpSectionKeyword::BOUNDS},
                      {"bound", LpSectionKeyword::BOUNDS},
                      {"binary", LpSectionKeyword::BIN},
                      {"binaries", LpSectionKeyword::BIN},
                      {"bin", LpSectionKeyword::BIN},
                      {"general", LpSectionKeyword::GEN},
                      {"generals", LpSectionKeyword::GEN},
                      {"gen", LpSectionKeyword::GEN},
                      {"integer", LpSectionKeyword::GEN},
                      {"integers", LpSectionKeyword::GEN},
                      {"semi-continuous", LpSectionKeyword::SEMI},
                      {"semi", LpSectionKeyword::SEMI},
                      {"semis", LpSectionKeyword::SEMI},
                      {"sos", LpSectionKeyword::SOS},
                      {"end", LpSectionKeyword::END}};

enum class SosType { SOS1, SOS2 };

enum class LpComparisonType { LEQ, L, EQ, G, GEQ };

struct ProcessedToken {
  ProcessedTokenType type;
  union {
    LpSectionKeyword keyword;
    SosType sostype;
    char* name;
    double value;
    LpComparisonType dir;
  };

  ProcessedToken(const ProcessedToken&) = delete;
  ProcessedToken(ProcessedToken&& t) : type(t.type) {
    switch (type) {
      case ProcessedTokenType::SECID:
        keyword = t.keyword;
        break;
      case ProcessedTokenType::SOSTYPE:
        sostype = t.sostype;
        break;
      case ProcessedTokenType::CONID:
      case ProcessedTokenType::VARID:
        name = t.name;
        break;
      case ProcessedTokenType::CONST:
        value = t.value;
        break;
      case ProcessedTokenType::COMP:
        dir = t.dir;
        break;
      default:;
    }
    t.type = ProcessedTokenType::NONE;
  }

  ProcessedToken(ProcessedTokenType t) : type(t){};

  ProcessedToken(LpSectionKeyword kw)
      : type(ProcessedTokenType::SECID), keyword(kw){};

  ProcessedToken(SosType sos)
      : type(ProcessedTokenType::SOSTYPE), sostype(sos){};

  ProcessedToken(ProcessedTokenType t, const std::string& s) : type(t) {
    assert(t == ProcessedTokenType::CONID || t == ProcessedTokenType::VARID);
#ifndef _WIN32
    name = strdup(s.c_str());
#else
    name = _strdup(s.c_str());
#endif
  };

  ProcessedToken(double v) : type(ProcessedTokenType::CONST), value(v){};

  ProcessedToken(LpComparisonType comp)
      : type(ProcessedTokenType::COMP), dir(comp){};

  ~ProcessedToken() {
    if (type == ProcessedTokenType::CONID || type == ProcessedTokenType::VARID)
      free(name);
  }
};

// how many raw tokens to cache
// set to how many tokens we may need to look ahead
#define NRAWTOKEN 3

const double kHighsInf = std::numeric_limits<double>::infinity();
class Reader {
 private:
#ifdef ZLIB_FOUND
  zstr::ifstream file;
#else
  std::ifstream file;
#endif
  std::string linebuffer;
  std::size_t linebufferpos;
  std::array<RawToken, NRAWTOKEN> rawtokens;
  std::vector<ProcessedToken> processedtokens;
  // store for each section a pointer to its begin and end (pointer to element
  // after last)
  std::map<LpSectionKeyword, std::pair<std::vector<ProcessedToken>::iterator,
                                       std::vector<ProcessedToken>::iterator> >
      sectiontokens;

  Builder builder;

  bool readnexttoken(RawToken&);
  void nextrawtoken(size_t howmany = 1);
  void processtokens();
  void splittokens();
  void processsections();
  void processnonesec();
  void processobjsec();
  void processconsec();
  void processboundssec();
  void processbinsec();
  void processgensec();
  void processsemisec();
  void processsossec();
  void processendsec();
  void parseexpression(std::vector<ProcessedToken>::iterator& it,
                       std::vector<ProcessedToken>::iterator end,
                       std::shared_ptr<Expression> expr, bool isobj);

 public:
  Reader(std::string filename) {
#ifdef ZLIB_FOUND
    try {
      file.open(filename);
    } catch (const strict_fstream::Exception& e) {
    }
#else
    file.open(filename);
#endif
    lpassert(file.is_open());
  };

  ~Reader() { file.close(); }

  Model read();
};

Model readinstance(std::string filename) {
  Reader reader(filename);
  return reader.read();
}

// convert string to lower-case, modifies string
static inline void tolower(std::string& s) {
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return std::tolower(c); });
}

static inline bool iskeyword(const std::string& str,
                             const std::string* keywords, const int nkeywords) {
  for (int i = 0; i < nkeywords; i++) {
    if (str == keywords[i]) {
      return true;
    }
  }
  return false;
}

static inline LpSectionKeyword parsesectionkeyword(const std::string& str) {
  // look up lower case
  auto it(sectionkeywordmap.find(str));
  if (it != sectionkeywordmap.end()) return it->second;

  return LpSectionKeyword::NONE;
}

Model Reader::read() {
  // std::clog << "Reading input, tokenizing..." << std::endl;
  this->linebufferpos = 0;
  // read first NRAWTOKEN token
  // if file ends early, then all remaining tokens are set to FLEND
  for (size_t i = 0; i < NRAWTOKEN; ++i)
    while (!readnexttoken(rawtokens[i]))
      ;

  processtokens();

  linebuffer.clear();
  linebuffer.shrink_to_fit();

  // std::clog << "Splitting tokens..." << std::endl;
  splittokens();

  // std::clog << "Setting up model..." << std::endl;
  //
  // Since
  //
  // "The problem statement must begin with the word MINIMIZE or
  // MAXIMIZE, MINIMUM or MAXIMUM, or the abbreviations MIN or MAX, in
  // any combination of upper- and lower-case characters. The word
  // introduces the objective function section."
  //
  // Use positivity of sectiontokens.count(LpSectionKeyword::OBJMIN) +
  // sectiontokens.count(LpSectionKeyword::OBJMAX) to identify garbage file
  //

  const int num_objective_section =
    sectiontokens.count(LpSectionKeyword::OBJMIN) +
    sectiontokens.count(LpSectionKeyword::OBJMAX);
  lpassert(num_objective_section>0);

  processsections();
  processedtokens.clear();
  processedtokens.shrink_to_fit();

  return builder.model;
}

void Reader::processnonesec() {
  lpassert(sectiontokens.count(LpSectionKeyword::NONE) == 0);
}

void Reader::parseexpression(std::vector<ProcessedToken>::iterator& it,
                             std::vector<ProcessedToken>::iterator end,
                             std::shared_ptr<Expression> expr, bool isobj) {
  if (it != end && it->type == ProcessedTokenType::CONID) {
    expr->name = it->name;
    ++it;
  }

  while (it != end) {
    std::vector<ProcessedToken>::iterator next = it;
    ++next;
    // const var
    if (next != end && it->type == ProcessedTokenType::CONST &&
        next->type == ProcessedTokenType::VARID) {
      std::string name = next->name;

      std::shared_ptr<LinTerm> linterm =
          std::shared_ptr<LinTerm>(new LinTerm());
      linterm->coef = it->value;
      linterm->var = builder.getvarbyname(name);
      //	 printf("LpReader: Term   %+g %s\n", linterm->coef,
      //name.c_str());
      expr->linterms.push_back(linterm);

      ++it;
      ++it;
      continue;
    }

    // const
    if (it->type == ProcessedTokenType::CONST) {
      //      printf("LpReader: Offset change from %+g by %+g\n", expr->offset, it->value);
      expr->offset += it->value;
      ++it;
      continue;
    }

    // var
    if (it->type == ProcessedTokenType::VARID) {
      std::string name = it->name;

      std::shared_ptr<LinTerm> linterm =
          std::shared_ptr<LinTerm>(new LinTerm());
      linterm->coef = 1.0;
      linterm->var = builder.getvarbyname(name);
      //	 printf("LpReader: Term   %+g %s\n", linterm->coef,
      //name.c_str());
      expr->linterms.push_back(linterm);

      ++it;
      continue;
    }

    // quadratic expression
    if (next != end && it->type == ProcessedTokenType::BRKOP) {
      ++it;
      while (it != end && it->type != ProcessedTokenType::BRKCL) {
        // const var hat const
        std::vector<ProcessedToken>::iterator next1 = it;  // token after it
        std::vector<ProcessedToken>::iterator next2 = it;  // token 2nd-after it
        std::vector<ProcessedToken>::iterator next3 = it;  // token 3rd-after it
        ++next1;
        ++next2;
        ++next3;
        if (next1 != end) {
          ++next2;
          ++next3;
        }
        if (next2 != end) ++next3;

        if (next3 != end && it->type == ProcessedTokenType::CONST &&
            next1->type == ProcessedTokenType::VARID &&
            next2->type == ProcessedTokenType::HAT &&
            next3->type == ProcessedTokenType::CONST) {
          std::string name = next1->name;

          lpassert(next3->value == 2.0);

          std::shared_ptr<QuadTerm> quadterm =
              std::shared_ptr<QuadTerm>(new QuadTerm());
          quadterm->coef = it->value;
          quadterm->var1 = builder.getvarbyname(name);
          quadterm->var2 = builder.getvarbyname(name);
          expr->quadterms.push_back(quadterm);

          it = ++next3;
          continue;
        }

        // var hat const
        if (next2 != end && it->type == ProcessedTokenType::VARID &&
            next1->type == ProcessedTokenType::HAT &&
            next2->type == ProcessedTokenType::CONST) {
          std::string name = it->name;

          lpassert(next2->value == 2.0);

          std::shared_ptr<QuadTerm> quadterm =
              std::shared_ptr<QuadTerm>(new QuadTerm());
          quadterm->coef = 1.0;
          quadterm->var1 = builder.getvarbyname(name);
          quadterm->var2 = builder.getvarbyname(name);
          expr->quadterms.push_back(quadterm);

          it = next3;
          continue;
        }

        // const var asterisk var
        if (next3 != end && it->type == ProcessedTokenType::CONST &&
            next1->type == ProcessedTokenType::VARID &&
            next2->type == ProcessedTokenType::ASTERISK &&
            next3->type == ProcessedTokenType::VARID) {
          std::string name1 = next1->name;
          std::string name2 = next3->name;

          std::shared_ptr<QuadTerm> quadterm =
              std::shared_ptr<QuadTerm>(new QuadTerm());
          quadterm->coef = it->value;
          quadterm->var1 = builder.getvarbyname(name1);
          quadterm->var2 = builder.getvarbyname(name2);
          expr->quadterms.push_back(quadterm);

          it = ++next3;
          continue;
        }

        // var asterisk var
        if (next2 != end && it->type == ProcessedTokenType::VARID &&
            next1->type == ProcessedTokenType::ASTERISK &&
            next2->type == ProcessedTokenType::VARID) {
          std::string name1 = it->name;
          std::string name2 = next2->name;

          std::shared_ptr<QuadTerm> quadterm =
              std::shared_ptr<QuadTerm>(new QuadTerm());
          quadterm->coef = 1.0;
          quadterm->var1 = builder.getvarbyname(name1);
          quadterm->var2 = builder.getvarbyname(name2);
          expr->quadterms.push_back(quadterm);

          it = next3;
          continue;
        }
        break;
      }
      if (isobj) {
        // only in the objective function, a quadratic term is followed by
        // "/2.0"
        std::vector<ProcessedToken>::iterator next1 = it;  // token after it
        std::vector<ProcessedToken>::iterator next2 = it;  // token 2nd-after it
        ++next1;
        ++next2;
        if (next1 != end) ++next2;

        lpassert(next2 != end);
        lpassert(it->type == ProcessedTokenType::BRKCL);
        lpassert(next1->type == ProcessedTokenType::SLASH);
        lpassert(next2->type == ProcessedTokenType::CONST);
        lpassert(next2->value == 2.0);
        it = ++next2;
      } else {
        lpassert(it != end);
        lpassert(it->type == ProcessedTokenType::BRKCL);
        ++it;
      }
      continue;
    }

    break;
  }
}

void Reader::processobjsec() {
  builder.model.objective = std::shared_ptr<Expression>(new Expression);
  if (sectiontokens.count(LpSectionKeyword::OBJMIN)) {
    builder.model.sense = ObjectiveSense::MIN;
    parseexpression(sectiontokens[LpSectionKeyword::OBJMIN].first,
                    sectiontokens[LpSectionKeyword::OBJMIN].second,
                    builder.model.objective, true);
    lpassert(sectiontokens[LpSectionKeyword::OBJMIN].first ==
             sectiontokens[LpSectionKeyword::OBJMIN]
                 .second);  // all section tokens should have been processed
  } else if (sectiontokens.count(LpSectionKeyword::OBJMAX)) {
    builder.model.sense = ObjectiveSense::MAX;
    parseexpression(sectiontokens[LpSectionKeyword::OBJMAX].first,
                    sectiontokens[LpSectionKeyword::OBJMAX].second,
                    builder.model.objective, true);
    lpassert(sectiontokens[LpSectionKeyword::OBJMAX].first ==
             sectiontokens[LpSectionKeyword::OBJMAX]
                 .second);  // all section tokens should have been processed
  }
}

void Reader::processconsec() {
  if (!sectiontokens.count(LpSectionKeyword::CON)) return;
  std::vector<ProcessedToken>::iterator& begin(
      sectiontokens[LpSectionKeyword::CON].first);
  std::vector<ProcessedToken>::iterator& end(
      sectiontokens[LpSectionKeyword::CON].second);
  while (begin != end) {
    std::shared_ptr<Constraint> con =
        std::shared_ptr<Constraint>(new Constraint);
    parseexpression(begin, end, con->expr, false);
    // should not be at end of section yet, but a comparison operator should be
    // next
    lpassert(begin != sectiontokens[LpSectionKeyword::CON].second);
    lpassert(begin->type == ProcessedTokenType::COMP);
    LpComparisonType dir = begin->dir;
    ++begin;

    // should still not be at end of section yet, but a right-hand-side value
    // should be next
    lpassert(begin != sectiontokens[LpSectionKeyword::CON].second);
    lpassert(begin->type == ProcessedTokenType::CONST);
    switch (dir) {
      case LpComparisonType::EQ:
        con->lowerbound = con->upperbound = begin->value;
        break;
      case LpComparisonType::LEQ:
        con->upperbound = begin->value;
        break;
      case LpComparisonType::GEQ:
        con->lowerbound = begin->value;
        break;
      default:
        lpassert(false);
    }
    builder.model.constraints.push_back(con);
    ++begin;
  }
}

void Reader::processboundssec() {
  if (!sectiontokens.count(LpSectionKeyword::BOUNDS)) return;
  std::vector<ProcessedToken>::iterator& begin(
      sectiontokens[LpSectionKeyword::BOUNDS].first);
  std::vector<ProcessedToken>::iterator& end(
      sectiontokens[LpSectionKeyword::BOUNDS].second);
  while (begin != end) {
    std::vector<ProcessedToken>::iterator next1 = begin;  // token after begin
    ++next1;

    // VAR free
    if (next1 != end && begin->type == ProcessedTokenType::VARID &&
        next1->type == ProcessedTokenType::FREE) {
      std::string name = begin->name;
      std::shared_ptr<Variable> var = builder.getvarbyname(name);
      var->lowerbound = -kHighsInf;
      var->upperbound = kHighsInf;
      begin = ++next1;
      continue;
    }

    std::vector<ProcessedToken>::iterator next2 =
        next1;  // token 2nd-after begin
    std::vector<ProcessedToken>::iterator next3 =
        next1;  // token 3rd-after begin
    std::vector<ProcessedToken>::iterator next4 =
        next1;  // token 4th-after begin
    if (next1 != end) {
      ++next2;
      ++next3;
      ++next4;
    }
    if (next2 != end) {
      ++next3;
      ++next4;
    }
    if (next3 != end) ++next4;

    // CONST COMP VAR COMP CONST
    if (next4 != end && begin->type == ProcessedTokenType::CONST &&
        next1->type == ProcessedTokenType::COMP &&
        next2->type == ProcessedTokenType::VARID &&
        next3->type == ProcessedTokenType::COMP &&
        next4->type == ProcessedTokenType::CONST) {
      lpassert(next1->dir == LpComparisonType::LEQ);
      lpassert(next3->dir == LpComparisonType::LEQ);

      double lb = begin->value;
      double ub = next4->value;

      std::string name = next2->name;
      std::shared_ptr<Variable> var = builder.getvarbyname(name);

      var->lowerbound = lb;
      var->upperbound = ub;

      begin = ++next4;
      continue;
    }

    // CONST COMP VAR
    if (next2 != end && begin->type == ProcessedTokenType::CONST &&
        next1->type == ProcessedTokenType::COMP &&
        next2->type == ProcessedTokenType::VARID) {
      double value = begin->value;
      std::string name = next2->name;
      std::shared_ptr<Variable> var = builder.getvarbyname(name);
      LpComparisonType dir = next1->dir;

      lpassert(dir != LpComparisonType::L && dir != LpComparisonType::G);

      switch (dir) {
        case LpComparisonType::LEQ:
          var->lowerbound = value;
          break;
        case LpComparisonType::GEQ:
          var->upperbound = value;
          break;
        case LpComparisonType::EQ:
          var->lowerbound = var->upperbound = value;
          break;
        default:
          lpassert(false);
      }
      begin = next3;
      continue;
    }

    // VAR COMP CONST
    if (next2 != end && begin->type == ProcessedTokenType::VARID &&
        next1->type == ProcessedTokenType::COMP &&
        next2->type == ProcessedTokenType::CONST) {
      double value = next2->value;
      std::string name = begin->name;
      std::shared_ptr<Variable> var = builder.getvarbyname(name);
      LpComparisonType dir = next1->dir;

      lpassert(dir != LpComparisonType::L && dir != LpComparisonType::G);

      switch (dir) {
        case LpComparisonType::LEQ:
          var->upperbound = value;
          break;
        case LpComparisonType::GEQ:
          var->lowerbound = value;
          break;
        case LpComparisonType::EQ:
          var->lowerbound = var->upperbound = value;
          break;
        default:
          lpassert(false);
      }
      begin = next3;
      continue;
    }

    lpassert(false);
  }
}

void Reader::processbinsec() {
  const LpSectionKeyword this_section_keyword = LpSectionKeyword::BIN;
  if (!sectiontokens.count(this_section_keyword)) return;
  std::vector<ProcessedToken>::iterator& begin(
      sectiontokens[this_section_keyword].first);
  std::vector<ProcessedToken>::iterator& end(
      sectiontokens[this_section_keyword].second);
  for (; begin != end; ++begin) {
    if (begin->type == ProcessedTokenType::SECID) {
      // Possible to have repeat of keyword for this section type
      lpassert(begin->keyword == this_section_keyword);
      continue;
    }
    lpassert(begin->type == ProcessedTokenType::VARID);
    std::string name = begin->name;
    std::shared_ptr<Variable> var = builder.getvarbyname(name);
    var->type = VariableType::BINARY;
    // Respect any bounds already declared
    if (var->upperbound == kHighsInf) var->upperbound = 1.0;
  }
}

void Reader::processgensec() {
  const LpSectionKeyword this_section_keyword = LpSectionKeyword::GEN;
  if (!sectiontokens.count(this_section_keyword)) return;
  std::vector<ProcessedToken>::iterator& begin(
      sectiontokens[this_section_keyword].first);
  std::vector<ProcessedToken>::iterator& end(
      sectiontokens[this_section_keyword].second);
  for (; begin != end; ++begin) {
    if (begin->type == ProcessedTokenType::SECID) {
      // Possible to have repeat of keyword for this section type
      lpassert(begin->keyword == this_section_keyword);
      continue;
    }
    lpassert(begin->type == ProcessedTokenType::VARID);
    std::string name = begin->name;
    std::shared_ptr<Variable> var = builder.getvarbyname(name);
    if (var->type == VariableType::SEMICONTINUOUS) {
      var->type = VariableType::SEMIINTEGER;
    } else {
      var->type = VariableType::GENERAL;
    }
  }
}

void Reader::processsemisec() {
  const LpSectionKeyword this_section_keyword = LpSectionKeyword::SEMI;
  if (!sectiontokens.count(this_section_keyword)) return;
  std::vector<ProcessedToken>::iterator& begin(
      sectiontokens[this_section_keyword].first);
  std::vector<ProcessedToken>::iterator& end(
      sectiontokens[this_section_keyword].second);
  for (; begin != end; ++begin) {
    if (begin->type == ProcessedTokenType::SECID) {
      // Possible to have repeat of keyword for this section type
      lpassert(begin->keyword == this_section_keyword);
      continue;
    }
    lpassert(begin->type == ProcessedTokenType::VARID);
    std::string name = begin->name;
    std::shared_ptr<Variable> var = builder.getvarbyname(name);
    if (var->type == VariableType::GENERAL) {
      var->type = VariableType::SEMIINTEGER;
    } else {
      var->type = VariableType::SEMICONTINUOUS;
    }
  }
}

void Reader::processsossec() {
  const LpSectionKeyword this_section_keyword = LpSectionKeyword::SOS;
  if (!sectiontokens.count(this_section_keyword)) return;
  std::vector<ProcessedToken>::iterator& begin(
      sectiontokens[this_section_keyword].first);
  std::vector<ProcessedToken>::iterator& end(
      sectiontokens[this_section_keyword].second);
  while (begin != end) {
    std::shared_ptr<SOS> sos = std::shared_ptr<SOS>(new SOS);

    // sos1: S1 :: x1 : 1  x2 : 2  x3 : 3

    // name of SOS is mandatory
    lpassert(begin->type == ProcessedTokenType::CONID);
    sos->name = begin->name;
    ++begin;

    // SOS type
    lpassert(begin != end);
    lpassert(begin->type == ProcessedTokenType::SOSTYPE);
    sos->type = begin->sostype == SosType::SOS1 ? 1 : 2;
    ++begin;

    while (begin != end) {
      // process all "var : weight" entries
      // when processtokens() sees a string followed by a colon, it classifies
      // this as a CONID but in a SOS section, this is actually a variable
      // identifier
      if (begin->type != ProcessedTokenType::CONID) break;
      std::string name = begin->name;
      std::vector<ProcessedToken>::iterator next = begin;
      ++next;
      if (next != end && next->type == ProcessedTokenType::CONST) {
        auto var = builder.getvarbyname(name);
        double weight = next->value;

        sos->entries.push_back({var, weight});

        begin = ++next;
        continue;
      }

      break;
    }

    builder.model.soss.push_back(sos);
  }
}

void Reader::processendsec() {
  lpassert(sectiontokens.count(LpSectionKeyword::END) == 0);
}

void Reader::processsections() {
  processnonesec();
  processobjsec();
  processconsec();
  processboundssec();
  processgensec();
  processbinsec();
  processsemisec();
  processsossec();
  processendsec();
}

void Reader::splittokens() {
  LpSectionKeyword currentsection = LpSectionKeyword::NONE;

  bool debug_open_section = false;
  for (std::vector<ProcessedToken>::iterator it(processedtokens.begin());
       it != processedtokens.end(); ++it) {
    // Look for section keywords
    if (it->type != ProcessedTokenType::SECID) continue;
    // currentsection is initially LpSectionKeyword::NONE, so the
    // first section ID will be a new section type
    //
    // Only record change of section and check for repeated
    // section if the keyword is for a different section. Allows
    // repetition of Integers and General (cf #1299) for example
    const bool new_section_type = currentsection != it->keyword;
    if (new_section_type) {
      if (currentsection != LpSectionKeyword::NONE) {
        // Current section is non-trivial, so mark its end, using the
        // value of currentsection to indicate that there is no open
        // section
        lpassert(debug_open_section);
        sectiontokens[currentsection].second = it;
        debug_open_section = false;
        currentsection = LpSectionKeyword::NONE;
      }
    }
    std::vector<ProcessedToken>::iterator next = it;
    ++next;
    if (next == processedtokens.end() ||
        next->type == ProcessedTokenType::SECID) {
      // Reached the end of the tokens or the new section is empty
      //
      // currentsection will be LpSectionKeyword::NONE unless the
      // second of two sections of the same type is empty and the
      // next section is of a new type, in which case mark the end of
      // the current section
      if (currentsection != LpSectionKeyword::NONE &&
          currentsection != next->keyword) {
        lpassert(debug_open_section);
        sectiontokens[currentsection].second = it;
        debug_open_section = false;
      }
      currentsection = LpSectionKeyword::NONE;
      lpassert(!debug_open_section);
      continue;
    }
    // Next section is non-empty
    if (new_section_type) {
      // Section type change
      currentsection = it->keyword;
      // Make sure the new section type has not occurred previously
      lpassert(sectiontokens.count(currentsection) == 0);
      // Remember the beginning of the new section: its the token
      // following the current one
      lpassert(!debug_open_section);
      sectiontokens[currentsection].first = next;
      debug_open_section = true;
    }
    // Always ends with either an open section or a section type of
    // LpSectionKeyword::NONE
    lpassert(debug_open_section != (currentsection == LpSectionKeyword::NONE));
  }
  // Check that the last section has been closed
  lpassert(currentsection == LpSectionKeyword::NONE);
}

void Reader::processtokens() {
  std::string svalue_lc;
  while (!rawtokens[0].istype(RawTokenType::FLEND)) {
    fflush(stdout);

    // Slash + asterisk: comment, skip everything up to next asterisk + slash
    if (rawtokens[0].istype(RawTokenType::SLASH) &&
        rawtokens[1].istype(RawTokenType::ASTERISK)) {
      do {
        nextrawtoken(2);
      } while (!(rawtokens[0].istype(RawTokenType::ASTERISK) &&
                 rawtokens[1].istype(RawTokenType::SLASH)) &&
               !rawtokens[0].istype(RawTokenType::FLEND));
      nextrawtoken(2);
      continue;
    }

    if (rawtokens[0].istype(RawTokenType::STR)) {
      svalue_lc = rawtokens[0].svalue;
      tolower(svalue_lc);
    }

    // long section keyword semi-continuous
    if (rawtokens[0].istype(RawTokenType::STR) &&
        rawtokens[1].istype(RawTokenType::MINUS) &&
        rawtokens[2].istype(RawTokenType::STR)) {
      std::string temp = rawtokens[2].svalue;
      tolower(temp);
      LpSectionKeyword keyword = parsesectionkeyword(svalue_lc + "-" + temp);
      if (keyword != LpSectionKeyword::NONE) {
        processedtokens.emplace_back(keyword);
        nextrawtoken(3);
        continue;
      }
    }

    // long section keyword subject to/such that
    if (rawtokens[0].istype(RawTokenType::STR) &&
        rawtokens[1].istype(RawTokenType::STR)) {
      std::string temp = rawtokens[1].svalue;
      tolower(temp);
      LpSectionKeyword keyword = parsesectionkeyword(svalue_lc + " " + temp);
      if (keyword != LpSectionKeyword::NONE) {
        processedtokens.emplace_back(keyword);
        nextrawtoken(2);
        continue;
      }
    }

    // other section keyword
    if (rawtokens[0].istype(RawTokenType::STR)) {
      LpSectionKeyword keyword = parsesectionkeyword(svalue_lc);
      if (keyword != LpSectionKeyword::NONE) {
        processedtokens.emplace_back(keyword);
        nextrawtoken();
        continue;
      }
    }

    // sos type identifier? "S1 ::" or "S2 ::"
    if (rawtokens[0].istype(RawTokenType::STR) &&
        rawtokens[1].istype(RawTokenType::COLON) &&
        rawtokens[2].istype(RawTokenType::COLON)) {
      lpassert(rawtokens[0].svalue.length() == 2);
      lpassert(rawtokens[0].svalue[0] == 'S' || rawtokens[0].svalue[0] == 's');
      lpassert(rawtokens[0].svalue[1] == '1' || rawtokens[0].svalue[1] == '2');
      processedtokens.emplace_back(
          rawtokens[0].svalue[1] == '1' ? SosType::SOS1 : SosType::SOS2);
      nextrawtoken(3);
      continue;
    }

    // constraint identifier?
    if (rawtokens[0].istype(RawTokenType::STR) &&
        rawtokens[1].istype(RawTokenType::COLON)) {
      processedtokens.emplace_back(ProcessedTokenType::CONID,
                                   rawtokens[0].svalue);
      nextrawtoken(2);
      continue;
    }

    // check if free
    if (rawtokens[0].istype(RawTokenType::STR) &&
        iskeyword(svalue_lc, LP_KEYWORD_FREE, LP_KEYWORD_FREE_N)) {
      processedtokens.emplace_back(ProcessedTokenType::FREE);
      nextrawtoken();
      continue;
    }

    // check if infinity
    if (rawtokens[0].istype(RawTokenType::STR) &&
        iskeyword(svalue_lc, LP_KEYWORD_INF, LP_KEYWORD_INF_N)) {
      processedtokens.emplace_back(kHighsInf);
      nextrawtoken();
      continue;
    }

    // assume var identifier
    if (rawtokens[0].istype(RawTokenType::STR)) {
      processedtokens.emplace_back(ProcessedTokenType::VARID,
                                   rawtokens[0].svalue);
      nextrawtoken();
      continue;
    }

    // + or -
    if (rawtokens[0].istype(RawTokenType::PLUS) ||
        rawtokens[0].istype(RawTokenType::MINUS)) {
      double sign = rawtokens[0].istype(RawTokenType::PLUS) ? 1.0 : -1.0;
      nextrawtoken();

      // another + or - for #948, #950
      if (rawtokens[0].istype(RawTokenType::PLUS) ||
          rawtokens[0].istype(RawTokenType::MINUS)) {
        sign *= rawtokens[0].istype(RawTokenType::PLUS) ? 1.0 : -1.0;
        nextrawtoken();
      }

      // +/- Constant
      if (rawtokens[0].istype(RawTokenType::CONS)) {
        processedtokens.emplace_back(sign * rawtokens[0].dvalue);
        nextrawtoken();
        continue;
      }

      // + [, + + [, - - [
      if (rawtokens[0].istype(RawTokenType::BRKOP) && sign == 1.0) {
        processedtokens.emplace_back(ProcessedTokenType::BRKOP);
        nextrawtoken();
        continue;
      }

      // - [, + - [, - + [
      if (rawtokens[0].istype(RawTokenType::BRKOP)) lpassert(false);

      // +/- variable name
      if (rawtokens[0].istype(RawTokenType::STR)) {
        processedtokens.emplace_back(sign);
        continue;
      }

      // +/- (possibly twice) followed by something that isn't a constant,
      // opening bracket, or string (variable name)
      if (rawtokens[0].istype(RawTokenType::GREATER)) {
        // ">" suggests that the file contains indicator constraints
        printf(
            "File appears to contain indicator constraints: cannot currently "
            "be handled by HiGHS\n");
      }
      lpassert(false);
    }

    // constant [
    if (rawtokens[0].istype(RawTokenType::CONS) &&
        rawtokens[1].istype(RawTokenType::BRKOP)) {
      lpassert(false);
    }

    // constant
    if (rawtokens[0].istype(RawTokenType::CONS)) {
      processedtokens.emplace_back(rawtokens[0].dvalue);
      nextrawtoken();
      continue;
    }

    // [
    if (rawtokens[0].istype(RawTokenType::BRKOP)) {
      processedtokens.emplace_back(ProcessedTokenType::BRKOP);
      nextrawtoken();
      continue;
    }

    // ]
    if (rawtokens[0].istype(RawTokenType::BRKCL)) {
      processedtokens.emplace_back(ProcessedTokenType::BRKCL);
      nextrawtoken();
      continue;
    }

    // /
    if (rawtokens[0].istype(RawTokenType::SLASH)) {
      processedtokens.emplace_back(ProcessedTokenType::SLASH);
      nextrawtoken();
      continue;
    }

    // *
    if (rawtokens[0].istype(RawTokenType::ASTERISK)) {
      processedtokens.emplace_back(ProcessedTokenType::ASTERISK);
      nextrawtoken();
      continue;
    }

    // ^
    if (rawtokens[0].istype(RawTokenType::HAT)) {
      processedtokens.emplace_back(ProcessedTokenType::HAT);
      nextrawtoken();
      continue;
    }

    // <=
    if (rawtokens[0].istype(RawTokenType::LESS) &&
        rawtokens[1].istype(RawTokenType::EQUAL)) {
      processedtokens.emplace_back(LpComparisonType::LEQ);
      nextrawtoken(2);
      continue;
    }

    // <
    if (rawtokens[0].istype(RawTokenType::LESS)) {
      processedtokens.emplace_back(LpComparisonType::L);
      nextrawtoken();
      continue;
    }

    // >=
    if (rawtokens[0].istype(RawTokenType::GREATER) &&
        rawtokens[1].istype(RawTokenType::EQUAL)) {
      processedtokens.emplace_back(LpComparisonType::GEQ);
      nextrawtoken(2);
      continue;
    }

    // >
    if (rawtokens[0].istype(RawTokenType::GREATER)) {
      processedtokens.emplace_back(LpComparisonType::G);
      nextrawtoken();
      continue;
    }

    // =
    if (rawtokens[0].istype(RawTokenType::EQUAL)) {
      processedtokens.emplace_back(LpComparisonType::EQ);
      nextrawtoken();
      continue;
    }

    // FILEEND should have been handled in condition of while()
    assert(!rawtokens[0].istype(RawTokenType::FLEND));

    // catch all unknown symbols
    lpassert(false);
    break;
  }
}

void Reader::nextrawtoken(size_t howmany) {
  assert(howmany > 0);
  assert(howmany <= NRAWTOKEN);
  static_assert(NRAWTOKEN == 3,
                "code below need to be adjusted if NRAWTOKEN changes");
  switch (howmany) {
    case 1: {
      rawtokens[0] = std::move(rawtokens[1]);
      rawtokens[1] = std::move(rawtokens[2]);
      while (!readnexttoken(rawtokens[2]))
        ;
      break;
    }
    case 2: {
      rawtokens[0] = std::move(rawtokens[2]);
      while (!readnexttoken(rawtokens[1]))
        ;
      while (!readnexttoken(rawtokens[2]))
        ;
      break;
    }
    case 3: {
      while (!readnexttoken(rawtokens[0]))
        ;
      while (!readnexttoken(rawtokens[1]))
        ;
      while (!readnexttoken(rawtokens[2]))
        ;
      break;
    }
    default: {
      size_t i = 0;
      // move tokens up
      for (; i < NRAWTOKEN - howmany; ++i)
        rawtokens[i] = std::move(rawtokens[i + howmany]);
      // read new tokens at end positions
      for (; i < NRAWTOKEN; ++i)
        // call readnexttoken() to overwrite current token
        // if it didn't actually read a token (returns false), then call again
        while (!readnexttoken(rawtokens[i]))
          ;
    }
  }
}

// return true, if token has been set; return false if skipped over whitespace
// only
bool Reader::readnexttoken(RawToken& t) {
  if (this->linebufferpos == this->linebuffer.size()) {
    // read next line if any are left.
    if (this->file.eof()) {
      t = RawTokenType::FLEND;
      return true;
    }
    std::getline(this->file, linebuffer);

    // drop \r
    if (!linebuffer.empty() && linebuffer.back() == '\r') linebuffer.pop_back();

    // reset linebufferpos
    this->linebufferpos = 0;
  }

  // check single character tokens
  char nextchar = this->linebuffer[this->linebufferpos];

  switch (nextchar) {
    // check for comment
    case '\\':
      // skip rest of line
      this->linebufferpos = this->linebuffer.size();
      return false;

    // check for bracket opening
    case '[':
      t = RawTokenType::BRKOP;
      this->linebufferpos++;
      return true;

    // check for bracket closing
    case ']':
      t = RawTokenType::BRKCL;
      this->linebufferpos++;
      return true;

    // check for less sign
    case '<':
      t = RawTokenType::LESS;
      this->linebufferpos++;
      return true;

    // check for greater sign
    case '>':
      t = RawTokenType::GREATER;
      this->linebufferpos++;
      return true;

    // check for equal sign
    case '=':
      t = RawTokenType::EQUAL;
      this->linebufferpos++;
      return true;

    // check for colon
    case ':':
      t = RawTokenType::COLON;
      this->linebufferpos++;
      return true;

    // check for plus
    case '+':
      t = RawTokenType::PLUS;
      this->linebufferpos++;
      return true;

    // check for hat
    case '^':
      t = RawTokenType::HAT;
      this->linebufferpos++;
      return true;

    // check for slash
    case '/':
      t = RawTokenType::SLASH;
      this->linebufferpos++;
      return true;

    // check for asterisk
    case '*':
      t = RawTokenType::ASTERISK;
      this->linebufferpos++;
      return true;

    // check for minus
    case '-':
      t = RawTokenType::MINUS;
      this->linebufferpos++;
      return true;

    // check for whitespace
    case ' ':
    case '\t':
      this->linebufferpos++;
      return false;

    // check for line end
    case ';':
    case '\n':  // \n should not happen due to using getline()
      this->linebufferpos = this->linebuffer.size();
      return false;

    case '\0':  // empty line
      lpassert(this->linebufferpos == this->linebuffer.size());
      return false;
  }

  // check for double value
  const char* startptr = this->linebuffer.data() + this->linebufferpos;
  char* endptr;
  double constant = strtod(startptr, &endptr);
  if (endptr != startptr) {
    t = constant;
    this->linebufferpos += endptr - startptr;
    return true;
  }

  // assume it's an (section/variable/constraint) identifier
  auto endpos =
      this->linebuffer.find_first_of("\t\n\\:+<>^= /-*[]", this->linebufferpos);
  if (endpos == std::string::npos)
    endpos = this->linebuffer.size();  // take complete rest of string
  if (endpos > this->linebufferpos) {
    t = std::string(this->linebuffer, this->linebufferpos,
                    endpos - this->linebufferpos);
    this->linebufferpos = endpos;
    return true;
  }

  lpassert(false);
  return false;
}
