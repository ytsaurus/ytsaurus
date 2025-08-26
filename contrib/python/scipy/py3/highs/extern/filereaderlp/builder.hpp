#ifndef __READERLP_BUILDER_HPP__
#define __READERLP_BUILDER_HPP__

#include <memory>
#include <string>
#include <unordered_map>

#include "model.hpp"

struct Builder {
  std::unordered_map<std::string, std::shared_ptr<Variable>> variables;

  Model model;

  std::shared_ptr<Variable> getvarbyname(const std::string& name) {
    auto it = variables.find(name);
    if (it != variables.end()) return it->second;
    auto newvar = std::shared_ptr<Variable>(new Variable(name));
    variables.insert(std::make_pair(name, newvar));
    model.variables.push_back(newvar);
    return newvar;
  }
};

#endif
