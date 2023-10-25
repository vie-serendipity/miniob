#pragma once

#include <cstring>
#include "sql/parser/parse_defs.h"

extern const char *AGG_FUN_NAME[];
const char *agg_fun_to_string(AggFun fun);
AggFun agg_fun_from_string(const char *s);

struct Aggregation {
  AggFun agg_fun;
  std::string relation_name;
  std::string attribute_name;
};