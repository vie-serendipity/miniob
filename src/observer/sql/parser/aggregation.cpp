//
// Created by my0sotis on 10/25/23.
//
#include "aggregation.h"

const char *AGG_FUN_NAME[] = {
    "max",
    "min",
    "count",
    "avg",
    "sum",
    "no_agg"
};

const char *agg_fun_to_string(AggFun fun)
{
  if (fun >= AGG_MAX && fun < NO_AGG) {
    return AGG_FUN_NAME[fun];
  }
  return "unknown";
}

AggFun agg_fun_from_string(const char *s)
{
  for (unsigned int i = 0; i < sizeof(AGG_FUN_NAME) / sizeof(AGG_FUN_NAME[0]); i++) {
    if (0 == strcmp(AGG_FUN_NAME[i], s)) {
      return (AggFun)i;
    }
  }
  return NO_AGG;
}