#pragma once

#include<string>

#include "common/rc.h"

RC string_to_date(const char* s, int& date);
inline bool is_leap_year(int year);
std::string date_to_string(int date);