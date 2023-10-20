#include<cstdio>
#include<string>

#include "date.h"

#include "common/rc.h"

RC string_to_date(const char* s, int& date) {
    int year, month, day;
    int ret = sscanf(s, "%d-%d-%d", &year, &month, &day);
    if (ret != 3) {
        return RC::INVALID_ARGUMENT;
    }

    if (year < 1900 || year > 9999 || month < 1 || month > 12) {
        return RC::INVALID_ARGUMENT;
    }
    int max_day_in_month[] = {0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (!is_leap_year(year) && month == 2) {
        if (day < 1 || day > 28) {
            return RC::INVALID_ARGUMENT;
        }
    } else {
        if (day < 1 || day > max_day_in_month[month]) {
            return RC::INVALID_ARGUMENT;
        }
    }

    date = 10000 * year + 100 * month + day;
    return RC::SUCCESS;
}

inline bool is_leap_year(int year) {
    return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

std::string date_to_string(int date) {
    char buf[15];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", (date % 100000000) / 10000, (date % 10000) / 100, date % 100);
    std::string str(buf);
    return str;
}