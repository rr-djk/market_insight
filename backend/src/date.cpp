#include "date.hpp"
#include <iomanip>
#include <sstream>

std::optional<Date> parse_date(const std::string& s)
{
    if (s.size() != 10 || s[4] != '-' || s[7] != '-') return std::nullopt;

    auto is_digit = [](char c) { return c >= '0' && c <= '9'; };

    for (unsigned int i : {0u, 1u, 2u, 3u, 5u, 6u, 8u, 9u})
        if (!is_digit(s[i])) return std::nullopt;

    unsigned int y = static_cast<unsigned int>(s[0]-'0')*1000u
                   + static_cast<unsigned int>(s[1]-'0')*100u
                   + static_cast<unsigned int>(s[2]-'0')*10u
                   + static_cast<unsigned int>(s[3]-'0');

    unsigned int m = static_cast<unsigned int>(s[5]-'0')*10u
                   + static_cast<unsigned int>(s[6]-'0');

    unsigned int d = static_cast<unsigned int>(s[8]-'0')*10u
                   + static_cast<unsigned int>(s[9]-'0');

    using namespace std::chrono;
    year_month_day ymd{year{static_cast<int>(y)}, month{m}, day{d}};

    if (!ymd.ok()) return std::nullopt;

    return sys_days{ymd};
}

std::string format_date(Date d)
{
    using namespace std::chrono;
    year_month_day ymd{d};
    std::ostringstream oss;
    oss << std::setfill('0')
        << std::setw(4) << static_cast<int>(ymd.year())       << '-'
        << std::setw(2) << static_cast<unsigned>(ymd.month()) << '-'
        << std::setw(2) << static_cast<unsigned>(ymd.day());
    return oss.str();
}
