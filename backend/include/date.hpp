#ifndef DATE_HPP
#define DATE_HPP

#include <chrono>
#include <string>
#include <optional>

/// Alias pour une date calendaire (jours depuis l'epoque Unix).
using Date = std::chrono::sys_days;

/**
 * @brief Convertit une chaine "YYYY-MM-DD" en Date.
 *
 * Valide le format, les separateurs et la coherence calendaire (via
 * year_month_day::ok()). Retourne std::nullopt si la date est invalide.
 */
inline std::optional<Date> parse_date(const std::string& s)
{
    if (s.size() != 10 || s[4] != '-' || s[7] != '-') return std::nullopt;

    auto is_digit = [](char c) { return c >= '0' && c <= '9'; };

    for (unsigned int i : {0, 1, 2, 3, 5, 6, 8, 9})
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

#endif
