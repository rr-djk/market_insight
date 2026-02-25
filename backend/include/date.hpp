#ifndef DATE_HPP
#define DATE_HPP

#include <chrono>
#include <optional>
#include <string>

/// Alias pour une date calendaire (jours depuis l'epoque Unix).
using Date = std::chrono::sys_days;

/**
 * @brief Convertit une chaine "YYYY-MM-DD" en Date.
 *
 * Valide le format, les separateurs et la coherence calendaire (via
 * year_month_day::ok()). Retourne std::nullopt si la date est invalide.
 */
std::optional<Date> parse_date(const std::string& s);

/**
 * @brief Convertit une Date en chaine "YYYY-MM-DD".
 */
std::string format_date(Date d);

#endif
