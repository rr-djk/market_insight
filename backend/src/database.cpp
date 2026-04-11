#include "database.hpp"
#include <fstream>
#include <stdexcept>

std::map<std::string, std::string> Database::load_env(const std::string& path) {
    std::map<std::string, std::string> env;
    std::ifstream file(path);

    if (!file.is_open()) {
        throw std::runtime_error("Impossible d'ouvrir " + path);
    }

    std::string line;
    while (std::getline(file, line)) {
        if (line.empty() || line[0] == '#') continue;

        auto pos = line.find('=');
        if (pos == std::string::npos) continue;

        env[line.substr(0, pos)] = line.substr(pos + 1);
    }

    return env;
}

Database::Database(const std::string& env_path) {
    auto env = load_env(env_path);

    std::string connstr =
        "host=localhost "
        "port=5432 "
        "dbname=" + env["POSTGRES_DB"] + " "
        "user=" + env["POSTGRES_USER"] + " "
        "password=" + env["POSTGRES_PASSWORD"];

    conn = std::make_unique<pqxx::connection>(connstr);
}

Database::~Database() = default;

std::vector<std::string> Database::get_symbols() {
    pqxx::work txn(*conn);
    auto result = txn.exec("SELECT symbol FROM symbols ORDER BY symbol");
    txn.commit();

    std::vector<std::string> symbols;
    std::size_t count = static_cast<std::size_t>(result.size());
    symbols.reserve(count);

    for (const auto& row : result) {
        symbols.push_back(row[0].as<std::string>());
    }

    return symbols;
}

std::vector<Price> Database::get_prices(const std::string& symbol,
                                         const std::string& start_date,
                                         const std::string& end_date) {
    std::string query =
        "SELECT EXTRACT(EPOCH FROM hp.trade_date)::int / 86400, "
        "       hp.open, hp.high, hp.low, hp.close, hp.volume "
        "FROM historical_prices hp "
        "JOIN symbols s ON hp.symbol_id = s.symbol_id "
        "WHERE s.symbol = $1";

    pqxx::params query_params;
    query_params.append(symbol); // $1
    int next_param = 2;

    if (!start_date.empty()) {
        query += " AND hp.trade_date >= $" + std::to_string(next_param++);
        query_params.append(start_date);
    }

    if (!end_date.empty()) {
        query += " AND hp.trade_date <= $" + std::to_string(next_param++);
        query_params.append(end_date);
    }

    query += " ORDER BY hp.trade_date";

    pqxx::work txn(*conn);
    auto result = txn.exec_params(query, query_params);
    txn.commit();

    std::vector<Price> prices;
    std::size_t count = static_cast<std::size_t>(result.size());
    prices.reserve(count);

    // Pas de vérification de NULL : le schéma SQL impose NOT NULL sur toutes les colonnes
    for (const auto& row : result) {
        Date date{std::chrono::days{row[0].as<int>()}};

        prices.push_back({
            date,
            row[1].as<double>(),
            row[2].as<double>(),
            row[3].as<double>(),
            row[4].as<double>(),
            row[5].as<long>()
        });
    }

    return prices;
}

std::unordered_map<std::string, std::vector<Price>>
Database::get_prices_bulk(const std::vector<std::string>& symbols,
                           const std::string& start_date,
                           const std::string& end_date) {
    if (symbols.empty()) return {};

    // txn.stream() utilise COPY en interne et ne supporte pas les paramètres
    // liés ($1, $2...). Les valeurs sont inlinées via txn.quote() qui échappe
    // correctement les chaînes pour éviter toute injection SQL.
    pqxx::work txn(*conn);

    std::string in_list = "(";
    for (std::size_t i = 0; i < symbols.size(); ++i) {
        if (i > 0) in_list += ", ";
        in_list += txn.quote(symbols[i]);
    }
    in_list += ")";

    std::string query =
        "SELECT s.symbol, "
        "       EXTRACT(EPOCH FROM hp.trade_date)::int / 86400, "
        "       hp.open, hp.high, hp.low, hp.close, hp.volume "
        "FROM historical_prices hp "
        "JOIN symbols s ON hp.symbol_id = s.symbol_id "
        "WHERE s.symbol IN " + in_list;

    if (!start_date.empty())
        query += " AND hp.trade_date >= " + txn.quote(start_date);
    if (!end_date.empty())
        query += " AND hp.trade_date <= " + txn.quote(end_date);

    query += " ORDER BY s.symbol, hp.trade_date";

    std::unordered_map<std::string, std::vector<Price>> data;

    for (const auto& [sym, date_days, open, high, low, close, volume]
         : txn.stream<std::string, int, double, double, double, double, long>(query)) {
        Date date{std::chrono::days{date_days}};
        data[sym].push_back({date, open, high, low, close, volume});
    }

    txn.commit();
    return data;
}

std::unordered_map<std::string, std::vector<Price>>
Database::get_close_prices_bulk(const std::vector<std::string>& symbols,
                                 const std::string& start_date,
                                 const std::string& end_date) {
    if (symbols.empty()) return {};

    // txn.stream() utilise COPY en interne — paramètres inlinés via txn.quote().
    pqxx::work txn(*conn);

    std::string in_list = "(";
    for (std::size_t i = 0; i < symbols.size(); ++i) {
        if (i > 0) in_list += ", ";
        in_list += txn.quote(symbols[i]);
    }
    in_list += ")";

    std::string query =
        "SELECT s.symbol, "
        "       EXTRACT(EPOCH FROM hp.trade_date)::int / 86400, "
        "       hp.close "
        "FROM historical_prices hp "
        "JOIN symbols s ON hp.symbol_id = s.symbol_id "
        "WHERE s.symbol IN " + in_list;

    if (!start_date.empty())
        query += " AND hp.trade_date >= " + txn.quote(start_date);
    if (!end_date.empty())
        query += " AND hp.trade_date <= " + txn.quote(end_date);

    query += " ORDER BY s.symbol, hp.trade_date";

    std::unordered_map<std::string, std::vector<Price>> data;

    for (const auto& [sym, date_days, close]
         : txn.stream<std::string, int, double>(query)) {
        Date date{std::chrono::days{date_days}};
        data[sym].push_back({date, 0.0, 0.0, 0.0, close, 0L});
    }

    txn.commit();
    return data;
}
