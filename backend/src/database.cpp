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

std::map<std::string, std::vector<Price>>
Database::get_prices_bulk(const std::vector<std::string>& symbols,
                           const std::string& start_date,
                           const std::string& end_date) {
    if (symbols.empty()) return {};

    // Construit le littéral tableau PostgreSQL : {AAPL,MSFT,...}
    std::string pg_array = "{";
    for (std::size_t i = 0; i < symbols.size(); ++i) {
        if (i > 0) pg_array += ',';
        pg_array += symbols[i];
    }
    pg_array += '}';

    std::string query =
        "SELECT s.symbol, "
        "       EXTRACT(EPOCH FROM hp.trade_date)::int / 86400, "
        "       hp.open, hp.high, hp.low, hp.close, hp.volume "
        "FROM historical_prices hp "
        "JOIN symbols s ON hp.symbol_id = s.symbol_id "
        "WHERE s.symbol = ANY($1::text[])";

    pqxx::params query_params;
    query_params.append(pg_array);
    int next_param = 2;

    if (!start_date.empty()) {
        query += " AND hp.trade_date >= $" + std::to_string(next_param++);
        query_params.append(start_date);
    }

    if (!end_date.empty()) {
        query += " AND hp.trade_date <= $" + std::to_string(next_param++);
        query_params.append(end_date);
    }

    query += " ORDER BY s.symbol, hp.trade_date";

    pqxx::work txn(*conn);
    auto result = txn.exec_params(query, query_params);
    txn.commit();

    std::map<std::string, std::vector<Price>> data;

    for (const auto& row : result) {
        const std::string symbol = row[0].as<std::string>();
        Date date{std::chrono::days{row[1].as<int>()}};

        data[symbol].push_back({
            date,
            row[2].as<double>(),
            row[3].as<double>(),
            row[4].as<double>(),
            row[5].as<double>(),
            row[6].as<long>()
        });
    }

    return data;
}

std::map<std::string, std::vector<Price>>
Database::get_close_prices_bulk(const std::vector<std::string>& symbols,
                                 const std::string& start_date,
                                 const std::string& end_date) {
    if (symbols.empty()) return {};

    // Construction de ARRAY[$1, $2, ..., $N] via paramètres libpqxx individuels
    // pour éviter toute injection par construction manuelle d'un littéral tableau.
    pqxx::params query_params;
    std::string array_placeholders = "ARRAY[";
    for (std::size_t i = 0; i < symbols.size(); ++i) {
        if (i > 0) array_placeholders += ',';
        array_placeholders += '$' + std::to_string(i + 1);
        query_params.append(symbols[i]);
    }
    array_placeholders += ']';

    std::string query =
        "SELECT s.symbol, "
        "       EXTRACT(EPOCH FROM hp.trade_date)::int / 86400, "
        "       hp.close "
        "FROM historical_prices hp "
        "JOIN symbols s ON hp.symbol_id = s.symbol_id "
        "WHERE s.symbol = ANY(" + array_placeholders + "::text[])";

    unsigned int next_param = static_cast<unsigned int>(symbols.size()) + 1u;

    if (!start_date.empty()) {
        query += " AND hp.trade_date >= $" + std::to_string(next_param++);
        query_params.append(start_date);
    }

    if (!end_date.empty()) {
        query += " AND hp.trade_date <= $" + std::to_string(next_param++);
        query_params.append(end_date);
    }

    query += " ORDER BY s.symbol, hp.trade_date";

    pqxx::work txn(*conn);
    auto result = txn.exec_params(query, query_params);
    txn.commit();

    std::map<std::string, std::vector<Price>> data;

    for (const auto& row : result) {
        const std::string sym = row[0].as<std::string>();
        Date date{std::chrono::days{row[1].as<int>()}};

        data[sym].push_back({
            date,
            0.0,
            0.0,
            0.0,
            row[2].as<double>(),
            0L
        });
    }

    return data;
}
