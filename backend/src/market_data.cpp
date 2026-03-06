#include "market_data.hpp"
#include <stdexcept>

MarketData::MarketData(Database& db,
                       const std::vector<std::string>& symbols,
                       const std::string& start_date,
                       const std::string& end_date) {
    for (const auto& symbol : symbols) {
        data[symbol] = db.get_prices(symbol, start_date, end_date);
    }
}

MarketData::MarketData(std::map<std::string, std::vector<Price>> raw_data)
    : data(std::move(raw_data)) {}

const std::vector<Price>& MarketData::get_prices(const std::string& symbol) const {
    return data.at(symbol);
}

const std::vector<std::string> MarketData::get_supported_symbols(Database& db) {
    return db.get_symbols();
}
