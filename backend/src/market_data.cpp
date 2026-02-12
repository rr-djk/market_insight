#include "market_data.hpp"
#include <stdexcept>

MarketData::MarketData(Database& db,
                       const std::vector<std::string>& symbols) {
    for (const auto& symbol : symbols) {
        data[symbol] = db.get_prices(symbol);
    }
}

MarketData::MarketData(std::map<std::string, std::vector<Price>> raw_data)
    : data(std::move(raw_data)) {}

const std::vector<Price>& MarketData::get_prices(const std::string& symbol) const {
    return data.at(symbol);
}

std::vector<std::string> MarketData::get_supported_symbols(Database& db) {
    return db.get_symbols();
}
