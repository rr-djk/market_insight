#include "portfolio.hpp"

Portfolio::Portfolio(double initial_cash)
  : initial_cash(initial_cash), available_cash(initial_cash) {}

  bool Portfolio::buy_stock(const std::string& symbol, unsigned int quantity,
                            double unit_price, const std::string& date) {
    double total_cost = quantity * unit_price;
    if (total_cost > available_cash) return false;

    available_cash -= total_cost;
    stock_positions[symbol] += quantity;
    transaction_history.push_back({date, symbol, OrderType::BUY, quantity, unit_price});
    return true;
  }

bool Portfolio::sell_stock(const std::string& symbol, unsigned int quantity,
                           double unit_price, const std::string& date) {
  auto it = stock_positions.find(symbol);
  if (it == stock_positions.end() || it->second < quantity) return false;

  available_cash += (quantity * unit_price);
  it->second -= quantity;
  // Retirer le symbole si plus aucune action detenue
  if (it->second == 0) stock_positions.erase(it);
  transaction_history.push_back({date, symbol, OrderType::SELL, quantity, unit_price});
  return true;
}

double Portfolio::compute_total_value(const std::unordered_map<std::string,
                                      double>& current_prices) const {
  double total = available_cash;
  for (const auto& [symbol, quantity] : stock_positions) {
    auto it = current_prices.find(symbol);
    if (it != current_prices.end()) total += quantity * it->second;
  }
  return total;
}

double Portfolio::get_initial_cash() const {
  return initial_cash;
}

double Portfolio::get_available_cash() const {
  return available_cash;
}

const std::unordered_map<std::string, unsigned int>& Portfolio::get_stock_positions() const {
  return stock_positions;
}

const std::vector<Transaction>& Portfolio::get_transaction_history() const {
  return transaction_history;
}
