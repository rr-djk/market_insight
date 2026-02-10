#ifndef TRANSACTION_HPP
#define TRANSACTION_HPP

#include <string>

enum class OrderType { BUY, SELL };

struct Transaction {
    std::string date;
    std::string symbol;
    OrderType order_type;
    int quantity;
    double unit_price;
};

#endif
