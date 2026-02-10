#ifndef PRICE_HPP
#define PRICE_HPP

#include <string>

struct Price {
    std::string date;
    double open;
    double high;
    double low;
    double close;
    long volume;
};

#endif
