#ifndef PRICE_HPP
#define PRICE_HPP

#include "date.hpp"

/// Prix journalier d'un symbole boursier.
struct Price {
    Date   date;
    double open;
    double high;
    double low;
    double close;
    long   volume;
};

#endif
