#ifndef BUY_AND_HOLD_STRATEGY_HPP
#define BUY_AND_HOLD_STRATEGY_HPP

#include "strategy.hpp"

class BuyAndHoldStrategy : public Strategy {
public:
    /**
     * Ne reequilibre jamais le portefeuille.
     * @param calendar_days_since_last_rebalance Ignore.
     * @return Toujours false.
     */
    bool should_rebalance(int /*calendar_days_since_last_rebalance*/) override {
        return false;
    }
};

#endif
