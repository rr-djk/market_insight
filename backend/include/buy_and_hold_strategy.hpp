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
    bool should_rebalance(unsigned int /*calendar_days_since_last_rebalance*/) override {
        return false;
    }

    /**
     * Reequilibre immediatement pour integrer le nouveau symbole au portefeuille.
     * @return Toujours true.
     */
    bool should_rebalance_on_new_symbol() override { return true; }

    /**
     * Pas d'iteration periodique : le moteur evalue chaque jour pour suivre
     * les valeurs et detecter l'apparition de nouveaux symboles.
     * @return 1 jour.
     */
    unsigned int get_period_days() const override { return 1u; }
};

#endif
