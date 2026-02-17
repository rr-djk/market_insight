#ifndef EQUAL_WEIGHT_STRATEGY_HPP
#define EQUAL_WEIGHT_STRATEGY_HPP

#include "strategy.hpp"

class EqualWeightStrategy : public Strategy {
public:
    /**
     * Cree une strategie d'equiponderation avec reequilibrage periodique.
     * @param period_days Nombre de jours calendaires entre chaque reequilibrage.
     */
    explicit EqualWeightStrategy(int period_days)
        : rebalance_period_days(period_days) {}

    /**
     * Reequilibre le portefeuille lorsque la periode configuree est atteinte.
     * @param calendar_days_since_last_rebalance Jours calendaires ecoules depuis le dernier reequilibrage.
     * @return true si le nombre de jours depasse ou egale la periode de reequilibrage.
     */
    bool should_rebalance(int calendar_days_since_last_rebalance) override {
        return calendar_days_since_last_rebalance >= rebalance_period_days;
    }

    /**
     * Les nouveaux symboles sont integres au prochain reequilibrage periodique.
     * @return Toujours false.
     */
    bool should_rebalance_on_new_symbol() override { return false; }

private:
    int rebalance_period_days;
};

#endif
