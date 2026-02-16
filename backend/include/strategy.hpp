#ifndef STRATEGY_HPP
#define STRATEGY_HPP

class Strategy {
public:
    virtual ~Strategy() = default;

    /**
     * Determine si le portefeuille doit etre reequilibre aujourd'hui.
     * @param calendar_days_since_last_rebalance Nombre de jours calendaires depuis le dernier reequilibrage.
     * @return true si le portefeuille doit etre reequilibre.
     */
    virtual bool should_rebalance(int calendar_days_since_last_rebalance) = 0;
};

#endif
