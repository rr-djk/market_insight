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
    virtual bool should_rebalance(unsigned int calendar_days_since_last_rebalance) = 0;

    /**
     * Determine si un reequilibrage doit avoir lieu quand un nouveau symbole apparait.
     * @return true si l'apparition d'un nouveau symbole declenche un reequilibrage.
     */
    virtual bool should_rebalance_on_new_symbol() = 0;

    /**
     * Retourne la periode d'iteration du moteur de backtest en jours calendaires.
     * Determine le pas utilise pour avancer dans le temps entre deux evaluations.
     * @return Nombre de jours entre deux evaluations.
     */
    virtual unsigned int get_period_days() const = 0;
};

#endif
