#ifndef TECHNICAL_INDICATORS_HPP
#define TECHNICAL_INDICATORS_HPP

#include <string>
#include <unordered_map>
#include <vector>
#include "market_data.hpp"
#include "price.hpp"

/**
 * Resultats des indicateurs techniques pour un symbole.
 * Chaque vecteur a la meme taille que le vecteur de prix du symbole.
 * Les valeurs de la periode de chauffe sont NaN.
 */
struct IndicatorSet {
    std::vector<double> sma;
    std::vector<double> ema;
    std::vector<double> rsi;
    std::vector<double> macd;
    std::vector<double> bollinger_upper;
    std::vector<double> bollinger_lower;
};

/**
 * Calcule la moyenne mobile simple (SMA) sur une fenetre glissante.
 * @param prices Historique de prix tries par date croissante.
 * @param period Taille de la fenetre (defaut : 20).
 * @return Vecteur de taille prices.size(). Les period-1 premiers elements sont NaN.
 */
std::vector<double> compute_sma(const std::vector<Price>& prices,
                                 unsigned int period = 20);

/**
 * Calcule la moyenne mobile exponentielle (EMA) sur une fenetre glissante.
 * La premiere valeur valide est la SMA des `period` premiers prix (amorçage).
 * @param prices Historique de prix tries par date croissante.
 * @param period Taille de la fenetre (defaut : 20).
 * @return Vecteur de taille prices.size(). Les period-1 premiers elements sont NaN.
 */
std::vector<double> compute_ema(const std::vector<Price>& prices,
                                 unsigned int period = 20);

/**
 * Calcule le Relative Strength Index (RSI) avec le lissage de Wilder.
 * @param prices Historique de prix tries par date croissante.
 * @param period Taille de la fenetre (defaut : 14).
 * @return Vecteur de taille prices.size(). Les period premiers elements sont NaN.
 * @note Le RSI est toujours dans [0, 100]. Si avg_loss vaut 0, RSI = 100.
 */
std::vector<double> compute_rsi(const std::vector<Price>& prices,
                                 unsigned int period = 14);

/**
 * Calcule la ligne MACD (EMA rapide - EMA lente).
 * @param prices Historique de prix tries par date croissante.
 * @param fast Periode de l'EMA rapide (defaut : 12).
 * @param slow Periode de l'EMA lente (defaut : 26).
 * @return Vecteur de taille prices.size(). Les slow-1 premiers elements sont NaN.
 */
std::vector<double> compute_macd(const std::vector<Price>& prices,
                                  unsigned int fast = 12,
                                  unsigned int slow = 26);

/**
 * Calcule la bande de Bollinger superieure (SMA + k * ecart-type).
 * @param prices Historique de prix tries par date croissante.
 * @param period Taille de la fenetre (defaut : 20).
 * @param std_dev Multiplicateur de l'ecart-type (defaut : 2.0).
 * @return Vecteur de taille prices.size(). Les period-1 premiers elements sont NaN.
 */
std::vector<double> compute_bollinger_upper(const std::vector<Price>& prices,
                                             unsigned int period  = 20,
                                             double       std_dev = 2.0);

/**
 * Calcule la bande de Bollinger inferieure (SMA - k * ecart-type).
 * @param prices Historique de prix tries par date croissante.
 * @param period Taille de la fenetre (defaut : 20).
 * @param std_dev Multiplicateur de l'ecart-type (defaut : 2.0).
 * @return Vecteur de taille prices.size(). Les period-1 premiers elements sont NaN.
 */
std::vector<double> compute_bollinger_lower(const std::vector<Price>& prices,
                                             unsigned int period  = 20,
                                             double       std_dev = 2.0);

/**
 * Calcule tous les indicateurs pour chaque symbole fourni.
 * Point d'entree du batch runner : c'est cette fonction qui est profilee.
 * @param market Donnees de marche contenant les historiques.
 * @param symbols Liste des symboles a traiter.
 * @return Map symbole -> IndicatorSet.
 */
std::unordered_map<std::string, IndicatorSet>
compute_all(const MarketData& market, const std::vector<std::string>& symbols);

#endif
