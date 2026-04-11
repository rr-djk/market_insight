#ifndef SCREENER_HPP
#define SCREENER_HPP

#include "database.hpp"
#include "price.hpp"
#include "screener_result.hpp"
#include <string>
#include <vector>

/**
 * Calcule des metriques financieres (Sharpe, volatilite, momentum) pour un
 * ensemble de symboles et les classe par critere.
 *
 * Les symboles sont charges par chunks pour maitriser l'empreinte memoire
 * (evite de charger les 33M lignes en une seule fois).
 */
class Screener {
public:
    /**
     * Construit un Screener.
     * @param db         Connexion PostgreSQL active.
     * @param chunk_size Nombre de symboles charges par requete (defaut : 500).
     * @param start_date Borne inferieure de l'historique au format "YYYY-MM-DD".
     *                   Vide pour utiliser tout l'historique disponible.
     * @param end_date   Borne superieure de l'historique au format "YYYY-MM-DD".
     *                   Vide pour utiliser tout l'historique disponible.
     */
    Screener(Database& db,
             unsigned int chunk_size = 500,
             const std::string& start_date = "",
             const std::string& end_date = "");

    /**
     * Calcule les metriques pour tous les symboles fournis.
     * @param symbols Liste de tickers a analyser.
     * @return Vecteur de ScreenerResult, un element par symbole ayant au moins 2 jours.
     */
    std::vector<ScreenerResult> run(const std::vector<std::string>& symbols);

    /**
     * Classe les resultats par Sharpe ratio decroissant.
     * Les entrees avec Sharpe NaN sont exclues.
     * @param results Resultats produits par run().
     * @param top_n   Nombre de resultats a retourner (tous si top_n >= taille).
     * @return Sous-ensemble trié, au plus top_n elements.
     */
    std::vector<ScreenerResult> rank_by_sharpe(std::vector<ScreenerResult> results,
                                                unsigned int top_n) const;

    /**
     * Classe les resultats par momentum 12 mois decroissant.
     * Les entrees avec momentum NaN sont exclues.
     * @param results Resultats produits par run().
     * @param top_n   Nombre de resultats a retourner (tous si top_n >= taille).
     * @return Sous-ensemble trié, au plus top_n elements.
     */
    std::vector<ScreenerResult> rank_by_momentum(std::vector<ScreenerResult> results,
                                                  unsigned int top_n) const;

private:
    Database& db;
    unsigned int chunk_size;
    std::string start_date;
    std::string end_date;

    /**
     * Calcule les rendements logarithmiques journaliers.
     * @param prices Historique de prix trie par date croissante.
     * @return Vecteur de taille prices.size()-1. Vide si prices.size() < 2.
     */
    std::vector<double> compute_log_returns(const std::vector<Price>& prices) const;

    /**
     * Calcule les trois metriques pour un symbole.
     * @param symbol Ticker du symbole.
     * @param prices Historique de prix (seul le champ close est utilise).
     * @return ScreenerResult avec les valeurs calculees (NaN si donnees insuffisantes).
     */
    ScreenerResult compute_metrics(const std::string& symbol,
                                   const std::vector<Price>& prices) const;
};

#endif
