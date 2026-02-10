#include <iostream>
#include "database.hpp"

int main() {
    try {
        Database db;

        auto symbols = db.get_symbols();
        std::cout << "Symboles en base : " << symbols.size() << std::endl;

        // Test : recuperer les prix d'AAPL sur une periode
        auto prices = db.get_prices("AAPL", "2024-01-01", "2024-01-31");
        std::cout << "\nAAPL - Janvier 2024 (" << prices.size() << " jours) :" << std::endl;

        for (const auto& p : prices) {
            std::cout << "  " << p.date
                      << "  O:" << p.open
                      << "  H:" << p.high
                      << "  L:" << p.low
                      << "  C:" << p.close
                      << "  V:" << p.volume
                      << std::endl;
        }

    } catch (const std::exception& e) {
        std::cerr << "Erreur : " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
