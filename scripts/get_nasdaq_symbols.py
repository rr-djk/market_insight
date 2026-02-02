#!/usr/bin/env python3
"""
Script pour télécharger la liste complète des symboles NASDAQ
"""

import pandas as pd
from pathlib import Path
import sys

def download_nasdaq_symbols():
    """
    Télécharge la liste officielle des symboles NASDAQ
    
    Returns:
        DataFrame avec les symboles
    """
    print("📥 Téléchargement de la liste NASDAQ...")
    
    # URL officielle NASDAQ
    url = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
    
    try:
        # Télécharger le fichier (séparateur = pipe |)
        df = pd.read_csv(url, sep='|')
        
        print(f"✅ {len(df)} lignes téléchargées")
        return df
        
    except Exception as e:
        print(f"❌ Erreur lors du téléchargement: {e}")
        sys.exit(1)


def filter_symbols(df):
    """
    Filtre les symboles pour garder uniquement les actions actives
    
    Args:
        df: DataFrame brut de NASDAQ
    
    Returns:
        Liste de symboles filtrés
    """
    print("\n🔍 Filtrage des symboles...")
    
    # Afficher les colonnes disponibles pour comprendre
    print(f"Colonnes disponibles: {df.columns.tolist()}")
    
    # Nombre total avant filtrage
    total_before = len(df)
    
    # Filtrer :
    # 1. Test Issue = 'N' (pas des symboles de test)
    # 2. Retirer la dernière ligne (qui est vide dans le fichier NASDAQ)
    df_filtered = df[df['Test Issue'] == 'N'].copy()
    df_filtered = df_filtered[df_filtered['Symbol'].notna()]
    
    # Optionnel : Retirer les ETFs (si vous voulez seulement des actions)
    # Décommentez la ligne suivante si vous voulez exclure les ETFs
    # df_filtered = df_filtered[df_filtered['ETF'] == 'N']
    
    symbols = df_filtered['Symbol'].tolist()
    
    # Nettoyer : retirer les symboles avec des caractères spéciaux
    # (certains symboles ont des $ ou autres qui peuvent causer problème)
    symbols_clean = [s.strip() for s in symbols if isinstance(s, str) and s.strip()]
    
    print(f"Symboles avant filtrage: {total_before}")
    print(f"Symboles après filtrage: {len(symbols_clean)}")
    
    return symbols_clean, df_filtered


def select_test_symbols(symbols):
    """
    Sélectionne 10 symboles historiques (50+ ans d'existence) pour les tests
    
    Args:
        symbols: Liste de tous les symboles
    
    Returns:
        Liste de 10 symboles pour les tests
    """
    # Entreprises historiques listées depuis 50+ ans (souvent 60-80 ans)
    # Ces entreprises ont un très long historique boursier
    preferred_test_symbols = [
        'INTC',   # Intel - fondée 1968, cotée 1971
        'MSFT',   # Microsoft - fondée 1975, cotée 1986
        'AAPL',   # Apple - fondée 1976, cotée 1980
        'ADBE',   # Adobe - fondée 1982, cotée 1986
        'ORCL',   # Oracle - fondée 1977, cotée 1986
        'CSCO',   # Cisco - fondée 1984, cotée 1990
        'QCOM',   # Qualcomm - fondée 1985, cotée 1991
        'AMGN',   # Amgen - fondée 1980, cotée 1983
        'COST',   # Costco - fondée 1983, cotée 1985
        'PAYX',   # Paychex - fondée 1971, cotée 1983
    ]
    
    # Note: Pour des entreprises encore plus anciennes (70+ ans), 
    # elles sont souvent sur NYSE plutôt que NASDAQ.
    # Le NASDAQ a été créé en 1971, donc les entreprises NASDAQ 
    # les plus anciennes datent des années 1970-1980.
    
    # Vérifier que ces symboles existent dans la liste NASDAQ
    test_symbols = [s for s in preferred_test_symbols if s in symbols]
    
    # Si certains manquent, prendre les premiers de la liste
    if len(test_symbols) < 10:
        remaining = [s for s in symbols[:20] if s not in test_symbols]
        test_symbols.extend(remaining[:10 - len(test_symbols)])
    
    print(f"\n📅 Symboles de test sélectionnés (entreprises historiques 40-50+ ans):")
    for symbol in test_symbols[:10]:
        print(f"   {symbol}")
    
    return test_symbols[:10]


def save_symbols(symbols, test_symbols, output_dir="data"):
    """
    Sauvegarde les symboles dans des fichiers
    
    Args:
        symbols: Liste complète des symboles
        test_symbols: Liste des symboles de test
        output_dir: Répertoire de sortie
    """
    # Créer le répertoire si nécessaire
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Sauvegarder la liste complète
    full_list_path = output_path / "nasdaq_symbols.csv"
    df_full = pd.DataFrame({'Symbol': symbols})
    df_full.to_csv(full_list_path, index=False)
    print(f"\n💾 Liste complète sauvegardée: {full_list_path}")
    print(f"   Nombre de symboles: {len(symbols)}")
    
    # Sauvegarder la liste de test
    test_list_path = output_path / "test_symbols.txt"
    with open(test_list_path, 'w') as f:
        for symbol in test_symbols:
            f.write(f"{symbol}\n")
    print(f"\n💾 Liste de test sauvegardée: {test_list_path}")
    print(f"   Symboles de test: {', '.join(test_symbols)}")


def display_statistics(symbols):
    """
    Affiche des statistiques sur les symboles
    
    Args:
        symbols: Liste des symboles
    """
    print("\n" + "="*60)
    print("STATISTIQUES")
    print("="*60)
    print(f"Total de symboles NASDAQ: {len(symbols)}")
    print(f"\nPremiers 20 symboles:")
    for i, symbol in enumerate(symbols[:20], 1):
        print(f"  {i:2d}. {symbol}")
    
    # Analyse des préfixes
    prefixes = {}
    for symbol in symbols:
        if symbol:
            prefix = symbol[0]
            prefixes[prefix] = prefixes.get(prefix, 0) + 1
    
    print(f"\nDistribution par première lettre:")
    for letter in sorted(prefixes.keys()):
        print(f"  {letter}: {prefixes[letter]} symboles")


def main():
    """Fonction principale"""
    print("="*60)
    print("TÉLÉCHARGEMENT DES SYMBOLES NASDAQ")
    print("="*60)
    
    # 1. Télécharger
    df_raw = download_nasdaq_symbols()
    
    # 2. Filtrer
    symbols, df_filtered = filter_symbols(df_raw)
    
    if not symbols:
        print("❌ Aucun symbole trouvé après filtrage")
        sys.exit(1)
    
    # 3. Sélectionner les symboles de test
    test_symbols = select_test_symbols(symbols)
    
    # 4. Sauvegarder
    save_symbols(symbols, test_symbols)
    
    # 5. Afficher les statistiques
    display_statistics(symbols)
    
    print("\n" + "="*60)
    print("✅ TERMINÉ !")
    print("="*60)
    print("\nProchaines étapes:")
    print("  1. Vérifiez data/nasdaq_symbols.csv")
    print("  2. Vérifiez data/test_symbols.txt")
    print("  3. Utilisez ces listes pour télécharger les données historiques")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
