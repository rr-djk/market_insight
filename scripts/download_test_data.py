#!/usr/bin/env python3
"""
Script de téléchargement des données historiques - VERSION TEST
Télécharge les données pour les 10 symboles de test uniquement
"""

import yfinance as yf
import pandas as pd
import time
import logging
from datetime import datetime
from pathlib import Path
import sys

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('download_test.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def read_test_symbols(filepath="data/test_symbols.txt"):
    """
    Lit la liste des symboles de test
    
    Args:
        filepath: Chemin vers le fichier contenant les symboles
    
    Returns:
        Liste des symboles
    """
    try:
        with open(filepath, 'r') as f:
            symbols = [line.strip() for line in f if line.strip()]
        logger.info(f"✅ {len(symbols)} symboles de test chargés depuis {filepath}")
        return symbols
    except FileNotFoundError:
        logger.error(f"❌ Fichier non trouvé: {filepath}")
        logger.info("Exécutez d'abord: python3 get_nasdaq_symbols.py")
        sys.exit(1)


def download_symbol_data(symbol, start_date="1970-01-01", end_date=None):
    """
    Télécharge les données historiques pour un symbole
    
    Args:
        symbol: Ticker du symbole
        start_date: Date de début (Yahoo ira chercher le plus tôt possible)
        end_date: Date de fin (None = aujourd'hui)
    
    Returns:
        DataFrame avec les données ou None si échec
    """
    try:
        logger.info(f"📥 Téléchargement de {symbol}...")
        
        # Télécharger via yfinance
        ticker = yf.Ticker(symbol)
        df = ticker.history(start=start_date, end=end_date, auto_adjust=False)
        
        if df.empty:
            logger.warning(f"⚠️  {symbol}: Aucune donnée disponible")
            return None
        
        # Réorganiser et nettoyer
        df = df.reset_index()
        
        # Renommer les colonnes pour cohérence
        df = df.rename(columns={
            'Date': 'Date',
            'Open': 'Open',
            'High': 'High', 
            'Low': 'Low',
            'Close': 'Close',
            'Volume': 'Volume'
        })
        
        # Sélectionner uniquement les colonnes nécessaires
        df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        
        # Ajouter le symbole
        df.insert(1, 'Symbol', symbol)
        
        # Nettoyer les NaN
        df = df.dropna()
        
        # Informations sur les données
        date_min = df['Date'].min()
        date_max = df['Date'].max()
        nb_rows = len(df)
        years = (date_max - date_min).days / 365.25
        
        logger.info(f"✅ {symbol}: {nb_rows:,} lignes | "
                   f"{date_min.strftime('%Y-%m-%d')} → {date_max.strftime('%Y-%m-%d')} "
                   f"({years:.1f} ans)")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Erreur pour {symbol}: {str(e)}")
        return None


def save_to_csv(df, symbol, output_dir="data/raw"):
    """
    Sauvegarde un DataFrame dans un fichier CSV
    
    Args:
        df: DataFrame à sauvegarder
        symbol: Symbole (pour le nom du fichier)
        output_dir: Répertoire de sortie
    
    Returns:
        True si succès, False sinon
    """
    if df is None or df.empty:
        return False
    
    try:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        filepath = output_path / f"{symbol}.csv"
        df.to_csv(filepath, index=False)
        
        logger.info(f"💾 Sauvegardé: {filepath} ({len(df):,} lignes)")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur sauvegarde {symbol}: {str(e)}")
        return False


def main():
    """Fonction principale"""
    
    print("="*70)
    print("TÉLÉCHARGEMENT DES DONNÉES HISTORIQUES - VERSION TEST")
    print("="*70)
    print()
    
    # 1. Lire les symboles de test
    symbols = read_test_symbols()
    
    if not symbols:
        logger.error("❌ Aucun symbole à télécharger")
        return 1
    
    print(f"Symboles à télécharger: {', '.join(symbols)}")
    print(f"Nombre de symboles: {len(symbols)}")
    print(f"Délai entre requêtes: 1 seconde")
    print(f"Temps estimé: ~{len(symbols)} secondes ({len(symbols)/60:.1f} minutes)")
    print()
    
    # 2. Télécharger chaque symbole
    successful = 0
    failed = 0
    failed_symbols = []
    all_data = []
    
    start_time = time.time()
    
    for idx, symbol in enumerate(symbols, 1):
        print(f"\n[{idx}/{len(symbols)}] Traitement de {symbol}")
        print("-" * 70)
        
        # Télécharger
        df = download_symbol_data(symbol, start_date="1970-01-01")
        
        if df is not None:
            # Sauvegarder
            if save_to_csv(df, symbol):
                successful += 1
                all_data.append(df)
            else:
                failed += 1
                failed_symbols.append(symbol)
        else:
            failed += 1
            failed_symbols.append(symbol)
        
        # Délai pour éviter le rate limiting (sauf pour le dernier)
        if idx < len(symbols):
            time.sleep(1)
    
    elapsed_time = time.time() - start_time
    
    # 3. Créer un fichier combiné (optionnel mais utile)
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df = combined_df.sort_values(['Symbol', 'Date'])
        combined_path = Path("data/raw") / "test_combined.csv"
        combined_df.to_csv(combined_path, index=False)
        logger.info(f"\n💾 Fichier combiné sauvegardé: {combined_path}")
    
    # 4. Afficher le résumé
    print()
    print("="*70)
    print("RÉSUMÉ DU TÉLÉCHARGEMENT")
    print("="*70)
    print(f"Symboles demandés:        {len(symbols)}")
    print(f"Téléchargements réussis:  {successful}")
    print(f"Échecs:                   {failed}")
    print(f"Temps total:              {elapsed_time:.1f} secondes ({elapsed_time/60:.1f} minutes)")
    
    if all_data:
        total_rows = sum(len(df) for df in all_data)
        print(f"Total de lignes:          {total_rows:,}")
        
        all_dates = pd.concat([df['Date'] for df in all_data])
        print(f"Période couverte:         {all_dates.min().strftime('%Y-%m-%d')} → "
              f"{all_dates.max().strftime('%Y-%m-%d')}")
    
    if failed_symbols:
        print(f"\n⚠️  Symboles échoués: {', '.join(failed_symbols)}")
    
    print()
    print("="*70)
    
    if successful > 0:
        print("✅ TÉLÉCHARGEMENT TERMINÉ AVEC SUCCÈS !")
        print()
        print("Les données sont disponibles dans: data/raw/")
        print("Fichiers individuels: SYMBOLE.csv")
        print("Fichier combiné: test_combined.csv")
        return 0
    else:
        print("❌ AUCUN TÉLÉCHARGEMENT RÉUSSI")
        return 1


if __name__ == "__main__":
    sys.exit(main())
