#!/usr/bin/env python3
"""
Script de téléchargement des données historiques - VERSION COMPLÈTE
Télécharge les données pour TOUS les symboles NASDAQ
ATTENTION: Ce script peut prendre plusieurs heures à s'exécuter
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
        logging.FileHandler('download_full.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


def read_all_symbols(filepath="data/nasdaq_symbols.csv"):
    """
    Lit la liste complète des symboles NASDAQ
    
    Args:
        filepath: Chemin vers le fichier CSV contenant les symboles
    
    Returns:
        Liste des symboles
    """
    try:
        df = pd.read_csv(filepath)
        symbols = df['Symbol'].tolist()
        logger.info(f"✅ {len(symbols)} symboles chargés depuis {filepath}")
        return symbols
    except FileNotFoundError:
        logger.error(f"❌ Fichier non trouvé: {filepath}")
        logger.info("Exécutez d'abord: python3 get_nasdaq_symbols.py")
        sys.exit(1)
    except Exception as e:
        logger.error(f"❌ Erreur lecture fichier: {str(e)}")
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
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur sauvegarde {symbol}: {str(e)}")
        return False


def display_progress(current, total, successful, failed, start_time):
    """
    Affiche la progression du téléchargement
    
    Args:
        current: Numéro actuel
        total: Total de symboles
        successful: Nombre de réussites
        failed: Nombre d'échecs
        start_time: Heure de début
    """
    elapsed = time.time() - start_time
    percentage = (current / total) * 100
    
    if current > 0:
        avg_time = elapsed / current
        remaining = (total - current) * avg_time
        eta_minutes = remaining / 60
        
        print(f"\n{'='*70}")
        print(f"Progression: {current}/{total} ({percentage:.1f}%)")
        print(f"Réussis: {successful} | Échecs: {failed}")
        print(f"Temps écoulé: {elapsed/60:.1f} min | ETA: {eta_minutes:.1f} min")
        print(f"{'='*70}")


def main():
    """Fonction principale"""
    
    print("="*70)
    print("TÉLÉCHARGEMENT DES DONNÉES HISTORIQUES - VERSION COMPLÈTE")
    print("="*70)
    print()
    print("⚠️  ATTENTION: Ce script peut prendre plusieurs heures à s'exécuter")
    print("⚠️  Yahoo Finance limite le nombre de requêtes")
    print("⚠️  Le script attend 1 seconde entre chaque téléchargement")
    print()
    
    # Demander confirmation
    response = input("Voulez-vous continuer ? (oui/non): ")
    if response.lower() not in ['oui', 'o', 'yes', 'y']:
        print("Téléchargement annulé.")
        return 0
    
    print()
    
    # 1. Lire tous les symboles
    symbols = read_all_symbols()
    
    if not symbols:
        logger.error("❌ Aucun symbole à télécharger")
        return 1
    
    print(f"Nombre de symboles: {len(symbols)}")
    print(f"Délai entre requêtes: 1 seconde")
    print(f"Temps estimé minimum: ~{len(symbols)/60:.1f} minutes ({len(symbols)/3600:.1f} heures)")
    print()
    print("Démarrage dans 5 secondes...")
    time.sleep(5)
    print()
    
    # 2. Télécharger chaque symbole
    successful = 0
    failed = 0
    failed_symbols = []
    
    start_time = time.time()
    
    for idx, symbol in enumerate(symbols, 1):
        # Afficher progression tous les 50 symboles
        if idx % 50 == 0:
            display_progress(idx, len(symbols), successful, failed, start_time)
        
        logger.info(f"\n[{idx}/{len(symbols)}] Traitement de {symbol}")
        
        # Télécharger
        df = download_symbol_data(symbol, start_date="1970-01-01")
        
        if df is not None:
            # Sauvegarder
            if save_to_csv(df, symbol):
                successful += 1
                logger.info(f"💾 {symbol} sauvegardé ({len(df):,} lignes)")
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
    
    # 3. Afficher le résumé
    print()
    print("="*70)
    print("RÉSUMÉ DU TÉLÉCHARGEMENT")
    print("="*70)
    print(f"Symboles demandés:        {len(symbols)}")
    print(f"Téléchargements réussis:  {successful}")
    print(f"Échecs:                   {failed}")
    print(f"Taux de réussite:         {(successful/len(symbols)*100):.1f}%")
    print(f"Temps total:              {elapsed_time/60:.1f} minutes ({elapsed_time/3600:.1f} heures)")
    
    if failed_symbols:
        print(f"\n⚠️  Nombre de symboles échoués: {len(failed_symbols)}")
        print("Voir download_full.log pour la liste complète")
        
        # Sauvegarder la liste des symboles échoués
        failed_path = Path("data") / "failed_symbols.txt"
        with open(failed_path, 'w') as f:
            for symbol in failed_symbols:
                f.write(f"{symbol}\n")
        logger.info(f"Liste des échecs sauvegardée: {failed_path}")
    
    print()
    print("="*70)
    
    if successful > 0:
        print("✅ TÉLÉCHARGEMENT TERMINÉ !")
        print()
        print(f"Les données sont disponibles dans: data/raw/")
        print(f"Nombre de fichiers créés: {successful}")
        print(f"Consultez download_full.log pour les détails")
        
        # Calculer statistiques approximatives
        estimated_size_mb = successful * 0.5  # ~500KB par fichier en moyenne
        print(f"Taille estimée: ~{estimated_size_mb:.0f} MB")
        
        return 0
    else:
        print("❌ AUCUN TÉLÉCHARGEMENT RÉUSSI")
        return 1


if __name__ == "__main__":
    sys.exit(main())
