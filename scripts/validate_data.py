#!/usr/bin/env python3
"""
Script de validation des données historiques téléchargées.
Vérifie l'intégrité des CSV et produit des statistiques.
"""

import os
import csv
from pathlib import Path
from collections import defaultdict
from datetime import datetime

DATA_DIR = Path(__file__).parent.parent / "data" / "raw"
SYMBOLS_FILE = Path(__file__).parent.parent / "data" / "nasdaq_symbols.csv"
FAILED_FILE = Path(__file__).parent.parent / "data" / "failed_symbols.txt"

EXPECTED_HEADER = ["Date", "Symbol", "Open", "High", "Low", "Close", "Volume"]

def validate_csv_files():
    """Valide tous les fichiers CSV et retourne les statistiques."""

    stats = {
        "total_files": 0,
        "total_rows": 0,
        "valid_files": 0,
        "invalid_files": [],
        "empty_data_files": [],  # Header only
        "header_mismatch": [],
        "parse_errors": [],
        "min_date": None,
        "max_date": None,
        "rows_per_file": {},
        "date_range_per_symbol": {},
    }

    csv_files = list(DATA_DIR.glob("*.csv"))
    stats["total_files"] = len(csv_files)

    print(f"Validation de {len(csv_files)} fichiers CSV...")
    print("-" * 50)

    for i, csv_file in enumerate(csv_files):
        symbol = csv_file.stem

        if (i + 1) % 1000 == 0:
            print(f"  Progression: {i + 1}/{len(csv_files)}")

        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)
                header = next(reader, None)

                # Vérifier le header
                if header != EXPECTED_HEADER:
                    stats["header_mismatch"].append((symbol, header))
                    continue

                rows = list(reader)
                row_count = len(rows)
                stats["rows_per_file"][symbol] = row_count
                stats["total_rows"] += row_count

                if row_count == 0:
                    stats["empty_data_files"].append(symbol)
                    continue

                # Extraire les dates (première colonne)
                dates = []
                for row in rows:
                    try:
                        # Format: "2024-01-15 00:00:00-05:00"
                        date_str = row[0].split(" ")[0]
                        date = datetime.strptime(date_str, "%Y-%m-%d")
                        dates.append(date)
                    except (ValueError, IndexError) as e:
                        pass

                if dates:
                    min_d = min(dates)
                    max_d = max(dates)
                    stats["date_range_per_symbol"][symbol] = (min_d, max_d)

                    if stats["min_date"] is None or min_d < stats["min_date"]:
                        stats["min_date"] = min_d
                    if stats["max_date"] is None or max_d > stats["max_date"]:
                        stats["max_date"] = max_d

                stats["valid_files"] += 1

        except Exception as e:
            stats["parse_errors"].append((symbol, str(e)))

    return stats

def load_expected_symbols():
    """Charge la liste des symboles attendus depuis nasdaq_symbols.csv."""
    symbols = set()
    if SYMBOLS_FILE.exists():
        with open(SYMBOLS_FILE, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'Symbol' in row:
                    symbols.add(row['Symbol'])
                elif 'symbol' in row:
                    symbols.add(row['symbol'])
    return symbols

def load_failed_symbols():
    """Charge la liste des symboles échoués."""
    symbols = set()
    if FAILED_FILE.exists():
        with open(FAILED_FILE, 'r') as f:
            for line in f:
                s = line.strip()
                if s:
                    symbols.add(s)
    return symbols

def print_report(stats, expected_symbols, failed_symbols):
    """Affiche le rapport de validation."""

    downloaded_symbols = set(stats["rows_per_file"].keys())

    print("\n" + "=" * 60)
    print("RAPPORT DE VALIDATION DES DONNÉES")
    print("=" * 60)

    print("\n📊 STATISTIQUES GÉNÉRALES")
    print("-" * 40)
    print(f"  Fichiers CSV analysés : {stats['total_files']:,}")
    print(f"  Fichiers valides      : {stats['valid_files']:,}")
    print(f"  Lignes de données     : {stats['total_rows']:,}")

    if stats["min_date"] and stats["max_date"]:
        print(f"\n📅 PÉRIODE COUVERTE")
        print("-" * 40)
        print(f"  Date la plus ancienne : {stats['min_date'].strftime('%Y-%m-%d')}")
        print(f"  Date la plus récente  : {stats['max_date'].strftime('%Y-%m-%d')}")
        years = (stats['max_date'] - stats['min_date']).days / 365.25
        print(f"  Étendue               : {years:.1f} années")

    # Statistiques sur les lignes par fichier
    if stats["rows_per_file"]:
        row_counts = list(stats["rows_per_file"].values())
        avg_rows = sum(row_counts) / len(row_counts)
        max_rows = max(row_counts)
        min_rows = min(row_counts)
        max_symbol = max(stats["rows_per_file"], key=stats["rows_per_file"].get)
        min_symbol = min(stats["rows_per_file"], key=stats["rows_per_file"].get)

        print(f"\n📈 LIGNES PAR FICHIER")
        print("-" * 40)
        print(f"  Moyenne : {avg_rows:,.0f} lignes")
        print(f"  Maximum : {max_rows:,} lignes ({max_symbol})")
        print(f"  Minimum : {min_rows:,} lignes ({min_symbol})")

    # Symboles manquants
    print(f"\n🔍 ANALYSE DES SYMBOLES")
    print("-" * 40)
    print(f"  Symboles attendus (NASDAQ)   : {len(expected_symbols):,}")
    print(f"  Symboles téléchargés         : {len(downloaded_symbols):,}")
    print(f"  Symboles échoués (logged)    : {len(failed_symbols):,}")

    missing = expected_symbols - downloaded_symbols - failed_symbols
    if missing:
        print(f"  Symboles manquants (autres)  : {len(missing):,}")
        if len(missing) <= 20:
            print(f"    → {', '.join(sorted(missing))}")

    # Problèmes détectés
    print(f"\n⚠️  PROBLÈMES DÉTECTÉS")
    print("-" * 40)

    if stats["empty_data_files"]:
        print(f"  Fichiers sans données (header only) : {len(stats['empty_data_files'])}")
        if len(stats["empty_data_files"]) <= 10:
            print(f"    → {', '.join(stats['empty_data_files'])}")
    else:
        print("  Fichiers sans données : 0")

    if stats["header_mismatch"]:
        print(f"  Headers incorrects : {len(stats['header_mismatch'])}")
        for sym, header in stats["header_mismatch"][:5]:
            print(f"    → {sym}: {header}")
    else:
        print("  Headers incorrects : 0")

    if stats["parse_errors"]:
        print(f"  Erreurs de parsing : {len(stats['parse_errors'])}")
        for sym, err in stats["parse_errors"][:5]:
            print(f"    → {sym}: {err}")
    else:
        print("  Erreurs de parsing : 0")

    # Top 10 symboles avec le plus de données
    print(f"\n🏆 TOP 10 SYMBOLES (plus de données)")
    print("-" * 40)
    sorted_symbols = sorted(stats["rows_per_file"].items(), key=lambda x: x[1], reverse=True)[:10]
    for symbol, rows in sorted_symbols:
        date_range = stats["date_range_per_symbol"].get(symbol)
        if date_range:
            print(f"  {symbol:8} : {rows:>6,} lignes ({date_range[0].strftime('%Y')} - {date_range[1].strftime('%Y')})")
        else:
            print(f"  {symbol:8} : {rows:>6,} lignes")

    # Résumé final
    print("\n" + "=" * 60)
    total_issues = len(stats["empty_data_files"]) + len(stats["header_mismatch"]) + len(stats["parse_errors"])
    if total_issues == 0:
        print("✅ VALIDATION RÉUSSIE - Aucun problème majeur détecté")
    else:
        print(f"⚠️  VALIDATION TERMINÉE - {total_issues} problème(s) mineur(s) détecté(s)")
    print("=" * 60)

def main():
    print("🔍 Démarrage de la validation des données...")
    print()

    # Charger les symboles attendus
    expected_symbols = load_expected_symbols()
    failed_symbols = load_failed_symbols()

    # Valider les fichiers
    stats = validate_csv_files()

    # Afficher le rapport
    print_report(stats, expected_symbols, failed_symbols)

if __name__ == "__main__":
    main()
