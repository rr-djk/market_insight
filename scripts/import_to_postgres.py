#!/usr/bin/env python3
"""
Import des données CSV vers PostgreSQL.
Utilise COPY pour des performances optimales.
"""

import os
import csv
import io
from pathlib import Path
from datetime import datetime

import psycopg2
from psycopg2 import sql

# Configuration
DATA_DIR = Path(__file__).parent.parent / "data" / "raw"

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": os.getenv("POSTGRES_DB", "market_insight"),
    "user": os.getenv("POSTGRES_USER", "market_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "credentials774420"),
}


def get_or_create_symbol(cursor, symbol: str) -> int:
    """Insère le symbole s'il n'existe pas et retourne son ID."""
    cursor.execute(
        """
        INSERT INTO symbols (symbol)
        VALUES (%s)
        ON CONFLICT (symbol) DO NOTHING
        RETURNING symbol_id
        """,
        (symbol,)
    )
    result = cursor.fetchone()

    if result:
        return result[0]

    # Le symbole existait déjà, récupérer son ID
    cursor.execute("SELECT symbol_id FROM symbols WHERE symbol = %s", (symbol,))
    return cursor.fetchone()[0]


def parse_date(date_str: str) -> str:
    """Extrait la date au format YYYY-MM-DD depuis le format CSV."""
    # Format: "1980-12-12 00:00:00-05:00" -> "1980-12-12"
    return date_str.split(" ")[0]


# Limite raisonnable pour un prix (10^11 avec 6 décimales = 10^5 max)
MAX_PRICE = 1_000_000_000  # 1 milliard - aucune action n'atteint ce prix


def is_valid_row(open_p, high, low, close, volume) -> bool:
    """Vérifie si les valeurs sont dans des limites raisonnables."""
    try:
        prices = [float(open_p), float(high), float(low), float(close)]
        vol = int(float(volume))
        return all(0 <= p < MAX_PRICE for p in prices) and vol >= 0
    except (ValueError, TypeError):
        return False


def import_csv_file(cursor, csv_path: Path, symbol_id: int) -> int:
    """Importe un fichier CSV en utilisant COPY pour la performance."""

    rows_imported = 0

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        header = next(reader, None)  # Skip header

        # Préparer les données pour COPY
        buffer = io.StringIO()
        rows_skipped = 0
        for row in reader:
            if len(row) < 7:
                continue

            # Filtrer les valeurs aberrantes
            if not is_valid_row(row[2], row[3], row[4], row[5], row[6]):
                rows_skipped += 1
                continue

            trade_date = parse_date(row[0])
            # symbol_id, trade_date, open, high, low, close, volume
            buffer.write(f"{symbol_id}\t{trade_date}\t{row[2]}\t{row[3]}\t{row[4]}\t{row[5]}\t{row[6]}\n")
            rows_imported += 1

        if rows_imported == 0:
            return 0

        buffer.seek(0)

        # Utiliser une table temporaire pour gérer ON CONFLICT
        cursor.execute("""
            CREATE TEMP TABLE temp_prices (
                symbol_id INTEGER,
                trade_date DATE,
                open NUMERIC(18, 6),
                high NUMERIC(18, 6),
                low NUMERIC(18, 6),
                close NUMERIC(18, 6),
                volume BIGINT
            ) ON COMMIT DROP
        """)

        cursor.copy_from(
            buffer,
            'temp_prices',
            columns=('symbol_id', 'trade_date', 'open', 'high', 'low', 'close', 'volume')
        )

        # Insérer depuis la table temporaire en ignorant les doublons
        cursor.execute("""
            INSERT INTO historical_prices (symbol_id, trade_date, open, high, low, close, volume)
            SELECT symbol_id, trade_date, open, high, low, close, volume
            FROM temp_prices
            ON CONFLICT (symbol_id, trade_date) DO NOTHING
        """)

    return rows_imported


def main():
    print("=" * 60)
    print("IMPORT DES DONNÉES VERS POSTGRESQL")
    print("=" * 60)

    csv_files = sorted(DATA_DIR.glob("*.csv"))
    total_files = len(csv_files)

    print(f"\nFichiers à importer : {total_files}")
    print(f"Base de données     : {DB_CONFIG['dbname']}")
    print("-" * 60)

    conn = psycopg2.connect(**DB_CONFIG)

    total_rows = 0
    files_imported = 0
    files_skipped = 0
    start_time = datetime.now()

    try:
        for i, csv_path in enumerate(csv_files):
            symbol = csv_path.stem

            # Progression
            if (i + 1) % 500 == 0 or i == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = (i + 1) / elapsed if elapsed > 0 else 0
                eta = (total_files - i - 1) / rate if rate > 0 else 0
                print(f"[{i + 1:>5}/{total_files}] {symbol:8} | {total_rows:>10,} lignes | ETA: {eta/60:.1f} min")

            with conn:  # Transaction par fichier
                with conn.cursor() as cursor:
                    symbol_id = get_or_create_symbol(cursor, symbol)
                    rows = import_csv_file(cursor, csv_path, symbol_id)

                    if rows > 0:
                        total_rows += rows
                        files_imported += 1
                    else:
                        files_skipped += 1

    except KeyboardInterrupt:
        print("\n\nInterruption utilisateur.")
    except Exception as e:
        print(f"\nErreur : {e}")
        raise
    finally:
        conn.close()

    # Résumé
    elapsed = (datetime.now() - start_time).total_seconds()

    print("\n" + "=" * 60)
    print("IMPORT TERMINÉ")
    print("=" * 60)
    print(f"Fichiers importés : {files_imported:,}")
    print(f"Fichiers ignorés  : {files_skipped:,} (vides)")
    print(f"Lignes importées  : {total_rows:,}")
    print(f"Durée             : {elapsed/60:.1f} minutes")
    print(f"Vitesse           : {total_rows/elapsed:,.0f} lignes/sec")
    print("=" * 60)


if __name__ == "__main__":
    main()
