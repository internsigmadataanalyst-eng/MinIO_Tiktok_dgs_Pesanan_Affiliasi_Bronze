import sys

print(">>> main.py started")

from src.pesanan_affiliasi.pipelines.run_daily_etl import run_daily_etl

if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv or "-n" in sys.argv
    if dry_run:
        print(">>> dry-run mode enabled")
    print(">>> calling run_daily_etl()")
    run_daily_etl(dry_run=dry_run)
    print(">>> main.py finished")