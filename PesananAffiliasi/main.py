print(">>> main.py started")

from src.pesanan_affiliasi.pipelines.run_daily_etl import run_daily_etl

if __name__ == "__main__":
    print(">>> calling run_daily_etl()")
    run_daily_etl()
    print(">>> main.py finished")