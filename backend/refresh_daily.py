from concurrent.futures import ThreadPoolExecutor, as_completed


from hoogvliet_core import refresh_hoogvliet_daily
from dirk_core import refresh_dirk_daily
# from ah_core import refresh_ah_daily_once
# from jumbo_core import refresh_jumbo_daily_once


TASKS = {
    "hoogvliet": refresh_hoogvliet_daily,
    "dirk": refresh_dirk_daily,
    # "ah": refresh_ah_daily_once,
    # "jumbo": refresh_jumbo_daily_once,
}


def main():
    print("=== Start daily refresh for all supermarkets (in parallel) ===")

    results = {}

    # max_workers = len(TASKS)
    with ThreadPoolExecutor(max_workers=len(TASKS)) as executor:
        future_to_name = {
            executor.submit(func): name
            for name, func in TASKS.items()
        }

        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                result = future.result()  
                results[name] = {"status": "ok", "result": result}
                print(f"[OK] {name} daily refresh finished: {result}")
            except Exception as e:
                results[name] = {"status": "error", "error": str(e)}
                print(f"[ERROR] {name} daily refresh failed: {e}")

    print("=== All daily refresh tasks finished ===")
    print("Summary:", results)


if __name__ == "__main__":
    main()
