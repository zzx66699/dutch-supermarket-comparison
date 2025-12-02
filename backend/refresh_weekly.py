from concurrent.futures import ThreadPoolExecutor, as_completed


from hoogvliet_core import refresh_hoogvliet_weekly
from dirk_core import refresh_dirk_weekly
from ah_core import refresh_ah_weekly
# from jumbo_core import refresh_jumbo_weekly


TASKS = {
    "hoogvliet": refresh_hoogvliet_weekly,
    "dirk": refresh_dirk_weekly,
    "ah": refresh_ah_weekly,
    # "jumbo": refresh_jumbo_weekly,
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

    print("=== All weekly refresh tasks finished ===")
    print("Summary:", results)


if __name__ == "__main__":
    main()
