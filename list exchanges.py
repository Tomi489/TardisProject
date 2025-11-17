import sys
import requests
import pandas as pd
from datetime import date
from pprint import pprint

def get_available_download_symbols(exchange: str, target_date: date) -> list[str]:
    """
    Lekérdezi és visszaadja a megadott tőzsdén a cél dátumán elérhető szimbólumok listáját.
    """
    api_url = f"https://api.tardis.dev/v1/exchanges/{exchange}"
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Hiba dobása, ha a HTTP kérés sikertelen (pl. 404, 500)
        info = response.json()
    except requests.exceptions.RequestException as e:
        print(f"Hiba történt az API hívás során: {e}", file=sys.stderr)
        return []

    if not info or 'availableSymbols' not in info:
        print(f"Hiba: Az API válasz nem tartalmazza az 'availableSymbols' kulcsot.", file=sys.stderr)
        return []

    availableSymbols = pd.DataFrame(info['availableSymbols'])
    
    # Időzóna-tudatos (UTC) Timestamp objektumok létrehozása a szűréshez
    fr = pd.Timestamp(target_date, tz='UTC')
    to = fr + pd.Timedelta(days=1)
    
    availableSymbols['availableSince_ts'] = pd.to_datetime(
        availableSymbols['availableSince'], utc=True
    )
    availableSymbols['availableTo_ts'] = pd.to_datetime(
        availableSymbols['availableTo'], utc=True
    )

    # Szűrés a cél dátumára aktív szimbólumokra.
    # A hiányzó 'availableTo' (NaT) végtelen érvényességet jelent.
    is_active_on_target_date = (
        (fr >= availableSymbols.availableSince_ts) & 
        (
            (to <= availableSymbols.availableTo_ts) |
            availableSymbols.availableTo_ts.isna()
        )
    )

    currdf = availableSymbols.loc[is_active_on_target_date]
    
    return currdf['id'].to_list()


def list_tardis_csv(exchange: str, channel: str, target_date: date) -> list[str]:
    """
    A megadott tőzsde, csatorna és nap letölthető fájljainak teljes URL-jeit generálja.
    """
    symbols = get_available_download_symbols(exchange, target_date)
    
    print(f"\nTalált szimbólumok {exchange} / {channel} / {target_date} napra: {len(symbols)} db.")
    
    if not symbols:
        return []
        
    date_str = pd.Timestamp(target_date).strftime('%Y/%m/%d')
    base_url = f"https://datasets.tardis.dev/v1/{exchange}/{channel}/{date_str}/"
    
    # URL-ek generálása list comprehension segítségével
    url_list = [f"{base_url}{symbol}.csv.gz" for symbol in symbols]
        
    return url_list

# --- Fő program futtatása ---

if __name__ == "__main__":
    EXCHANGE = "binance-european-options"
    CHANNEL = "trade"
    TARGET_DATE = date(2025, 5, 1)
    
    print(f"Keresés: {EXCHANGE}, Adattípus: {CHANNEL}, Dátum: {TARGET_DATE}")

    files = list_tardis_csv(EXCHANGE, CHANNEL, TARGET_DATE)
    
    if files:
        print(f"\n--- Letölthető URL-ek (összesen: {len(files)}) ---")
        pprint(files)
    else:
        print("\nNem található letölthető fájl a megadott feltételekkel.")