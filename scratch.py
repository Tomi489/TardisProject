# pip install tardis-dev
# requires Python >=3.6
if False:
    #
    # pick exchanges to deal with
    #
    
    import requests
    import pandas as pd
    resp = requests.get("https://api.tardis.dev/v1/exchanges")
    resp.raise_for_status()
    exchanges = resp.json()
    for ex in exchanges:
        print(f"{ex['id']:<20} {ex['name']:<25} since {ex['availableSince'][:10]}")
    exdf = pd.DataFrame(exchanges)
    #
    import re
    patterns = [r'[qQ]uote', r'[dD]epth', r'[Bb]ook']
    compiled = [re.compile(p) for p in patterns]
    def has_option_quotes(row):
        channels = list(row.availableChannels)
        has_quotes = any( reg.search(ch) for reg in compiled for ch in channels)
        is_option = 'option' in row.id 
        not_expired = pd.isna(row.availableTo)
        #not_expired = True
        return has_quotes and is_option and not_expired
    df = exdf.loc[exdf.apply(has_option_quotes, axis=1)]
    #
    from pprint import pprint
    for key, row in df.iterrows():
        pprint ((row.id, row.availableChannels))


if False:
    #
    # get the symbol list 
    #

    import requests
    exchange = 'binance-european-options'
    exchange = 'bitmex'
    resp = requests.get(f"https://api.tardis.dev/v1/exchanges/{exchange}")
    resp.raise_for_status()
    info = resp.json()
    availableChannels = info['availableChannels']
    availableSymbols = pd.DataFrame(info['availableSymbols'])
    print(info['id'])
    print(availableChannels)
    print(availableSymbols)
    
if False:
    #
    # get stream of lines from an exchange
    #

    import asyncio
    from tardis_client import TardisClient, Channel
    #
    async def replay():
        tardis_client = TardisClient()
        #
        # replay method returns Async Generator
        # https://rickyhan.com/jekyll/update/2018/01/27/python36.html
        messages = tardis_client.replay(
            exchange="binance-european-options",
            from_date="2025-08-01",
            to_date="2025-08-02",
            filters=[Channel(name="trade", symbols=["XBTUSD","ETHUSD"]), Channel("orderBookL2", ["XBTUSD"])],
        )
        #
        # this will print all trades and orderBookL2 messages for XBTUSD
        # and all trades for ETHUSD for bitmex exchange
        # between 2019-06-01T00:00:00.000Z and 2019-06-02T00:00:00.000Z (whole first day of June 2019)
        async for local_timestamp, message in messages:
            # local timestamp is a Python datetime that marks timestamp when given message has been received
            # message is a message object as provided by exchange real-time stream
            #print(message)
            print(pd.Timestamp(local_timestamp))
    #
    asyncio.run(replay())
    
if False:
    from tardis_dev import datasets
    datasets.download(
        exchange="binance-european-options",
        data_types=[
            'book_snapshot_25'
        ],
        from_date="2025-05-01",
        to_date="2025-05-01",
        symbols=['OPTIONS','FUTURES'],
        api_key="YOUR API KEY (optionally)",
    )


    import requests
    from bs4 import BeautifulSoup
    def list_tardis_csv(exchange, channel, date):
        url = f"https://datasets.tardis.dev/v1/{exchange}/{channel}/{date:%Y/%m/%d}/"
        print(url)
        resp = requests.get(url)
        soup = BeautifulSoup(resp.text, "html.parser")
        #return [a['href'] for a in soup.find_all('a') if a['href'].endswith('.csv.gz')]
        return resp.text
    from datetime import date
    files = list_tardis_csv("binance-european-options", "trade", date(2025, 5, 1))
    print(files)
            

if False:

    import pandas as pd
    for L1 in pd.read_csv('datasets/deribit_options_chain_2023-11-01_OPTIONS.csv.gz', chunksize=100):
        break
    print(L1)

    trades = pd.read_csv('datasets/deribit_trades_2023-11-01_OPTIONS.csv.gz')

    trades.to_parquet('datasets/deribit_trades_2023-11-01_OPTIONS.parquet')

    
