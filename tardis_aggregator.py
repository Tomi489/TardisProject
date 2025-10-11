import asyncio
import logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)-8.8s] %(name)s -- %(message)s",
)
logging.getLogger().setLevel(logging.INFO)

import tardis_dataio as dio
import tardis_selection as sel
import time
import pandas as pd

async def test_loader():
    selector = sel.StreamSelection(exchange='binance',
                                   datatype='arr',
                                   symboltype='option')
    loader = dio.load(selector, prefix='prod1026',
                      fr='2022-10-27 01:00:00', to='2022-10-27 02:00:00', localfile=False)
    return await loader

def process_depth(row):
    bids = pd.DataFrame(np.stack(row.bids).astype(float),columns='p s'.split())
    asks = pd.DataFrame(np.stack(row.asks).astype(float),columns='p s'.split())

    return bids.p[0]

async def test_iterator(iterator):
    res = []
    async for df in iterator:
        res.append(df)
    return pd.concat(res)

def reindex(df):
    grid = pd.date_range('2022-10-27 01:00:00','2022-10-27 02:00:00', freq='1s')
    rei = df.set_index('E').groupby('s').apply(lambda x: x.reindex(grid,method='ffill'))
    print (rei.reset_index(drop=True).groupby('s').apply(len))

def deribit_option_process(df_option, df_perp):
    df = df_option
    df['dte'] = (pd.to_datetime(df.exp) - df.timestamp)/pd.Timedelta('365D')
    df['Sbid'] = df_perp.bid
    #etc etc
    
    df = df.set_index(['underlying','timestamp'])
    return df

def spot_process(df):
    for col in ['p','q']:
        df[col] = df[col].astype(float)
    return df


async def biglist2(fr='2022-10-30 00:00:00', to=None, freq='1min', step=1, as_of_date=None):

    fr = pd.Timestamp(fr)
    if to is None:
        to = fr + pd.Timedelta('1D')
    else :
        to = pd.Timestamp(to)

    grid = pd.date_range(fr,to,freq=freq)

    names = ['deribit_option', 'deribit_quotes']
    sels = [
        sel.StreamSelection('deribit','options_chain','option','OPTION'),
        sel.StreamSelection('deribit','quotes','perp','ETH-PERPETUAL'),
    ]
    aits = [dio.raw_data_iterator(sel, prefix='prod1030',
                                  localfile=False,
                                  force_reload=False,
                                  as_of_date=as_of_date,
                                  fr=fr, to=to) for sel in sels]

    df_lists = {name:[] for name in names}

    async for dfs in dio.azip_chunks(aits, grid, step=step):
        if any(x.empty for x in dfs): continue
        dfmap = dict(zip(names,dfs))

        for name, df in dfmap.items():
            print (name,df.timestamp.min(), df.timestamp.max())

        deribit_option = option_process(dfmap['deribit_option'], dfmap['deribit_perp'])
        df_lists['deribit_option'].append(deribit_option)


    df_all = {k:pd.concat(df_lists[k]) for k in dfmap}
    for k in dfmap:
        df_all[k].to_parquet(f'/my/notebooks/{k}_{fr.date()}.parquet')

    return df_all

if __name__ == "__main__":
    for fr in ['2023-01-'+s for s in '25 26 27 28 29'.split()]:
        fr = pd.to_datetime(fr)
        asyncio.run(biglist2(fr, to, freq='1min', step=5, as_of_date='2023-02-10'))

