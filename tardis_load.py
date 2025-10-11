from tardis_dev import datasets
import os
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
import pandas as pd
import itertools
import tardis_project.tardis_dataio as dio
import shared.pandas_utils as pu

datadir = '/my/tardis_project/datasets/'
fname_fmt_root='{}_{}_{}_OPTIONS'
fname_fmt_csv=fname_fmt_root + '.csv.gz'
fname_fmt_parquet=fname_fmt_root + '.parquet'


async def main_loop(stream_selector, **kwargs):
    process_chunk = stream_selector.get_chunk_processor()
    max_chunks = kwargs.get('max_chunks',0)
    ichunk = 0
    async for start, end, accumulator in get_chunk(
            stream_selector, **kwargs):
        ichunk += 1
        df = process_chunk(accumulator)
        await dio.save(start, end, stream_selector, df, **kwargs)
        if max_chunks > 0 and ichunk >= max_chunks: break

def get_chunk(stream_selector, **kwargs):
    path = download_options(stream_selector.exchange, date, stream_selector.datatype)
    for chunk in enumerate(pd.read_csv(path, iterator=True, chunksize=chunksize)):
        yield chunk
        

def download_options(exchange, date, datatype='options_chain'):
    date = pd.Timestamp(date)
    date_next = date + pd.Timedelta("1D")
    date = str(date.date())
    date_next = str(date_next.date())
    fname = fname_fmt_csv.format(exchange, datatype, date)
    if not os.path.exists(datadir+fname):
        logger.info(f'downloading {fname}')
        datasets.download(
            exchange=exchange,
            data_types=[
                #        "incremental_book_L2",
                #        "trades",
                #        "quotes",
                #        "derivative_ticker",
                #        "book_snapshot_25",
                #        "liquidations"
                #        "options_chain",
                datatype,
            ],
            from_date=date,
            to_date=date_next,
            symbols=['OPTIONS'],
            api_key="YOUR API KEY (optionally)",
        )
    else:
        logger.info(f'using cached {fname}')
    return datadir+fname

def load_data():
    # use cache or not
    #data = load_data_from_db()
    data = pd.read_parquet('../data/lh_indexoptiondata.parquet')

    data['exp'] = pd.to_datetime(data.expirationDate) + pd.Timedelta('16H') # everything is NY time
    data['pc'] = data.putcall
    data['K'] = data.strike
    data['T'] = (data.exp - data.timestamp)/pd.Timedelta('365D')
    data['Pbid'] = data.bid
    data['Pask'] = data.ask
    data['Qbid'] = data.bidSize
    data['Qask'] = data.askSize
    data['Plast'] = data.Plast
    data['Qlast'] = data.volume
    data['Vol24'] = data.volume
    data['A'] = data.volume
    data['d'] = data.delta
    data['underlying'] = data.underlyingSymbol
    datafut =data.query('secType == "FUT"').reset_index().set_index(['symbol','timestamp'])
    data = data.reset_index().set_index(['underlying','timestamp'])
    data['S'] = datafut.Plast
    data['Sq'] = datafut.Qlast
    data['Sbid'] = datafut.Pbid
    data['Sask'] = datafut.Pask

    options = data.reset_index().set_index(['underlying','timestamp']).query('secType == "FO"')[
        'symbol exp pc K T Pbid Pask Qbid Qask '
        'S Sq Sbid Sask '
        'Plast Qlast Vol24 d A'.split()]

    return options

# df = pcpb.clean_one(options, lh=True)
