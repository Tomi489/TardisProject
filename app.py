#! /usr/bin/python3
import asyncio
import shared.utils as utils
import logging
logger = logging.getLogger('data_subsampler')
import tardis_aggregator as aggregator

@utils.interruptible()
async def subsample_data(fr, to, as_of_date, freq, step):
    await aggregator.biglist2(fr=fr, to=to,  as_of_date=as_of_date, freq=freq, step=step)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Subsample Binance Data')
    parser.add_argument('command', choices=['subsample'])
    parser.add_argument('--fr', default=None)
    parser.add_argument('--to', default=None)
    parser.add_argument('--as_of_date', default='2023-02-10')
    parser.add_argument('--freq', default='1min')
    parser.add_argument('--step', default=1, type=int)
    args = vars(parser.parse_args())
    command = args.pop('command')
    if command == 'subsample':
        df = asyncio.run(subsample_data(**args))
    else:
        parser.usage()



