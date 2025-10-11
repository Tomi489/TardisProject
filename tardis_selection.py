import asyncio
import pandas as pd
import logging
logger = logging.getLogger(__name__)

SYMBOLTYPE_OPTION = 'option'

class StreamSelection:
    def __init__(self, exchange, datatype, symboltype='option',
                 symbols=None, symbol_filter=None):
        self.exchange = exchange
        self.datatype = datatype
        self.symboltype = symboltype
        # normalize symbols to upper case
        if isinstance(symbols, str):
            symbols = symbols.split(',')
        self._symbols = [s.upper() for s in symbols] \
            if symbols else None
        self.symbol_filter = symbol_filter
        self.reinit()

    def reinit(self):
        self.symbols = self._extract()
        self.stream_list = self._compute_stream_list()
        
    def __str__(self):
        return f'{self.exchange} {self.datatype} {self.symboltype} {self.symbols} ({len(self.symbols)} symbols)'
        
    #--------------------------------------------------------------
    # how to process output
    
    def get_payload_processor(self):
        if self.exchange == 'deribit':
            if self.datatype == 'options_chain':
                return self._deribit_options_chain_payload_processor()
        raise NotImplementedError
        
    def get_chunk_processor(self):
        if self.exchange == 'deribit':
            if self.datatype == 'options_chain':
                return self._deribit_options_chain_chunk_processor()
        raise NotImplementedError

    def extract_symbols(self, data_df):
        if len(data_df) == 0: return []
        if self.exchange == 'deribit':
            if self.datatype == 'options_chain':
                return data_df.symbol.unique()
        raise NotImplementedError

    def groupby_symbol(self, data_df):
        if self.exchange == 'deribit':
            return data_df.groupby('symbol')
        raise NotImplementedError

    def filter_by_time(self, fr, to):
        if fr is None: fr = 0
        if to is None: to = 'now'
        fr = pd.to_datetime(fr)
        to = pd.to_datetime(to)
        if fr>to:
            logger.error(f'from {fr} is later than to {to}')
            raise RuntimeError
        if self.exchange == 'deribit':
            def _filter(df):
                if self.symboltype == SYMBOLTYPE_OPTION:
                    idx = pd.to_datetime(df.E) > fr
                    idx &= pd.to_datetime(df.E) <= to
                else:
                    idx = df.s.isin(self.symbols)
                    idx &= pd.to_datetime(df.E) > fr
                    idx &= pd.to_datetime(df.E) <= to
                return df.loc[idx]
            return _filter
        raise NotImplementedError

    def _extract(self):
        if self.exchange == 'deribit':
            return [s.upper() for s in
                    self._deribit_extract(self._symbols,
                                          self.symbol_filter)]
        raise NotImplementedError
        
    def _compute_stream_list(self):
        if self.exchange == 'deribit':
            return self._deribit_compute_stream_list()
        raise NotImplementedError
        
    def _create_filter(self, key=None, **kwargs):
        if key is None or key == 'NO_FILTER':
            return lambda x: True
        raise NotImplementedError

    def _deribit_extract(self, symbols, symbol_filter):
        '''access exchange resources to come up with a list of symbols
        The logic of determining what to listen to'''
        return self.symbols #always give all possible symbols
    
    def _deribit_compute_stream_list(self):
        if self.datatype == 'options_chain':
            return ['options_chain']
        raise NotImplementedError
    #--------------------------------------------------------------
    # deribit options_chain

    def _deribit_options_chain_payload_processor(self):
        def process(count, accumulator, payload, timestamp):
            if 'data' not in payload:
                return count
            data = payload['data']
            df = pd.DataFrame(data)
            df['timestamp'] = timestamp
            accumulator.append(df)
            return count + len(data)
        return process

    def _deribit_options_chain_chunk_processor(self):
        def process(accumulator):
            if len(accumulator) == 0: return pd.DataFrame()
            df =  pd.concat(accumulator)
            df.timestampize(['timestamp','local_timestamp','expiration'],inplace=True)
            return df
        return process


class DataSelection(StreamSelection):
    def __init__(self, exchange, datatype, symboltype,
                 symbols=None, symbol_filter=None, fr=0, to='now'):
        self.fr = fr
        self.to = to
        StreamSelection.__init__(self, exchange, datatype, symboltype,
                                 symbols, symbol_filter)
