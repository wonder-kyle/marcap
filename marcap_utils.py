# -*- coding: utf-8 -*-
# marcap_utils.py - 시가총액 데이터를 위한 유틸함수
#1995-05-02 ~ 2018-10-31 시가총액 데이터

from datetime import datetime
import numpy as np
import pandas as pd
import glob

def marcap_date(date):
    '''
    지정한 날짜의 시가총액 순위 데이터
    :param datetime theday: 날짜
    :return: DataFrame
    '''
    date = pd.to_datetime(date)
    csv_file = 'marcap/data/marcap-%s.csv.gz' % (date.year)

    result = None
    try:
        df = pd.read_csv(csv_file, dtype={'Code':str}, parse_dates=['Date'])
        result = df[[ 'Date', 'Code', 'Name', 
                          'Open', 'High', 'Low', 'Close', 'Volume', 'Amount', 
                          'Changes', 'ChagesRatio', 'Marcap', 'Stocks', 'MarcapRatio', 
                          'ForeignShares', 'ForeignRatio', 'Rank']]
        result = result[result['Date'] == date]
        result = result.sort_values(['Date','Rank'])
    except Exception as e:
        return None
    result.reset_index(drop=True, inplace=True)
    return result

def marcap_date_range(start, end, code=None, low_memory=False, chunksize=10**5):
    '''
    지정한 기간 데이터 가져오기
    :param datetime start: 시작일
    :param datetime end: 종료일
    :param (str|list|set) code: 종목코드 혹은 종목코드 리스트 (지정하지 않으면 모든 종목)
    :param low_memory: chunk로 나누어 road 여부 (메모리 절약)
    :param chunksize: 하나의 chunk 단위 결정
    :return: DataFrame
    '''
    start = pd.to_datetime(start)
    end = pd.to_datetime(end)
    df_list = []
    for year in range(start.year, end.year + 1):
        try:
            csv_file = 'marcap/data/marcap-%s.csv.gz' % (year)
            if low_memory:
                chunks = pd.read_csv(csv_file, chunksize=chunksize, dtype={'Code':str}, parse_dates=['Date'])
                df = pd.concat(_chunk_codefilter(chunks, code))
            else:
                df = pd.read_csv(csv_file, dtype={'Code':str}, parse_dates=['Date'])
                if code:
                    if type(code) in [list, set]:
                        df = df[df['Code'].isin(code)]
                    elif type(code)==str: 
                        df = df[code == df['Code']]
                    else: pass
            df_list.append(df)
        except Exception as e:
            print('Error')
            pass
    df_merged = pd.concat(df_list)
    df_merged = df_merged[(start <= df_merged['Date']) & (df_merged['Date'] <= end)]  
    df_merged = df_merged.sort_values(['Date','Rank'])
    df_merged.reset_index(drop=True, inplace=True)
    return df_merged.rename(columns={'ChagesRatio':'ChangesRatio'})

def _chunk_codefilter(chunks, code):
    '''
    pandas.read_csv 중 chunk를 code에 따라 필터링
    :param pandas.io.parsers.TextFileReader chunks: chunks
    :param (str|list|set) code: 종목코드 혹은 종목코드 리스트 (지정하지 않으면 모든 종목)
    :yield: chunks의 단위 chunk
    '''
    if type(code) in [list, set]:
        for chunk in chunks:
            mask = chunk.iloc[:,0].isin(code)
            if mask.all():
                yield chunk
            else:
                yield chunk.loc[mask]
    elif type(code)==str:
        for chunk in chunks:
            mask = chunk.iloc[:,0]==code
            if mask.all():
                yield chunk
            else:
                yield chunk.loc[mask]
    else:
        for chunk in chunks:
            yield chunk