from datetime import date
import calendar
import pandas as pd
from typing import Callable, Optional

from subgrounds.subgraph import SyntheticField

from oif.helpers import (
  block_of_timestamp,
  get_month_start_end_timestamp,
  get_price_token_address,
  get_subgraph_sync_block,
  sg,
  web3,
  uniswap_v2,
  uniswap_v3,
  balancer_v2,
  sushiswap,
  month_name_map
)

# ================================================================
# OHM liquidity
# ================================================================

# Define some synthetic fields to make things easier
uniswap_v2.Pair.protocol = SyntheticField.constant('UNISWAP_V2')
uniswap_v2.Pair.name = SyntheticField(
  lambda token0_symbol, token1_symbol: f'{token0_symbol}-{token1_symbol}',
  SyntheticField.STRING,
  [uniswap_v2.Pair.token0.symbol, uniswap_v2.Pair.token1.symbol]
)

uniswap_v3.Pool.protocol = SyntheticField.constant('UNISWAP_V3')
uniswap_v3.Pool.name = SyntheticField(
  lambda token0_symbol, token1_symbol: f'{token0_symbol}-{token1_symbol}',
  SyntheticField.STRING,
  [uniswap_v3.Pool.token0.symbol, uniswap_v3.Pool.token1.symbol]
)

sushiswap.Pair.protocol = SyntheticField.constant('SUSHISWAP')
sushiswap.Pair.name = SyntheticField(
  lambda token0_symbol, token1_symbol: f'{token0_symbol}-{token1_symbol}',
  SyntheticField.STRING,
  [sushiswap.Pair.token0.symbol, sushiswap.Pair.token1.symbol]
)

balancer_v2.Pool.protocol = SyntheticField.constant('BALANCER_V2')
balancer_v2.Pool.address = SyntheticField(
  lambda id_: id_[0:42],
  SyntheticField.STRING,
  balancer_v2.Pool.id
)

def uniswap_v2_liquidity(token_address: str, block_number: Optional[int]) -> pd.DataFrame:
  """ Returns dataframe with columns: [Address, Protocol, Name, OHM Liquidity] (at block :attr:`block_number`) """
  if block_number is not None:
    univ2_pairs0 = uniswap_v2.Query.pairs(
      where=[uniswap_v2.Pair.token0 == token_address],
      block={'number': block_number}
    )
    univ2_pairs1 = uniswap_v2.Query.pairs(
      where=[uniswap_v2.Pair.token1 == token_address],
      block={'number': block_number}
    )
  else:
    univ2_pairs0 = uniswap_v2.Query.pairs(
      where=[uniswap_v2.Pair.token0 == token_address],
    )
    univ2_pairs1 = uniswap_v2.Query.pairs(
      where=[uniswap_v2.Pair.token1 == token_address],
    )

  return sg.query_df([
    univ2_pairs0.id,
    univ2_pairs0.protocol,
    univ2_pairs0.name,
    univ2_pairs0.reserve0,
    univ2_pairs1.id,
    univ2_pairs1.protocol,
    univ2_pairs1.name,
    univ2_pairs1.reserve1,
  ], columns=['Address', 'Protocol', 'Symbol', 'OHM_liquidity'], concat=True)

def uniswap_v3_liquidity(token_address: str, block_number: Optional[int]) -> pd.DataFrame:
  """ Returns dataframe with columns: [Address, Protocol, Name, OHM Liquidity] (at block :attr:`block_number`) """
  if block_number is not None:
    univ3_pairs0 = uniswap_v3.Query.pools(
      where=[uniswap_v3.Pool.token0 == token_address],
      block={'number': block_number}
    )
    univ3_pairs1 = uniswap_v3.Query.pools(
      where=[uniswap_v3.Pool.token1 == token_address],
      block={'number': block_number}
    )
  else:
    univ3_pairs0 = uniswap_v3.Query.pools(
      where=[uniswap_v3.Pool.token0 == token_address],
    )
    univ3_pairs1 = uniswap_v3.Query.pools(
      where=[uniswap_v3.Pool.token1 == token_address],
    )

  return sg.query_df([
    univ3_pairs0.id,
    univ3_pairs0.protocol,
    univ3_pairs0.name,
    univ3_pairs0.totalValueLockedToken0,
    univ3_pairs1.id,
    univ3_pairs1.protocol,
    univ3_pairs1.name,
    univ3_pairs1.totalValueLockedToken1,
  ], columns=['Address', 'Protocol', 'Symbol', 'OHM_liquidity'], concat=True)

def sushiswap_liquidity(token_address: str, block_number: Optional[int]) -> pd.DataFrame:
  """ Returns dataframe with columns: [Address, Protocol, Name, OHM Liquidity] (at block :attr:`block_number`) """
  if block_number is not None:
    sushi_pairs0 = sushiswap.Query.pairs(
      where=[sushiswap.Pair.token0 == token_address],
      block={'number': block_number}
    )
    sushi_pairs1 = sushiswap.Query.pairs(
      where=[sushiswap.Pair.token1 == token_address],
      block={'number': block_number}
    )
  else:
    sushi_pairs0 = sushiswap.Query.pairs(
      where=[sushiswap.Pair.token0 == token_address],
    )
    sushi_pairs1 = sushiswap.Query.pairs(
      where=[sushiswap.Pair.token1 == token_address],
    )

  return sg.query_df([
    sushi_pairs0.id,
    sushi_pairs0.protocol,
    sushi_pairs0.name,
    sushi_pairs0.reserve0,
    sushi_pairs1.id,
    sushi_pairs1.protocol,
    sushi_pairs1.name,
    sushi_pairs1.reserve1,
  ], columns=['Address', 'Protocol', 'Symbol', 'OHM_liquidity'], concat=True)

def balancer_v2_liquidity(token_address: str, block_number: Optional[int]) -> pd.DataFrame:
  """ Returns dataframe with columns: [Address, Protocol, Name, OHM Liquidity] (at block :attr:`block_number`) """
  if block_number is not None:
    bal_pairs = balancer_v2.Query.pools(
      where={'tokensList_contains': [token_address]},
      block={'number': block_number}
    )
  else:
    bal_pairs = balancer_v2.Query.pools(
      where={'tokensList_contains': [token_address]},
    )

  # Use slightly different method to calculate get pool name and 
  # OHM liquidity as the subgraph deals with multi-token pools 
  # differently than Uniswap and other DEXes
  (id_, symbols, balances) = sg.query([
    bal_pairs.id,
    bal_pairs.tokens.symbol,
    bal_pairs.tokens.balance
  ])

  if type(id_) == str:
    data = [{
      'Address': id_[0:42],
      'Protocol': 'BALANCER_V2',
      'Symbol': '-'.join(symbols),
      'OHM_liquidity': next(filter(lambda tup: tup[0] == 'OHM', zip(symbols, balances)))[1]
    }]
  else:
    data = [{
      'Address': id_[0:42],
      'Protocol': 'BALANCER_V2',
      'Symbol': '-'.join(symbols),
      'OHM_liquidity': next(filter(lambda tup: tup[0] == 'OHM', zip(symbols, balances)))[1]
    } for id_, symbols, balances in zip(id_, symbols, balances)]

  return pd.DataFrame(data)

def token_liquidity(token_address: str, timestamp: Optional[int] = None) -> pd.DataFrame:
  """ Returns a dataframe containing the following columns: Protocol, Address, Symbol, OHM_liquidity
  and USD_liquidity. The OHM_liquidity is measured at timestamp :attr:`timestamp`.
  """
  token_price = get_price_token_address(token_address, timestamp)

  if timestamp is not None:
    block_number = block_of_timestamp(timestamp)
    current_block = web3.eth.get_block_number()
    
    if block_number > current_block:
      block_number = current_block - 5  # Subtract a couple blocks to account for graph-node delay
  else:
    block_number = None

  full_df = pd.concat([
    uniswap_v2_liquidity(token_address, block_number),
    uniswap_v3_liquidity(token_address, block_number),
    sushiswap_liquidity(token_address, block_number),
    balancer_v2_liquidity(token_address, block_number),
  ])

  # Add USD value of OHM liquidity
  full_df['USD_liquidity'] = full_df['OHM_liquidity'] * token_price

  return full_df.set_index(['Protocol', 'Address', 'Symbol'])

def token_liquidity_MoM(token_address: str, month: int, year: int):
  (start, end) = get_month_start_end_timestamp(year, month)

  # If current month, set end timestamp to None (to get current data)
  if month == date.today().month and year == date.today().year:
    end = None
    
  # Get months string labels
  (previous_month_str, current_month_str) = (
    month_name_map[12 if month == 1 else month-1],
    month_name_map[month]
  )

  # Get data for previous month
  df0 = token_liquidity(token_address, start).rename(columns={
    'OHM_liquidity': 'OHM_liq_{}'.format(previous_month_str),
    'USD_liquidity': 'USD_liq_{}'.format(previous_month_str),
  })

  # Get data for target month
  df1 = token_liquidity(token_address, end).rename(columns={
    'OHM_liquidity': 'OHM_liq_{}'.format(current_month_str),
    'USD_liquidity': 'USD_liq_{}'.format(current_month_str),
  })

  # Calculate MoM change
  df = pd.concat([df0, df1], axis=1)
  df['OHM_liq_{}'.format(previous_month_str)].fillna(0.0, inplace=True)
  df['USD_liq_{}'.format(previous_month_str)].fillna(0.0, inplace=True)
  df['OHM_liq_{}'.format(current_month_str)].fillna(0.0, inplace=True)
  df['USD_liq_{}'.format(current_month_str)].fillna(0.0, inplace=True)
  df['OHM_liq_MoM_change'] = df['OHM_liq_{}'.format(current_month_str)] - df['OHM_liq_{}'.format(previous_month_str)]
  df['USD_liq_MoM_change'] = df['USD_liq_{}'.format(current_month_str)] - df['USD_liq_{}'.format(previous_month_str)]
  df['OHM_liq_MoM_change_percent'] = df['OHM_liq_MoM_change'] / df['OHM_liq_{}'.format(previous_month_str)]
  df['USD_liq_MoM_change_percent'] = df['USD_liq_MoM_change'] / df['USD_liq_{}'.format(previous_month_str)]
  
  # Reorder columns to make the dataframe pretty
  return df[[
    'OHM_liq_{}'.format(previous_month_str),
    'OHM_liq_{}'.format(current_month_str),
    'OHM_liq_MoM_change',
    'OHM_liq_MoM_change_percent',
    'USD_liq_{}'.format(previous_month_str),
    'USD_liq_{}'.format(current_month_str),
    'USD_liq_MoM_change',
    'USD_liq_MoM_change_percent',
  ]]


# ================================================================
# OHM volume
# ================================================================
def uniswap_v2_volume(token_address: str, start_timestamp: int, end_timestamp: Optional[int] = None):
  univ2_pairs0 = uniswap_v2.Query.pairs(
    where=[uniswap_v2.Pair.token0 == token_address],
  )
  univ2_pairs1 = uniswap_v2.Query.pairs(
    where=[uniswap_v2.Pair.token1 == token_address],
  )

  df_meta = sg.query_df([
    univ2_pairs0.id,
    univ2_pairs0.protocol,
    univ2_pairs0.name,
    univ2_pairs1.id,
    univ2_pairs1.protocol,
    univ2_pairs1.name,
  ], columns=['Address', 'Protocol', 'Symbol'], concat=True).set_index('Address')

  if end_timestamp is not None:
    pair_day_data0 = uniswap_v2.Query.pairDayDatas(where=[
      uniswap_v2.PairDayData.token0 == token_address,
      uniswap_v2.PairDayData.date >= start_timestamp,
      uniswap_v2.PairDayData.date < end_timestamp
    ])

    pair_day_data1 = uniswap_v2.Query.pairDayDatas(where=[
      uniswap_v2.PairDayData.token1 == token_address,
      uniswap_v2.PairDayData.date >= start_timestamp,
      uniswap_v2.PairDayData.date < end_timestamp
    ])
  else:
    pair_day_data0 = uniswap_v2.Query.pairDayDatas(where=[
      uniswap_v2.PairDayData.token0 == token_address,
      uniswap_v2.PairDayData.date >= start_timestamp,
    ])

    pair_day_data1 = uniswap_v2.Query.pairDayDatas(where=[
      uniswap_v2.PairDayData.token1 == token_address,
      uniswap_v2.PairDayData.date >= start_timestamp,
    ])

  df_volume = sg.query_df([
    pair_day_data0.pairAddress,
    pair_day_data0.dailyVolumeToken0,
    pair_day_data1.pairAddress,
    pair_day_data1.dailyVolumeToken1,
  ], columns=['Address', 'OHM_volume'], concat=True).groupby('Address').sum()
  
  return pd.concat([
    df_meta,
    df_volume
  ], axis=1).fillna(0.0).reset_index().set_index(['Protocol', 'Address', 'Symbol'])

def uniswap_v3_volume(token_address: str, start_timestamp: int, end_timestamp: Optional[int] = None) -> pd.DataFrame:
  univ3_pools0 = uniswap_v3.Query.pools(
    where=[uniswap_v3.Pool.token0 == token_address],
  )
  univ3_pools1 = uniswap_v3.Query.pools(
    where=[uniswap_v3.Pool.token1 == token_address],
  )

  if end_timestamp is not None:
    pool_day_data0 = univ3_pools0.poolDayData(where=[
      uniswap_v3.PoolDayData.date >= start_timestamp,
      uniswap_v3.PoolDayData.date < end_timestamp
    ])

    pool_day_data1 = univ3_pools1.poolDayData(where=[
      uniswap_v3.PoolDayData.date >= start_timestamp,
      uniswap_v3.PoolDayData.date < end_timestamp
    ])
  else:
    pool_day_data0 = univ3_pools0.poolDayData(where=[
      uniswap_v3.PoolDayData.date >= start_timestamp,
    ])

    pool_day_data1 = univ3_pools1.poolDayData(where=[
      uniswap_v3.PoolDayData.date >= start_timestamp,
    ])

  return sg.query_df([
    univ3_pools0.id,
    univ3_pools0.protocol,
    univ3_pools0.name,
    pool_day_data0.volumeToken0,
    univ3_pools1.id,
    univ3_pools1.protocol,
    univ3_pools1.name,
    pool_day_data1.volumeToken1,
  ], columns=['Address', 'Protocol', 'Symbol', 'OHM_volume'], concat=True).groupby(['Protocol', 'Address', 'Symbol']).sum()

def sushiswap_volume(token_address: str, start_timestamp: int, end_timestamp: Optional[int] = None) -> pd.DataFrame:
  sushi_pairs0 = sushiswap.Query.pairs(
    where=[sushiswap.Pair.token0 == token_address],
  )
  sushi_pairs1 = sushiswap.Query.pairs(
    where=[sushiswap.Pair.token1 == token_address],
  )

  if end_timestamp is not None:
    pool_day_data0 = sushi_pairs0.dayData(where=[
      sushiswap.PairDayData.date >= start_timestamp,
      sushiswap.PairDayData.date < end_timestamp
    ])

    pool_day_data1 = sushi_pairs1.dayData(where=[
      sushiswap.PairDayData.date >= start_timestamp,
      sushiswap.PairDayData.date < end_timestamp
    ])
  else:
    pool_day_data0 = sushi_pairs0.dayData(where=[
      sushiswap.PairDayData.date >= start_timestamp,
    ])

    pool_day_data1 = sushi_pairs1.dayData(where=[
      sushiswap.PairDayData.date >= start_timestamp,
    ])

  return sg.query_df([
    sushi_pairs0.id,
    sushi_pairs0.protocol,
    sushi_pairs0.name,
    pool_day_data0.volumeToken0,
    sushi_pairs1.id,
    sushi_pairs1.protocol,
    sushi_pairs1.name,
    pool_day_data1.volumeToken1,
  ], columns=['Address', 'Protocol', 'Symbol', 'OHM_volume'], concat=True).groupby(['Protocol', 'Address', 'Symbol']).sum()

def balancer_v2_volume(
  token_address: str,
  start_timestamp: int,
  end_timestamp: Optional[int] = None
) -> pd.DataFrame:
  bal_pools = balancer_v2.Query.pools(where={
    'tokensList_contains': [token_address]
  })

  # First query to get names of pools
  (id_, symbols) = sg.query([
    bal_pools.id,
    bal_pools.tokens.symbol,
  ])

  if type(id_) == str:
    data = [{
      'Address': id_[0:42],
      'Protocol': 'BALANCER_V2',
      'Symbol': '-'.join(symbols),
    }]
  else:
    data = [{
      'Address': id_[0:42],
      'Protocol': 'BALANCER_V2',
      'Symbol': '-'.join(symbols),
    } for id_, symbols in zip(id_, symbols)]
  
  pools_metadata = pd.DataFrame(data).set_index('Address')

  # Second query to get swaps of pools
  if end_timestamp is not None:
    swaps0 = bal_pools.swaps(where=[
      balancer_v2.Swap.tokenOut == token_address,
      balancer_v2.Swap.timestamp > start_timestamp,
      balancer_v2.Swap.timestamp <= end_timestamp,
    ])
    swaps1 = bal_pools.swaps(where=[
      balancer_v2.Swap.tokenIn == token_address,
      balancer_v2.Swap.timestamp > start_timestamp,
      balancer_v2.Swap.timestamp <= end_timestamp,
    ])
  else:
    swaps0 = bal_pools.swaps(where=[
      balancer_v2.Swap.tokenOut == token_address,
      balancer_v2.Swap.timestamp > start_timestamp,
    ])
    swaps1 = bal_pools.swaps(where=[
      balancer_v2.Swap.tokenIn == token_address,
      balancer_v2.Swap.timestamp > start_timestamp,
    ])

  df0 = sg.query_df([
    bal_pools.address,
    swaps0.tokenAmountOut,
  ], columns=['Address', 'OHM_volume'])

  df1 = sg.query_df([
    bal_pools.address,
    swaps1.tokenAmountIn,
  ], columns=['Address', 'OHM_volume'])

  swaps_df = pd.concat([df0, df1]).groupby('Address').sum()
  return pd.concat([pools_metadata, swaps_df], axis=1).reset_index().set_index(['Protocol', 'Address', 'Symbol'])

def token_volume(
  token_address: str,
  start_timestamp: int,
  end_timestamp: Optional[int] = None
) -> pd.DataFrame:
  """ Returns a dataframe containing the following columns: Protocol, Address, Symbol and OHM_volume.
  The OHM_volume is measured for the period starting at :attr:`start_timestamp` and ending at
  :attr:`end_timestamp`.
  """
  full_df = pd.concat([
    uniswap_v2_volume(token_address, start_timestamp, end_timestamp),
    uniswap_v3_volume(token_address, start_timestamp, end_timestamp),
    sushiswap_volume(token_address, start_timestamp, end_timestamp),
    balancer_v2_volume(token_address, start_timestamp, end_timestamp)
  ])

  return full_df

def token_volume_MoM(token_address: str, month: int, year: int):
  # Get month start and end timestamp
  (start, end) = get_month_start_end_timestamp(year, month)

  # If current month, set end timestamp to None (to get current data)
  if month == date.today().month and year == date.today().year:
    end = None

  # Get token price at month start and end
  (start_token_price, end_token_price) = (
    get_price_token_address(token_address, start),
    get_price_token_address(token_address, end if end is not None else date.today())
  )
    
  # Get months string labels
  (previous_month_str, current_month_str) = (
    month_name_map[12 if month == 1 else month-1],
    month_name_map[month]
  )

  # Get (month, year) pair for previous month
  (prev_month, prev_month_year) = (
    month - 1 if month > 1 else 12,
    year if month > 1 else year - 1
  )
  (prev_month_start, prev_month_end) = get_month_start_end_timestamp(prev_month_year, prev_month)

  # Get data for previous month
  df0 = token_volume(token_address, prev_month_start, prev_month_end).rename(columns={
    'OHM_volume': 'OHM_vol_{}'.format(previous_month_str),
  })
  df0['USD_vol_{}'.format(previous_month_str)] = df0['OHM_vol_{}'.format(previous_month_str)] * start_token_price

  # Get data for target month
  df1 = token_volume(token_address, start, end).rename(columns={
    'OHM_volume': 'OHM_vol_{}'.format(current_month_str),
  })
  df1['USD_vol_{}'.format(current_month_str)] = df1['OHM_vol_{}'.format(current_month_str)] * end_token_price

  df = df0.combine_first(df1)
  df['OHM_vol_{}'.format(previous_month_str)].fillna(0.0, inplace=True)
  df['USD_vol_{}'.format(previous_month_str)].fillna(0.0, inplace=True)
  df['OHM_vol_{}'.format(current_month_str)].fillna(0.0, inplace=True)
  df['USD_vol_{}'.format(current_month_str)].fillna(0.0, inplace=True)
  df['OHM_vol_MoM_change'] = df['OHM_vol_{}'.format(current_month_str)] - df['OHM_vol_{}'.format(previous_month_str)]
  df['USD_vol_MoM_change'] = df['USD_vol_{}'.format(current_month_str)] - df['USD_vol_{}'.format(previous_month_str)]
  df['OHM_vol_MoM_change_percent'] = df['OHM_vol_MoM_change'] / df['OHM_vol_{}'.format(previous_month_str)]
  df['USD_vol_MoM_change_percent'] = df['USD_vol_MoM_change'] / df['USD_vol_{}'.format(previous_month_str)]
  
  # Reorder columns
  return df[[
    'OHM_vol_{}'.format(previous_month_str),
    'OHM_vol_{}'.format(current_month_str),
    'OHM_vol_MoM_change',
    'OHM_vol_MoM_change_percent',
    'USD_vol_{}'.format(previous_month_str),
    'USD_vol_{}'.format(current_month_str),
    'USD_vol_MoM_change',
    'USD_vol_MoM_change_percent',
  ]]