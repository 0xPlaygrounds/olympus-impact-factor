from datetime import date
import pandas as pd

from subgrounds.subgraph import SyntheticField

from oif.helpers import get_price_token_address, sg, uniswap_v2, uniswap_v3, balancer_v2, sushiswap

# ================================================================
# OHM liquidity
# ================================================================

uniswap_v2.Pair.exchange = SyntheticField.constant('UNISWAP_V2')
uniswap_v2.Pair.name = SyntheticField(
  lambda token0_symbol, token1_symbol: f'{token0_symbol}-{token1_symbol}',
  SyntheticField.STRING,
  [uniswap_v2.Pair.token0.symbol, uniswap_v2.Pair.token1.symbol]
)

uniswap_v3.Pool.exchange = SyntheticField.constant('UNISWAP_V3')
uniswap_v3.Pool.name = SyntheticField(
  lambda token0_symbol, token1_symbol: f'{token0_symbol}-{token1_symbol}',
  SyntheticField.STRING,
  [uniswap_v3.Pool.token0.symbol, uniswap_v3.Pool.token1.symbol]
)

sushiswap.Pair.exchange = SyntheticField.constant('SUSHISWAP')
sushiswap.Pair.name = SyntheticField(
  lambda token0_symbol, token1_symbol: f'{token0_symbol}-{token1_symbol}',
  SyntheticField.STRING,
  [sushiswap.Pair.token0.symbol, sushiswap.Pair.token1.symbol]
)

balancer_v2.Pool.exchange = SyntheticField.constant('BALANCER_V2')

def token_liquidity(token_address: str) -> pd.DataFrame:
  token_price = get_price_token_address(token_address, date.today())
  
  # Uniswap V2
  univ2_pairs0 = uniswap_v2.Query.pairs(where=[uniswap_v2.Pair.token0 == token_address])
  univ2_pairs1 = uniswap_v2.Query.pairs(where=[uniswap_v2.Pair.token1 == token_address])

  univ2_pairs_df = sg.query_df([
    univ2_pairs0.id,
    univ2_pairs0.exchange,
    univ2_pairs0.name,
    univ2_pairs0.reserve0,
    univ2_pairs1.id,
    univ2_pairs1.exchange,
    univ2_pairs1.name,
    univ2_pairs1.reserve1,
  ], columns=['address', 'exchange', 'symbol', 'OHM_reserve'], concat=True)

  # Uniswap V3
  univ3_pairs0 = uniswap_v3.Query.pools(where=[uniswap_v3.Pool.token0 == token_address])
  univ3_pairs1 = uniswap_v3.Query.pools(where=[uniswap_v3.Pool.token1 == token_address])

  univ3_pairs_df = sg.query_df([
    univ3_pairs0.id,
    univ3_pairs0.exchange,
    univ3_pairs0.name,
    univ3_pairs0.totalValueLockedToken0,
    univ3_pairs1.id,
    univ3_pairs1.exchange,
    univ3_pairs1.name,
    univ3_pairs1.totalValueLockedToken1,
  ], columns=['address', 'exchange', 'symbol', 'OHM_reserve'], concat=True)

  # Sushiswap
  sushi_pairs0 = sushiswap.Query.pairs(where=[sushiswap.Pair.token0 == token_address])
  sushi_pairs1 = sushiswap.Query.pairs(where=[sushiswap.Pair.token1 == token_address])

  sushi_pairs_df = sg.query_df([
    sushi_pairs0.id,
    sushi_pairs0.exchange,
    sushi_pairs0.name,
    sushi_pairs0.reserve0,
    sushi_pairs1.id,
    sushi_pairs1.exchange,
    sushi_pairs1.name,
    sushi_pairs1.reserve1,
  ], columns=['address', 'exchange', 'symbol', 'OHM_reserve'], concat=True)

  # Balancer
  bal_pairs = balancer_v2.Query.pools(where={
    'tokensList_contains': [token_address]
  })

  (id_, symbols, balances) = sg.query([
    bal_pairs.id,
    bal_pairs.tokens.symbol,
    bal_pairs.tokens.balance
  ])

  if type(id_) == str:
    data = [{
      'address': id_[0:42],
      'exchange': 'BALANCER_V2',
      'symbol': '-'.join(symbols),
      'OHM_reserve': next(filter(lambda tup: tup[0] == 'OHM', zip(symbols, balances)))[1]
    }]
  else:
    data = [{
      'address': id_[0:42],
      'exchange': 'BALANCER_V2',
      'symbol': '-'.join(symbols),
      'OHM_reserve': next(filter(lambda tup: tup[0] == 'OHM', zip(symbols, balances)))[1]
    } for id_, symbols, balances in zip(id_, symbols, balances)]

  bal_pairs_df = pd.DataFrame(data)

  full_df = pd.concat([
    univ2_pairs_df,
    univ3_pairs_df,
    sushi_pairs_df,
    bal_pairs_df
  ])

  full_df['OHM_reserve_USD'] = full_df['OHM_reserve'] * token_price

  return full_df