from datetime import datetime
import pandas as pd
from typing import Optional

from pycoingecko import CoinGeckoAPI
from web3 import Web3
from etherscan import Etherscan
from subgrounds.subgrounds import Subgrounds
from subgrounds.subgraph import FieldPath, SyntheticField

from oif.models import Transfer
from api_keys import ALCHEMY_API_KEY, ETHERSCAN_API_KEY

# ================================================================
# Alchemy
# ================================================================
web3 = Web3(Web3.HTTPProvider(f'https://eth-mainnet.alchemyapi.io/v2/{ALCHEMY_API_KEY}'))

transfer_event_sighash = web3.keccak(text="Transfer(address,address,uint256)").hex()

def timestamp_of_block(block_number: int) -> int:
  return web3.eth.get_block(block_number)['timestamp']

def get_token_transfers(token_address: str, startblock: int, endblock: int) -> list[Transfer]:
  if endblock - startblock > 2000:
    cursor = startblock
    data = []
    while cursor < endblock:
      new_data = web3.eth.get_logs({
        'fromBlock': cursor,
        'toBlock': cursor + 2000,
        'address': Web3.toChecksumAddress(token_address),
        'topics': [transfer_event_sighash]
      })

      data = data + new_data
      cursor += 2000
      print(f'get_token_transfers: {cursor}-{cursor+2000}: {len(new_data)} transfers detected!')

  else:
    data = web3.eth.get_logs({
      'fromBlock': startblock,
      'toBlock': endblock,
      'address': Web3.toChecksumAddress(token_address),
      'topics': [transfer_event_sighash]
    })

  return [Transfer(
    block_number=log['blockNumber'],
    amount=int(log['data'], 16),
    from_='0x' + log['topics'][1].hex()[26:],
    to='0x' + log['topics'][2].hex()[26:]
  ) for log in data]

# ================================================================
# Etherscan
# ================================================================
etherscan = Etherscan(ETHERSCAN_API_KEY)


# ================================================================
# Coingecko
# ================================================================
cg = CoinGeckoAPI()



# ================================================================
# The Graph
# ================================================================
sg = Subgrounds()

uniswap_v2 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2')
uniswap_v3 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')
sushiswap = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/sushiswap/exchange')

def datetime_sfield(timestamp_field: FieldPath | SyntheticField) -> SyntheticField:
  return SyntheticField(
    lambda ts: str(datetime.fromtimestamp(ts)),
    SyntheticField.STRING,
    timestamp_field
  )

def adj_amount_sfield(
  amount_field: FieldPath | SyntheticField,
  decimals_field: FieldPath | SyntheticField
) -> SyntheticField:
  return SyntheticField(
    lambda amount, decimals: amount / (10 ** decimals),
    SyntheticField.FLOAT,
    [
      amount_field,
      decimals_field
    ]
  )

uniswap_v2.Mint.exchange = SyntheticField.constant('UNISWAP_V2')
uniswap_v2.Mint.datetime = datetime_sfield(uniswap_v2.Mint.timestamp)

sushiswap.Mint.exchange = SyntheticField.constant('SUSHISWAP')
sushiswap.Mint.datetime = datetime_sfield(sushiswap.Mint.timestamp)

def get_uniswap_v2_deposits(address: str):
  univ2_mints = uniswap_v2.Query.mints(
    orderBy=uniswap_v2.Mint.timestamp,
    orderDirecion='asc',
    where=[
      uniswap_v2.Mint.to == address
    ]
  )

  return sg.query_df([
    univ2_mints.datetime,
    univ2_mints.exchange,
    univ2_mints.pair.token0.symbol,
    univ2_mints.pair.token1.symbol,
    univ2_mints.amount0,
    univ2_mints.amount1,
  ])


def get_sushiswap_deposits(address: str):
  sushi_mints = sushiswap.Query.mints(
    orderBy=sushiswap.Mint.timestamp,
    orderDirecion='asc',
    where=[
      sushiswap.Mint.to == address
    ]
  )

  return sg.query_df([
    sushi_mints.datetime,
    sushi_mints.exchange,
    sushi_mints.pair.token0.symbol,
    sushi_mints.pair.token1.symbol,
    sushi_mints.amount0,
    sushi_mints.amount1,
  ])


def get_uniswap_v2_token_liquidity(token_address: str, block_number: Optional[int] = None) -> float:
  if block_number is not None:
    fpath = uniswap_v2.Query.token(id=token_address, block={'number': block_number}).totalLiquidity
  else:
    fpath = uniswap_v2.Query.token(id=token_address).totalLiquidity

  return sg.query([fpath])

def get_sushiswap_token_liquidity(token_address: str, block_number: Optional[int] = None) -> float:
  if block_number is not None:
    fpath = sushiswap.Query.token(id=token_address, block={'number': block_number}).liquidity    
  else:
    fpath = sushiswap.Query.token(id=token_address).liquidity

  return sg.query([fpath])
