from datetime import date, datetime
import pytz
from typing import Optional, Tuple
from tracemalloc import start
import pandas as pd
from functools import cache

from pycoingecko import CoinGeckoAPI
from web3 import Web3
from etherscan import Etherscan
from subgrounds.subgrounds import Subgrounds
from subgrounds.subgraph import Subgraph, FieldPath, SyntheticField


from oif.models import Balance, Transfer
from api_keys import ALCHEMY_API_KEY, ETHERSCAN_API_KEY
from oif.abi import erc20_abi

# ================================================================
# Alchemy
# ================================================================
web3 = Web3(Web3.HTTPProvider(f'https://eth-mainnet.alchemyapi.io/v2/{ALCHEMY_API_KEY}'))

transfer_event_sighash = web3.keccak(text="Transfer(address,address,uint256)").hex()

@cache
def timestamp_of_block(block_number: int) -> int:
  """ Returns the timestamp of block :attr:`block_number` """
  return web3.eth.get_block(block_number)['timestamp']

@cache
def block_of_timestamp(target_timestamp: int) -> int:
  """ Returns the block number that is closest in time to the :attr:`target_timestamp` """
  avg_block_time = 15

  block_number = web3.eth.get_block_number()
  block = web3.eth.get_block(block_number)

  while block['timestamp'] > target_timestamp:
    decrease_blocks = int((block['timestamp'] - target_timestamp) / avg_block_time)

    if (decrease_blocks < 1):
      break
    
    block_number -= decrease_blocks
    
    block = web3.eth.get_block(block_number)

  return block['number']


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

def get_erc20_token_balances(token_address: str, holder_address: str, block_interval: int, startblock: int, endblock: int):
  cursor = startblock
  data = []
  erc20_contract = web3.eth.contract(address=token_address, abi=erc20_abi)
  decimals = erc20_contract.functions.decimals().call()

  while cursor <= endblock:
    new_balance = erc20_contract.functions.balanceOf(holder_address).call({}, cursor) / pow(10,decimals)
    new_data = [{
      'block_number': cursor,
      'holder': holder_address,
      'balance': new_balance
    }]

    data = data + new_data

    cursor += block_interval
  
  return [Balance(
    block_number=log['block_number'],
    amount=log['balance'],
    holder=log['holder']
  ) for log in data]

# ================================================================
# Etherscan
# ================================================================
etherscan = Etherscan(ETHERSCAN_API_KEY)


# ================================================================
# Coingecko
# ================================================================
cg = CoinGeckoAPI()


@cache
def get_price_token_id(
  coin_id: str,
  t: Optional[int | str | datetime | date] = None,
  currency: str = 'usd'
) -> float:
  """ Returns the price of coin with id :attr:`coin_id` at time :attr:`t`. If :attr:`t` is a string,
  it is assumed to be in the required coingecko format (i.e.: DD-MM-YYYY). If it is an integer, then the
  time is assumed to be a unix timestamp. If :attr:`t` is None, then the current price is returned.

  Args:
    coin_id (str): Coingecko coin id
    t (int | str | datetime | date, optional): Time at which to get the price. Defaults to None.
    currency (str, optional): Currency of the price value. Defaults to 'usd'.

  Returns:
    float: Coin price
  """
  if t is None:
    return cg.get_price(coin_id, currency)[coin_id][currency]

  match t:
    case int() as timestamp:
      date_ = date.fromtimestamp(timestamp)
      date_str = f'{date_.day:02}-{date_.month:02}-{date_.year}'
    
    case date() | datetime() as date_:
      date_str = f'{date_.day:02}-{date_.month:02}-{date_.year}'

    case str() as date_:
      date_str = date_

  data = cg.get_coin_history_by_id(id=coin_id, date=date_str)
  return data['market_data']['current_price'][currency]


@cache
def get_coin_id_of_address(address: str, network: str = 'ethereum') -> str:
  """ Returns the coingecko ID of the ERC20 token with address :attr:`address` on
  network :attr:`network`

  Args:
    address (str): The address of the ERC20
    network (str, optional): The network on which the ERC20 is deployed. Defaults to 'ethereum'.

  Raises:
    Exception: The coingecko error

  Returns:
    str: The coingecko token ID
  """
  try:
    data = cg.get_coin_info_from_contract_address_by_id(id=network, contract_address=address)
    return data['id']
  except KeyError:
    raise Exception('token_of_address: {}'.format(data['error']))


@cache
def get_price_token_address(
  address: str,
  t: Optional[int | str | datetime | date] = None,
  network: str = 'ethereum',
  currency: str = 'usd'
) -> float:
  """ Returns the price of the ERC20 with address :attr:`address` on network :attr:`network` at
  time :attr:`t`. If :attr:`t` is a string, it is assumed to be in the required coingecko format
  (i.e.: DD-MM-YYYY). If it is an integer, then the time is assumed to be a unix timestamp. If
  :attr:`t` is None, then the current price is returned.

  Args:
    address (str): The address of the ERC20 contract
    t (int | str | datetime | date, optional): Time at which to get the price. Defaults to None.
    network (str, optional): The network on which the ERC20 is deployed. Defaults to 'ethereum'.
    currency (str, optional): Currency of the price value. Defaults to 'usd'.
  
  Returns:
    float: Coin price
  """
  coin_id = get_coin_id_of_address(address, network)
  return get_price_token_id(coin_id, t, currency)


# ================================================================
# The Graph
# ================================================================
sg = Subgrounds()

uniswap_v2 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v2')
uniswap_v3 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')
sushiswap = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/sushiswap/exchange')
balancer_v2 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/balancer-labs/balancer-v2')

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


def get_uniswap_v2_token_total_liquidity(token_address: str, block_number: Optional[int] = None) -> float:
  if block_number is not None:
    fpath = uniswap_v2.Query.token(id=token_address, block={'number': block_number}).totalLiquidity
  else:
    fpath = uniswap_v2.Query.token(id=token_address).totalLiquidity

  return sg.query([fpath])

def get_uniswap_v3_token_total_liquidity(token_address: str, block_number: Optional[int] = None) -> float:
  if block_number is not None:
    fpath = uniswap_v3.Query.token(id=token_address, block={'number': block_number}).totalValueLocked    
  else:
    fpath = uniswap_v3.Query.token(id=token_address).totalValueLocked

  return sg.query([fpath])

def get_sushiswap_token_total_liquidity(token_address: str, block_number: Optional[int] = None) -> float:
  if block_number is not None:
    fpath = sushiswap.Query.token(id=token_address, block={'number': block_number}).liquidity    
  else:
    fpath = sushiswap.Query.token(id=token_address).liquidity

  return sg.query([fpath])

def get_balancer_v2_token_total_liquidity(token_address: str, block_number: Optional[int] = None) -> float:
  if block_number is not None:
    fpath = balancer_v2.Query.token(id=token_address, block={'number': block_number}).totalBalanceNotional    
  else:
    fpath = balancer_v2.Query.token(id=token_address).totalBalanceNotional

  return sg.query([fpath])

def get_subgraph_sync_block(subgraph: Subgraph) -> int:
  return sg.query([
    subgraph.Query._meta.block.number
  ])


# ================================================================
# Misc
# ================================================================
def get_month_start_end_timestamp(year: int, month: int) -> Tuple[int, int]:
  """ Returns the unix timestamps corresponding to the first and last seconds of
  the :attr:`month`th month of the year :attr:`year`.

  Args:
    year (int): Year
    month (int): Month number

  Returns:
    Tuple[int, int]: Unix timestamps of first and last seconds of the specified month
  """
  first_day = datetime(year, month, 1).replace(tzinfo=pytz.UTC)
  first_day_next_month = datetime(year, month + 1 if month < 12 else 1, 1).replace(tzinfo=pytz.UTC)

  first_day.timestamp()

  return (
    int(first_day.timestamp()),
    int(first_day_next_month.timestamp()) - 1
  )


month_name_map = {
  1: 'Jan',
  2: 'Feb',
  3: 'Mar',
  4: 'Apr',
  5: 'May',
  6: 'Jun',
  7: 'Jul',
  8: 'Aug',
  9: 'Sep',
  10: 'Oct',
  11: 'Nov',
  12: 'Dec',
}