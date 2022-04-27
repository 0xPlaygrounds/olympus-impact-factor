
from typing import Callable
import pandas as pd
import requests
from oif.models import Balance

from oif.helpers import get_erc20_token_balances, get_token_transfers, get_erc20_transfers, web3
from oif.constants import ADAI_ADDRESS, LIQUITY_STABILITY_POOL, LIQUITY_ALLOCATOR_V1
from oif.abi import rari_abi, liquity_abi

# ================================================================
# AAVE Token Transfers
# ================================================================

def get_adai_transfers(allocator_addresses: list[str], start_block: int, end_block: int):

    address_data = []

    for address in allocator_addresses:
        new_data = get_erc20_transfers(address,ADAI_ADDRESS,start_block, end_block)
        new_data['blockNum'] = new_data["blockNum"].apply(int, base=16)
        address_data.append(new_data)

    final_df = pd.concat(address_data)
    final_df.reset_index(drop=True, inplace=True)

    return final_df

def get_adai_balances(owner_addresses: list[str], start_block: int, end_block: int, block_interval: int):
    balances = []
    
    for owner in owner_addresses:
        new_balances = get_erc20_token_balances(ADAI_ADDRESS,owner,block_interval,start_block,end_block)
        balances = balances + new_balances

    df = pd.DataFrame(balances)
    return df

# ================================================================
# Rari Allocator
# ================================================================

def get_rari_balances(token_address: str, holder_address: str, startblock: int, endblock: int, block_interval: int):
  cursor = startblock
  data = []
  rari_contract = web3.eth.contract(address=token_address, abi=rari_abi)
  decimals = rari_contract.functions.decimals().call()

  while cursor <= endblock:
    new_snapshot = rari_contract.functions.getAccountSnapshot(holder_address).call({}, cursor) 
    new_balance = new_snapshot[3] / pow(10,decimals)
    new_data = [{
      'block_number': cursor,
      'holder': holder_address,
      'balance': new_balance
    }]

    data = data + new_data

    cursor += block_interval

  return pd.DataFrame([Balance(
    block_number=log['block_number'],
    amount=log['balance'],
    holder=log['holder']
  ) for log in data])

# ================================================================
# Liquity Allocator
# ================================================================

def get_liquity_balances(startblock: int, endblock: int, block_interval: int):
  cursor = startblock
  data = []
  stability_pool = web3.eth.contract(address=LIQUITY_STABILITY_POOL, abi=liquity_abi)

  while cursor <= endblock:
    lusd_balance = stability_pool.functions.getCompoundedLUSDDeposit(LIQUITY_ALLOCATOR_V1).call({}, cursor) / 1e18
    eth_balance = stability_pool.functions.getDepositorETHGain(LIQUITY_ALLOCATOR_V1).call({}, cursor) / 1e18
    lqty_balance = stability_pool.functions.getDepositorLQTYGain(LIQUITY_ALLOCATOR_V1).call({}, cursor) / 1e18
    new_data = [{
      'block_number': cursor,
      'lusd_balance': lusd_balance,
      'eth_balance': eth_balance,
      'lqty_balance': lqty_balance
    }]

    data = data + new_data

    cursor += block_interval

  return pd.DataFrame(data)

