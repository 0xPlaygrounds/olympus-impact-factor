
import pandas as pd

from oif.helpers import get_erc20_token_balances, get_token_transfers
from oif.constants import ADAI_ADDRESS

# ================================================================
# AAVE Token Transfers
# ================================================================

def get_adai_transfers(allocator_addresses: list[str], start_block: int, end_block: int):

    # Ensure that any addresses being passed in are in lower case to match the Transfer data.
    a = (map(lambda x: x.lower(), allocator_addresses))
    allocator_addresses = list(a)

    transfers = get_token_transfers(ADAI_ADDRESS, start_block, end_block)
    
    blocks = []
    addresses = []
    direction = []
    token_name = []
    amount = []

    for transfer in transfers:
        if transfer.to in allocator_addresses:
            blocks.append(transfer.block_number)
            addresses.append(transfer.to)
            direction.append('to')
            token_name.append('aDAI')
            amount.append(transfer.amount / 1e18)
        elif transfer.from_ in allocator_addresses:
            blocks.append(transfer.block_number)
            addresses.append(transfer.from_)
            direction.append('from')
            token_name.append('aDAI')
            amount.append(transfer.amount / 1e18)

    d = {'block_number': blocks, 'address': addresses, 'direction': direction, 'token': token_name, 'amount': amount}

    df = pd.DataFrame(d)
    return df

def get_adai_balances(owner_addresses: list[str], start_block: int, end_block: int, block_interval: int):
    balances = []
    
    for owner in owner_addresses:
        new_balances = get_erc20_token_balances(ADAI_ADDRESS,owner,block_interval,start_block,end_block)
        balances = balances + new_balances

    df = pd.DataFrame(balances)
    return df
