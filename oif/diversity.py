from collections import defaultdict
from typing import TypedDict

import pandas as pd

from oif.helpers import get_token_transfers, timestamp_of_block, web3

LATEST_BLOCK = 14625663


class Token(TypedDict):
    contract: str
    start_block: int
    decimals: int


def get_constants() -> dict[str: Token]:
    ohm = Token(
        contract="0x64aa3364F17a4D01c6f1751Fd97C2BD3D7e7f1D5",
        start_block=13782589,
        decimals=9,
    )

    sohm = Token(
        contract="0x04906695D6D12CF5459975d7C3C03356E4Ccd460",
        start_block=12622596,
        decimals=9,
    )

    gohm = Token(
        contract="0x0ab87046fBb341D058F17CBC4c1133F25a20a52f",
        start_block=13674957,
        decimals=18,
    )

    return {"ohm": ohm, "sohm": sohm, "gohm": gohm}


def get_data(tokens: list[Token]):
    data = {}
    for token, values in tokens.items():
        try:
            data[token] = pd.read_csv(
                f"data/{token}_transactions.csv", index_col="block_number"
            ).drop("Unnamed: 0", axis=1)
        except FileNotFoundError:
            transactions = get_token_transfers(
                values["contract"],
                startblock=values["start_block"],
                endblock=LATEST_BLOCK,
            )
            df = pd.DataFrame(transactions, index_col="block_number").rename(
                {"from_": "from"}, axis=1
            )
            df.to_csv(f"data/{token}_transactions.csv")
            data[token] = df
    return data


def calc_balances(token: Token, df: pd.DataFrame):
    dic = defaultdict(int)
    for _, row in df.iterrows():
        dic[row["from"]] -= int(row["amount"])
        dic[row["to"]] += int(row["amount"])

    return pd.DataFrame(
        [
            {"address": k, "balance": v / (10 ** token["decimals"])}
            for k, v in dic.items()
        ]
    )

def two_months_block_number(data):
    latest_timestamp = timestamp_of_block(data["ohm"]["block_number"])



def main():
    tokens = get_constants()

    data = get_data(tokens)

    # Substack #1, unmerged
    balances = {
        token: calc_balances(tokens[token], data) for token, data in data
    }
    print(balances)


if __name__ == "__main__":
    main()
