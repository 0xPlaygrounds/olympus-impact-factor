from dataclasses import dataclass

@dataclass
class Transfer:
  block_number: int
  amount: float
  from_: str
  to: str

@dataclass
class Balance:
  block_number: int
  amount: float
  holder: str
