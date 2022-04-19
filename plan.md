# OIF V1

## Requirements
1. [ ] Active liquidity
    1. [ ] Daily stablecoin LP revenue (Mainnet)
        - [ ] Balancer
            - [ ] OHM/DAI/ETH (0xc45d42f801105e861e86658648e3678ad7aa70f9)
        - [ ] Sushi
            - [ ] OHM/LUSD (0x46e4d8a1322b9448905225e52f914094dbd6dddf)
            - [ ] OHM/DAI (0x055475920a8c93cffb64d039a8205f7acc7722d3)
        - [ ] Curve
            - [ ] FRAX/3pool (0xd632f22692fac7611d2aa1c0d552930d43caed3b)

    2. [ ] Daily farming stablecoin revenue (Mainnet)
        - [ ] Aave
            - [ ] aDAI (0x028171bca77440897b824ca71d1c56cac55b68a3)
            - [ ] Allocator V1 (0x0e1177e47151be72e5992e0975000e73ab5fd9d4)
            - [ ] Allocator V2 (0x0d33c811d0fcc711bcb388dfb3a152de445be66f)
        - [ ] Convex
            - [ ] cvxCRV ()
            - [ ] Allocator V1 (0xdfc95aaf0a107daae2b350458ded4b7906e7f728)
            - [ ] Allocator V2 (0x2d643df5de4e9ba063760d475beaa62821c71681)
        - [ ] Frax
            - [ ] Allocator (0xde7b85f52577b113181921a7aa8fc0c22e309475)
        - [ ] Liquity
            - [ ] Allocator (0x97b3Ef4C558Ec456D59Cb95c65BFB79046E31fCA)
            - [ ] Stability pool ()
        - [ ] Rari
            - [ ] Allocator (0x061C8610A784b8A1599De5B1157631e35180d818)
2. [ ] OHM Productivity
    1. [ ] Total OHM liquidity on DEXs (Mainnet)
        - [ ] Sushi
        - [ ] Balancer
        - [ ] Uniswap V2
        - [ ] Uniswap V3
    2. [ ] Total OHM volume on DEXs (Mainnet)
        - [ ] Sushi
        - [ ] Balancer
        - [ ] Uniswap V2
        - [ ] Uniswap V3
3. [ ] OHM Network health
    1. [ ] Wallets holding OHM (Self Custody)
    2. [ ] Wallets holding OHM greater than two months
    3. [ ] OHM holder diversity: GINI coefficient

### 1.1 Daily stablecoin LP revenue (Mainnet)
Given: 
- List of LP tokens to monitor

Data:
- Etherscan (alternatively, use web3.py + Alchemy):
  - List of transfers filtered by LP token address and treasury address
- The Graph: 
  - LP reserves at time of transfer(s)
  - Current LP reserves

Calculation:
- Use transfer data to get number of LP shares
- Calculate stablecoin value of treasury LP shares at time of deposit(s)
- Calculate current stablecoin value of treasury LP shares
- Calculate difference between stablecoin value to get stablecoin LP fees

### 1.2 Daily stablecoin LP revenue (Mainnet)
Data:
- web3.py (Alchemy):
  - (Aave) Aave aToken transfers to/from Aave allocators
  - (Aave) Current aToken balances of Aave allocators


### 2.1 Total OHM liquidity on DEXs (Mainnet)
Given:
- List of DEXes of interest

Data:
- The Graph:
  - LP containing OHM
  - Current LP reserves
  - Historical LP reserves 

Calculation:
- Sum OHM liquidity across DEX LPs

### 2.2 Total OHM volume on DEXs (Mainnet)
Given:
- List of DEXes of interest

Data:
- The Graph:
  - LP containing OHM
  - Current cumulative volume
  - Historical cumulative volume

Calculation:
- Sum OHM liquidity across DEX LPs

### 3.1 Wallets holding OHM (Self Custody)
Data:
- web3.py (Alchemy):
  - Historical transfers

Calculation:
- Reconstruct wallet balances in-memory using all historical transfers of OHM
  - Include a special attribute to each user's balance called `time_since_zero` (see 3.2)
- Count number of non-zero wallet balances 

### 3.2 Wallets holding OHM greater than two months
Re-using wallet balances reconstruction from 3.1

Calculation:
- Use the `time_since_zero` value of current wallet balances to get the number of wallets holding OHM for >2 months 

### 3.3 OHM holder diversity: GINI coefficient
Re-using wallet balances reconstruction from 3.1

Calculation:
- Calculate GINI using reconstructed current (or historical) wallet balances


## Development environment
Language: Python
Version: >=3.10
Libraries:
- subgrounds
- web3
- etherscan-python
- pandas
- ipykernel