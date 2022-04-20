# olympus-impact-factor
Data infrastructure and analytics for OIF metrics 

## OlympusDAO OIF Overview

The Olympus Impact Factor (OIF) is a metric that quantifies and communicates the impact of OlympusDAO across DeFi. 

OIF quantifies the impact of OlympusDAO by measuring the growth, influence, and productivity of the Olympus ecosystem. 

The OIF metric is comprised of three main categories: 

1. Active Daily Revenue: In the context of OIF, daily revenue measures the degree to which the Olympus treasury enhances the stability and strength of the ecosystem

2. OHM Productivity: In the context of OIF, OHM productivity measures the utilization of the OHM token in various key financial activities. Key economic activities include but are not limited to:

- Trading on DEXs and CEXs
- Collateral for lending and borrowing
- Vault interactions etc.  

3. Olympus Network Health: In the context of OIF, network health measures the growth and utilization of the treasury and DAO resources to grow and expand the Olympus ecosystem. Questions that this category answer includes:

- How many Olympus-supported protocols exist?
- How many OHM-focused protocols are incubated within the Olympus ecosystem?
- What is the growth rate of Olympus-supported projects and ecosystems?
- What is the monthly change in OHM distribution and diversity etc.?

## Development
### Installation
Step 1. Install `poetry`<br>
Run `pip install poetry`

Step 2. Install dependencies<br>
Run `poetry install`

Step 3. Create a file called `api_keys.py` in the root directory with the following:
```python
ALCHEMY_API_KEY: str = 'YOUR_KEY'
ETHERSCAN_API_KEY: str = 'YOUR_KEY'
```