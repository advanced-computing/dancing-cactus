# How electricity price respond to the dynamics of supply and demand and fuel mix?
## Overview
We are going to analyze the relationship between electricity price and the dynamics of demand and supply. Generally, higher demand is expected to correlate with increased electricity price. Based on the hourly electricity data provided by NY state, we will examine how wholesale price fluctuates in response to demand shifts.

Another factor influencing price is the fuel mix. When a larger scale of electricity is generated from fossil fuels, price become increasingly sensitive to volatility in global energy markets, such as oil and natural gas.

Futhermore, we also focus on the environmental externalities of energy consumption, analyzing its impact on indicators such as air quality.

## Potential Research Question
1. How does the Locational Based Marginal Price(LBMP) in NYC fluctuate according to the change in demand? (Although LBMP is a wholesale price, not a retail price, analyzing LBMP instead of retail price is insightful because LBMP reflects dynamic demand shifts more clearly than retail price, which does not change so often compared to the wholesale price.)

2. How does the change in fuel mix during the day affect the LBMP in NY state? I can expect that LBMP will fall during daytime or sunny days and rise during night and rainy days because renewables such as solar energy can generate electricity at a cheaper price.
How does the change in energy prices, for example, the rise of the price of natural gas, affect the LBMP? We can of course include several kinds of energy sources, like oil and coal.

(If time allows)

3. What is the relationship between electricity consumption and air quality in the NY state? (We can analyze the relation between absolute electricity usage and air quality, or the proportion of green energy and air quality. At the same time, air quality has multiple aspects, such as CO/PM2.5/NO2, which also brings us space for in-depth discussion)

# Data Source
* NY state Energy Market & Operational Data: https://www.nyiso.com/real-time-dashboard https://mis.nyiso.com/public/P-24Alist.htm
* Oil price: https://www.eia.gov/dnav/pet/pet_pri_spt_s1_d.htm
* Natural gas price: https://www.eia.gov/dnav/ng/hist/rngwhhdD.htm
* Air Quality https://www.epa.gov/outdoor-air-quality-data/download-daily-data

# Target Visualization

## Know/Unknown
#### Known:
* First, wholesale electricity prices such as LBMP are highly sensitive to short-term demand fluctuations. During peak demand hours, marginal generation units with higher production costs are dispatched, leading to sharp price increases. This mechanism is well-documented in electricity market theory.

* Second, the fuel mix plays a crucial role in price formation. In NYISO, natural gas-fired plants frequently serve as marginal generators due to their operational flexibility and significant share in load-following capacity. As a result, wholesale electricity prices are expected to exhibit strong sensitivity to natural gas price fluctuations.

#### Unknow: 
* Despite these stylized facts, several important uncertainties remain.

* First, the magnitude and timing of the dynamic adjustment between demand shocks and LBMP are unclear. It is not obvious whether price responses are immediate, persistent, or asymmetric across peak and off-peak periods.

## Expected challenges
1. High-Frequency Data Complexity

The hourly (or sub-hourly) nature of LBMP and demand data introduces substantial volatility and noise. Short-term price spikes may obscure systematic patterns, requiring careful filtering or aggregation strategies.

2. External Confounding Factors

Weather conditions (temperature, precipitation, solar radiation) simultaneously affect demand, renewable generation, and air quality. Failing to control for these factors may bias estimated relationships.

3. Air Quality Attribution

Air quality indicators such as PM2.5 or NO₂ are influenced by multiple emission sources beyond electricity generation (e.g., transportation, industrial activity). Isolating the contribution of electricity demand may therefore require additional controls or robustness checks.

