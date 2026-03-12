import streamlit as st

def main() -> None:
    with st.expander("Initial Proposal (Original)"):
        st.markdown(
        """
        ### 1. Introduction:
        We plan to study the relationship between energy price fluctuations and macroeconomic outcomes, with a particular focus on how short-term energy price shocks transmit into broader economic indicators such as inflation and industrial activity. More broadly, the project aims to explore how high-frequency energy market data can be used to understand economic volatility and policy-relevant dynamics in real time. Moreover, we also focus on the environmental externalities of energy consumption, analyzing its impact on indicators such as air quality.
        
        ### 2. Potential Research Question:
        * How quickly do energy prices adjust in response to changes in supply and demand across different energy sources (including electricity from wind and solar, oil, coal, natural gas, bioenergy, and nuclear power)?
        * What is the relationship between electricity consumption and air quality in the United States?
        * How has the composition of electricity generation by energy source (oil, coal, natural gas, and renewables) changed over time, and what is the relationship between shifts in this composition and overall economic performance?
        
        ### 3. Database will be used:
        * FRED:5-Year Breakeven Inflation Rate (Daily) https://fred.stlouisfed.org/series/T5YIE
        * Cboe Crude Oil ETF Volatility Index (Daily) https://www.cboe.com/us/indices/dashboard/ovx/
        * Electricity demand (Daily)  https://www.iea.org/data-and-statistics/data-tools/real-time-electricity-tracker?from=2026-1-3&to=2026-2-2&category=demand&country=USA
        * FRED: Global price of Brent Crude (Monthly) https://fred.stlouisfed.org/series/POILBREUSDM
        * Electricity generation by fuel type (Monthly) / Electricity consumption (Monthly) https://www.eia.gov/electricity/data.php
        * Air Quality status across the US (hourly)  https://explore.openaq.org/#1.2/20/40
        
        ### 4. Link to the Notebook
        https://github.com/advanced-computing/dancing-cactus/pull/1#issue-3887518136
        
        ### 5. Target visualization
        Q1: We need more time to find the suitable visualization.
        Q2: 
        Q3: 
        
        ### 6. Know/Unknown
        Known:
                *Existing evidence indicates that the share of renewable (green) energy in overall energy consumption has been steadily increasing over time.
                *We hypothesize a negative relationship between electricity consumption and air quality in New York City, particularly through increased emissions associated with higher electricity demand.
        Unknow: 
                *The dynamic adjustment process of energy prices in response to supply–demand shocks (as outlined in Research Question 1) remains unclear.
                *It is also unknown whether the relationship between changes in the electricity generation mix and economic performance (Research Question 3) varies systematically across countries or regions.
        
        ### 7. Expected challenges
        * Data availability and quality: Suitable, high-frequency, and comparable datasets may be difficult to obtain, particularly for cross-country analysis. Regarding Q1, It may be challenging to identify appropriate indicators that capture the change in supply-demand. 
        * Weak or ambiguous empirical relationships: The relationships of interest may be statistically weak or obscured by noise, making it difficult to identify clear patterns.
        * Correlation versus causation: Observed associations may reflect correlation rather than causal effects, limiting the strength of policy conclusions.
        * Visualization constraints: Some relationships may be difficult to represent clearly through visualizations alone, especially when multiple confounding factors are present.
        * Interpretive limitations: Findings may be highly aggregated or context-specific, resulting in conclusions that are broad but offer limited actionable insight.
        """)
    
    st.header("Overview")
    st.markdown(
        """
        We are going to find how electricity price change in response to some factors,
        such as the dynamics of demand and supply and fule mix.
        
        Futhermore, we also focus on the environmental externalities of energy consumption, 
        analyzing its impact on indicators such as air quality.
        """)
    
    st.subheader("Potential Research Question")
    st.markdown(
        """
        * How does the Locational Based Marginal Price(LBMP) in NYC fluctuate according to the change in demand? (Although LBMP is a wholesale price, not a retail price, analyzing LBMP instead of retail price is insightful because LBMP reflects dynamic demand shifts more clearly than retail price, 
        which does not change so often compared to the wholesale price.)
        * How does the change in fuel mix during the day affect the LBMP in NY state? I can expect that LBMP will fall during daytime or sunny days and rise during night and rainy days because renewables such as solar energy can generate electricity at a cheaper price. 
        * How does the change in energy prices, for example, the rise of the price of natural gas, affect the LBMP? We can of course include several kinds of energy sources, like oil and coal.
        (If time allows)
        * What is the relationship between electricity consumption and air quality in the NY state? (We can analyze the relation between absolute electricity usage and air quality, or the proportion of green energy and air quality. At the same time, air quality has multiple aspects, 
        such as CO/PM2.5/NO2, which also brings us space for in-depth discussion)
        """)
    
    st.subheader("Data Source")
    st.markdown(
        """
        * NY state Energy Market & Operational Data: https://www.nyiso.com/real-time-dashboard  
        https://mis.nyiso.com/public/P-24Alist.htm
        * Oil price: https://www.eia.gov/dnav/pet/pet_pri_spt_s1_d.htm
        * Natural gas price: https://www.eia.gov/dnav/ng/hist/rngwhhdD.htm
        * Air Quality https://www.epa.gov/outdoor-air-quality-data/download-daily-data
        """)
    
    st.subheader("Know/Unknown")
    st.markdown(
        """
        #### Known:    
        First, wholesale electricity prices such as LBMP are highly sensitive to short-term demand fluctuations. During peak demand hours, marginal generation units with higher production costs are dispatched, leading to sharp price increases. This mechanism is well-documented in electricity market theory.
        Second, the fuel mix plays a crucial role in price formation. In NYISO, natural gas-fired plants frequently serve as marginal generators due to their operational flexibility and significant share in load-following capacity. As a result, wholesale electricity prices are expected to exhibit strong sensitivity to natural gas price fluctuations.
        
        #### Unknow: 
        Despite these stylized facts, several important uncertainties remain.
        First, the magnitude and timing of the dynamic adjustment between demand shocks and LBMP are unclear. It is not obvious whether price responses are immediate, persistent, or asymmetric across peak and off-peak periods.
        """)
    
    st.subheader("Expected challenges")
    st.markdown(
        """
        1. High-Frequency Data Complexity
        The hourly (or sub-hourly) nature of LBMP and demand data introduces substantial volatility and noise. Short-term price spikes may obscure systematic patterns, requiring careful filtering or aggregation strategies.
        
        2. External Confounding Factors
        Weather conditions (temperature, precipitation, solar radiation) simultaneously affect demand, renewable generation, and air quality. Failing to control for these factors may bias estimated relationships.
        
        3. Air Quality Attribution
        Air quality indicators such as PM2.5 or NO₂ are influenced by multiple emission sources beyond electricity generation (e.g., transportation, industrial activity). Isolating the contribution of electricity demand may therefore require additional controls or robustness checks.
        """)

    st.subheader("Changes from the initial proposal")
    st.markdown(
        """
        * Narrowed down the research question to focus specifically on the relationship between electricity prices, 
        supply and demand, and the fuel mix.
        * Narrowed the geographic scope from the national and global level to specifically focus 
        on New York State and New York City.
        * Refined the research focus by shifting away from broad macroeconomic indicators (such as inflation) to 
        deeply analyze the dynamics of wholesale electricity prices, specifically the Locational Based Marginal Price (LBMP).
        * Completely revised the initial proposal to reflect this new focus.
        * Refined the selection of required datasets.
        * Switched from monthly to daily datasets to allow for a more detailed analysis. 
        """)
    
if __name__ == "__main__":
    main()