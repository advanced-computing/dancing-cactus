import streamlit as st

page_proposal = st.Page("proposal.py", title="Our Proposal")
page_market = st.Page("market_analysis.py", title="Energy Market Dashboard")

pg = st.navigation([page_proposal, page_market])
pg.run()
