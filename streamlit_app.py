import streamlit as st

import time

start_time = time.time()

page_proposal = st.Page("proposal.py", title="Our Proposal")
page_market = st.Page("market_analysis.py", title="Energy Market Dashboard")

pg = st.navigation([page_proposal, page_market])
pg.run()

elapsed = time.time() - start_time
st.caption(f"Page loaded in {elapsed:.2f} seconds")
