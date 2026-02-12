import streamlit as st
import pandas as pd

st.title("タクシーデータ分析アプリ")

# 1. ユーザーに数字を選ばせる（これがJupyterにはない機能！）
rows = st.slider("何行表示しますか？", min_value=1, max_value=20, value=5)

# 2. データを読み込む
data = pd.read_csv("random_taxi_samples.csv")

# 3. 選ばれた行数だけ表示する
st.write(f"{rows} 行のデータを表示中...")
st.dataframe(data.head(rows))
