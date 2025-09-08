# %%
import akshare as ak
import fetch_kline

# stock_zh_a_spot_em_df = ak.stock_zh_a_spot_em()
# print(stock_zh_a_spot_em_df)
# stock_zh_a_spot_em_df.to_csv("2025-07-17.csv", index=False)
# %%
# code = "002326"
# df = ak.stock_zh_a_hist(
#     symbol=code,
#     period="daily",
#     # start_date=start,
#     # end_date=end,
#     # adjust=adjust,
# )
# print(df)


# %%

print(
    fetch_kline.get_kline("002326", "20250715", "20250717", "qfq", datasource="mootdx")
)
