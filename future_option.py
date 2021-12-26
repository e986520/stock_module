import yfinance as yf
from stock_module.mongo import *


def to_excel():
    end = pd.to_datetime(
        [
            "2019/01/16",
            "2019/02/20",
            "2019/03/20",
            "2019/04/17",
            "2019/05/15",
            "2019/06/19",
            "2019/07/17",
            "2019/08/21",
            "2019/09/18",
            "2019/10/16",
            "2019/11/20",
            "2019/12/18",
            "2020/01/15",
            "2020/02/19",
            "2020/03/18",
            "2020/04/15",
            "2020/05/20",
            "2020/06/17",
            "2020/07/15",
            "2020/08/19",
            "2020/09/16",
            "2020/10/21",
            "2020/11/18",
            "2020/12/16",
            "2021/01/20",
            "2021/02/17",
            "2021/03/17",
            "2021/04/21",
            "2021/05/19",
            "2021/06/16",
            "2021/07/21",
            "2021/08/18",
            "2021/09/15",
            "2021/10/20",
            "2021/11/17",
            "2021/12/15",
            "2022/01/19",
            "2022/02/16",
            "2022/03/16",
            "2022/04/20",
            "2022/05/18",
            "2022/06/15",
            "2022/07/20",
            "2022/08/17",
            "2022/09/21",
            "2022/10/19",
            "2022/11/16",
            "2022/12/21",
        ]
    )

    df = get_data("future_option")
    df = df.set_index("date")
    df = df.sort_index()
    df.insert(0, "結算日", 0)
    df.insert(5, "外資期貨留倉與結算差", df["外資期貨留倉"])
    df.insert(8, "自營期貨留倉與結算差", df["自營期貨留倉"])
    df.insert(12, "外資BC金額與結算差", df["外資BC金額"])
    df.insert(13, "外資SC金額", df["外資BC金額"] - df["外資CALL金額"])
    df.insert(14, "外資SC金額與結算差", df["外資SC金額"])
    df.insert(18, "外資BP金額與結算差", df["外資BP金額"])
    df.insert(19, "外資SP金額", df["外資BP金額"] - df["外資PUT金額"])
    df.insert(20, "外資SP金額與結算差", df["外資SP金額"])
    df.insert(23, "外資SC/SP比", round(df["外資SC金額"] / df["外資SP金額"], 3))
    df.insert(27, "自營BC金額與結算差", df["自營BC金額"])
    df.insert(28, "自營SC金額", df["自營BC金額"] - df["自營CALL金額"])
    df.insert(29, "自營SC金額與結算差", df["自營SC金額"])
    df.insert(33, "自營BP金額與結算差", df["自營BP金額"])
    df.insert(34, "自營SP金額", df["自營BP金額"] - df["自營PUT金額"])
    df.insert(35, "自營SP金額與結算差", df["自營SP金額"])
    df.insert(38, "自營SC/SP比", round(df["自營SC金額"] / df["自營SP金額"], 3))

    end_in_df = []
    for i in range(len(end) - 1):
        if end[i] in df.index:
            end_in_df.append(end[i])
            df.loc[end[i], "結算日"] = 1
            if end[i] + pd.to_timedelta("1 day") in df.index:
                df.loc[end[i] : end[i + 1], "外資期貨留倉與結算差"] = df["外資期貨留倉"] - df.loc[end[i], "外資期貨留倉"]
                df.loc[end[i] : end[i + 1], "自營期貨留倉與結算差"] = df["自營期貨留倉"] - df.loc[end[i], "自營期貨留倉"]
                df.loc[end[i] : end[i + 1], "外資BC金額與結算差"] = df["外資BC金額"] - df.loc[end[i], "外資BC金額"]
                df.loc[end[i] : end[i + 1], "外資SC金額與結算差"] = df["外資SC金額"] - df.loc[end[i], "外資SC金額"]
                df.loc[end[i] : end[i + 1], "外資BP金額與結算差"] = df["外資BP金額"] - df.loc[end[i], "外資BP金額"]
                df.loc[end[i] : end[i + 1], "外資SP金額與結算差"] = df["外資SP金額"] - df.loc[end[i], "外資SP金額"]
                df.loc[end[i] : end[i + 1], "自營BC金額與結算差"] = df["自營BC金額"] - df.loc[end[i], "自營BC金額"]
                df.loc[end[i] : end[i + 1], "自營SC金額與結算差"] = df["自營SC金額"] - df.loc[end[i], "自營SC金額"]
                df.loc[end[i] : end[i + 1], "自營BP金額與結算差"] = df["自營BP金額"] - df.loc[end[i], "自營BP金額"]
                df.loc[end[i] : end[i + 1], "自營SP金額與結算差"] = df["自營SP金額"] - df.loc[end[i], "自營SP金額"]

    df = df.drop(["外資BC金額", "外資SC金額", "外資BP金額", "外資SP金額", "自營BC金額", "自營SC金額", "自營BP金額", "自營SP金額"], axis=1)
    df = df.fillna(0)

    market = yf.download("^TWII", start=df.index[0], end=(df.index[-1] + pd.to_timedelta("1 day")))
    market_close = market.Close
    df.insert(0, "加權指數", round(market_close, 2))
    df.insert(1, "漲跌", df["加權指數"] - df["加權指數"].shift(1))
    df.insert(2, "漲跌%", df["加權指數"].pct_change())
    df = df[df.index >= end_in_df[0]]

    df.tail(2).to_excel("籌碼更新.xlsx")
    return

def option_price(date):
    market = 0
    time = pd.to_datetime(date)
    dfs = pd.read_html(
        f"https://www.taifex.com.tw/cht/3/optDailyMarketReport?queryType=2&marketCode={market}&commodity_id=TXO&queryDate={time.year}%2F{time.month}%2F{time.day}&MarketCode={market}&commodity_idt=TXO"
    )
    try:
        df = dfs[4]
        call = df[df["買賣權"] == "Call"]
        put = df[df["買賣權"] == "Put"]
    except:
        print("fail!maybe it's a holiday")
        return
    
    if "W" in call.loc[0, "到期月份(週別)"]:
        call = call[call.loc[:, "到期月份(週別)"].str.contains("W")]
    call = call.set_index(["到期月份(週別)"])
    call = call.apply(lambda x: pd.to_numeric(x, errors="coerce"))
    call = call[["履約價", "最後成交價", "*未沖銷契約量"]].dropna()
    call["換算台指"] = call["履約價"] + call["最後成交價"] - call["最後成交價"].shift(-2)
    call = call.nlargest(4, "*未沖銷契約量")
    call = call.sort_values("履約價")

    if "W" in put.loc[1, "到期月份(週別)"]:
        put = put[put.loc[:, "到期月份(週別)"].str.contains("W")]
    put = put.set_index(["到期月份(週別)"])
    put = put.apply(lambda x: pd.to_numeric(x, errors="coerce"))
    put = put[["履約價", "最後成交價", "*未沖銷契約量"]].dropna()
    put["換算台指"] = put["履約價"] - put["最後成交價"] + put["最後成交價"].shift(2)
    put = put.nlargest(4, "*未沖銷契約量")
    put = put.sort_values("履約價")

    df = pd.concat([put, call])
    df = df.drop("最後成交價", axis=1)
    df.to_excel("大盤支撐壓力更新.xlsx")
    return
