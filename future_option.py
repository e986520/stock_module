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
