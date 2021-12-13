import json
import requests
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from stock_module.mongo import *

ua = UserAgent()
headers = {"UserAgent": ua.random}
stocks_list = pd.DataFrame(db["stocks_list"].find()).set_index("stock_id").drop(columns="_id")
stocks_id = stocks_list.index


def crawl_stocks_list():
    # 上市櫃股票代號表
    r1 = requests.get("https://isin.twse.com.tw/isin/C_public.jsp?strMode=2", headers=headers)
    r2 = requests.get("https://isin.twse.com.tw/isin/C_public.jsp?strMode=4", headers=headers)
    soup1 = BeautifulSoup(r1.text, "lxml")
    soup2 = BeautifulSoup(r2.text, "lxml")
    trs1 = soup1.find_all("tr")
    trs2 = soup2.find_all("tr")

    stocks_list = {}
    for tr in trs1:
        if "ESVUFR" in tr.text:
            tds = tr.find_all("td")
            stock_id = tds[0].text.split("　")[0].strip()
            name = tds[0].text.split("　")[1]
            market = tds[3].text
            industry = tds[4].text
            stocks_list[stock_id] = [name, market, industry]
        if "ESVTFR" in tr.text:
            tds = tr.find_all("td")
            stock_id = tds[0].text.split("　")[0].strip()
            name = tds[0].text.split("　")[1]
            market = tds[3].text
            industry = tds[4].text
            stocks_list[stock_id] = [name, market, industry]
        if "EDSDDR" in tr.text:
            tds = tr.find_all("td")
            stock_id = tds[0].text.split("　")[0].strip()
            name = tds[0].text.split("　")[1]
            market = tds[3].text
            industry = "存託憑證"
            stocks_list[stock_id] = [name, market, industry]

    for tr in trs2:
        if "ESVUFR" in tr.text:
            tds = tr.find_all("td")
            stock_id = tds[0].text.split("　")[0].strip()
            name = tds[0].text.split("　")[1]
            market = tds[3].text
            industry = tds[4].text
            stocks_list[stock_id] = [name, market, industry]

    df = pd.DataFrame.from_dict(stocks_list, orient="index", columns=["名稱", "市場", "產業"])
    df.index.name = "stock_id"

    # 增加細產業欄位
    industrys = []
    for id in df.index:
        url = f"https://fubon-ebrokerdj.fbs.com.tw/Z/ZC/ZCS/ZCS_{id}.djhtm"
        r = requests.get(url=url, headers=headers)
        soup = BeautifulSoup(r.text, "lxml")
        tds = soup.select(".t3t1")
        for td in tds:
            industry = td.text.strip().split("\r\n")
        industry = "".join(industry)
        industrys.append(industry)
    df["細產業"] = industrys
    df = df.reset_index()

    # 先清空資料庫裡的舊資料
    try:
        db["stocks_list"].remove()
    except:
        print("There was not old datas to be removed.")

    # 再存新的近去
    json_data = json.loads(df.to_json(orient="records"))
    save_to_mongo(json_data, "stocks_list")

    return


def crawl_price(date):
    # 上市
    # 將時間物件變成字串：'20180102'
    datestr = date.strftime("%Y%m%d")
    try:
        r = requests.get(
            f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date={datestr}&type=ALLBUT0999",
            headers=headers,
        )
    except Exception as e:
        print("**WARRN: cannot get stock price at", datestr)
        print(e)
        return None
    content = r.text.strip().replace("=", "")
    lines = content.split("\n")
    lines = list(filter(lambda l: len(l.split('",')) > 10, lines))
    content = "\n".join(lines)
    if content == "":
        return None
    try:
        df1 = pd.read_csv(StringIO(content), header=0, index_col=0)
    except Exception as e:
        print(e)
        return None
    df1 = df1.iloc[:, :8].drop(columns="成交筆數")
    for i in df1.index:
        if i not in stocks_id:
            df1 = df1.drop(i)
    df1 = df1.reset_index()
    df1 = df1.rename(columns={"證券代號": "stock_id"})

    # 上櫃
    datestr1 = str(int(date.strftime("%Y")) - 1911)
    datestr2 = date.strftime("%m")
    datestr3 = date.strftime("%d")
    try:
        r = requests.get(
            f"https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&o=csv&d={datestr1}/{datestr2}/{datestr3}&s=0,asc,0",
            headers=headers,
        )
    except Exception as e:
        print("**WARRN: cannot get stock price at", datestr)
        print(e)
        return None
    content = r.text.strip()
    if content == "":
        return None
    try:
        df2 = pd.read_csv(StringIO(content), skiprows=2, skipfooter=8, header=0, index_col=0, engine="python")
    except Exception as e:
        print(e)
        return None
    df2 = df2.iloc[:, :9].drop(columns=["均價 ", "漲跌"])
    for i in df2.index:
        if len(i) != 4:
            df2 = df2.drop(i)
    df2 = df2.reset_index()
    df2 = df2.rename(
        columns={
            "代號": "stock_id",
            "名稱": "證券名稱",
            "收盤 ": "收盤價",
            "開盤 ": "開盤價",
            "最高 ": "最高價",
            "最低": "最低價",
            "成交股數  ": "成交股數",
            "成交金額(元)": "成交金額",
        }
    )
    df = pd.concat([df1, df2]).drop(columns="證券名稱")
    df = df.set_index("stock_id")
    df = df.apply(lambda s: s.str.replace(",", ""))
    df = df.apply(lambda s: pd.to_numeric(s, errors="coerce"))
    df = df.reset_index()
    df.insert(1, "date", date)

    json_data = json.loads(df.to_json(orient="records"))
    return json_data


def crawl_legal_person(date):

    # 上市
    datestr = date.strftime("%Y%m%d")

    try:
        r = requests.get(
            f"https://www.twse.com.tw/fund/T86?response=csv&date={datestr}&selectType=ALLBUT0999", headers=headers
        )
    except:
        return None

    try:
        df = pd.read_csv(StringIO(r.text), header=1).dropna(how="all", axis=1).dropna(how="any")
    except:
        return None

    df = df.astype(str).apply(lambda s: s.str.replace(",", ""))
    df["stock_id"] = df["證券代號"].str.replace("=", "").str.replace('"', "")
    df = df.drop(["證券代號"], axis=1)
    df.insert(1, "date", date)
    df = df.set_index(["stock_id", "date"])
    df = df["投信買賣超股數"]
    df1 = pd.DataFrame(df)

    # 上櫃
    datestr1 = str(int(date.strftime("%Y")) - 1911)
    datestr2 = date.strftime("%m")
    datestr3 = date.strftime("%d")

    try:
        r = requests.get(
            f"https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=csv&se=EW&t=D&d={datestr1}/{datestr2}/{datestr3}&s=0,asc",
            headers=headers,
        )
    except:
        return None

    try:
        df = pd.read_csv(StringIO(r.text), header=1).dropna(how="all", axis=1).dropna(how="any")
    except:
        return None

    df = df.astype(str).apply(lambda s: s.str.replace(",", ""))
    df["stock_id"] = df["代號"].str.replace("=", "").str.replace('"', "")
    df = df.drop(["代號"], axis=1)
    df = df.rename(columns={"投信-買賣超股數": "投信買賣超股數"})
    df.insert(1, "date", date)
    df = df.set_index(["stock_id", "date"])
    df = df["投信買賣超股數"]
    df2 = pd.DataFrame(df)

    df = pd.concat([df1, df2])

    df = df.apply(lambda s: pd.to_numeric(s, errors="coerce")).dropna(how="all", axis=1)
    df["投信買賣超股數"] = round(df["投信買賣超股數"] / 1000, 0)
    df.columns = ["投信買賣超張數"]
    df = df.reset_index()
    df = df.set_index("stock_id")
    for i in df.index:
        if i not in stocks_id:
            df = df.drop(i)
    df = df.reset_index()

    json_data = json.loads(df.to_json(orient="records"))

    return json_data

def crawl_future_option(date):
    time = pd.to_datetime(date)

    # 大台總留倉量
    try:
        dfs = pd.read_html(
            f"https://www.taifex.com.tw/cht/3/futDailyMarketReport?queryType=2&commodity_id=TX&queryDate={time.year}%2F{time.month}%2F{time.day}"
        )
        df = dfs[4][["到期月份(週別)", "*未沖銷契約量"]]
        df_1 = pd.DataFrame(df["*未沖銷契約量"]).head(2).T
        df_1.index = [time]
        df_1.columns = ["近月大台留倉", "遠月大台留倉"]
    except:
        return None

    # 當日法人大小台期貨籌碼
    try:
        dfs = pd.read_html(
            f"https://www.taifex.com.tw/cht/3/futContractsDate?queryType=1&commodityId=TXF&queryDate={time.year}%2F{time.month}%2F{time.day}"
        )
        df = dfs[3].dropna().drop([0, 1, 3, 4, 5, 6, 8, 9, 10, 11, 12, 14], axis=1).drop([2, 4, 6, 7, 8, 9])
        df.columns = ["", "當日交易", "總留倉"]
        df = df.set_index("")
        df.index = ["自營期貨", "外資期貨"]
        df = pd.DataFrame(df.unstack()).T
        df.index = [time]
        df1 = df.astype(int)

        dfs = pd.read_html(
            f"https://www.taifex.com.tw/cht/3/futContractsDate?queryType=1&commodityId=MXF&queryDate={time.year}%2F{time.month}%2F{time.day}"
        )
        df = dfs[3].dropna().drop([0, 1, 3, 4, 5, 6, 8, 9, 10, 11, 12, 14], axis=1).drop([2, 4, 6, 7, 8, 9])
        df.columns = ["", "當日交易", "總留倉"]
        df = df.set_index("")
        df.index = ["自營期貨", "外資期貨"]
        df = pd.DataFrame(df.unstack()).T
        df.index = [time]
        df2 = df.astype(int)

        df = (df1 + (df2 / 4)).astype(int)
        df = df.droplevel(0, axis=1)
        df.columns = ["自營期貨增減", "外資期貨增減", "自營期貨留倉", "外資期貨留倉"]
        df_2 = df.reindex(columns=["外資期貨留倉", "外資期貨增減", "自營期貨留倉", "自營期貨增減"])
    except:
        return None

    # 當日法人選擇權籌碼
    try:
        dfs = pd.read_html(
            f"https://www.taifex.com.tw/cht/3/callsAndPutsDate?queryType=1&commodityId=TXO&queryDate={time.year}%2F{time.month}%2F{time.day}"
        )
        df = dfs[3].dropna().drop([0, 1, 2, 4, 5, 6, 7, 8, 9, 12, 13], axis=1).drop([2, 4, 7])
        df.columns = ["", "BUY留倉口數", "BUY留倉金額", "總留倉口數", "總留倉金額"]
        df = df.set_index("")
        df.index = ["自營CALL", "外資CALL", "自營PUT", "外資PUT"]
        df = pd.DataFrame(df.unstack()).T
        df.index = [time]
        df = df.droplevel(0, axis=1)
        df.columns = [
            "自營BC口數",
            "外資BC口數",
            "自營BP口數",
            "外資BP口數",
            "自營BC金額",
            "外資BC金額",
            "自營BP金額",
            "外資BP金額",
            "自營CALL口數",
            "外資CALL口數",
            "自營PUT口數",
            "外資PUT口數",
            "自營CALL金額",
            "外資CALL金額",
            "自營PUT金額",
            "外資PUT金額",
        ]
        df = df.drop(["自營BC口數", "外資BC口數", "自營BP口數", "外資BP口數"], axis=1)
        df = df.reindex(
            columns=[
                "外資CALL口數",
                "外資CALL金額",
                "外資BC金額",
                "外資PUT口數",
                "外資PUT金額",
                "外資BP金額",
                "自營CALL口數",
                "自營CALL金額",
                "自營BC金額",
                "自營PUT口數",
                "自營PUT金額",
                "自營BP金額",
            ]
        )
        df = df.astype(int)
        df.insert(6, "外資CALL/PUT比", round(df["外資CALL金額"] / df["外資PUT金額"], 3))
        df.insert(7, "外資BC/BP比", round(df["外資BC金額"] / df["外資BP金額"], 3))
        df.insert(14, "自營CALL/PUT比", round(df["自營CALL金額"] / df["自營PUT金額"], 3))
        df.insert(15, "自營BC/BP比", round(df["自營BC金額"] / df["自營BP金額"], 3))
        df_3 = df
    except:
        return None

    # 散戶籌碼
    try:
        dfs = pd.read_html(
            f"https://www.taifex.com.tw/cht/3/futDailyMarketReport?queryType=2&commodity_id=MTX&queryDate={time.year}%2F{time.month}%2F{time.day}"
        )
        df = dfs[4][["到期月份(週別)", "*未沖銷契約量"]]
        df1 = pd.DataFrame(df["*未沖銷契約量"]).tail(1)
        df1.index = [time]
        df1.columns = ["小台未沖銷契約量"]

        dfs = pd.read_html(
            f"https://www.taifex.com.tw/cht/3/futContractsDate?queryType=1&commodityId=MXF&queryDate={time.year}%2F{time.month}%2F{time.day}"
        )
        df = dfs[3].dropna()
        df2 = df[[9, 11]].tail(1)
        df2.index = [time]
        df2.columns = ["法人看多", "法人看空"]
        df = df1.join(df2)
        df = df.astype(int)
        df["散戶看多"] = df["小台未沖銷契約量"] - df["法人看多"]
        df["散戶看空"] = df["小台未沖銷契約量"] - df["法人看空"]
        df["散戶未平倉"] = df["散戶看多"] - df["散戶看空"]
        df["散戶多空比"] = round(df["散戶未平倉"] / df["小台未沖銷契約量"] * 100, 2)
        df_4 = df.drop(["小台未沖銷契約量", "法人看多", "法人看空"], axis=1)
    except:
        return None

    # 大額交易人期貨籌碼
    try:
        dfs = pd.read_html(
            f"https://www.taifex.com.tw/cht/3/largeTraderFutQry?contractId=TX&queryDate={time.year}%2F{time.month}%2F{time.day}"
        )
        df = dfs[3].droplevel([0, 1], axis=1)
        df = df.iloc[:, [2, 4, 6, 8]].tail(2)
        df.columns = ["五大多", "十大多", "五大空", "十大空"]
        df.index = ["近月", "所有契約"]
        df = df.apply(lambda x: x.str.replace(",", "").str.strip(")").str.split("("))
        df["五大多"] = df["五大多"].apply(lambda x: x[0])
        df["十大多"] = df["十大多"].apply(lambda x: x[0])
        df["五大空"] = df["五大空"].apply(lambda x: x[0])
        df["十大空"] = df["十大空"].apply(lambda x: x[0])
        df = df.astype(int)
        df["五大留倉"] = df["五大多"] - df["五大空"]
        df["十大留倉"] = df["十大多"] - df["十大空"]
        df = df[["五大留倉", "十大留倉"]].T
        df["遠月"] = df["所有契約"] - df["近月"]
        df = df.drop("所有契約", axis=1)
        df = pd.DataFrame(df.T.unstack()).T
        df.index = [time]
        df_5 = df.droplevel(0, axis=1)
        df_5.columns = ["近月五大留倉", "遠月五大留倉", "近月十大留倉", "遠月十大留倉"]
    except:
        return None

    # 大額交易人選擇權
    try:
        dfs = pd.read_html(
            f"https://www.taifex.com.tw/cht/3/largeTraderOptQry?contractId=TXO&queryDate={time.year}%2F{time.month}%2F{time.day}"
        )
        df = dfs[3].droplevel([0, 1], axis=1)
        df = df.iloc[[0, 1, 3, 4], [2, 4, 6, 8]]
        df.columns = ["五大BUY", "十大BUY", "五大SELL", "十大SELL"]
        df.index = ["周選CALL", "月選CALL", "周選PUT", "月選PUT"]
        df = df.apply(lambda x: x.str.replace(",", "").str.strip(")").str.split("("))
        df["五大BUY"] = df["五大BUY"].apply(lambda x: x[0])
        df["十大BUY"] = df["十大BUY"].apply(lambda x: x[0])
        df["五大SELL"] = df["五大SELL"].apply(lambda x: x[0])
        df["十大SELL"] = df["十大SELL"].apply(lambda x: x[0])
        df = df.apply(lambda s: pd.to_numeric(s, errors="coerce"))
        df = pd.DataFrame(df.T.unstack()).T
        df.index = [time]
        df = df.droplevel(0, axis=1)
        df.columns = [
            "周選五大BC",
            "周選十大BC",
            "周選五大SC",
            "周選十大SC",
            "月選五大BC",
            "月選十大BC",
            "月選五大SC",
            "月選十大SC",
            "周選五大BP",
            "周選十大BP",
            "周選五大SP",
            "周選十大SP",
            "月選五大BP",
            "月選十大BP",
            "月選五大SP",
            "月選十大SP",
        ]
        df["周選五大CALL"] = df["周選五大BC"] - df["周選五大SC"]
        df["周選五大PUT"] = df["周選五大BP"] - df["周選五大SP"]
        df["周選十大CALL"] = df["周選十大BC"] - df["周選十大SC"]
        df["周選十大PUT"] = df["周選十大BP"] - df["周選十大SP"]
        df["月選五大CALL"] = df["月選五大BC"] - df["月選五大SC"]
        df["月選五大PUT"] = df["月選五大BP"] - df["月選五大SP"]
        df["月選十大CALL"] = df["月選十大BC"] - df["月選十大SC"]
        df["月選十大PUT"] = df["月選十大BP"] - df["月選十大SP"]
        df_6 = df.iloc[:, 16:]
    except:
        return None

    # 全部合併
    try:
        df = df_1.join([df_2, df_3, df_4, df_5, df_6])
        df.index.name = "date"
        df = df.reset_index()
        json_data = json.loads(df.to_json(orient="records"))
    except:
        return None

    return json_data

def crawl_ADL(date):
    # 上市
    datestr = date.strftime("%Y%m%d")
    try:
        r1 = requests.get(
            f"https://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date={datestr}&type=MS", headers=headers
        )
    except:
        return None

    try:
        df1 = pd.read_csv(StringIO(r1.text), header=1)
    except:
        return None

    rise1, rise_limit1 = df1.iloc[19, 2].strip(")").split("(")
    fall1, fall_limit1 = df1.iloc[20, 2].strip(")").split("(")
    flat1 = df1.iloc[21, 2]

    # 上櫃
    datestr1 = str(int(date.strftime("%Y")) - 1911)
    datestr2 = date.strftime("%m")
    datestr3 = date.strftime("%d")

    try:
        r2 = requests.get(
            f"https://www.tpex.org.tw/web/stock/aftertrading/market_highlight/highlight_result.php?l=zh-tw&o=csv&d={datestr1}/{datestr2}/{datestr3}",
            headers=headers,
        )
    except:
        return None

    try:
        text = r2.text.replace(" ", "").replace("\r", "").split("\n")
        rise2 = text[8].split(",")[0].split(":")[1].strip('"')
        rise_limit2 = text[8].split(",")[1].split(":")[1].strip('"')
        fall2 = text[9].split(",")[0].split(":")[1].strip('"')
        fall_limit2 = text[9].split(",")[1].split(":")[1].strip('"')
        flat2 = text[10].split(",")[0].split(":")[1].strip('"')
    except (IndexError):
        return None

    arr = [
        int(rise1) + int(rise2),
        int(fall1) + int(fall2),
        int(rise_limit1) + int(rise_limit2),
        int(fall_limit1) + int(fall_limit2),
        int(flat1) + int(flat2),
    ]
    df = pd.DataFrame(arr, index=["Advance", "Decline", "Rise_limit", "Fall_limit", "Flat"]).T
    df.insert(0, "date", date)

    json_data = json.loads(df.to_json(orient="records"))

    return json_data


def crawl_margin_trading(date):
    # 上市
    datestr = date.strftime("%Y%m%d")
    try:
        r = requests.get(
            f"https://www.twse.com.tw/exchangeReport/MI_MARGN?response=csv&date={datestr}&selectType=ALL",
            headers=headers,
        )
    except:
        return None
    try:
        df = pd.read_csv(StringIO(r.text.replace("=", "")), header=1, skiprows=6, skipfooter=7, engine="python")
        df = df[["股票代號", "今日餘額", "限額", "今日餘額.1", "限額.1"]]
        df.columns = ["stock_id", "融資餘額", "融資限額", "融券餘額", "融券限額"]
        df1 = df.astype(str).apply(lambda s: s.str.replace(",", ""))

    except:
        return None
    # 上櫃
    datestr1 = str(int(date.strftime("%Y")) - 1911)
    datestr2 = date.strftime("%m")
    datestr3 = date.strftime("%d")

    try:
        r = requests.get(
            f"https://www.tpex.org.tw/web/stock/margin_trading/margin_balance/margin_bal_result.php?l=zh-tw&o=csv&charset=UTF-8&d={datestr1}/{datestr2}/{datestr3}&s=0,asc",
            headers=headers,
        )
    except:
        return None

    try:
        df = pd.read_csv(StringIO(r.text), header=1, skiprows=1, skipfooter=21, engine="python")
        df = df[["代號", "  資餘額", " 資限額", "  券餘額", " 券限額"]]
        df.columns = ["stock_id", "融資餘額", "融資限額", "融券餘額", "融券限額"]
        df2 = df.astype(str).apply(lambda s: s.str.replace(",", ""))
    except:
        return None

    df = pd.concat([df1, df2])
    df = df.set_index("stock_id")
    df = df.apply(lambda s: pd.to_numeric(s, errors="coerce"))
    for i in df.index:
        if i not in stocks_id:
            df = df.drop(i)
    df["融資使用率"] = round(df["融資餘額"] / df["融資限額"] * 100, 2)
    df["融券使用率"] = round(df["融券餘額"] / df["融券限額"] * 100, 2)
    df = df.reset_index()
    df.insert(1, "date", date)

    json_data = json.loads(df.to_json(orient="records"))
    return json_data

def crawl_rich_person(date):
    date = str(date)[:10].replace("-", "")
    # 把每支股票合成一個dataframe
    df = pd.DataFrame()
    fail = 0
    for i, id in enumerate(stocks_list.index):
        if fail >= 5:
            return None
        print(f"crawling stock {id} ({i+1}/{len(stocks_list.index)})")
        res = requests.post(
            f"https://www.tdcc.com.tw/smWeb/QryStockAjax.do?scaDates={date}&scaDate={date}&SqlMethod=StockNo&StockNo={id}&StockName=&REQ_OPR=SELECT&clkStockNo={id}&clkStockName=",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.55 Safari/537.36 Edg/96.0.1054.34"
            },
        )
        try:
            data = pd.read_html(res.text)[0]
            rich_person = data.dropna().iloc[-1, 4]
            poor_person = data.dropna().iloc[1:10, 4].astype(float).sum()
            all_people = float(data.drop(0, axis=1).dropna().iloc[-1, 1])
            all_stocks = float(data.drop(0, axis=1).dropna().iloc[-1, 2]) / 1000
            mean = round(all_stocks / all_people, 2)
        except:
            # 停頓10秒後再試一次
            time.sleep(10)
            try:
                res = requests.post(
                    f"https://www.tdcc.com.tw/smWeb/QryStockAjax.do?scaDates={date}&scaDate={date}&SqlMethod=StockNo&StockNo={id}&StockName=&REQ_OPR=SELECT&clkStockNo={id}&clkStockName=",
                    headers={
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.55 Safari/537.36 Edg/96.0.1054.34"
                    },
                )
                data = pd.read_html(res.text)[0]
                rich_person = data.dropna().iloc[-1, 4]
                poor_person = data.dropna().iloc[1:10, 4].astype(float).sum()
                all_people = float(data.drop(0, axis=1).dropna().iloc[-1, 1])
                all_stocks = float(data.drop(0, axis=1).dropna().iloc[-1, 2]) / 1000
                mean = round(all_stocks / all_people, 2)
            # 還是失敗的話就停止
            except:
                print("There is no data!")
                fail += 1

        arr = [id, rich_person, poor_person, mean]
        df2 = pd.DataFrame(arr, index=["stock_id", "千張大戶", "散戶", "均張"]).T
        df = pd.concat([df, df2])

    df.insert(0, "date", pd.to_datetime(date))
    df["千張大戶"] = pd.to_numeric(df["千張大戶"], errors="coerce")
    json_data = json.loads(df.to_json(orient="records"))

    return json_data

def crawl_monthly_revenue(date):
    # 上市
    url = f"https://mops.twse.com.tw/nas/t21/sii/t21sc03_{str(date.year - 1911)}_{str(date.month)}.html"
    print(url)
    r = requests.get(url, headers=headers)
    r.encoding = "big5"
    dfs = pd.read_html(StringIO(r.text))
    df = pd.concat([df for df in dfs if df.shape[1] <= 11 and df.shape[1] > 5])
    df.columns = df.columns.get_level_values(1)
    df = df[["公司代號", "當月營收", "上月比較增減(%)", "去年同月增減(%)", "備註"]]
    df["當月營收"] = df["當月營收"] / 1000
    df = df.rename(columns={"公司代號": "stock_id", "上月比較增減(%)": "MOM%", "去年同月增減(%)": "YOY%"})
    df = df.set_index("stock_id")
    df1 = df.drop(["合計", "總計"])

    # 上櫃
    url = f"https://mops.twse.com.tw/nas/t21/otc/t21sc03_{str(date.year - 1911)}_{str(date.month)}.html"
    print(url)
    r = requests.get(url, headers=headers)
    r.encoding = "big5"
    dfs = pd.read_html(StringIO(r.text))
    df = pd.concat([df for df in dfs if df.shape[1] <= 11 and df.shape[1] > 5])
    df.columns = df.columns.get_level_values(1)
    df = df[["公司代號", "當月營收", "上月比較增減(%)", "去年同月增減(%)", "備註"]]
    df["當月營收"] = df["當月營收"] / 1000
    df = df.rename(columns={"公司代號": "stock_id", "上月比較增減(%)": "MOM%", "去年同月增減(%)": "YOY%"})
    df = df.set_index("stock_id")
    df2 = df.drop(["合計", "總計"])

    df = pd.concat([df1, df2])
    df["MOM%"] = pd.to_numeric(df["MOM%"], "coerce")
    df["YOY%"] = pd.to_numeric(df["YOY%"], "coerce")
    df = df.reset_index()
    df.insert(1, "date", date)

    json_data = json.loads(df.to_json(orient="records"))
    return json_data


def crawl_finance(stocks=False):
    if stocks:
        new_stocks_list = stocks
    else:
        new_stocks_list = stocks_list.index
    # 開始加入新資料
    for i, id in enumerate(stocks_list.index):
        print(f"crawling stock {id} ({i+1}/{len(stocks_list.index)})")
        # 損益表
        url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcq/zcq0.djhtm?b=Q&a={id}"
        r = requests.get(url=url, headers=headers)
        soup = BeautifulSoup(r.text, "lxml")
        table = soup.select("#oMainTable")
        df = pd.read_html(str(table))[0].T
        df.columns = df.loc[0]
        df = df.drop(0)
        df1 = df.iloc[:, [0, 3, 9, -9]]

        # 財務比率表
        url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zcr/zcr_{id}.djhtm"
        r = requests.get(url=url, headers=headers)
        soup = BeautifulSoup(r.text, "lxml")
        table = soup.select("#oMainTable")
        df = pd.read_html(str(table))[0].T.drop(columns=0)
        df.columns = df.loc[0]
        df = df.drop(0)
        df2 = df.iloc[:, [0, 7, 9, 10, 11, 17, 19]]

        try:
            # 合併
            df = pd.merge(df1, df2)
            df.insert(0, "stock_id", str(id))
            df = df.rename(columns={"期別": "date", "每股淨值(F)(TSE公告數)": "每股淨值"})
            df["date"] = pd.to_datetime(df["date"].str[:4] + "Q" + df["date"].str[-2]) + pd.offsets.QuarterEnd()
            df = df.set_index(["stock_id", "date"])
            df = df.astype(float)
            df["稅前淨利率"] = df["稅前淨利率"] * 0.01
            df["稅後淨利率"] = df["稅後淨利率"] * 0.01
            df["稅率"] = round(df["稅後淨利率"] / df["稅前淨利率"], 4)
            df["稅後純益"] = round(df["營業收入淨額"] * df["稅後淨利率"], 2)
            df["營業毛利率"] = df["營業毛利率"] * 0.01
            df["營業利益率"] = df["營業利益率"] * 0.01
            df = df.drop(columns=["稅前淨利率", "稅後淨利率", "營業收入淨額"])
            df = df.reindex(columns=["營業費用", "營業毛利率", "營業利益率", "稅率", "稅後純益", "每股盈餘", "每股淨值", "每股現金流量"])
            df = df.reset_index().dropna()

        except Exception as e:
            print(e)
            continue
        if datetime.today().date() - pd.offsets.QuarterEnd() == df.date[0]:
            json_data = json.loads(df.head(1).to_json(orient="records"))
            save_to_mongo(json_data, "finance")
        else:
            print("No new data!")
            continue

    delete_data(1111, db["finance"])

    return
