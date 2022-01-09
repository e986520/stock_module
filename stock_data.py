import pandas as pd
from stock_module.mongo import *

stocks_list = pd.DataFrame(db["stocks_list"].find()).drop(columns="_id").set_index("stock_id")


class Select_Data:
    def __init__(self):
        # 基本面資料
        self.月營收 = get_data("monthly_revenue", "當月營收", 1111)
        self.當月營收 = self.月營收.iloc[-1]
        self.營收備註 = get_data("monthly_revenue", "備註", 60)
        self.當月營收備註 = self.營收備註.iloc[-1]
        self.YOY = get_data("monthly_revenue", "YOY%", 60)
        self.近一月YOY = self.YOY.iloc[-1]
        self.MOM = get_data("monthly_revenue", "MOM%", 60)
        self.近一月MOM = self.MOM.iloc[-1]
        self.近三年最高營收 = self.月營收.iloc[-36:].max()
        self.毛利率 = get_data("finance", "營業毛利率", 180)
        self.近一季毛利率 = self.毛利率.iloc[-1]
        self.營業費用 = get_data("finance", "營業費用", 180)
        self.近一季營業費用 = self.營業費用.iloc[-1]
        self.業外收入及支出 = 0
        self.稅後淨利 = get_data("finance", "稅後純益", 180)
        self.近一季稅後淨利 = self.稅後淨利.iloc[-1]
        self.每股盈餘 = get_data("finance", "每股盈餘", 180)
        self.近一季每股盈餘 = self.每股盈餘.iloc[-1]
        self.營業利益率 = get_data("finance", "營業利益率", 180)
        self.近一季營業利益率 = self.營業利益率.iloc[-1]
        self.每股淨值 = get_data("finance", "每股淨值", 180)
        self.近一季每股淨值 = self.每股淨值.iloc[-1]
        self.預估稅前純益 = self.當月營收 * 3 * self.近一季毛利率 - self.近一季營業費用 + self.業外收入及支出
        self.近兩年稅率 = get_data("finance", "稅率", 888).iloc[-8:]
        self.稅率 = self.近兩年稅率[(self.近兩年稅率 > 0.6) & (self.近兩年稅率 < 1)].mean()
        self.預估EPS = self.預估稅前純益 * self.稅率 * self.近一季每股盈餘 / self.近一季稅後淨利 * 4
        self.預估股價地板 = self.預估EPS * 10
        self.預估股價夾板 = self.預估EPS * 15
        self.預估股價天花板 = self.預估EPS * 20
        self.淨值比預估股價 = (self.預估EPS + self.近一季每股淨值) * 1.2

        # 技術面資料
        self.收盤價 = get_data("price", "收盤價", 99)
        self.目前股價 = self.收盤價.iloc[-1]
        self.昨日股價 = self.收盤價.iloc[-2]
        self.近五日最高價 = self.收盤價.iloc[-5:].max()
        self.漲幅 = self.收盤價.pct_change()
        self.月線扣抵值 = self.收盤價.iloc[-21]
        self.季線扣抵值 = self.收盤價.iloc[-61]
        self.成交量 = get_data("price", "成交股數", 99) / 1000
        self.量比 = self.成交量.pct_change() + 1
        self.成交值 = get_data("price", "成交金額", 10)
        self.當日成交值 = self.成交值.iloc[-1]
        self.五日均量 = self.成交量.iloc[-5:].mean()
        self.近一季最高價 = self.收盤價.iloc[-60:].max()
        self.近一季最低價 = self.收盤價.iloc[-60:].min()
        self.SMA5 = self.收盤價.rolling(5).mean()
        self.SMA10 = self.收盤價.rolling(10).mean()
        self.SMA20 = self.收盤價.rolling(20).mean()
        self.SMA60 = self.收盤價.rolling(60).mean()

        # 籌碼面資料
        self.投信買賣超張數 = get_data("legal_person", "投信買賣超張數", 31)
        self.投信買賣超金額 = self.投信買賣超張數 * self.收盤價
        self.當日投信買賣超 = self.投信買賣超張數.iloc[-1]
        self.近三日投信買賣超 = self.投信買賣超張數.iloc[-3:].sum()
        self.近一周投信買賣超 = self.投信買賣超張數.iloc[-5:].sum()
        self.近兩周投信買賣超 = self.投信買賣超張數.iloc[-10:].sum()
        self.近一月投信買賣超 = self.投信買賣超張數.iloc[-20:].sum()
        self.融資使用率 = get_data("margin_trading", "融資使用率", 6)
        self.融券使用率 = get_data("margin_trading", "融券使用率", 6)
        self.當日融資使用率 = self.融資使用率.iloc[-1]
        self.當日融券使用率 = self.融券使用率.iloc[-1]
        self.當日券資比 = self.當日融券使用率 / self.當日融資使用率 * 100
        self.千張大戶比例 = get_data("rich_person", "千張大戶", 99)
        self.千張大戶增減 = self.千張大戶比例.iloc[-1] - self.千張大戶比例.iloc[-2]
        self.近月千張大戶增減 = self.千張大戶比例.iloc[-1] - self.千張大戶比例.iloc[-5]
        self.散戶比例 = get_data("rich_person", "散戶", 99)
        self.散戶增減 = self.散戶比例.iloc[-1] - self.散戶比例.iloc[-2]
        self.近月散戶增減 = self.散戶比例.iloc[-1] - self.散戶比例.iloc[-5]
        self.均張 = get_data("rich_person", "均張", 99)
        self.當周均張 = self.均張.iloc[-1]
        self.近月最高均張 = self.均張.iloc[-5:].max()
        self.均張增減 = self.均張.pct_change() * 100
        self.當周均張增減 = self.均張增減.iloc[-1]
        self.近月均張增減 = ((self.均張.iloc[-1] / self.均張.iloc[-5]) - 1) * 100

    # 選股清單
    def select_list(self, select_stock, PE=0):
        price = [self.目前股價[x] for x in select_stock]
        rise = [round(self.漲幅[x].iloc[-1] * 100, 2) for x in select_stock]
        volume = [round(self.當日成交值[x] / 100000000, 1) for x in select_stock]
        volume_percent = [round(self.量比[x].iloc[-1], 1) for x in select_stock]
        leagal_person_today = list(map(lambda x: self.legal_person_except(x, "today"), select_stock))
        leagal_person_month = list(map(lambda x: self.legal_person_except(x, "month"), select_stock))
        rich_person = [round(self.千張大戶增減[x], 2) for x in select_stock]
        rich_person_month = [round(self.近月千張大戶增減[x], 2) for x in select_stock]
        poor_person = [round(self.散戶增減[x], 2) for x in select_stock]
        poor_person_month = [round(self.近月散戶增減[x], 2) for x in select_stock]
        mean = [round(self.當周均張增減[x], 2) for x in select_stock]
        mean_month = [round(self.近月均張增減[x], 2) for x in select_stock]
        remark = [self.當月營收備註[x] for x in select_stock]

        try:
            tax = [round(self.稅率[x], 2) for x in select_stock]

            if PE == 0:
                appraisal = [round(self.淨值比預估股價[x], 2) for x in select_stock]
            else:
                appraisal = [round(self.預估EPS[x] * PE, 2) for x in select_stock]

        except:
            tax = 0
            appraisal = 0

        df = stocks_list.loc[select_stock].drop(columns="市場")
        df["股價"] = price
        df["漲幅"] = list(map(lambda x: str(x) + "%", rise))
        df["成值"] = list(map(lambda x: str(x) + "E", volume))
        df["量比"] = volume_percent
        df["估價"] = appraisal
        df["空間"] = round((df["估價"] / df["股價"] - 1) * 100, 1)
        df["稅率"] = tax
        df["投信"] = leagal_person_today
        df["近月投信"] = leagal_person_month
        df["大戶"] = list(map(lambda x: str(x) + "%", rich_person))
        df["近月大戶"] = list(map(lambda x: str(x) + "%", rich_person_month))
        df["散戶"] = list(map(lambda x: str(x) + "%", poor_person))
        df["近月散戶"] = list(map(lambda x: str(x) + "%", poor_person_month))
        df["均張"] = list(map(lambda x: str(x) + "%", mean))
        df["近月均張"] = list(map(lambda x: str(x) + "%", mean_month))
        df["營收備註"] = remark
        df = df.sort_values(["空間", "投信"], ascending=False)
        df["空間"] = df["空間"].astype(str) + "%"

        return df

    def legal_person_except(self, stock, range):
        if range == "today":
            try:
                leagal_person = int(self.當日投信買賣超[stock])
            except:
                leagal_person = 0
            return leagal_person
        if range == "month":
            try:
                leagal_person = int(self.近一月投信買賣超[stock])
            except:
                leagal_person = 0
            return leagal_person


