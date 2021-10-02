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
        self.YOY = get_data("monthly_revenue", "YOY%", 60)
        self.近一月YOY = self.YOY.iloc[-1]
        self.MOM = get_data("monthly_revenue", "MOM%", 60)
        self.近一月MOM = self.MOM.iloc[-1]
        self.近三年最高營收 = self.月營收.iloc[-36:].max()
        self.近兩年稅率 = get_data("finance", "稅率", 888).iloc[-8:]
        self.近兩年稅率 = self.近兩年稅率[self.近兩年稅率 > 0.6]
        self.平均稅率 = self.近兩年稅率[self.近兩年稅率 < 1].mean()
        self.稅率 = self.平均稅率
        self.預估股價地板 = (
            (self.當月營收 * 3 * self.近一季毛利率 - self.近一季營業費用 + self.業外收入及支出) * self.稅率 * self.近一季每股盈餘 / self.近一季稅後淨利 * 4 * 10
        )
        self.預估股價夾板 = (
            (self.當月營收 * 3 * self.近一季毛利率 - self.近一季營業費用 + self.業外收入及支出) * self.稅率 * self.近一季每股盈餘 / self.近一季稅後淨利 * 4 * 15
        )
        self.預估股價天花板 = (
            (self.當月營收 * 3 * self.近一季毛利率 - self.近一季營業費用 + self.業外收入及支出) * self.稅率 * self.近一季每股盈餘 / self.近一季稅後淨利 * 4 * 20
        )
        self.淨值比預估股價 = (
            ((self.當月營收 * 3 * self.近一季毛利率 - self.近一季營業費用 + self.業外收入及支出) * self.稅率 * self.近一季每股盈餘 / self.近一季稅後淨利 * 4)
            + self.近一季每股淨值
        ) * 1.2
        self.預估稅前純益 = self.當月營收 * 3 * self.近一季毛利率 - self.近一季營業費用 + self.業外收入及支出

        # 技術面資料
        self.收盤價 = get_data("price", "收盤價", 99)
        self.目前股價 = self.收盤價.iloc[-1]
        self.昨日股價 = self.收盤價.iloc[-2]
        self.漲幅 = self.目前股價 / self.昨日股價
        self.月線扣抵值 = self.收盤價.iloc[-20]
        self.季線扣抵值 = self.收盤價.iloc[-60]
        self.成交量 = get_data("price", "成交股數", 6) / 1000
        self.當日成交量 = self.成交量.iloc[-1]
        self.昨日成交量 = self.成交量.iloc[-2]
        self.成交值 = get_data("price", "成交金額", 6)
        self.當日成交值 = self.成交值.iloc[-1]
        self.五日均量 = self.成交量.iloc[-5:].mean()
        self.近一季最高價 = self.收盤價.iloc[-60:].max()
        self.近一季最低價 = self.收盤價.iloc[-60:].min()
        self.SMA5 = self.收盤價.iloc[-5:].mean()
        self.SMA10 = self.收盤價.iloc[-10:].mean()
        self.SMA20 = self.收盤價.iloc[-20:].mean()
        self.SMA60 = self.收盤價.iloc[-60:].mean()

        # 籌碼面資料
        self.投信買賣超張數 = get_data("legal_person", "投信買賣超張數", 6)
        self.當日投信買賣超 = self.投信買賣超張數.iloc[-1]
        self.融資使用率 = get_data("margin_trading", "融資使用率", 6)
        self.融券使用率 = get_data("margin_trading", "融券使用率", 6)
        self.當日融資使用率 = self.融資使用率.iloc[-1]
        self.當日融券使用率 = self.融券使用率.iloc[-1]
        self.當日券資比 = self.當日融券使用率 / self.當日融資使用率 * 100

    # 選股清單
    def select_list(self, select_stock, PE=0):
        # list(select_stock[select_stock].index)
        price = [self.目前股價[x] for x in select_stock]
        rise = [round((self.漲幅[x] - 1) * 100, 2) for x in select_stock]
        volume = [round(self.當日成交值[x] / 100000000, 1) for x in select_stock]
        volume_percent = [round(self.當日成交量[x] / self.昨日成交量[x], 1) for x in select_stock]
        leagal_person = list(map(lambda x: self.legal_person_except(x), select_stock))

        try:
            tax = [round(self.平均稅率[x], 2) for x in select_stock]
            remark = [self.當月營收備註[x] for x in select_stock]

            if PE == 0:
                appraisal = [round(self.淨值比預估股價[x], 2) for x in select_stock]
            else:
                appraisal = [
                    round(self.預估稅前純益[x] * self.稅率[x] * self.近一季每股盈餘[x] / self.近一季稅後淨利[x] * 4 * PE, 2)
                    for x in select_stock
                ]

        except:
            tax = 0
            appraisal = 0
            remark = "-"

        df = stocks_list.loc[select_stock].drop(columns="市場")
        df["股價"] = price
        df["漲幅"] = list(map(lambda x: str(x) + "%", rise))
        df["成值"] = list(map(lambda x: str(x) + "E", volume))
        df["量比"] = volume_percent
        df["估價"] = appraisal
        df["空間"] = round((df["估價"] / df["股價"] - 1) * 100, 1)
        df["稅率"] = tax
        df["投信"] = leagal_person
        df["營收備註"] = remark
        df = df.sort_values(["空間", "投信"], ascending=False)
        df["空間"] = df["空間"].astype(str) + "%"

        return df

    def legal_person_except(self, stock):
        try:
            leagal_person = int(self.當日投信買賣超[stock])
        except:
            leagal_person = 0
        return leagal_person
