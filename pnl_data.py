import datetime as dt
import pandas as pd
import os
allFileList = os.listdir()
total = pd.DataFrame()
for x in allFileList:
    if '_pnl.csv' in x:
        total = total.append(pd.read_csv(x,index_col="instr"))
for col in total.columns[1:]:
    total[col] = [round(num,2)for num in total[col]]
total.to_csv(dt.date.today().strftime('%Y%m%d') + '_dailypnl.csv')
print('done')
