import os
import pandas as pd

file_root=os.path.dirname(os.path.realpath('销售数据表.py'))
file_root=file_root.replace('\\','/')
print('文件夹路径是'+file_root)
# 得到sql查询的结果
df_sql = pd.read_excel(f'{file_root}/销售数据查询表/销售数据查询表-天.xlsx')
# 得到锁粉客户数

df = pd.DataFrame()
for r, d, f in os.walk(f'{file_root}/锁粉数/日'):
    for s in f:
        df_xinzeng = pd.read_excel(f'{r}/{s}')
        df_xinzeng['日期'] = s.replace(".xls","")
        df = pd.concat([df, df_xinzeng])
# df['日期'] = df['日期'].str.replace('.xls', "")
year = df['日期'].apply(lambda x: x[0:4])
month = df['日期'].apply(lambda x: x[4:6])
day = df['日期'].apply(lambda x: x[6:8])
df['日期'] = year + "/" + month + "/" + day  # 添加日期列
df_suofen = df.groupby(['日期', '昵称']).aggregate({'客户数': 'sum'}).reset_index()
df_suofen = df_suofen.rename(columns={'客户数': '锁粉数', '昵称': '姓名'})
# 得到进群人数

df1 = pd.DataFrame()
for r, d, f in os.walk(f'{file_root}/入群数/日'):
    for s in f:
        df_xinzeng = pd.read_excel(f'{r}/{s}')

        df1 = pd.concat([df1, df_xinzeng])
df1['日期'] = df1['时间范围'].str.split(' ', expand=True)[0]
df1['日期'] = df1['日期'].str.replace('-', '/')
df1 = df1.rename(columns={'Unnamed: 1': '组别-不准','Unnamed: 2': '姓名-不全','所属客服':'姓名'})
# 清洗微信群表
df_s = pd.read_excel(f'{file_root}/整合资产表.xlsx')
# df_s = df_s.loc[df_s['是否加粉号'] == '是']
df1 = pd.merge(df1, df_s['微信号'], left_on='工作微信号', right_on='微信号')  # 交集
df1 = df1.loc[df1['群名称'].str.contains('宠粉') == True]
df_ruqun = df1.groupby(['日期', '姓名']).aggregate({'新增成员总数': 'sum'}).reset_index()
df_ruqun = df_ruqun.rename(columns={'新增成员总数': '入群数'})
data = pd.merge(df_sql, df_suofen, on=['日期', '姓名'], how='left')
data = pd.merge(data, df_ruqun, on=['日期', '姓名'], how='left')
data['锁粉率'] = (data['锁粉数'] / data['总粉丝数']).apply(lambda x: '%.2f%%' % (x * 100))
data['入群率'] = (data['入群数'] / data['加粉数']).apply(lambda x: '%.0f%%' % (x * 100))
data = data.sort_values('日期', ascending=False)

# 处理按月
df_sqlm = pd.read_excel(f'{file_root}/销售数据查询表/销售数据查询表-月.xlsx')

dfm = pd.DataFrame()
for r, d, f in os.walk(f'{file_root}/锁粉数/月'):
    for s in f:
        df_xinzeng = pd.read_excel(f'{r}/{s}')
        dfm = pd.concat([dfm, df_xinzeng])

df_suofenm = dfm.groupby(['昵称']).aggregate({'客户数': 'sum'}).reset_index()
df_suofenm = df_suofenm.rename(columns={'客户数': '锁粉数', '昵称': '姓名'})
# 得到进群人数

dfm1 = pd.DataFrame()
for r, d, f in os.walk(f'{file_root}/入群数/月'):
    for s in f:
        df_xinzeng = pd.read_excel(f'{r}/{s}')
        dfm1 = pd.concat([dfm1, df_xinzeng])
dfm1 = dfm1.rename(columns={'Unnamed: 1': '组别-不准','Unnamed: 2': '姓名-不全','所属客服':'姓名'})
# 清洗微信群表
dfm1 = pd.merge(dfm1, df_s['微信号'], left_on='工作微信号', right_on='微信号')  # 交集
dfm1 = dfm1.loc[dfm1['群名称'].str.contains('宠粉') == True]
df_ruqunm = dfm1.groupby(['姓名']).aggregate({'新增成员总数': 'sum'}).reset_index()
df_ruqunm = df_ruqunm.rename(columns={'新增成员总数': '入群数'})
datam = pd.merge(df_sqlm, df_suofenm, on=['姓名'], how='left')
datam = pd.merge(datam, df_ruqunm, on=['姓名'], how='left')
datam['月锁粉率'] = (datam['锁粉数'] / datam['月均总粉丝数']).apply(lambda x: '%.2f%%' % (x * 100))
datam['月入群率'] = (datam['入群数'] / datam['全月累计加粉数']).apply(lambda x: '%.0f%%' % (x * 100))
datam = datam.sort_values('日期', ascending=False)
# 处理按周
# 按周的锁粉数
year = pd.to_datetime(df['日期']).dt.year.apply(str)
week = pd.to_datetime(df['日期']).dt.week.apply(str)
df['周'] = year + week
df_suofenw = df.groupby(['周', '昵称']).aggregate({'客户数': 'sum'}).reset_index()
df_suofenw = df_suofenw.rename(columns={'客户数': '锁粉数', '昵称': '姓名'})
# 按周的入群数
year1 = pd.to_datetime(df1['日期']).dt.year.apply(str)
week1 = pd.to_datetime(df1['日期']).dt.week.apply(str)
df1['周'] = year1 + week1
df_ruqunw = df1.groupby(['周', '姓名']).aggregate({'新增成员总数': 'sum'}).reset_index()
df_ruqunw = df_ruqunw.rename(columns={'新增成员总数': '入群数'})
# 按周的sql查询结果
df_sqlw = pd.read_excel(f'{file_root}/销售数据查询表/销售数据查询表-周.xlsx')
df_sqlw = df_sqlw.rename(columns={'日期': '周'})
df_sqlw['周'] = df_sqlw['周'].apply(str)
# 找到周包含的日期区间
df_sql['周'] = pd.to_datetime(df_sql['日期']).dt.year.apply(str) + pd.to_datetime(df_sql['日期']).dt.week.apply(str)
calendar = df_sql[['日期', '周']]
ma = calendar['日期'].groupby(calendar['周']).max().reset_index().rename(columns={'日期': '本周最大日期'})
mi = calendar['日期'].groupby(calendar['周']).min().reset_index().rename(columns={'日期': '本周最小日期'})
calendar = pd.merge(calendar, mi, on=['周'], how='left')
calendar = pd.merge(calendar, ma, on=['周'], how='left')
calendar.drop_duplicates(subset=['周', '本周最小日期', '本周最大日期'], inplace=True)
# 合并结果
dataw = pd.merge(df_sqlw, df_suofenw, on=['周', '姓名'], how='left')
dataw = pd.merge(dataw, df_ruqunw, on=['周', '姓名'], how='left')
dataw['周锁粉率'] = (dataw['锁粉数'] / dataw['周均总粉丝数']).apply(lambda x: '%.2f%%' % (x * 100))
dataw['周入群率'] = (dataw['入群数'] / dataw['周累计加粉数']).apply(lambda x: '%.0f%%' % (x * 100))
dataw = pd.merge(dataw, calendar[['周', '本周最小日期', '本周最大日期']], on=['周'], how='left')
dataw = dataw.sort_values('周', ascending=False)
with pd.ExcelWriter(f'{file_root}/结果表.xlsx') as writer:
    data.to_excel(writer, sheet_name='按天', index=False)
    datam.to_excel(writer, sheet_name='按月', index=False)
    dataw.to_excel(writer, sheet_name='按周', index=False)