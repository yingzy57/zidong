import pandas as pd
import os
file_root=os.path.dirname(os.path.realpath('salesrank.py'))
file_root=file_root.replace('\\','/')
print('文件夹路径是'+file_root)
for r,d,f in os.walk(f'{file_root}/当日数据源'):
    for s in f :
        if '业绩统计' in s :
            yeji=pd.read_excel(f'{file_root}/当日数据源/{s}')
        elif '销售目标' in s :
            mubiao=pd.read_excel(f'{file_root}/当日数据源/{s}')
        elif '加粉' in s :
            fans=pd.read_excel(f'{file_root}/当日数据源/{s}')
for r,d,f in os.walk(f'{file_root}/前一日数据源'):
    for s in f :
        yesterday = pd.read_excel(f'{file_root}/前一日数据源/{s}')
#得到累计销售额、锁粉客户数
yeji=yeji[['昵称','客户数','累计销售额']]
yeji=yeji.rename(columns={'客户数':'锁粉客户数','昵称':'姓名'})

yesterday=yesterday[['昵称','累计销售额']]
yesterday=yesterday.rename(columns={'累计销售额':'昨日销售额','昵称':'姓名'})

#得到加粉数
#取数据源最大日期的粉丝数
calendar=fans[['日期']]
ma=calendar['日期'].max()
fans=fans.loc[fans['日期']==ma]
#获取报表跑数的年月日
year=ma.year
month=ma.month
day=ma.day
ma_date=str(year)+'.'+str(month)+'.'+str(day)
date=str(month)+'.'+str(day)

fans=fans[['姓名','总好友数']]
gro=fans.groupby(['姓名']).aggregate({'总好友数':'sum'}).reset_index()
fans=gro.rename(columns={'总好友数':'个号粉丝总数'})

#合并数据源
mer=pd.merge(mubiao,yeji,on='姓名',how='left')
data=pd.merge(mer,fans,on='姓名',how='left')

data['目标达成率']=(data['累计销售额']/data['销售目标']).apply(lambda x: '%.2f%%' % (x*100))
data=data[['部门','组别','姓名','累计销售额','销售目标','目标达成率','个号粉丝总数','锁粉客户数']]
data=data.sort_values(['部门','累计销售额'],ascending=False)


data['排名']=data.groupby('部门')['累计销售额'].rank(ascending=False, method='first')
data=pd.merge(data,yesterday,on='姓名',how='left')
data['对比昨日增长额']=data['累计销售额']-data['昨日销售额']
data['昨日排名']=data.groupby('部门')['昨日销售额'].rank(ascending=False, method='first')
data['较昨日名次变化']=data['昨日排名']-data['排名']

team=data.groupby(['组别']).aggregate({'累计销售额':'sum','销售目标':'sum','对比昨日增长额':'sum'}).reset_index()
team['目标达成率']=(team['累计销售额']/team['销售目标']).apply(lambda x: '%.2f%%' % (x*100))
team=team.sort_values(['组别','累计销售额'],ascending=False)

data=data.drop(['组别','昨日销售额','昨日排名'],axis=1)


with pd.ExcelWriter(f'{file_root}/社群销售数据排行榜{ma_date}.xlsx') as writer:
    data.to_excel(writer, sheet_name=f'销售{date}',index=False)
    team.to_excel(writer, sheet_name=f'团队{date}',index=False)