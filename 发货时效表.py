import pandas as pd
import os
file_root=r'E:\10-数据统一输出\报表自动化\发货时效表\商品维度订单表（自定义）'
df=pd.DataFrame()
for r,d,f in os.walk(file_root):
    for s in f :
        xinzeng=pd.read_csv(f'{r}/{s}')
        df=pd.concat([df,xinzeng])
data=df[df['交易成功时间'].notnull()==True]
data['交易成功时间']=pd.to_datetime(data['交易成功时间'])
data['买家付款时间']=pd.to_datetime(data['买家付款时间'])
data['发货时效']=data['交易成功时间']-data['买家付款时间']
data['订单创建时间']=pd.to_datetime(data['订单创建时间'])
data['订单创建日期']=data['订单创建时间'].dt.date
def getsort(a):
    if '绘本图书' in a :
        return '合作品牌'
    if '品沐' in a :
        return '合作品牌'
    elif '合作品牌' in a:
        return '合作品牌'
    elif '彩厨' in a:
        return '合作品牌'
    else:
        return '自营品牌'
data['分类']=data.apply(lambda x : getsort(x['商品名称']),axis=1)
data['年月']=data['订单创建时间'].dt.year.apply(str)+'/'+data['订单创建时间'].dt.month.apply(str)
data['周数']=data['订单创建时间'].dt.year.apply(str)+'/'+data['订单创建时间'].dt.isocalendar().week.apply(str)
calendar=data[['订单创建日期','周数']]
ma=calendar['订单创建日期'].groupby(calendar['周数']).max().reset_index().rename(columns={'订单创建日期':'本周最大日期'})
mi=calendar['订单创建日期'].groupby(calendar['周数']).min().reset_index().rename(columns={'订单创建日期':'本周最小日期'})
calendar=pd.merge(calendar,mi,on=['周数'],how='left')
calendar=pd.merge(calendar,ma,on=['周数'],how='left')
calendar.drop_duplicates(subset=['周数','本周最小日期','本周最大日期'],inplace=True)
calendar['日期区间']=calendar['本周最小日期'].apply(str)+'~'+calendar['本周最大日期'].apply(str)
data=pd.merge(data,calendar[['周数','日期区间']],on=['周数'],how='left')
mea=data.groupby(['年月','分类']).aggregate({'发货时效':'mean'}).reset_index().rename(columns={'发货时效':'平均时长'})
ma=data.groupby(['年月','分类']).aggregate({'发货时效':'max'}).reset_index().rename(columns={'发货时效':'最长时长'})
med=data.groupby(['年月','分类']).aggregate({'发货时效':'median'}).reset_index().rename(columns={'发货时效':'时长中位数'})
res=pd.merge(mea,ma,on=['年月','分类'])
res=pd.merge(res,med,on=['年月','分类'])
mea2=data.groupby(['日期区间','分类']).aggregate({'发货时效':'mean'}).reset_index().rename(columns={'发货时效':'平均时长'})
ma2=data.groupby(['日期区间','分类']).aggregate({'发货时效':'max'}).reset_index().rename(columns={'发货时效':'最长时长'})
med2=data.groupby(['日期区间','分类']).aggregate({'发货时效':'median'}).reset_index().rename(columns={'发货时效':'时长中位数'})
res2=pd.merge(mea2,ma2,on=['日期区间','分类'])
res2=pd.merge(res2,med2,on=['日期区间','分类'])
res=res.rename(columns={'年月':'日期'})
res2=res2.rename(columns={'日期区间':'日期'})
result=pd.concat([res,res2])
result['平均时长']=result['平均时长'].apply(str).str.split('.',expand=True)[0]
result['最长时长']=result['最长时长'].apply(str).str.split('.',expand=True)[0]
result['时长中位数']=result['时长中位数'].apply(str).str.split('.',expand=True)[0]
result=result.pivot_table(values=['平均时长','最长时长','时长中位数'],index=['日期'],columns='分类',aggfunc={'平均时长':sum,'最长时长':sum,'时长中位数':sum})#一种实现方式
result=result.reset_index()
result.columns = [' '.join(col) for col in result.columns]
with pd.ExcelWriter(r'E:\10-数据统一输出\报表自动化\发货时效表\发货时效-结果表.xlsx') as writer:
    result.to_excel(writer, sheet_name='发货时效表',index=False)