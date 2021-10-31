#通过有赞商品维度订单表跑出发货时效（发货时间-买家付款时间）、退款表跑出退款时效（退款完成时间-申请时间）、售后维权模块得到订单评价星级统计&明细
import pandas as pd
import os
#处理发货时效表
a=input('请输入明细展示开始时间yyyy-mm-dd：')
start_time=pd.to_datetime(a)

file_root=r'E:\10-数据统一输出\报表自动化\发货时效&售后时长&订单评价\商品维度订单表（自定义）'
df=pd.DataFrame()
for r,d,f in os.walk(file_root):
    for s in f :
        xinzeng=pd.read_csv(f'{r}/{s}')
        df=pd.concat([df,xinzeng])
data=df[df['商品发货时间'].notnull()==True]
data['商品发货时间']=pd.to_datetime(data['商品发货时间'])
data['买家付款时间']=pd.to_datetime(data['买家付款时间'])
data['发货时效']=data['商品发货时间']-data['买家付款时间']
data['订单创建时间']=pd.to_datetime(data['订单创建时间'])
data['订单创建日期']=data['订单创建时间'].dt.date
def getsort(a):
    if '绘本图书' in a :
        return '2.三方'
    if '品沐' in a :
        return '2.三方'
    if '合作品牌' in a:
        return '2.三方'
    if '彩厨' in a:
        return '2.三方'
    else:
        return '1.自营'
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
#处理退款表
#读取退款数据源
file_root=r'E:\10-数据统一输出\报表自动化\发货时效&售后时长&订单评价\退款表'
df=pd.DataFrame()
for r,d,f in os.walk(file_root):
    for s in f:
        df_xinzeng=pd.read_csv(f'{r}/{s}',low_memory=False)
        df=pd.concat([df,df_xinzeng])
#剔除退款未完成的数据
tuikuan=df[df['退款完成时间'].notnull()==True]
tuikuan['申请时间']=pd.to_datetime(tuikuan['申请时间'])
tuikuan['退款完成时间']=pd.to_datetime(tuikuan['退款完成时间'])
#退款表添加需要用的列
tuikuan['退款时长']=tuikuan['退款完成时间']-tuikuan['申请时间']
tuikuan['申请日期']=tuikuan['申请时间'].dt.date
tuikuan['分类']=tuikuan.apply(lambda x : getsort(x['商品名称']),axis=1)
tuikuan['年月']=tuikuan['申请时间'].dt.year.apply(str)+'/'+tuikuan['申请时间'].dt.month.apply(str)
tuikuan['周数']=tuikuan['申请时间'].dt.year.apply(str)+'/'+tuikuan['申请时间'].dt.isocalendar().week.apply(str)
#得到退款周数和日期的日历表
calendar_t=tuikuan[['申请日期','周数']]
ma_t=calendar_t['申请日期'].groupby(calendar_t['周数']).max().reset_index().rename(columns={'申请日期':'本周最大日期'})
mi_t=calendar_t['申请日期'].groupby(calendar_t['周数']).min().reset_index().rename(columns={'申请日期':'本周最小日期'})
calendar_t=pd.merge(calendar_t,mi_t,on=['周数'],how='left')
calendar_t=pd.merge(calendar_t,ma_t,on=['周数'],how='left')
calendar_t.drop_duplicates(subset=['周数','本周最小日期','本周最大日期'],inplace=True)
#退款明细添加按周日期区间列
calendar_t['日期区间']=calendar_t['本周最小日期'].apply(str)+'~'+calendar_t['本周最大日期'].apply(str)
tuikuan=pd.merge(tuikuan,calendar_t[['周数','日期区间']],on=['周数'],how='left')
#求退款时长的按月统计数据
mea_t=tuikuan.groupby(['年月','分类']).aggregate({'退款时长':'mean'}).reset_index().rename(columns={'退款时长':'平均时长'})
ma_t=tuikuan.groupby(['年月','分类']).aggregate({'退款时长':'max'}).reset_index().rename(columns={'退款时长':'最长时长'})
med_t=tuikuan.groupby(['年月','分类']).aggregate({'退款时长':'median'}).reset_index().rename(columns={'退款时长':'时长中位数'})
#合并出退款按月的结果
res_t=pd.merge(mea_t,ma_t,on=['年月','分类'])
res_t=pd.merge(res_t,med_t,on=['年月','分类'])
#求退款时长的按周统计数据
mea2_t=tuikuan.groupby(['日期区间','分类']).aggregate({'退款时长':'mean'}).reset_index().rename(columns={'退款时长':'平均时长'})
ma2_t=tuikuan.groupby(['日期区间','分类']).aggregate({'退款时长':'max'}).reset_index().rename(columns={'退款时长':'最长时长'})
med2_t=tuikuan.groupby(['日期区间','分类']).aggregate({'退款时长':'median'}).reset_index().rename(columns={'退款时长':'时长中位数'})
#合并出退款按周的结果
res2_t=pd.merge(mea2_t,ma2_t,on=['日期区间','分类'])
res2_t=pd.merge(res2_t,med2_t,on=['日期区间','分类'])
res_t=res_t.rename(columns={'年月':'日期'})
res2_t=res2_t.rename(columns={'日期区间':'日期'})
result_tuikuan=pd.concat([res_t,res2_t])
result_tuikuan['平均时长']=result_tuikuan['平均时长'].apply(str).str.split('.',expand=True)[0]
result_tuikuan['最长时长']=result_tuikuan['最长时长'].apply(str).str.split('.',expand=True)[0]
result_tuikuan['时长中位数']=result_tuikuan['时长中位数'].apply(str).str.split('.',expand=True)[0]
result_tuikuan=result_tuikuan.pivot_table(values=['平均时长','最长时长','时长中位数'],index=['日期'],columns='分类',aggfunc={'平均时长':sum,'最长时长':sum,'时长中位数':sum})#一种实现方式
#处理订单评价
pingjia=pd.read_csv(r'E:\10-数据统一输出\报表自动化\发货时效&售后时长&订单评价\订单评价\订单评价明细.csv',encoding='gb18030')
mingxi=pingjia.loc[pingjia['评价日期']>= a]

# mingxi=pd.read_csv(r'E:\10-数据统一输出\报表自动化\发货时效&售后时长&订单评价\订单评价\订单评价明细_新增.csv',encoding='gb18030')
pingjia_group=pingjia.groupby(['评价日期','星级']).aggregate({'订单号':'count'}).unstack()
pingjia_group=pingjia_group.droplevel(0,axis=1)
pingjia_num=pingjia.groupby('评价日期').aggregate({'订单号':'count'})
pingjia_num=pingjia_num.rename(columns={'订单号':'评价数量'})
pingjia_result=pd.merge(pingjia_num,pingjia_group,on='评价日期')

date=pingjia['评价日期'].max()
#结果写出
with pd.ExcelWriter(r'E:\10-数据统一输出\报表自动化\发货时效&售后时长&订单评价\发货时效&退款时长&订单评价'+date+'.xlsx') as writer:
    result.to_excel(writer, sheet_name='发货时效表')
    result_tuikuan .to_excel(writer, sheet_name='退款时长表')
    pingjia_result.to_excel(writer, sheet_name='评价汇总')
    mingxi.to_excel(writer, sheet_name='评价明细',index=False)