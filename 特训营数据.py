import pandas as pd
import os
import datetime
#处理订单结算表
file_root=r'E:\10-数据统一输出\报表自动化\特训营数据\订单维度结算报表'
df=pd.DataFrame()
for r,d,f in os.walk(file_root):
    for s in f:
        df_xinzeng=pd.read_excel(f'{r}/{s}')
        df=pd.concat([df,df_xinzeng])
df=df[(df['结算状态']!='待付款')&(df['结算状态']!='订单关闭') &(df['结算状态']!='不结算,全额退款')]
df['下单日期']=df['下单时间'].map(lambda x:x.strftime('%Y/%m/%d'))#新增一列不带时间的日期列
#累计购买用户数
df_all=df[df['成交金额']!=0]
df_all=df_all.drop_duplicates(subset=['客户手机号','分销员手机号'])
df_all=df_all.groupby('分销员昵称').aggregate({'客户手机号':'count'})
df_all=df_all.rename(columns={'客户手机号':'累计购买用户数'})
#累计购买金额
df_all_amount=df[df['成交金额']!=0]
df_all_amount=df.groupby('分销员昵称').aggregate({'成交金额':'sum'})
df_all_amount=df_all_amount.rename(columns={'成交金额':'累计购买金额'})
#当日购买用户数
df['下单日期']=pd.to_datetime(df['下单时间']).dt.date
df_daily=df.loc[df['下单日期']==df['下单日期'].max()]
df_daily=df_daily[df_daily['成交金额']!=0]
df_daily=df_daily.drop_duplicates(subset=['客户手机号','分销员手机号'])
df_daily=df_daily.groupby('分销员昵称').aggregate({'客户手机号':'count'})
df_daily=df_daily.rename(columns={'客户手机号':'当日购买用户数'})
#当日购买金额
df_daily_amount=df.loc[df['下单日期']==df['下单日期'].max()]
df_daily_amount=df_daily_amount[df_daily_amount['成交金额']!=0]
df_daily_amount=df_daily_amount.groupby('分销员昵称').aggregate({'成交金额':'sum'})
df_daily_amount=df_daily_amount.rename(columns={'成交金额':'当日购买金额'})
#处理销售员表
df_s=pd.read_excel(r'E:\10-数据统一输出\报表自动化\特训营数据\分销员账号明细\分销员账号明细.xlsx',sheet_name='销售手机号年月汇总')
df_s=df_s.loc[df_s['渠道']!='企微']
df_s=df_s[['姓名','渠道']].drop_duplicates(subset=['姓名','渠道'])
#合并结果
df_res=pd.merge(df_s,df_all,left_on='姓名',right_index=True)
df_res=pd.merge(df_res,df_daily,left_on='姓名',right_index=True,how='left')
df_res=pd.merge(df_res,df_all_amount,left_on='姓名',right_index=True,how='left')
df_res=pd.merge(df_res,df_daily_amount,left_on='姓名',right_index=True,how='left')
df_res=df_res.sort_values('当日购买用户数',ascending=False)
df_res['累计购买客单价']=round(df_res['累计购买金额']/df_res['累计购买用户数'],0)
df_res['当日购买客单价']=round(df_res['当日购买金额']/df_res['当日购买用户数'],0)

#累计每天购买用户数
df_all_daily=df[df['成交金额']!=0]
df_all_daily=df_all_daily.drop_duplicates(subset=['客户手机号','分销员手机号','下单日期'])
df_all_daily=df_all_daily.groupby(['分销员昵称','下单日期']).aggregate({'客户手机号':'count'}).reset_index()
df_all_daily=df_all_daily.rename(columns={'客户手机号':'购买用户数','分销员昵称':'姓名'})
#累计每天购买金额
df_all_amount_daily=df[df['成交金额']!=0]
df_all_amount_daily=df.groupby(['分销员昵称','下单日期']).aggregate({'成交金额':'sum'}).reset_index()
df_all_amount_daily=df_all_amount_daily.rename(columns={'成交金额':'购买金额','分销员昵称':'姓名'})
#合并每天结果
df_res_daily=pd.merge(df_s,df_all_daily,left_on='姓名',right_on='姓名')
df_res_daily=pd.merge(df_res_daily,df_all_amount_daily,left_on=['姓名','下单日期'],right_on=['姓名','下单日期'])
df_res_daily['购买客单价']=round(df_res_daily['购买金额']/df_res_daily['购买用户数'],0)
#结果写入excel
with pd.ExcelWriter(r'E:\10-数据统一输出\报表自动化\特训营数据\结果表.xlsx') as writer:
    df_res.to_excel(writer, sheet_name='数据源',index=False)
    df_res_daily.to_excel(writer, sheet_name='每天数据源',index=False)