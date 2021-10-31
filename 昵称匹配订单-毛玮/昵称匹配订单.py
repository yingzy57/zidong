
import pandas as pd
import os
file_root=os.path.dirname(os.path.realpath('昵称匹配订单.py'))
file_root=file_root.replace('\\','/')
print('文件夹路径是'+file_root)
df=pd.DataFrame()
for r,d,f in os.walk(f'{file_root}/有赞按天下载'):
    for s in f:
        df_xinzeng=pd.read_excel(f'{r}/{s}')
        df=pd.concat([df,df_xinzeng])
df['订单日']=df['下单时间'].dt.day
df['订单月份']=df['下单时间'].dt.month
df['订单月份']=df['订单月份'].apply(str)+'月'

data=pd.DataFrame()
for r,d,f in os.walk(f'{file_root}/句子导出'):
    for s in f:
        df_xinzeng=pd.read_csv(f'{r}/{s}')
        # df_xinzeng['客户月份']=s
        data=pd.concat([data,df_xinzeng])
# data['客户月份']=data['客户月份'].str.replace('.csv',"")
data['添加时间']=pd.to_datetime(data['添加时间'])
data['客户月份']=data['添加时间'].dt.month
data['客户月份']=data['客户月份'].apply(str)+'月'

data=data.drop_duplicates(subset=['昵称'])
data['昵称']=data['昵称'].replace('	', '', regex=True)
df['客户昵称']=df['客户昵称'].replace('	', '', regex=True)
data_res=pd.merge(data[['昵称','客户月份']],df,left_on='昵称',right_on='客户昵称')
# data_res=data_res[(data_res['分销员昵称']=='袁慧礼')|(data_res['分销员昵称']=='熊琴')|(data_res['分销员昵称']=='崔佳雯')|(data_res['分销员昵称']=='杨小林')|(data_res['分销员昵称']=='尤迪')]
data_res['下单人数']=data_res['客户月份']+data_res['订单月份']+data_res['客户手机号'].apply(str)
pivot_count=data_res.pivot_table(values='下单人数',index=['客户月份','订单月份'],aggfunc=lambda x: len(x.unique()),fill_value=0,margins=False).reset_index()
result=data_res.groupby(['客户月份','订单月份']).aggregate({'成交金额':'sum','订单号':'count'}).reset_index()
res=pd.merge(result,pivot_count,on=['客户月份','订单月份'],how='left')

#有效订单
df_youxiao=df[(df['成交金额']!=0)&(df['成交金额']!=0.01)]
df_youxiao=pd.merge(data[['昵称','客户月份']],df_youxiao,left_on='昵称',right_on='客户昵称')
df_youxiao['下单人数']=df_youxiao['客户月份']+df_youxiao['订单月份']+df_youxiao['客户手机号'].apply(str)
pivot_youxiao=df_youxiao.pivot_table(values='下单人数',index=['客户月份','订单月份'],aggfunc=lambda x: len(x.unique()),fill_value=0,margins=False).reset_index()
result_youxiao=df_youxiao.groupby(['客户月份','订单月份']).aggregate({'成交金额':'sum','订单号':'count'}).reset_index()
res_youxiao=pd.merge(result_youxiao,pivot_youxiao,on=['客户月份','订单月份'],how='left')

#合并有赞各月订单后看有效下单人数
df_youxiao['合并下单人数']=df_youxiao['客户月份']+df_youxiao['客户手机号'].apply(str)
pivot_youxiao_hebing=df_youxiao.pivot_table(values='合并下单人数',index=['客户月份'],aggfunc=lambda x: len(x.unique()),fill_value=0,margins=False).reset_index()
result_youxiao_hebing=df_youxiao.groupby(['客户月份']).aggregate({'成交金额':'sum','订单号':'count'}).reset_index()
res_youxiao_hebing=pd.merge(result_youxiao_hebing,pivot_youxiao_hebing,on=['客户月份'],how='left')

#好友接下来每月累计下单人数，6月在6，6+7，6+7+8，6+7+8+9的下单人数
def get_num(df_youxiao):
    res = df_youxiao.pivot_table(values='合并下单人数', index=['客户月份'], aggfunc=lambda x: len(x.unique()),
                                                  fill_value=0, margins=False).reset_index()
    return res
#切片出好友产生订单的月份
#6月客户

df_youxiao_678=df_youxiao.loc[(df_youxiao['下单时间']>='2021/6/10')&(df_youxiao['下单时间']<='2021/9/9')]
df_youxiao_678=df_youxiao_678.loc[df_youxiao_678['客户月份']=='6月']
df_youxiao_67=df_youxiao.loc[(df_youxiao['下单时间']>='2021/6/10')&(df_youxiao['下单时间']<='2021/8/9')]
df_youxiao_67=df_youxiao_67.loc[df_youxiao_67['客户月份']=='6月']
df_youxiao_6=df_youxiao.loc[(df_youxiao['下单时间']>='2021/6/10')&(df_youxiao['下单时间']<='2021/7/9')]
df_youxiao_6=df_youxiao_6.loc[df_youxiao_6['客户月份']=='6月']
#7月客户
df_youxiao_789=df_youxiao.loc[(df_youxiao['订单月份']=='7月')|(df_youxiao['订单月份']=='8月')|(df_youxiao['订单月份']=='9月')]
df_youxiao_789=df_youxiao_789.loc[df_youxiao_789['客户月份']=='7月']
df_youxiao_78=df_youxiao.loc[(df_youxiao['订单月份']=='7月')|(df_youxiao['订单月份']=='8月')]
df_youxiao_78=df_youxiao_78.loc[df_youxiao_78['客户月份']=='7月']
df_youxiao_7=df_youxiao.loc[(df_youxiao['订单月份']=='7月')]
df_youxiao_7=df_youxiao_7.loc[df_youxiao_7['客户月份']=='7月']
#8月客户
df_youxiao_89=df_youxiao.loc[(df_youxiao['订单月份']=='8月')|(df_youxiao['订单月份']=='9月')]
df_youxiao_89=df_youxiao_89.loc[df_youxiao_89['客户月份']=='8月']
df_youxiao_8=df_youxiao.loc[(df_youxiao['订单月份']=='8月')]
df_youxiao_8=df_youxiao_8.loc[df_youxiao_8['客户月份']=='8月']
#9月客户
df_youxiao_9=df_youxiao.loc[(df_youxiao['订单月份']=='9月')]
df_youxiao_9=df_youxiao_9.loc[df_youxiao_9['客户月份']=='9月']
#得到透视结果
res6_678=get_num(df_youxiao_678)
res6_678=res6_678.rename(columns={'合并下单人数':'6.10-9.9下单客户数'})
res6_78=get_num(df_youxiao_67)
res6_78=res6_78.rename(columns={'合并下单人数':'6.10-8.9下单客户数'})
res6_6=get_num(df_youxiao_6)
res6_6=res6_6.rename(columns={'合并下单人数':'6.10-7.9下单客户数'})

res7_789=get_num(df_youxiao_789)
res7_789=res7_789.rename(columns={'合并下单人数':'789月下单客户数'})
res7_78=get_num(df_youxiao_78)
res7_78=res7_78.rename(columns={'合并下单人数':'78月下单客户数'})
res7_7=get_num(df_youxiao_7)
res7_7=res7_7.rename(columns={'合并下单人数':'7月下单客户数'})
res8_89=get_num(df_youxiao_89)
res8_89=res8_89.rename(columns={'合并下单人数':'89月下单客户数'})
res8_8=get_num(df_youxiao_8)
res8_8=res8_8.rename(columns={'合并下单人数':'8月下单客户数'})
res9_9=get_num(df_youxiao_9)
res9_9=res9_9.rename(columns={'合并下单人数':'9月下单客户数'})
#结果合并
res_con=pd.concat([res6_6,res6_78,res6_678,res7_7,res7_78,res7_789,res8_8,res8_89,res9_9])




#核对未归因金额
a=df.groupby('订单月份').aggregate({'成交金额':'sum'}).reset_index()
a=a.rename(columns={'成交金额':'有赞订单金额'})
b=result.groupby('订单月份').aggregate({'成交金额':'sum'}).reset_index()
b=b.rename(columns={'成交金额':'归因金额'})
hedui=pd.merge(b,a,on='订单月份')
hedui['未归因金额']=hedui['有赞订单金额']-hedui['归因金额']

#得到有赞订单  句子好友名单匹配的订单  的差集
mer_notnull=data_res[data_res['订单号'].notnull()==True]
chaji=pd.concat([df, mer_notnull[['订单号']], mer_notnull[['订单号']]]).drop_duplicates(['订单号'],keep=False)
#计算未归因成功的下单人数，用未归因成功的订单透视出人数，剔除无效订单
chaji=chaji[(chaji['成交金额']!=0)&(chaji['成交金额']!=0.01)]
chaji['下单人数']=chaji['订单月份']+chaji['客户手机号'].apply(str)
pivot_chaji=chaji.pivot_table(values='下单人数',index=['订单月份'],aggfunc=lambda x: len(x.unique()),fill_value=0,margins=False).reset_index()
pivot_chaji=pivot_chaji.rename(columns={'下单人数':'未归因成功下单人数'})
#把未归因成功下单人数加入核对结果
hedui_result=pd.concat([hedui,pivot_chaji],axis=1)

with pd.ExcelWriter(f'{file_root}/结果.xlsx') as writer:
    res.to_excel(writer, sheet_name='全量订单',index=False)
    res_youxiao.to_excel(writer, sheet_name='有效订单',index=False)
    res_youxiao_hebing.to_excel(writer, sheet_name='合并有效下单人数',index=False)
    res_con.to_excel(writer, sheet_name='累计有效下单人数',index=False)
    hedui_result.to_excel(writer, sheet_name='核对',index=False)#写入excel
    chaji.to_excel(writer, sheet_name='未归因订单',index=False)

