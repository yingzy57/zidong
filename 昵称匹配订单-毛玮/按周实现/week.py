
import pandas as pd
import os
import hashlib
def md5(x):
    md5_val = hashlib.md5(x.encode('utf8')).hexdigest()
    return md5_val

file_root=os.path.dirname(os.path.realpath('week.py'))
file_root=file_root.replace('\\','/')
print('文件夹路径是'+file_root)
df=pd.DataFrame()
for r,d,f in os.walk(f'{file_root}/商城端数据'):
    for s in f:
        if s != '.DS_Store':
            df_xinzeng=pd.read_excel(f'{r}/{s}',dtype={'客户手机号':str})
            df=pd.concat([df,df_xinzeng])

df['订单日']=df['下单时间'].dt.day
df['订单月份']=df['下单时间'].dt.month
df['订单月份']=df['订单月份'].apply(str)+'月'
df['订单周数']='本周'

data=pd.DataFrame()
for r,d,f in os.walk(f'{file_root}/句子端数据'):
    for s in f:
        if s != '.DS_Store':
            df_xinzeng=pd.read_excel(f'{r}/{s}')
            data=pd.concat([data,df_xinzeng])

#处理新老客户
old=pd.read_csv(f'{file_root}/老客户列表/老客户-毛炜.csv')
df['客户手机号']=df['客户手机号'].apply(str)
df['客户手机号'] = df['客户手机号'].map(md5)
#把首次下单时间合并到订单明细中
df=pd.merge(df,old,on='客户手机号',how='left')
#添加类型字段，首单表截至到活动日期2021年10月16日（含），能匹配到下单日期的都是老用户，一定包含2021，所以用0判断即可
def neworold(a):
    if '老客户'  in a:
        return '老用户'
    else:
        return '新用户'
df['类型']=df['类型'].apply(str)
df['类型']=df.apply(lambda x : neworold(x['类型']),axis=1)


data['添加时间']=pd.to_datetime(data['添加时间'])
data['客户月份']=data['添加时间'].dt.month
data['客户月份']=data['客户月份'].apply(str)+'月'
data['客户周数']='本周'


data=data.drop_duplicates(subset=['昵称'])
data['昵称']=data['昵称'].replace('	', '', regex=True)
df['客户昵称']=df['客户昵称'].replace('	', '', regex=True)
data_res=pd.merge(data[['昵称','客户周数']],df,left_on='昵称',right_on='客户昵称')
# data_res=data_res[(data_res['分销员昵称']=='袁慧礼')|(data_res['分销员昵称']=='熊琴')|(data_res['分销员昵称']=='崔佳雯')|(data_res['分销员昵称']=='杨小林')|(data_res['分销员昵称']=='尤迪')]
data_res['下单人数']=data_res['客户手机号'].apply(str)
pivot_count=data_res.pivot_table(values='下单人数',index=['客户周数','订单周数','类型'],aggfunc=lambda x: len(x.unique()),fill_value=0,margins=False).reset_index()
result=data_res.groupby(['客户周数','订单周数','类型']).aggregate({'成交金额':'sum','订单号':'count'}).reset_index()
res=pd.merge(result,pivot_count,on=['客户周数','订单周数','类型'],how='left')

#有效订单
df_youxiao=df[(df['成交金额']!=0)&(df['成交金额']!=0.01)]
#根据订单金额和结算状态做筛选
df_youxiao=df_youxiao[(df_youxiao['结算状态']!='待付款')&(df_youxiao['结算状态']!='订单关闭')&(df_youxiao['结算状态']!='不结算,全额退款')]
#有效订单和好友匹配
df_youxiao=pd.merge(data[['昵称','客户周数']],df_youxiao,left_on='昵称',right_on='客户昵称')
df_youxiao['下单人数']=df_youxiao['客户手机号'].apply(str)
pivot_youxiao=df_youxiao.pivot_table(values='下单人数',index=['客户周数','订单周数','类型'],aggfunc=lambda x: len(x.unique()),fill_value=0,margins=False).reset_index()
result_youxiao=df_youxiao.groupby(['客户周数','订单周数','类型']).aggregate({'成交金额':'sum','订单号':'count'}).reset_index()
res_youxiao=pd.merge(result_youxiao,pivot_youxiao,on=['客户周数','订单周数','类型'],how='left')

#合并有赞各月订单后看有效下单人数
df_youxiao['合并下单人数']=df_youxiao['客户周数']+df_youxiao['客户手机号'].apply(str)
pivot_youxiao_hebing=df_youxiao.pivot_table(values='合并下单人数',index=['客户周数','类型'],aggfunc=lambda x: len(x.unique()),fill_value=0,margins=False).reset_index()
result_youxiao_hebing=df_youxiao.groupby(['客户周数','类型']).aggregate({'成交金额':'sum','订单号':'count'}).reset_index()
res_youxiao_hebing=pd.merge(result_youxiao_hebing,pivot_youxiao_hebing,on=['客户周数','类型'],how='left')


#核对未归因金额，
# 订单数据源用订单周数分组统计，得到有赞订单金额
a=df.groupby(['订单周数','类型']).aggregate({'成交金额':'sum'}).reset_index()
a=a.rename(columns={'成交金额':'有赞订单金额'})
# 匹配结果用订单周数分组统计，得到归因成功金额
b=result.groupby(['订单周数','类型']).aggregate({'成交金额':'sum'}).reset_index()
b=b.rename(columns={'成交金额':'归因金额'})
#合并以上，得到未归因金额
hedui=pd.merge(b,a,on=['订单周数','类型'])
hedui['未归因金额']=hedui['有赞订单金额']-hedui['归因金额']

#得到有赞订单  句子好友名单匹配的订单  的差集
mer_notnull=data_res[data_res['订单号'].notnull()==True]
chaji=pd.concat([df, mer_notnull[['订单号']], mer_notnull[['订单号']]]).drop_duplicates(['订单号'],keep=False)
#计算未归因成功的下单人数，用未归因成功的订单透视出人数，剔除无效订单
chaji=chaji[(chaji['成交金额']!=0)&(chaji['成交金额']!=0.01)]
chaji['下单人数']=chaji['订单周数']+chaji['客户手机号'].apply(str)
pivot_chaji=chaji.pivot_table(values='下单人数',index=['订单周数','类型'],aggfunc=lambda x: len(x.unique()),fill_value=0,margins=False).reset_index()
pivot_chaji=pivot_chaji.rename(columns={'下单人数':'未归因成功下单人数'})
#把未归因成功下单人数加入核对结果
hedui_result=pd.concat([hedui,pivot_chaji],axis=1)



with pd.ExcelWriter(f'{file_root}/按周结果.xlsx') as writer:
    res.to_excel(writer, sheet_name='全量订单',index=False)
    res_youxiao.to_excel(writer, sheet_name='有效订单',index=False)
    res_youxiao_hebing.to_excel(writer, sheet_name='合并有效下单人数',index=False)
    hedui_result.to_excel(writer, sheet_name='核对',index=False)#写入excel
    chaji.to_excel(writer, sheet_name='未归因订单',index=False)
    data_res.to_excel(writer, sheet_name='好友订单明细',index=False)
    df_youxiao.to_excel(writer, sheet_name='有效订单明细',index=False)

