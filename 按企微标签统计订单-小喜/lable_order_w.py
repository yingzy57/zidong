
import pandas as pd
import os
import datetime

file_root=os.path.dirname(os.path.realpath('lable_order_w.py'))
file_root=file_root.replace('\\','/')
print('文件夹路径是'+file_root)
fans=pd.DataFrame()
for r,d,f in os.walk(f'{file_root}/数据源'):
    for s in f :
        if '用户昵称' in s :
            jiafenhao=pd.read_excel(f'{file_root}/数据源/{s}',sheet_name='加粉号')
        elif '下单金额' in s:
            order=pd.read_excel(f'{file_root}/数据源/{s}')
        elif '微伴' in s:
            xinzeng = pd.read_excel(f'{file_root}/数据源/{s}',header=[4])
            fans = pd.concat([fans, xinzeng])

#unionid去重计算加粉数
fans = fans.drop_duplicates(subset=['unionid'])
#新增添加日期列
fans['添加时间']=pd.to_datetime(fans['添加时间'])
fans['添加日期']=fans['添加时间'].dt.date
#根据一列内容生成新的一列
def getlable(a):
    if '新包裹卡片' in a :
        return '新包裹卡片'
    elif '新包裹卡片'  not in a and '包裹卡片' in a  :
        return '包裹卡片'
    elif 'AI'  in a :
        return 'AI语音加密'
fans['秒回标签']=fans.apply(lambda x : getlable(x['企业标签']),axis=1)
#计算加粉数
fans_num=fans.groupby(['添加日期','秒回标签']).aggregate({'unionid':'count'}).reset_index()
fans_num=fans_num.rename(columns={'unionid':'加粉数'})

#只留下加粉号的订单
order=pd.merge(order,jiafenhao,left_on='账号',right_on='手机号',how='inner')
#匹配好友列表和订单
fans_d = fans.drop_duplicates(subset=['客户名称'])
data=pd.merge(order,fans_d,left_on='昵称',right_on='客户名称',how='inner')

#计算订单金额和下单人数
amount=data.groupby(['添加日期','秒回标签']).aggregate({'下单金额':'sum'}).reset_index()

#下单用户数
pivot_count=data.pivot_table(values='buyer_mobile',index=['添加日期','秒回标签'],aggfunc=lambda x: len(x.unique()),fill_value=0,margins=False).reset_index()
pivot_count=pivot_count.rename(columns={'buyer_mobile':'下单人数'})
#合并粉丝数、下单人数、下单金额
mer=pd.merge(fans_num,pivot_count,on=['添加日期','秒回标签'],how='left')
res=pd.merge(mer,amount,on=['添加日期','秒回标签'],how='left')
#用多重索引的方法把秒回标签放在列索引
res=res.set_index(['添加日期','秒回标签'])
res_data=res.unstack(level=1)
with pd.ExcelWriter(f'{file_root}/标签统计订单（微伴）.xlsx') as writer:
    res_data.to_excel(writer, sheet_name='统计结果',index=True)
    data.to_excel(writer, sheet_name='明细',index=True)
