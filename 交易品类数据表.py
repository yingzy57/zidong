
import pandas as pd
import os
file_root_goodsorder=r'E:\10-数据统一输出\报表自动化\交易品类数据\商品维度订单表（自定义）'
file_root_goodslist=r'E:\10-数据统一输出\报表自动化\交易品类数据\商品分类表'
#读取商品维度订单表
df_goodsorder=pd.DataFrame()
for r,d,f in os.walk(file_root_goodsorder):
    for s in f:
        df_xizeng=pd.read_csv(f'{r}/{s}')
        df_goodsorder=pd.concat([df_goodsorder,df_xizeng])
#读取商品分类表
df_goodslist=pd.DataFrame()
for r,d,f in os.walk(file_root_goodslist):
    for s in f:
        df_xizeng=pd.read_excel(f'{r}/{s}')
        df_goodslist=pd.concat([df_goodslist,df_xizeng])
#分别得到自营的订单明细和分销的订单明细（商品维度）
df_goodsorder['订单创建日期']=pd.to_datetime(df_goodsorder['订单创建时间']).map(lambda x:x.strftime('%Y/%m/%d'))
df_goodsorder['订单创建年月']=pd.to_datetime(df_goodsorder['订单创建时间']).map(lambda x:x.strftime('%Y/%m'))
df_goodsorder=df_goodsorder.replace('\t','',regex=True).replace('	', '', regex=True)
# df_goodsorder_ziying=df_goodsorder.loc[df_goodsorder['订单类型']!='分销供货订单']
# df_goodsorder_fenxiao=df_goodsorder.loc[df_goodsorder['订单类型']=='分销供货订单']

df_goodsorder['大类']=df_goodsorder['订单类型'].map(lambda x: '2.分销' if x =='分销供货订单' else '1.线上小程序')
#剔除0元拼团订单
df_goodsorder=df_goodsorder.loc[df_goodsorder['商品名称'].str.contains('0元拼团')==False]
#0级分类
def getzero(a):
    if '旗下品牌' in a :
        return '2.旗下品牌'
    elif 'aag' in a :
        return '2.旗下品牌'
    elif 'KittyYoyo' in a:
        return '2.旗下品牌'
    elif '光合星球' in a:
        return '2.旗下品牌'
    elif '合作品牌' in a:
        return '3.合作品牌'
    elif '绘本' in a:
        return '3.合作品牌'
    elif '成人服饰' in a:
        return '3.合作品牌'
    elif '个护清洁' in a:
        return '3.合作品牌'
    else:
        return '1.babycare'
df_goodslist['分组（必填）']=df_goodslist['分组（必填）'].apply(str)
df_goodslist['0级分类']=df_goodslist.apply(lambda x : getzero(x['分组（必填）']),axis=1)
#1级分类
def getone(a):
    if '快消' in a :
        return '1.快消'
    elif '用品' in a:
        return '2.用品'
    elif '玩具' in a and '趣味玩具' not in a:
        return '3.玩具'
    elif '孕产' in a:
        return '4.孕产'
    elif 'woobaby' in a:
        return '1.woobaby'
    elif '光合星球' in a:
        return '2.光合星球'
    elif '辅食' in a:
        return '2.光合星球'
    elif 'bckid' in a:
        return '3.bckid'
    elif 'aag' in a:
        return '4.aag'
    elif '宠物' in a:
        return '6.宠物'
    elif 'KittyYoyo' in a:
        return '6.宠物'
    elif '小n' in a:
        return '7.小N'
    elif 'wiya' in a:
        return '5.wiya'
    elif '生活家居' in a:
        return '1.生活家居'
    elif '童装服饰' in a:
        return '3.童装服饰'
    elif '美味' in a:
        return '5.休闲食品'
    elif '趣味玩具' in a:
        return '6.趣味玩具'
    elif '美妆' in a:
        return '7.美妆日化'
    elif '个护' in a:
        return '4.个护家清'
    elif '成人服饰' in a:
        return '2.成人服饰'
    elif '绘本' in a:
        return '8.绘本'
    else:
        return '5.未分出'
df_goodslist['1级分类']=df_goodslist.apply(lambda x : getone(x['分组（必填）']),axis=1)
#得到2级分类
df_goodslist['2级分类']=df_goodslist['1级分类']
df_goodslist=df_goodslist.fillna({'二级类目':'未区分'})
df_goodslist['2级分类'].loc[df_goodslist['二级类目'].str.contains('尿裤')]='1.1尿裤'
df_goodslist['2级分类'].loc[df_goodslist['二级类目'].str.contains('湿巾')]='1.2湿巾'
df_goodslist['2级分类'].loc[df_goodslist['二级类目'].str.contains('云柔巾')]='1.3云柔巾'
df_goodslist['2级分类'].loc[df_goodslist['二级类目'].str.contains('棉柔巾')]='1.4棉柔巾'
#匹配自营的结果
df_goodslist_gbcf=df_goodslist.drop_duplicates(subset=['商品id（必填）'])#商品ID去重后的分类表

data=pd.merge(df_goodsorder[['订单号','订单创建日期','订单创建年月','商品名称','商品规格','规格编码','商品规格ID','商品ID','商品实际成交金额','买家手机号','订单类型','大类']],df_goodslist_gbcf[['商品id（必填）','0级分类','1级分类','2级分类']],left_on=['商品ID'],right_on=['商品id（必填）'],how='left')
data_isnull=data[data['0级分类'].isnull()]
data_notnull=data[data['0级分类'].notnull()]
data=data.fillna({'0级分类':'4.匹配不到分类，请检查分类表商品id','1级分类':' ','2级分类':' '})
#按日期自营销售额
data_pivottwo=data.pivot_table(values='商品实际成交金额',index=['大类','0级分类','1级分类','2级分类'],columns='订单创建日期',aggfunc='sum',fill_value=0)
data_pivotone=data.pivot_table(values='商品实际成交金额',index=['大类','0级分类','1级分类'],columns='订单创建日期',aggfunc='sum',fill_value=0)
data_pivotzero=data.pivot_table(values='商品实际成交金额',index=['大类','0级分类'],columns='订单创建日期',aggfunc='sum',fill_value=0)
data_pivot=data.pivot_table(values='商品实际成交金额',index=['大类'],columns='订单创建日期',aggfunc='sum',fill_value=0)
#按月自营销售额
data_pivottwo_m=data.pivot_table(values='商品实际成交金额',index=['大类','0级分类','1级分类','2级分类'],columns='订单创建年月',aggfunc='sum',fill_value=0)
data_pivotone_m=data.pivot_table(values='商品实际成交金额',index=['大类','0级分类','1级分类'],columns='订单创建年月',aggfunc='sum',fill_value=0)
data_pivotzero_m=data.pivot_table(values='商品实际成交金额',index=['大类','0级分类'],columns='订单创建年月',aggfunc='sum',fill_value=0)
data_pivot_m=data.pivot_table(values='商品实际成交金额',index=['大类'],columns='订单创建年月',aggfunc='sum',fill_value=0)
#按日期自营订单量
data_pivottwo_dingdan=data.pivot_table(values='订单号',index=['大类','0级分类','1级分类','2级分类'],columns='订单创建日期',aggfunc='count',fill_value=0)
data_pivotone_dingdan=data.pivot_table(values='订单号',index=['大类','0级分类','1级分类'],columns='订单创建日期',aggfunc='count',fill_value=0)
data_pivotzero_dingdan=data.pivot_table(values='订单号',index=['大类','0级分类'],columns='订单创建日期',aggfunc='count',fill_value=0)
data_pivot_dingdan=data.pivot_table(values='订单号',index=['大类'],columns='订单创建日期',aggfunc='count',fill_value=0)
#按月自营订单量
data_pivottwo_m_dingdan=data.pivot_table(values='订单号',index=['大类','0级分类','1级分类','2级分类'],columns='订单创建年月',aggfunc='count',fill_value=0)
data_pivotone_m_dingdan=data.pivot_table(values='订单号',index=['大类','0级分类','1级分类'],columns='订单创建年月',aggfunc='count',fill_value=0)
data_pivotzero_m_dingdan=data.pivot_table(values='订单号',index=['大类','0级分类'],columns='订单创建年月',aggfunc='count',fill_value=0)
data_pivot_m_dingdan=data.pivot_table(values='订单号',index=['大类'],columns='订单创建年月',aggfunc='count',fill_value=0)

#按日购买人数
data['2级品类购买人数'] = data['2级分类'] + data['买家手机号'].apply(str) + data['订单创建日期']
data['1级品类购买人数'] = data['1级分类'] + data['买家手机号'].apply(str) + data['订单创建日期']  # 品类购买人数去重规则
data['0级品类购买人数'] = data['0级分类'] + data['买家手机号'].apply(str) + data['订单创建日期']
data['大类购买人数'] = data['大类'] + data['买家手机号'].apply(str) + data['订单创建日期'] # 购买人数去重规则
data_pivottwo_count=data.pivot_table(values='2级品类购买人数',index=['大类','0级分类','1级分类','2级分类'],columns='订单创建日期',aggfunc=lambda x: len(x.unique()),fill_value=0)
data_pivotone_count=data.pivot_table(values='1级品类购买人数',index=['大类','0级分类','1级分类'],columns='订单创建日期',aggfunc=lambda x: len(x.unique()),fill_value=0)
data_pivotzero_count=data.pivot_table(values='0级品类购买人数',index=['大类','0级分类'],columns='订单创建日期',aggfunc=lambda x: len(x.unique()),fill_value=0)
data_pivot_count=data.pivot_table(values='大类购买人数',index=['大类'],columns='订单创建日期',aggfunc=lambda x: len(x.unique()),fill_value=0)
#按月购买人数
data['2级品类购买人数'] = data['2级分类'] + data['买家手机号'].apply(str) + data['订单创建年月']
data['1级品类购买人数'] = data['1级分类'] + data['买家手机号'].apply(str) + data['订单创建年月']  # 品类购买人数去重规则
data['0级品类购买人数'] = data['0级分类'] + data['买家手机号'].apply(str) + data['订单创建年月']
data['大类购买人数'] = data['大类'] + data['买家手机号'].apply(str) + data['订单创建年月']# 购买人数去重规则
data_pivottwo_count_m=data.pivot_table(values='2级品类购买人数',index=['大类','0级分类','1级分类','2级分类'],columns='订单创建年月',aggfunc=lambda x: len(x.unique()),fill_value=0)
data_pivotone_count_m=data.pivot_table(values='1级品类购买人数',index=['大类','0级分类','1级分类'],columns='订单创建年月',aggfunc=lambda x: len(x.unique()),fill_value=0)
data_pivotzero_count_m=data.pivot_table(values='0级品类购买人数',index=['大类','0级分类'],columns='订单创建年月',aggfunc=lambda x: len(x.unique()),fill_value=0)
data_pivot_count_m=data.pivot_table(values='大类购买人数',index=['大类'],columns='订单创建年月',aggfunc=lambda x: len(x.unique()),fill_value=0)

def chuli(df,df1,df2):
    price = round(df / df1)  #销售额/下单人数=客单价
    price_dingdan = round(df / df2)  # 销售额/订单数=订单单价
    df['备注'] = '销售额'
    df1['备注'] = '下单人数'
    price['备注'] = '客单价'
    df2['备注'] = '订单数'
    price_dingdan['备注'] = '订单单价'
    res = pd.concat([df, price, price_dingdan]) 
    return res# 结果合并
#获取按月的4个结果
two_m=chuli(data_pivottwo_m,data_pivottwo_count_m,data_pivottwo_m_dingdan)
one_m=chuli(data_pivotone_m,data_pivotone_count_m,data_pivotone_m_dingdan)
zero_m=chuli(data_pivotzero_m,data_pivotzero_count_m,data_pivotzero_m_dingdan)
dalei_m=chuli(data_pivot_m,data_pivot_count_m,data_pivot_m_dingdan)
#获取按日的4个结果
two=chuli(data_pivottwo,data_pivottwo_count,data_pivottwo_dingdan)
one=chuli(data_pivotone,data_pivotone_count,data_pivotone_dingdan)
zero=chuli(data_pivotzero,data_pivotzero_count,data_pivotzero_dingdan)
dalei=chuli(data_pivot,data_pivot_count,data_pivot_dingdan)



with pd.ExcelWriter(r'E:\10-数据统一输出\报表自动化\交易品类数据\交易品类结果表.xlsx') as writer:
    dalei_m.to_excel(writer, sheet_name='大类_月', index=True)
    zero_m.to_excel(writer, sheet_name='0级_月', index=True)
    one_m.to_excel(writer, sheet_name='1级_月', index=True)
    two_m.to_excel(writer, sheet_name='2级_月',index=True)
    dalei.to_excel(writer, sheet_name='大类_日', index=True)
    zero.to_excel(writer, sheet_name='0级_日', index=True)
    one.to_excel(writer, sheet_name='1级_日', index=True)
    two.to_excel(writer, sheet_name='2级_日',index=True)
    data_isnull.to_excel(writer, sheet_name='未匹配到明细',index=False)
    df_goodslist_gbcf.to_excel(writer, sheet_name='分类表',index=False)