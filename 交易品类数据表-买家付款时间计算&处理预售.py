
import pandas as pd
import os

file_root_goodsorder=r'E:\10-数据统一输出\报表自动化\交易品类数据\商品维度订单表（自定义）'
file_root_goodslist=r'E:\10-数据统一输出\报表自动化\交易品类数据\商品分类表'
file_root_yushou=r'E:\10-数据统一输出\报表自动化\交易品类数据\预售定金表'
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
#读取预售定金表
df_yushou=pd.DataFrame()
for r,d,f in os.walk(file_root_yushou):
    for s in f:
        df_xizeng=pd.read_excel(f'{r}/{s}')
        df_yushou=pd.concat([df_yushou,df_xizeng])


#分别得到自营的订单明细和分销的订单明细（商品维度）
df_goodsorder=df_goodsorder[df_goodsorder['买家付款时间'].notnull()==True]#取买家付款时间非空
df_goodsorder['买家付款日期']=pd.to_datetime(df_goodsorder['买家付款时间']).map(lambda x:x.strftime('%Y/%m/%d'))
df_goodsorder['买家付款年月']=pd.to_datetime(df_goodsorder['买家付款时间']).map(lambda x:x.strftime('%Y/%m'))
df_goodsorder=df_goodsorder.replace('\t','',regex=True).replace('	', '', regex=True)



#处理全量订单
df_goodsorder['大类']=df_goodsorder['订单类型'].map(lambda x: '2.分销' if x =='分销供货订单' else '1.线上小程序')
#剔除0元拼团订单
df_goodsorder=df_goodsorder.loc[(df_goodsorder['商品名称'].str.contains('0元拼团')==False)&(df_goodsorder['商品名称'].str.contains('拼团抢兔单')==False)]
# #商品ID匹配GMV，商品+规格id匹配成本
df_goodsorder['商品ID']=df_goodsorder['商品ID'].apply(str)

#处理商品规格id空值把nan替换为空
ISnull=df_goodsorder[df_goodsorder['商品规格ID'].isnull()==True]
ISnull.fillna({'商品ID':'','商品规格ID':''})
ISnull['商品规格ID']=ISnull['商品规格ID'].apply(str)
#处理商品规格ID非空，取整变成str
Notnull=df_goodsorder[df_goodsorder['商品规格ID'].notnull()==True]
Notnull['商品规格ID']=Notnull['商品规格ID'].apply(int)
Notnull['商品规格ID']=Notnull['商品规格ID'].apply(str)
#把商品规格的空和非空合并
df_goodsorder=pd.concat([Notnull,ISnull])
df_goodsorder['商品+规格id']=df_goodsorder.apply(lambda x:x['商品ID']+x['商品规格ID'],axis=1)

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
df_goodslist.fillna({'商品id（必填）':'','规格id':''})
df_goodslist['商品id（必填）']=df_goodslist['商品id（必填）'].apply(str)
df_goodslist['商品+规格id']=df_goodslist['商品+规格id'].apply(str)
#匹配结果
df_goodslist_gbcf=df_goodslist.drop_duplicates(subset=['商品id（必填）'])#商品ID去重后的分类表
df_goodslist_cb=df_goodslist.drop_duplicates(subset=['商品+规格id'])#规格ID去重后的分类表


#处理成本

#规格ID匹配成本
data_guodu=pd.merge(df_goodsorder[['订单号','买家付款日期','买家付款年月','商品名称','商品规格','规格编码','商品规格ID','商品ID','商品+规格id','商品实际成交金额','买家手机号','订单类型','大类']],df_goodslist_gbcf[['商品id（必填）','0级分类','1级分类','2级分类']],left_on=['商品ID'],right_on=['商品id（必填）'],how='left')
data_cb=pd.merge(data_guodu,df_goodslist_cb[['商品+规格id','成本']],left_on=['商品+规格id'],right_on=['商品+规格id'],how='left')
#未匹配到成本的明细
data_cb_isnull=data_cb[data_cb['成本'].isnull()]
# data_cb_isnull_qc=data_cb_isnull.drop_duplicates('商品+规格id')
# data_cb_isnull_qc=data_cb_isnull_qc[['商品ID','商品规格ID','商品+规格id','商品名称']]
#无分类无成本的空值填充
data_cb=data_cb.fillna({'0级分类':'4.匹配不到','1级分类':' ','2级分类':' ','成本':0})
#添加毛利列
data_cb['毛利']=data_cb['商品实际成交金额']-data_cb['成本']

#按日期成本
data_cb_pivottwo=data_cb.pivot_table(values='成本',index=['大类','0级分类','1级分类','2级分类'],columns='买家付款日期',aggfunc='sum',fill_value=0)
data_cb_pivotone=data_cb.pivot_table(values='成本',index=['大类','0级分类','1级分类'],columns='买家付款日期',aggfunc='sum',fill_value=0)
data_cb_pivotzero=data_cb.pivot_table(values='成本',index=['大类','0级分类'],columns='买家付款日期',aggfunc='sum',fill_value=0)
data_cb_pivot=data_cb.pivot_table(values='成本',index=['大类'],columns='买家付款日期',aggfunc='sum',fill_value=0)
#按月成本
data_cb_pivottwo_m=data_cb.pivot_table(values='成本',index=['大类','0级分类','1级分类','2级分类'],columns='买家付款年月',aggfunc='sum',fill_value=0)
data_cb_pivotone_m=data_cb.pivot_table(values='成本',index=['大类','0级分类','1级分类'],columns='买家付款年月',aggfunc='sum',fill_value=0)
data_cb_pivotzero_m=data_cb.pivot_table(values='成本',index=['大类','0级分类'],columns='买家付款年月',aggfunc='sum',fill_value=0)
data_cb_pivot_m=data_cb.pivot_table(values='成本',index=['大类'],columns='买家付款年月',aggfunc='sum',fill_value=0)
#按日期毛利
data_ml_pivottwo=data_cb.pivot_table(values='毛利',index=['大类','0级分类','1级分类','2级分类'],columns='买家付款日期',aggfunc='sum',fill_value=0)
data_ml_pivotone=data_cb.pivot_table(values='毛利',index=['大类','0级分类','1级分类'],columns='买家付款日期',aggfunc='sum',fill_value=0)
data_ml_pivotzero=data_cb.pivot_table(values='毛利',index=['大类','0级分类'],columns='买家付款日期',aggfunc='sum',fill_value=0)
data_ml_pivot=data_cb.pivot_table(values='毛利',index=['大类'],columns='买家付款日期',aggfunc='sum',fill_value=0)
#按月毛利
data_ml_pivottwo_m=data_cb.pivot_table(values='毛利',index=['大类','0级分类','1级分类','2级分类'],columns='买家付款年月',aggfunc='sum',fill_value=0)
data_ml_pivotone_m=data_cb.pivot_table(values='毛利',index=['大类','0级分类','1级分类'],columns='买家付款年月',aggfunc='sum',fill_value=0)
data_ml_pivotzero_m=data_cb.pivot_table(values='毛利',index=['大类','0级分类'],columns='买家付款年月',aggfunc='sum',fill_value=0)
data_ml_pivot_m=data_cb.pivot_table(values='毛利',index=['大类'],columns='买家付款年月',aggfunc='sum',fill_value=0)



#处理10月的预售订单中，付款时间在11月的订单拿出来，匹配定金金额，定金部分计入10月，尾款部分计入11月
#拿到10月预售订单
df_goodsorder_yushou=df_goodsorder.loc[(df_goodsorder['订单类型']=='预售订单')&(df_goodsorder['买家付款年月']=='2021/11')]
#剔除10月预售以后的主订单
df_goodsorder_zhu=df_goodsorder.loc[(df_goodsorder['订单类型']!='预售订单')|((df_goodsorder['订单类型']=='预售订单')&(df_goodsorder['买家付款年月']!='2021/11'))]
#预售订单匹配预售金额
df_yushou['商品ID']=df_yushou['商品ID'].apply(str)
yushou=pd.merge(df_goodsorder_yushou,df_yushou,left_on='商品ID',right_on='商品ID',how='left')
yushou['尾款金额']=yushou['商品实际成交金额']-yushou['定金金额']
#预售订单明细整合成和主订单相同的格式，其中尾款金额计入11月，定金金额计入10月
yushou_11yue=yushou
yushou_11yue=yushou_11yue.rename(columns={'商品实际成交金额':'商品总实付','尾款金额':'商品实际成交金额'})
yushou_11yue=yushou_11yue.drop(['商品总实付','定金金额'],axis=1)
yushou_10yue=yushou
yushou_10yue=yushou_10yue.rename(columns={'商品实际成交金额':'商品总实付','定金金额':'商品实际成交金额','买家付款时间':'买家实际付款时间','订单创建时间':'买家付款时间'})#10月支付的预售订单的订单，计入10月（按照订单创建时间）

yushou_10yue=yushou_10yue.drop(['商品总实付','尾款金额','买家付款日期','买家付款年月','买家实际付款时间'],axis=1)
yushou_10yue['订单创建时间']=yushou_10yue['买家付款时间']#10月预售的定金金额计算不需要订单创建时间这个字段，订单创建时间=买家付款时间列示
yushou_10yue['买家付款日期']=pd.to_datetime(yushou_10yue['买家付款时间']).map(lambda x:x.strftime('%Y/%m/%d'))
yushou_10yue['买家付款年月']=pd.to_datetime(yushou_10yue['买家付款时间']).map(lambda x:x.strftime('%Y/%m'))

#合并预售和主订单，得到订单数据基数表（订单号存在重复：10月的预售订单这部分）
con=pd.concat([yushou_11yue,yushou_10yue])
df_goodsorder_gmv=pd.concat([df_goodsorder_zhu,con])


#处理GMV
#商品id匹配GMV的分组
data=pd.merge(df_goodsorder_gmv[['订单号','买家付款日期','买家付款年月','商品名称','商品规格','规格编码','商品规格ID','商品ID','商品实际成交金额','买家手机号','订单类型','大类']],df_goodslist_gbcf[['商品id（必填）','0级分类','1级分类','2级分类']],left_on=['商品ID'],right_on=['商品id（必填）'],how='left')
#未匹配到分类的明细
data_isnull=data[data['0级分类'].isnull()]
# data_notnull=data[data['0级分类'].notnull()]
#未分类的空值填充
data=data.fillna({'0级分类':'4.匹配不到','1级分类':' ','2级分类':' '})


#按日期自营销售额
data_pivottwo=data.pivot_table(values='商品实际成交金额',index=['大类','0级分类','1级分类','2级分类'],columns='买家付款日期',aggfunc='sum',fill_value=0)
data_pivotone=data.pivot_table(values='商品实际成交金额',index=['大类','0级分类','1级分类'],columns='买家付款日期',aggfunc='sum',fill_value=0)
data_pivotzero=data.pivot_table(values='商品实际成交金额',index=['大类','0级分类'],columns='买家付款日期',aggfunc='sum',fill_value=0)
data_pivot=data.pivot_table(values='商品实际成交金额',index=['大类'],columns='买家付款日期',aggfunc='sum',fill_value=0)
#按月自营销售额
data_pivottwo_m=data.pivot_table(values='商品实际成交金额',index=['大类','0级分类','1级分类','2级分类'],columns='买家付款年月',aggfunc='sum',fill_value=0)
data_pivotone_m=data.pivot_table(values='商品实际成交金额',index=['大类','0级分类','1级分类'],columns='买家付款年月',aggfunc='sum',fill_value=0)
data_pivotzero_m=data.pivot_table(values='商品实际成交金额',index=['大类','0级分类'],columns='买家付款年月',aggfunc='sum',fill_value=0)
data_pivot_m=data.pivot_table(values='商品实际成交金额',index=['大类'],columns='买家付款年月',aggfunc='sum',fill_value=0)
#按日期订单量（预售部分拆分订单量不应重复计入，和成本采用同一个数据源）
data_pivottwo_dingdan=data_cb.pivot_table(values='订单号',index=['大类','0级分类','1级分类','2级分类'],columns='买家付款日期',aggfunc='count',fill_value=0)
data_pivotone_dingdan=data_cb.pivot_table(values='订单号',index=['大类','0级分类','1级分类'],columns='买家付款日期',aggfunc='count',fill_value=0)
data_pivotzero_dingdan=data_cb.pivot_table(values='订单号',index=['大类','0级分类'],columns='买家付款日期',aggfunc='count',fill_value=0)
data_pivot_dingdan=data_cb.pivot_table(values='订单号',index=['大类'],columns='买家付款日期',aggfunc='count',fill_value=0)
#按月订单量（预售部分拆分订单量不应重复计入，和成本采用同一个数据源）
data_pivottwo_m_dingdan=data_cb.pivot_table(values='订单号',index=['大类','0级分类','1级分类','2级分类'],columns='买家付款年月',aggfunc='count',fill_value=0)
data_pivotone_m_dingdan=data_cb.pivot_table(values='订单号',index=['大类','0级分类','1级分类'],columns='买家付款年月',aggfunc='count',fill_value=0)
data_pivotzero_m_dingdan=data_cb.pivot_table(values='订单号',index=['大类','0级分类'],columns='买家付款年月',aggfunc='count',fill_value=0)
data_pivot_m_dingdan=data_cb.pivot_table(values='订单号',index=['大类'],columns='买家付款年月',aggfunc='count',fill_value=0)

#按日购买人数
data['2级品类购买人数'] = data['2级分类'] + data['买家手机号'].apply(str) + data['买家付款日期']
data['1级品类购买人数'] = data['1级分类'] + data['买家手机号'].apply(str) + data['买家付款日期']  # 品类购买人数去重规则
data['0级品类购买人数'] = data['0级分类'] + data['买家手机号'].apply(str) + data['买家付款日期']
data['大类购买人数'] = data['大类'] + data['买家手机号'].apply(str) + data['买家付款日期'] # 购买人数去重规则
data_pivottwo_count=data.pivot_table(values='2级品类购买人数',index=['大类','0级分类','1级分类','2级分类'],columns='买家付款日期',aggfunc=lambda x: len(x.unique()),fill_value=0)
data_pivotone_count=data.pivot_table(values='1级品类购买人数',index=['大类','0级分类','1级分类'],columns='买家付款日期',aggfunc=lambda x: len(x.unique()),fill_value=0)
data_pivotzero_count=data.pivot_table(values='0级品类购买人数',index=['大类','0级分类'],columns='买家付款日期',aggfunc=lambda x: len(x.unique()),fill_value=0)
data_pivot_count=data.pivot_table(values='大类购买人数',index=['大类'],columns='买家付款日期',aggfunc=lambda x: len(x.unique()),fill_value=0)
#按月购买人数
data['2级品类购买人数'] = data['2级分类'] + data['买家手机号'].apply(str) + data['买家付款年月']
data['1级品类购买人数'] = data['1级分类'] + data['买家手机号'].apply(str) + data['买家付款年月']  # 品类购买人数去重规则
data['0级品类购买人数'] = data['0级分类'] + data['买家手机号'].apply(str) + data['买家付款年月']
data['大类购买人数'] = data['大类'] + data['买家手机号'].apply(str) + data['买家付款年月']# 购买人数去重规则
data_pivottwo_count_m=data.pivot_table(values='2级品类购买人数',index=['大类','0级分类','1级分类','2级分类'],columns='买家付款年月',aggfunc=lambda x: len(x.unique()),fill_value=0)
data_pivotone_count_m=data.pivot_table(values='1级品类购买人数',index=['大类','0级分类','1级分类'],columns='买家付款年月',aggfunc=lambda x: len(x.unique()),fill_value=0)
data_pivotzero_count_m=data.pivot_table(values='0级品类购买人数',index=['大类','0级分类'],columns='买家付款年月',aggfunc=lambda x: len(x.unique()),fill_value=0)
data_pivot_count_m=data.pivot_table(values='大类购买人数',index=['大类'],columns='买家付款年月',aggfunc=lambda x: len(x.unique()),fill_value=0)

def chuli(df,df1,df2,df3,df4):
    price = round(df / df1)  #销售额/下单人数=客单价
    price_dingdan = round(df / df2)  # 销售额/订单数=订单单价
    maoli_per=df4 / df
    maoli_per=maoli_per.applymap(lambda x: '%.2f%%' % (x*100))#2位小数
    df['备注'] = '销售额'
    df1['备注'] = '下单人数'
    price['备注'] = '客单价'
    df2['备注'] = '订单数'
    price_dingdan['备注'] = '订单单价'
    df3['备注']='成本'
    df4['备注']='毛利'
    maoli_per['备注']='毛利率'
    res = pd.concat([df, price, price_dingdan,df3,df4,maoli_per])
    return res# 结果合并
#获取按月的4个结果
two_m=chuli(data_pivottwo_m,data_pivottwo_count_m,data_pivottwo_m_dingdan,data_cb_pivottwo_m,data_ml_pivottwo_m)
one_m=chuli(data_pivotone_m,data_pivotone_count_m,data_pivotone_m_dingdan,data_cb_pivotone_m,data_ml_pivotone_m)
zero_m=chuli(data_pivotzero_m,data_pivotzero_count_m,data_pivotzero_m_dingdan,data_cb_pivotzero_m,data_ml_pivotzero_m)
dalei_m=chuli(data_pivot_m,data_pivot_count_m,data_pivot_m_dingdan,data_cb_pivot_m,data_ml_pivot_m)
#获取按日的4个结果
two=chuli(data_pivottwo,data_pivottwo_count,data_pivottwo_dingdan,data_cb_pivottwo,data_ml_pivottwo)
one=chuli(data_pivotone,data_pivotone_count,data_pivotone_dingdan,data_cb_pivotone,data_ml_pivotone)
zero=chuli(data_pivotzero,data_pivotzero_count,data_pivotzero_dingdan,data_cb_pivotzero,data_ml_pivotzero)
dalei=chuli(data_pivot,data_pivot_count,data_pivot_dingdan,data_cb_pivot,data_ml_pivot)


#核对
hedui=data_cb.loc[(data_cb['大类']=='1.线上小程序')&(data_cb['2级分类']=='2.用品')&((data_cb['买家付款年月']=='2021/01')|(data_cb['买家付款年月']=='2021/11'))]

with pd.ExcelWriter(r'E:\10-数据统一输出\报表自动化\交易品类数据\交易品类结果表.xlsx') as writer:
    hedui.to_excel(writer, sheet_name='核对用品成本',index=False)
    data_cb_isnull.to_excel(writer, sheet_name='未匹配明细-成本',index=False)
    dalei_m.to_excel(writer, sheet_name='大类_月', index=True)
    zero_m.to_excel(writer, sheet_name='0级_月', index=True)
    one_m.to_excel(writer, sheet_name='1级_月', index=True)
    two_m.to_excel(writer, sheet_name='2级_月',index=True)
    dalei.to_excel(writer, sheet_name='大类_日', index=True)
    zero.to_excel(writer, sheet_name='0级_日', index=True)
    one.to_excel(writer, sheet_name='1级_日', index=True)
    two.to_excel(writer, sheet_name='2级_日',index=True)
    # yushou_10yue.to_excel(writer, sheet_name='预售_计入10月',index=True)
    # yushou_11yue.to_excel(writer, sheet_name='预售_计入11月',index=True)
    data_isnull.to_excel(writer, sheet_name='未匹配明细-分类',index=False)
    df_goodslist_gbcf.to_excel(writer, sheet_name='分类表',index=False)