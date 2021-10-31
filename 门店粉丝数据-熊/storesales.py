import os
import pandas as pd
file_root=os.path.dirname(os.path.realpath('storesales.py'))
file_root=file_root.replace('\\','/')
print('文件夹路径是'+file_root)
store=pd.DataFrame()
for r,d,f in os.walk(f'{file_root}/销售数据-门店数据源'):
    for s in f:
        df_xinzeng=pd.read_excel(f'{r}/{s}',skiprows=[0])
        store=pd.concat([store,df_xinzeng])
#取门店数据的需要的列
data=store[['日期','门店名称','线下支付金额','线上支付金额']]
data=data.loc[(data['门店名称']!='数云小店')&(data['门店名称'].str.contains('联营')==False)&(data['门店名称'].str.contains('虚拟门店')==False)]
#日度门店数据
data=data.rename(columns={'线下支付金额':'门店销售额','线上支付金额':'门店用户线上销售额'})
data['门店名称']=data['门店名称'].fillna('总计')

#月度门店销售额
data_month=data.groupby(by=['门店名称']).aggregate({'门店销售额':'sum','门店用户线上销售额':'sum'}).reset_index()
data_month['线上交易/门店交易']=(data_month['门店用户线上销售额']/data_month['门店销售额']).apply(lambda x: '%.2f%%' % (x*100))
data_month['线上交易/总门店交易']=(data_month['门店用户线上销售额']/(data_month['门店用户线上销售额']+data_month['门店销售额'])).apply(lambda x: '%.2f%%' % (x*100))
#读取小程序数据源
xiaochengxu=pd.DataFrame()
for r,d,f in os.walk(f'{file_root}/销售数据-小程序数据源'):
    for s in f:
        df_xinzeng=pd.read_excel(f'{r}/{s}',skiprows=[0])
        xiaochengxu=pd.concat([xiaochengxu,df_xinzeng])

#日度数据

#小程序销售额
data_online=xiaochengxu[['日期','支付金额']]
data_online=data_online.rename(columns={'支付金额':'小程序销售额'})
#小程序销售额与门店销售额合并在一张表
mer=pd.merge(data,data_online,on='日期',how='left')
#字段排序、值排序
mer=mer[['日期','门店名称','小程序销售额','门店销售额','门店用户线上销售额']]
mer=mer.sort_values(['门店销售额','门店用户线上销售额'],ascending=[True,True])
#把小程序销售额合并单元格
result=mer.pivot_table(values=['门店销售额','门店用户线上销售额'],index=['日期','小程序销售额','门店名称'])
result['线上交易/门店交易']=(result['门店用户线上销售额']/result['门店销售额']).apply(lambda x: '%.2f%%' % (x*100))
result['线上交易/总门店交易']=(result['门店用户线上销售额']/(result['门店用户线上销售额']+result['门店销售额'])).apply(lambda x: '%.2f%%' % (x*100))
result=result[['门店销售额','门店用户线上销售额','线上交易/门店交易','线上交易/总门店交易']]


#取数据源最大最小日期
calendar=data_online[['日期']]
calendar=calendar[calendar['日期']!='总计']#删除总计行
ma=calendar['日期'].max()
mi=calendar['日期'].min()


with pd.ExcelWriter(f'{file_root}/各门店每日销售数据{mi}-{ma}.xlsx') as writer:
    result.to_excel(writer, sheet_name='按天_门店引流关联占比门店',merge_cells=True)

    data_month.to_excel(writer, sheet_name='按月_门店引流关联占比门店',index=False,merge_cells=True)


