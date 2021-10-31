import os
import pandas as pd
import numpy as np
file_root=os.path.dirname(os.path.realpath('sucaidata.py'))
file_root=file_root.replace('\\','/')
print('文件夹路径是'+file_root)
#读取数据源
quan=pd.DataFrame()
qun=pd.DataFrame()
for r,d,f in os.walk(f'{file_root}/数据源'):
    for s in f :
        if '素材商品清单' in s :
            qingdan=pd.read_excel(f'{file_root}/数据源/{s}')
        elif 'goods' in s and 'Goods_youzan' not in s :
            goods=pd.read_csv(f'{file_root}/数据源/{s}',encoding='GB18030')
            calendar=goods[['日期']]
            goods=goods[['商品名称','浏览量','访客数','付款人数','付款商品件数','商品付款金额']]
        elif'Goods_youzan' in s :
            goods_youzan=pd.read_csv(f'{file_root}/数据源/{s}')
        elif '商品维度结算报表' in s :
            js=pd.read_excel(f'{file_root}/数据源/{s}')
        elif '销售' in s :
            team=pd.read_excel(f'{file_root}/数据源/{s}')
        elif '发朋友圈' in s :
            xin=pd.read_excel(f'{file_root}/数据源/{s}')
            quan=pd.concat([quan,xin])
        elif '导出精准推送任务记录' in s :
            x=pd.read_excel(f'{file_root}/数据源/{s}')
            qun=pd.concat([qun,x])

#取数据源最大最小日期
ma=calendar['日期'].max()
mi=calendar['日期'].min()


#处理发群数
qun_split=qun['推送结果'].str.split('/',expand=True)
qun['推送成功']=qun_split[0]
qun['推送失败']=qun_split[1]
qun['推送成功']=qun['推送成功'].apply(int)
qun['推送失败']=qun['推送失败'].apply(int)
qun['群成员数量']=qun['推送成功']+qun['推送失败']
#团队维度
qun_sum=qun.groupby('推送类型').aggregate({'推送成功':'sum','群成员数量':'sum'})
#销售维度
qun_xiaoshou=qun.groupby('创建子账号').aggregate({'推送成功':'sum','群成员数量':'sum'}).reset_index()
qun_xiaoshou=qun_xiaoshou.rename(columns={'创建子账号':'姓名','推送成功':'群推广数量'})
#团队维度，把计算的发群数写到每个商品的行
q=qun_sum['推送成功'].values
m=qun_sum['群成员数量'].values
qingdan['群推广数量']=int(q)
qingdan['群成员数量']=int(m)


#处理发朋友圈数据
#根据一列内容生成新的一列
def getgoodsname(a):
    for i,v  in qingdan['文案关键字'].iteritems():
        if v in a:
            goodsname=qingdan.loc[qingdan['文案关键字']==v]['商品名称']
            goodsname=goodsname.drop_duplicates(keep='first')
            return goodsname
#在朋友圈数据中添加商品名称列
quan['商品名称']=quan.apply(lambda x : getgoodsname(x['文本']),axis=1)
#只取清单中有对应商品的朋友圈数据
quan=quan[quan['商品名称'].notnull()==True]
#只取发送成功的朋友圈
quan=quan[quan['发送状态']=='发送成功']
#计算商品发圈数量，团队维度
quan_res=quan.groupby(['商品名称']).aggregate({'文本':'count'})
quan_res=quan_res.rename(columns={'文本':'朋友圈个号推广数量'})
#计算商品发圈数量，销售维度
quan_xiaoshou=quan.groupby(['操作客服','商品名称']).aggregate({'文本':'count'}).reset_index()
quan_xiaoshou=quan_xiaoshou.rename(columns={'文本':'朋友圈个号推广数量','操作客服':'姓名'})
#把发圈数量合并到商品清单中,得到guodu表
guodu=pd.merge(qingdan,quan_res,on='商品名称',how='left')


#处理商城导出的商品转化表,得到guodu1
goods=goods.groupby(['商品名称']).aggregate({'浏览量':'sum','访客数':'sum','付款人数':'sum','付款商品件数':'sum','商品付款金额':'sum'}).reset_index()
goods['单品转化率']=goods['付款人数']/goods['访客数']
goods['商品名称']=goods['商品名称'].str.replace('~','').str.replace(' ','')
#处理商品清单
qingdan['商品名称']=qingdan['商品名称'].str.replace('~','').str.replace(' ','')
#从商品表中提取出待跟踪数据的商品，得到中间表
guodu1=pd.merge(guodu,goods,on='商品名称',how='left')
guodu1=guodu1.rename(columns={'付款人数':'支付人数','付款商品件数':'支付件数','商品付款金额':'支付金额'})


#处理商品维度订单表，剔除买家未付款订单
goods_youzan=goods_youzan[['订单号','买家手机号','商品名称','商品数量','买家付款时间','商品实际成交金额']]
goods_youzan=goods_youzan[goods_youzan['买家付款时间'].notnull()==True]
#通过订单号到结算表拿到订单的销售归属
js=js[['订单号','分销员昵称']]
js=js.drop_duplicates(subset=['订单号'])
dingdan=pd.merge(goods_youzan,js,on='订单号',how='left')
#通过对照表拿到订单的所属部门
dingdan=pd.merge(dingdan,team,left_on='分销员昵称',right_on='姓名',how='left').reset_index()
dingdan=dingdan[(dingdan['部门']=='天猫销售组')|(dingdan['部门']=='京东销售组')]
#处理结算报表,得到区分天猫京东团队的支付人数和件数

#下单人数，团队维度
pay_renshu=dingdan.pivot_table(values='买家手机号',index=['商品名称'],columns=['部门'],aggfunc=lambda x: len(x.unique()),fill_value=0,margins=False).reset_index()
pay_renshu=pay_renshu.rename(columns={'天猫销售组':'天猫支付人数','京东销售组':'京东支付人数'})
#商品件数，团队维度
pay_jianshu=dingdan.pivot_table(values='商品数量',index=['商品名称'],columns=['部门'],aggfunc='sum',fill_value=0,margins=False).reset_index()
pay_jianshu=pay_jianshu.rename(columns={'天猫销售组':'天猫支付件数','京东销售组':'京东支付件数'})
#支付金额，团队维度
pay_amount=dingdan.pivot_table(values='商品实际成交金额',index=['商品名称'],columns=['部门'],aggfunc='sum',fill_value=0,margins=False).reset_index()
pay_amount=pay_amount.rename(columns={'天猫销售组':'天猫支付金额','京东销售组':'京东支付金额'})
#下单人数和商品件数、支付金额合并,团队维度
dingdan_res=pd.merge(pay_renshu,pay_jianshu,on='商品名称')
dingdan_res=pd.merge(dingdan_res,pay_amount,on='商品名称')

#下单人数，销售维度
pay_renshu_xiaoshou=dingdan.pivot_table(values='买家手机号',index=['部门','姓名','商品名称'],aggfunc=lambda x: len(x.unique()),fill_value=0,margins=False).reset_index()
pay_renshu_xiaoshou=pay_renshu_xiaoshou.rename(columns={'买家手机号':'支付人数'})
#商品件数，销售维度
pay_jianshu_xiaoshou=dingdan.pivot_table(values='商品数量',index=['部门','姓名','商品名称'],aggfunc='sum',fill_value=0,margins=False).reset_index()
pay_jianshu_xiaoshou=pay_jianshu_xiaoshou.rename(columns={'商品数量':'支付件数'})
#支付金额，销售维度
pay_amount_xiaoshou=dingdan.pivot_table(values='商品实际成交金额',index=['部门','姓名','商品名称'],aggfunc='sum',fill_value=0,margins=False).reset_index()
pay_amount_xiaoshou=pay_amount_xiaoshou.rename(columns={'商品实际成交金额':'支付金额'})
#下单人数和商品件数、支付金额合并,销售维度
dingdan_xiaoshou=pd.merge(pay_renshu_xiaoshou,pay_jianshu_xiaoshou,on=['部门','姓名','商品名称'])
dingdan_xiaoshou=pd.merge(dingdan_xiaoshou,pay_amount_xiaoshou,on=['部门','姓名','商品名称'])



#把商城数据和天猫京东数据合并得到guodu1表，得到guodu2
guodu2=pd.merge(guodu1,dingdan_res,on='商品名称',how='left')
guodu2['天猫支付人数']=guodu2['京东支付人数'].replace(np.NaN,0,regex=True)
guodu2['京东支付人数']=guodu2['京东支付人数'].replace(np.NaN,0,regex=True)
guodu2['天猫支付件数']=guodu2['天猫支付件数'].replace(np.NaN,0,regex=True)
guodu2['京东支付件数']=guodu2['京东支付件数'].replace(np.NaN,0,regex=True)
guodu2['天猫支付金额']=guodu2['天猫支付金额'].replace(np.NaN,0,regex=True)
guodu2['京东支付金额']=guodu2['京东支付金额'].replace(np.NaN,0,regex=True)

#计算商城占比
zhifu_notnull=guodu2[guodu2['支付人数']!=0]
zhifu_isnull=guodu2[guodu2['支付人数']==0]

#人数
zhifu_notnull['天猫支付人数商城占比']=(zhifu_notnull['天猫支付人数']/zhifu_notnull['支付人数']).apply(lambda x: '%.2f%%' % (x*100))
zhifu_notnull['京东支付人数商城占比']=(zhifu_notnull['京东支付人数']/zhifu_notnull['支付人数']).apply(lambda x: '%.2f%%' % (x*100))
#件数
zhifu_notnull['天猫支付件数商城占比']=(zhifu_notnull['天猫支付件数']/zhifu_notnull['支付件数']).apply(lambda x: '%.2f%%' % (x*100))
zhifu_notnull['京东支付件数商城占比']=(zhifu_notnull['京东支付件数']/zhifu_notnull['支付件数']).apply(lambda x: '%.2f%%' % (x*100))
#金额
zhifu_notnull['天猫支付金额商城占比']=(zhifu_notnull['天猫支付金额']/zhifu_notnull['支付金额']).apply(lambda x: '%.2f%%' % (x*100))
zhifu_notnull['京东支付金额商城占比']=(zhifu_notnull['京东支付金额']/zhifu_notnull['支付金额']).apply(lambda x: '%.2f%%' % (x*100))
#
zhifu_isnull['天猫支付人数商城占比']=0
zhifu_isnull['京东支付人数商城占比']=0
zhifu_isnull['天猫支付件数商城占比']=0
zhifu_isnull['京东支付件数商城占比']=0
zhifu_isnull['天猫支付金额商城占比']=0
zhifu_isnull['京东支付金额商城占比']=0
#团队的结果
tuandui=pd.concat([zhifu_notnull,zhifu_isnull])
tuandui['推广时间']= str(mi) +'-'+ str(ma)
tuandui=tuandui.sort_values('素材序号',ascending=True)
#销售的结果
mer=pd.merge(dingdan_xiaoshou,quan_xiaoshou,on=['商品名称','姓名'],how='outer')#合并发圈数
mer=pd.merge(mer,qun_xiaoshou,on=['姓名'],how='outer')#合并发群数
xiaoshou=pd.merge(mer,qingdan,on=['商品名称'],how='outer')
xiaoshou=xiaoshou.rename(columns={'群推广数量_x':'群推广数量','群成员数量_x':'群成员数量','群推广数量_y':'群推广数量_团队','群成员数量_y':'群成员数量_团队'})
xiaoshou_notnull=xiaoshou[xiaoshou['素材序号'].notnull()==True]
xiaoshou_notnull['推广时间']= str(mi) +'-'+ str(ma)
xiaoshou_result=pd.merge(xiaoshou_notnull,dingdan_res,on='商品名称',how='left')

with pd.ExcelWriter(f'{file_root}/素材推广情况结果表{mi}-{ma}.xlsx') as writer:
    tuandui.to_excel(writer, sheet_name='每日素材团队推广效果', index=False)
    xiaoshou_result.to_excel(writer, sheet_name='每日素材销售推广效果', index=False)
    dingdan.to_excel(writer, sheet_name='订单明细', index=False)
    goods.to_excel(writer, sheet_name='商城明细', index=False)