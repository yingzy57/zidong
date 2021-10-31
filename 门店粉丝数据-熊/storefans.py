import os
import pandas as pd
import json
import requests
import datetime

file_root=os.path.dirname(os.path.realpath('storefans.py'))
file_root=file_root.replace('\\','/')
print('文件夹路径是'+file_root)
#读取小程序数据源，小程序入会数
xiaochengxu=pd.DataFrame()
for r,d,f in os.walk(f'{file_root}/销售数据-门店数据源'):
    for s in f:
        df_xinzeng=pd.read_excel(f'{r}/{s}',skiprows=[0])
        xiaochengxu=pd.concat([xiaochengxu,df_xinzeng])
xiaochengxu=xiaochengxu[['日期','门店名称','新增会员数']]
xiaochengxu=xiaochengxu.rename(columns={'新增会员数':'小程序入会数'})
#维护门店名称
xiaochengxu['门店名称']=xiaochengxu['门店名称'].str.replace('华东区', '').str.replace('华南区', '').str.replace('华中区', '').str.replace('华北区', '')
#得到小程序入会数按店按天的结果
xiaochengxu_pot=xiaochengxu.pivot_table(values='小程序入会数',index=['日期','门店名称']).reset_index()
xiaochengxu_pot=xiaochengxu_pot.loc[(xiaochengxu_pot['门店名称']!='数云小店')&(xiaochengxu_pot['门店名称'].str.contains('虚拟门店')==False)]

#读取企微新增数据源，获取企微新增人数
qiwei=pd.DataFrame()
for r,d,f in os.walk(f'{file_root}/加粉数据-客户列表'):
    for s in f:
        df_xinzeng=pd.read_excel(f'{r}/{s}',skiprows=[0,1,2])
        qiwei=pd.concat([qiwei,df_xinzeng])
qiwei=qiwei[['客户名称','添加人','添加人帐号','添加人所属部门','添加时间']]
qiwei['添加时间']=pd.to_datetime(qiwei['添加时间'])
qiwei['日期']=qiwei['添加时间'].dt.date
#判断应该计入的门店
qiwei=qiwei.loc[(qiwei['添加人所属部门'].str.contains('华中区')==True)|(qiwei['添加人所属部门'].str.contains('华南区')==True)| (qiwei['添加人所属部门'].str.contains('华东区')==True)| (qiwei['添加人所属部门'].str.contains('华北区')==True)]
#剔除不应该计入的门店
qiwei=qiwei.loc[(qiwei['添加人所属部门']!='数云小店')&(qiwei['添加人所属部门'].str.contains('虚拟门店')==False)]
#根据添加人所属部门得到门店名称
qiwei['企微门店名称']=qiwei['添加人所属部门'].str.split('/',expand=True)[2].str.split('[',expand=True)[0]
#透视出企微新增人数按店按天的结果
qiwei_pot=qiwei.pivot_table(values='客户名称',index=['企微门店名称','日期'],aggfunc='count').reset_index()
qiwei_pot=qiwei_pot.rename(columns={'客户名称':'企微新增人数'})

#获取群成员新增人数
#根据时间戳获取时间格式的函数
def gettime(timeStamp):
    dateArray = datetime.datetime.fromtimestamp(timeStamp)
    jointime = dateArray.strftime("%Y/%m/%d %H:%M:%S")
    return jointime
#微信群名单，检查是否有未维护链接的群聊，需要链接全部维护爬取全部数据
qun=pd.read_excel((f'{file_root}/加粉数据-每日维护数据源/客户群列表.xlsx'))
qunroot=pd.read_excel((f'{file_root}/加粉数据-每日维护数据源/门店名称&群聊信息表.xlsx'),sheet_name='门店群聊链接')
qun=qun.loc[(qun['群名'].str.contains('宠粉')==True)&(qun['群主'].str.contains('babycare专属顾问云小默')==True)]
chaji = pd.concat([qun, qunroot, qunroot]).drop_duplicates(['群名'],keep=False)
jiaoji=pd.merge(qun[['群名']],qunroot,on='群名')
#空dataframe用来存储每一个群的群成员明细
merbers=pd.DataFrame()
for url in jiaoji['链接']:

#记得修改cookie，企微

    headers = {'Accept': 'application/json',
               'Cookie': 'pgv_pvid=8702423126; wwrtx.i18n_lan=zh; wwrtx.c_gdpr=0; _ga=GA1.2.2138374076.1633767087; tvfe_boss_uuid=27d6487bba46acf5; RK=knb1wYKfEH; ptcz=cc45513a90bad0e91f05faddce15551691359d1a39520a99e517ab760f0cbaab; wwrtx.ref=direct; wwrtx.refid=35304162502219757; _gid=GA1.2.332190109.1637113020; wwrtx.d2st=a9317568; wwrtx.sid=V_CVeF-W8pcgpWYFEnEoeQv0n1R_K9g2tiHtyW_6gagvfoYAf3j5kBRE5min6rGL; wwrtx.ltype=1; wwrtx.vst=G0sqHOOzYMaZ6K7jzWfXJg_uQ-ble1B6RALjgpSHJM2pL0ezBxWPPIm1gAT_ySm4RTPHXEA9340UTnibPRJOsTOMeYZ9bicvB8aNJRh1jDLIH_SnQUllntLu2QEFS28h1vWR2EwwTeHCPqo-J5ouEyQ13xojfNAo1BbPN14OFR0cl8YQrOPUtbn8OKP5msxej46KuXaiRjq8wvZGMC75IrtKh-rFhOVxa2xH33Oc3lVCcLQl2gRHqlxRSIsMS1oYRBcpYpA5FPxhark3NmN--A; wwrtx.vid=1688857201249092; wxpay.corpid=1970325100456971; wxpay.vid=1688857201249092; wwrtx.cs_ind=; wwrtx.logined=true; _gat=1'}
    html = requests.get(url, headers=headers)
    if html.status_code == 200:
        res = json.loads(html.text)
        data = res.get('data').get('members')
        xinzeng_members = pd.DataFrame(data)
        xinzeng_members['jointime']= xinzeng_members['jointime'].map(lambda x :gettime(x) )
        xinzeng_members['jointime']=pd.to_datetime(xinzeng_members['jointime'])
        xinzeng_members['joindate']=xinzeng_members['jointime'].dt.date
        qun_name=jiaoji.loc[jiaoji['链接']==url]['群名']
        xinzeng_members['群名']=qun_name.values[0]
        merbers = pd.concat([merbers, xinzeng_members])
        merbers['群聊门店名称']=merbers['群名'].str.replace('babycare', '').str.split('宠粉',expand=True)[0]
    else :
        print(html.text)
#透视出群成员新增人数按店按天的结果
merbers_pot=merbers.pivot_table(values='name',index=['群聊门店名称','joindate'],aggfunc='count').reset_index()
merbers_pot=merbers_pot.rename(columns={'name':'群新增人数','joindate':'日期'})
merbers_pot=merbers_pot.loc[(merbers_pot['群聊门店名称']!='数云小店')&(merbers_pot['群聊门店名称'].str.contains('虚拟门店')==False)]

name_check=pd.read_excel((f'{file_root}/加粉数据-每日维护数据源/门店名称&群聊信息表.xlsx'),sheet_name='门店名称对照表')
#把店名统一
qiwei_result=pd.merge(qiwei_pot,name_check,on='企微门店名称')[['日期','门店名称','企微新增人数']]
members_result=pd.merge(merbers_pot,name_check,on='群聊门店名称')[['日期','门店名称','群新增人数']]
#把3张按店按天的结果合并,准备日期格式统一为字符串
qiwei_result['日期']=qiwei_result['日期'].apply(str)
members_result['日期']=members_result['日期'].apply(str)
#把3张按店按天的结果合并
mer=pd.merge(xiaochengxu_pot,qiwei_result,on=['日期','门店名称'],how='left')
data=pd.merge(mer,members_result,on=['日期','门店名称'],how='left')
data['入群占比']=(data['群新增人数']/data['企微新增人数']).apply(lambda x: '%.0f%%' % (x*100))
data=data.sort_values(by=['门店名称','日期'])
data_daily_store=data.pivot_table(values=['小程序入会数','企微新增人数','群新增人数'],index=['门店名称','日期'])
data_daily_store['入群占比']=(data_daily_store['群新增人数']/data_daily_store['企微新增人数']).apply(lambda x: '%.0f%%' % (x*100))

data_daily=data.pivot_table(values=['小程序入会数','企微新增人数','群新增人数'],index='日期',aggfunc='sum')
data_daily['入群占比']=(data_daily['群新增人数']/data_daily['企微新增人数']).apply(lambda x: '%.0f%%' % (x*100))

data_store=data.pivot_table(values=['小程序入会数','企微新增人数','群新增人数'],index='门店名称',aggfunc='sum')
data_store['入群占比']=(data_store['群新增人数']/data_store['企微新增人数']).apply(lambda x: '%.0f%%' % (x*100))


#取数据源最大最小日期
calendar=xiaochengxu[['日期']]
calendar=calendar[calendar['日期']!='总计']#删除总计行
ma=calendar['日期'].max()
mi=calendar['日期'].min()

with pd.ExcelWriter(f'{file_root}/各门店每日粉丝数据{mi}-{ma}.xlsx') as writer:
    data_daily.to_excel(writer, sheet_name='按天',index=True)
    data_store.to_excel(writer, sheet_name='按店',index=True)
    data_daily_store.to_excel(writer, sheet_name='按店按天',index=True)
    chaji.to_excel(writer, sheet_name='未维护链接的群聊',index=False)

