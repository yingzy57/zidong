
#读取有赞结算订单表
fill_root=r'E:\9-python\zidong\昵称匹配订单-许路\有赞结算订单表'
js=pd.DataFrame()
for r,d,f in os.walk(fill_root):
    for s in f:
        df_xinzeng=pd.read_excel(f'{r}/{s}')
        js=pd.concat([js,df_xinzeng])
js['订单日']=js['下单时间'].dt.day
js['订单月份']=js['下单时间'].dt.month
js['订单月份']=js['订单月份'].apply(str)+'月'
#根据订单金额和结算状态做筛选
js=js[(js['成交金额']!=0)&(js['结算状态']!='待付款')&(js['结算状态']!='订单关闭')&(js['结算状态']!='不结算,全额退款')]
#读取需要匹配订单的好友名单（只有昵称可用）
file_root=r'E:\9-python\zidong\昵称匹配订单-许路\好友名单'
mingdan=pd.DataFrame()
for r,d,f in os.walk(file_root):
    for s in f:
        df_xinzeng=pd.read_excel(f'{r}/{s}')
        huodongdate=s[:s.rfind(".")]
        mingdan=pd.concat([mingdan,df_xinzeng])


#好友名单添加成为好友的月份列
mingdan['助力时间']=pd.to_datetime(mingdan['助力时间'])
mingdan['客户月份']=mingdan['助力时间'].dt.month
mingdan['客户月份']=mingdan['客户月份'].apply(str)+'月'
#好友名单昵称去重
mingdan=mingdan.drop_duplicates(subset=['昵称'])
#根据昵称匹配有赞订单
mer=pd.merge(mingdan,js,left_on='昵称',right_on='客户昵称')
#得到计算下单用户数的唯一值
mer['下单人数']=mer['客户月份']+mer['订单月份']+mer['客户手机号'].apply(str)
#下单用户数
pivot_count=mer.pivot_table(values='下单人数',index=['客户月份','订单月份'],aggfunc=lambda x: len(x.unique()),fill_value=0,margins=False).reset_index()
#订单量和成交金额
gro=mer.groupby(['客户月份','订单月份']).aggregate({'成交金额':'sum','订单号':'count'}).reset_index()
gro=gro.rename(columns={'订单号':'订单数'})
#合并 下单用户数和订单量&成交金额
data=pd.merge(gro,pivot_count,on=['客户月份','订单月份'],how='left')

#用户在成为好友以后的7天和30天的订单
huodongdate=pd.to_datetime(huodongdate)
days7=huodongdate+ datetime.timedelta(7)
days30=huodongdate+ datetime.timedelta(30)
js7days=js.loc[(js['下单时间']<days7)&(js['下单时间']>=huodongdate)]
js30days=js.loc[(js['下单时间']<days30)&(js['下单时间']>=huodongdate)]
#用户在成为好友以后的7天下单次数
mer7days=pd.merge(mingdan,js7days,left_on='昵称',right_on='客户昵称')
gro7days=mer7days.groupby(['昵称']).aggregate({'成交金额':'sum','订单号':'count'}).reset_index()
gro7days=gro7days.rename(columns={'成交金额':'7日成交金额','订单号':'7日订单数'})
#用户在成为好友以后的30天下单次数
mer30days=pd.merge(mingdan,js30days,left_on='昵称',right_on='客户昵称')
gro30days=mer30days.groupby(['昵称']).aggregate({'成交金额':'sum','订单号':'count'}).reset_index()
gro30days=gro30days.rename(columns={'成交金额':'30日成交金额','订单号':'30日订单数'})
#把下单次数合并到名单明细表中
hebing30days=pd.merge(mingdan,gro30days[['昵称','30日订单数','30日成交金额']],on='昵称',how='left')
hebing=pd.merge(hebing30days,gro7days[['昵称','7日订单数','7日成交金额']],on='昵称',how='left')

#从数据库查询是否新用户，新用户活动前首单时间，数据库至少需要更新到活动日期
yconnect = create_engine('mysql+mysqldb://root:root@localhost:3306/bbc?charset=utf8mb4')  #建立连接
old=pd.read_sql_query('SELECT nickname as "昵称",min( createtime ) AS "活动前首单时间" FROM payorderjs WHERE amount!=0    and settlement_status NOT IN ( "待付款", "订单关闭", "不结算,全额退款")   group by nickname',yconnect)
old=old.loc[old['活动前首单时间']<huodongdate]
#匹配判断新老用户
mingxi=pd.merge(hebing,old,left_on='昵称',right_on='昵称',how='left')
#根据活动前首单时间不为null判断为老用户
def neworold(a):
    if '0' not in a:
        return '新用户'
    elif '0' in a:
        return '老用户'
mingxi['活动前首单时间']=mingxi['活动前首单时间'].apply(str)
mingxi['新老用户']=mingxi.apply(lambda x : neworold(x['活动前首单时间']),axis=1)
#活动后首单时间
new_shoudan=mer.sort_values('下单时间',ascending=True)#下单时间升序
new_shoudan.drop_duplicates(subset='昵称',keep='first',inplace=True)
new_shoudan=new_shoudan.rename(columns={'下单时间':'活动后首单时间'})

#活动后首单时间添加到明细表中
mingxi_result=pd.merge(mingxi,new_shoudan[['昵称','活动后首单时间']],on='昵称',how='left')

with pd.ExcelWriter(r'E:\9-python\zidong\昵称匹配订单-许路\昵称匹配订单.xlsx') as writer:
    data.to_excel(writer, sheet_name='统计结果',index=False)
    mingxi_result.to_excel(writer, sheet_name='明细',index=False)#写入excel






