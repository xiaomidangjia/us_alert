import json
import requests
import pandas as pd
import time
import numpy as np
import os
import re
import datetime
from requests import Request,Session
from requests.exceptions import ConnectionError,Timeout,TooManyRedirects
from dingtalkchatbot.chatbot import DingtalkChatbot

webhook = 'https://oapi.dingtalk.com/robot/send?access_token=a9789d2739819eab19b07dcefe30df3fcfd9f815bf198ced54c71c557f09e7d9'
xiaoding = DingtalkChatbot(webhook)
session = Session()

addresses = ['bc1q0qfzuge7vr5s2xkczrjkccmxemlyyn8mhx298v','bc1qm34lsc65zpw79lxes69zkqmk6ee3ewf0j77s3h']
types = [1,2]

while True:
    tot_values = []
    for i in range(len(addresses)):
        sub_address = addresses[i]
        sub_type = types[i]
        # 未确认的交易中是否有 监控的地址
        url_1 = 'https://services.tokenview.io/vipapi/pending/ntx/btc/' + sub_address + '?apikey=5u0dNQPd55eoEwFPwF2A'
        logo = 0
        while logo == 0:
            try:
                response = session.get(url_1)
                data = json.loads(response.text)
                #print(data)
                code = str(data['code'])
                if code == '1':
                    ins = data['data'][0]['inputs']
                    addrs = []
                    values = []
                    for j in range(len(ins)):
                        dic = ins[j]
                        addrs.append(dic['address'])
                        values.append(dic['value'])
                    df = pd.DataFrame({'address':addrs,'value':values})
                    df_group = df.groupby(['address'],as_index=False)['value'].sum()
                    #print(df_group)
                    sub_df_group = df_group[df_group.address == sub_address]
                    now_time_1 = str(datetime.datetime.now())[0:19]
                    if len(sub_df_group)==1:
                        #读取文件，看是否已经报过
                        alter_df = pd.read_csv('pending_alter.csv')
                        alter_df['time'] = alter_df['date'].apply(lambda x:str(x)[0:13])
                        time_now = str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))[0:13]
                        sub_alter_df = alter_df[(alter_df.address==sub_address) &(alter_df.time==time_now)]
                        # 开始报警
                        if len(sub_alter_df) == 0:
                            sub_df_group = sub_df_group.reset_index(drop=True)
                            value = round(sub_df_group['value'][0],2)
                            if sub_type == 1:
                                alert = 'USA政府持币地址'
                                #推送钉钉
                                #xiaoding.send_text(msg=content,is_auto_at=True)
                                title_msg = '【警报 — %s】'%(alert)
                                text_msg = '%s该地址有%s个BTC正在转出【pending】,点击链接查看。'%(now_time_1,value)

                                msg_url = 'https://www.oklink.com/cn/btc/address/' + sub_address
                                xiaoding.send_link(title=title_msg, text=text_msg, message_url=msg_url ,pic_url= 'http://ruusug320.hn-bkt.clouddn.com/oklink_1.jpg')
                            else:
                                alert = '疑似门头沟持币地址'
                                #推送钉钉
                                #xiaoding.send_text(msg=content,is_auto_at=True)
                                title_msg = '【警报 — %s】'%(alert)
                                text_msg = '%s该地址有%s个BTC正在转出【pending】,点击链接查看。'%(now_time_1,value)

                                msg_url = 'https://www.oklink.com/cn/btc/address/' + sub_address
                                xiaoding.send_link(title=title_msg, text=text_msg, message_url=msg_url ,pic_url= 'http://ruusug320.hn-bkt.clouddn.com/oklink_1.jpg')
                            logo = 1
                            # 更新文件
                            next_df = pd.concat([alter_df[['date','address']],pd.DataFrame({'date':time_now,'address':sub_address},index=[0])])
                            next_df['date'] = pd.to_datetime(next_df['date'])
                            next_df.to_csv('pending_alter.csv',index=False)
                        else:
                            logo = 1
                    else:
                        logo = 1

                else:
                    logo = 1
            except:
                logo = 0
        # 地址余额是否有变动 监控的地址
        url_2 = 'https://services.tokenview.io/vipapi/addr/b/btc/' + sub_address + '?apikey=5u0dNQPd55eoEwFPwF2A'
        logo = 0
        while logo == 0:
            try:
                response = session.get(url_2)
                data = json.loads(response.text)
                #print(data)
                code = str(data['code'])
                now_value = round(float(data['data']),2)
                if code == '1':
                    tot_values.append(now_value)
                    # 读取上一时刻的余额数据
                    name = 'pre_data_' + str(i+1) + '.csv' 
                    pre_data = pd.read_csv(name)
                    pre_data['date'] = pd.to_datetime(pre_data['date'])
                    pre_data = pre_data.sort_values(by='date')
                    pre_data = pre_data.reset_index(drop=True)
                    change = round(now_value - pre_data['value'][len(pre_data)-1],2)
                    now_time = str(datetime.datetime.now())[0:19]
                    #有btc转出时，余额变少了
                    if change < 0:
                        if sub_type == 1:
                            alert = 'USA政府持币地址'
                            #推送钉钉
                            #xiaoding.send_text(msg=content,is_auto_at=True)
                            title_msg = '【警报 — %s】'%(alert)
                            text_msg = '北京时间%s该地址有%s个BTC转出,点击链接查看。'%(now_time,str(-change))

                            msg_url = 'https://www.oklink.com/cn/btc/address/' + sub_address
                            xiaoding.send_link(title=title_msg, text=text_msg, message_url=msg_url ,pic_url= 'http://ruusug320.hn-bkt.clouddn.com/oklink_1.jpg')


                        else:
                            alert = '疑似门头沟持币地址'
                            #推送钉钉
                            #xiaoding.send_text(msg=content,is_auto_at=True)
                            title_msg = '【警报 — %s】'%(alert)
                            text_msg = '北京时间%s该地址有%s个BTC转出,点击链接查看。'%(now_time,str(-change))

                            msg_url = 'https://www.oklink.com/cn/btc/address/' + sub_address
                            xiaoding.send_link(title=title_msg, text=text_msg, message_url=msg_url ,pic_url= 'http://ruusug320.hn-bkt.clouddn.com/oklink_1.jpg')


                        logo = 1
                    #有btc转入时，余额变多了    
                    elif change > 1:
                        if sub_type == 1:
                            alert = 'USA政府持币地址'
                            #推送钉钉
                            #xiaoding.send_text(msg=content,is_auto_at=True)
                            title_msg = '【警报 — %s】'%(alert)
                            text_msg = '北京时间%s该地址有%s个BTC转入,点击链接查看。'%(now_time,str(change))
                            msg_url = 'https://www.oklink.com/cn/btc/address/' + sub_address
                            xiaoding.send_link(title=title_msg, text=text_msg, message_url=msg_url ,pic_url= 'http://ruusug320.hn-bkt.clouddn.com/oklink_1.jpg')

                        else:
                            alert = '疑似门头沟持币地址'
                            #推送钉钉
                            #xiaoding.send_text(msg=content,is_auto_at=True)
                            title_msg = '【警报 — %s】'%(alert)
                            text_msg = '北京时间%s该地址有%s个BTC转入,点击链接查看。'%(now_time,str(change))

                            msg_url = 'https://www.oklink.com/cn/btc/address/' + sub_address
                            xiaoding.send_link(title=title_msg, text=text_msg, message_url=msg_url ,pic_url= 'http://ruusug320.hn-bkt.clouddn.com/oklink_1.jpg')

                        logo = 1
                    #防止粉尘攻击
                    else:
                        logo = 1

                    #把最新数据写入csv文件中
                    date_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    df_now = pd.DataFrame({'date':date_now,'address':sub_address,'value':now_value},index=[0]) 
                    df_now = pd.concat([pre_data,df_now])
                    df_now['date'] = pd.to_datetime(df_now['date'])
                    df_now = df_now.sort_values(by='date')
                    df_now = df_now.reset_index(drop=True)
                    df_now = df_now[-10:]
                    #print(df_now)
                    df_now.to_csv(name,index=False)

                else:
                    logo = 1

            except:
                logo = 0
    #每天早上8点，下午13点，晚上21点报
    date_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    hour_now = str(date_now)[11:13]

    print(date_now,hour_now)
    if hour_now in ('08','13','21'):
        #读取文件，看是否已经报过
        alter_df = pd.read_csv('yue_alert.csv')
        alter_df['time'] = alter_df['date'].apply(lambda x:str(x)[0:13])
        sub_alter_df_1 = alter_df[(alter_df.address=='us') &(alter_df.time==date_now[0:13])] 
        now_time_2 = str(datetime.datetime.now())[0:19]
        if len(sub_alter_df_1) == 0:
            now_value_us = np.sum(tot_values[0:len(addresses)-1])
            sub_alter_df_2 = alter_df[(alter_df.address=='us')]
            sub_alter_df_2['date'] = pd.to_datetime(sub_alter_df_2['date'])
            sub_alter_df_2 = sub_alter_df_2.sort_values(by='date')
            sub_alter_df_2 = sub_alter_df_2.reset_index(drop=True)

            last_time = sub_alter_df_2['date'][len(sub_alter_df_2)-1]
            last_value_1 = sub_alter_df_2['total'][len(sub_alter_df_2)-1]
            

            if now_value_us - last_value_1 == 0:
                alert = 'USA'
                #推送钉钉
                #xiaoding.send_text(msg=content,is_auto_at=True)
                title_msg = '【每日黑天鹅监控 — %s】'%(alert)
                text_msg = '从%s到%s,该地址所有BTC余额无变化。'%(last_time,now_time_2)
                msg_url = 'https://www.oklink.com/'
                xiaoding.send_link(title=title_msg, text=text_msg, message_url=msg_url ,pic_url= 'http://ruusug320.hn-bkt.clouddn.com/oklink_1.jpg')
            else:
                alert = 'USA'
                #推送钉钉
                #xiaoding.send_text(msg=content,is_auto_at=True)
                title_msg = '【每日黑天鹅监控 — %s】'%(alert)
                text_msg = '从%s到%s,该地址余额增加%s个BTC。'%(last_time,now_time_2,str(round(now_value_us-last_value_1,2)))
                msg_url = 'https://www.oklink.com/'
                xiaoding.send_link(title=title_msg, text=text_msg, message_url=msg_url ,pic_url= 'http://ruusug320.hn-bkt.clouddn.com/oklink_1.jpg')

        sub_alter_df_3 = alter_df[(alter_df.address=='gox') &(alter_df.time==date_now[0:13])] 
        if len(sub_alter_df_3) == 0:
            now_value_gox = np.sum(tot_values[len(addresses)-1])
            sub_alter_df_4 = alter_df[(alter_df.address=='gox')]
            sub_alter_df_4['date'] = pd.to_datetime(sub_alter_df_4['date'])
            sub_alter_df_4 = sub_alter_df_4.sort_values(by='date')
            sub_alter_df_4 = sub_alter_df_4.reset_index(drop=True)

            last_time = sub_alter_df_4['date'][len(sub_alter_df_4)-1]
            last_value_2 = sub_alter_df_4['total'][len(sub_alter_df_4)-1]
            if now_value_gox - last_value_2 == 0:
                alert = 'GOX'
                #推送钉钉
                #xiaoding.send_text(msg=content,is_auto_at=True)
                title_msg = '【每日黑天鹅监控 — %s】'%(alert)
                text_msg = '从%s到%s,该地址余额无变化。'%(last_time,now_time_2)
                msg_url = 'https://www.oklink.com/cn/btc/address/1FeexV6bAHb8ybZjqQMjJrcCrHGW9sb6uF'
                xiaoding.send_link(title=title_msg, text=text_msg, message_url=msg_url ,pic_url= 'http://ruusug320.hn-bkt.clouddn.com/oklink_1.jpg')

            else:
                alert = 'GOX'
                #推送钉钉
                #xiaoding.send_text(msg=content,is_auto_at=True)
                title_msg = '【每日黑天鹅监控 — %s】'%(alert)
                text_msg = '从%s到%s,该地址余额增加%s个BTC。'%(last_time,now_time_2,str(round(now_value_gox-last_value_2,2)))
                msg_url = 'https://www.oklink.com/cn/btc/address/1FeexV6bAHb8ybZjqQMjJrcCrHGW9sb6uF'
                xiaoding.send_link(title=title_msg, text=text_msg, message_url=msg_url ,pic_url= 'http://ruusug320.hn-bkt.clouddn.com/oklink_1.jpg')  

        #把当前数据插入
        next_df_us = pd.concat([alter_df[['date','address','total']],pd.DataFrame({'date':[date_now,date_now],'address':['us','gox'],'total':[now_value_us,now_value_gox]})])
        next_df_us['date'] = pd.to_datetime(next_df_us['date'])
        next_df_us.to_csv('yue_alert.csv')  

        time.sleep(1)