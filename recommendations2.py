#-*- coding: UTF-8 -*-
import csv
import MySQLdb
import time,datetime
date=0
tmpsportdic={}
tmpscenerydic={}
length=10
def loaddata():
    #sportdic={userid:{8:1,9:2,10:3}}
    #line[1]:userid
    #line[7]:tag
    #recomsport:
    #userid week time freq
    db = MySQLdb.connect("localhost", "root", "root", "respire",charset="utf8")
    cursor = db.cursor()
    sql = "SELECT * from datanow"
    try:
        cursor.execute(sql)
        results=cursor.fetchall()
        for line in results:
            str1 = str(line[0])
            week=datetime.datetime.strptime(str1, "%Y-%m-%d %H:%M:%S").weekday()+1
            tag=u"运动健身"
            tmp1=line[7]
            if line[7]== tag:
                start=str1.find(" ")
                end=str1.find(':')
                hour=str1[start+1:end]
                if tmpsportdic.has_key(str(line[1])):
                    oldfreq=tmpsportdic[str(line[1])]
                    if oldfreq.has_key(hour):
                        oldfreq[hour]=oldfreq[hour]+1
                    else:
                        oldfreq[hour]=1
                else:
                    freq={}
                    freq[hour]=1
                    tmpsportdic[str(line[1])]=freq
            tag2=u"旅游景点"
            if line[7]==tag2:
                start=str1.find(" ")
                end=str1.find(':')
                hour=str1[start+1:end]
                if tmpscenerydic.has_key(str(line[1])):
                    oldfreq=tmpscenerydic[str(line[1])]
                    if oldfreq.has_key(hour):
                        oldfreq[hour]=oldfreq[hour]+1
                    else:
                        oldfreq[hour]=1
                else:
                    freq={}
                    freq[hour]=1
                    tmpscenerydic[str(line[1])]=freq
    except:
            print "Error: unable to fecth data"
    return week
                    
def process(week):
    db = MySQLdb.connect("localhost", "root", "root", "respire")
    cursor = db.cursor()
    for k in tmpsportdic:
        tmp=tmpsportdic[k]
        sorttmp=sorted(tmp.iteritems(),key=lambda d:d[0],reverse=False)
        #数据3秒采集一次，默认在运动场所呆半小时以上才算运动，假如一天运动几次，记录每次运动的起始时间
        #timedic记录运动的起始时间
        time=-2
        timedic=[]
        for d in sorttmp:
            if d[1] >= length:
                if int(d[0])!=time+1:
                    timedic.append(int(d[0]))
                    time=int(d[0])
                else:
                    continue
        for t in timedic:
            sql = "select * from recomsport where userid=%s AND week=%s AND time=%s"
            try:
                params = (k, week, t)
                cursor.execute(sql, params)
                results = cursor.fetchall()
                if len(results) == 0:
                    sql = 'INSERT INTO recomsport(userid,week,time,freq)VALUES (%s,%s,%s,%s)'
                    try:
                        freq = 1
                        value = [k, week, t, freq]
                        cursor.execute(sql, value)
                        db.commit()
                    except:
                        db.rollback()
                else:
                    sql = "UPDATE recomsport SET freq=freq+1 where userid=\"%s\" and week=%d and time=%d"%(k,week,t)
                    try:
                        cursor.execute(sql)
                        db.commit()
                    except:
                        db.rollback()
            except:
                print "Error: unable to fecth data"
    for k in tmpscenerydic:
        tmp = tmpscenerydic[k]
        sorttmp = sorted(tmp.iteritems(), key=lambda d: d[0], reverse=False)
        # 数据3秒采集一次，默认在运动场所呆半小时以上才算运动，假如一天运动几次，记录每次运动的起始时间
        # timedic记录运动的起始时间
        time = -2
        timedic = []
        for d in sorttmp:
            if d[1] >= length:
                if int(d[0]) != time + 1:
                    timedic.append(int(d[0]))
                    time = int(d[0])
                else:
                    continue
        for t in timedic:
            sql = "select * from recomscenery where userid=%s AND week=%s AND time=%s"
            try:
                params = (k, week, t)
                cursor.execute(sql, params)
                results = cursor.fetchall()
                if len(results) == 0:
                    sql = 'INSERT INTO recomscenery(userid,week,time,freq)VALUES (%s,%s,%s,%s)'
                    try:
                        freq = 1
                        value = [k, week, t, freq]
                        cursor.execute(sql, value)
                        db.commit()
                    except:
                        db.rollback()
                else:
                    sql = "UPDATE recomsscenery SET freq=freq+1 where userid=\"%s\" and week=%d and time=%d" % (k, week, t)
                    try:
                        cursor.execute(sql)
                        db.commit()
                    except:
                        db.rollback()
            except:
                print "Error: unable to fecth data"
def writeresult(theweek):
    #将结果写入推荐表中 recommand2和recommand1一样，都是每天更新的表
    #首先将昨天的记录删除
    db = MySQLdb.connect("localhost", "root", "root", "respire",charset="utf8")
    cursor = db.cursor()
    sql="DELETE FROM recommand2"
    try:
        cursor.execute(sql)
        db.commit()
    except:
        db.rollback()
    #插入新的结果
    sql = "select DISTINCT userid from recomscenery"
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
    except:
        print "Error: unable to fecth data"
    for line in results:
        userid=line[0]
        sql="select MAX(freq) from recomscenery where userid=%s and week=%s"
        try:
            params=(userid,theweek)
            cursor.execute(sql,params)
            res = cursor.fetchall()
            resfreq=0
            for r in res:
                resfreq=int(r[0])
                break
            sql="select time from recomscenery where userid=%s and week=%s and freq=%s"
            try:
                params=(userid,theweek,resfreq)
                cursor.execute(sql,params)
                res=cursor.fetchall()
                # 插入推荐表
                for r in res:
                    restime=int(r[0])
                    sql = 'INSERT INTO recommand2(hour,tag,userid)VALUES (%s,%s,%s)'
                    try:

                        value = [restime,"旅游景点",userid]
                        cursor.execute(sql, value)
                        db.commit()
                    except:
                        db.rollback()
            except:
                print "Error: unable to fecth data"
        except:
            print "Error: unable to fecth data"
    # 运动场所
    sql = "select DISTINCT userid from recomsport"
    try:
        cursor.execute(sql)
        results = cursor.fetchall()
    except:
        print "Error: unable to fetch data"
    for line in results:
        userid = line[0]
        sql = "select MAX(freq) from recomsport where userid=%s and week=%s"
        try:
            params = (userid, theweek)
            cursor.execute(sql, params)
            res = cursor.fetchall()
            resfreq = 0
            for r in res:
                resfreq = int(r[0])
                break
            sql = "select time from recomsport where userid=%s and week=%s and freq=%s"
            try:
                params = (userid, theweek, resfreq)
                cursor.execute(sql, params)
                res = cursor.fetchall()
                # 插入推荐表
                for r in res:
                    restime = int(r[0])
                    sql = 'INSERT INTO recommand2(hour,tag,userid)VALUES (%s,%s,%s)'
                    try:
                        value = [restime, "运动健身", userid]
                        cursor.execute(sql, value)
                        db.commit()
                    except:
                        db.rollback()
            except:
                print "Error: unable to fecth data"
        except:
            print "Error: unable to fecth data"




def main():
    week=loaddata()
    process(week)
    writeresult(week)


if __name__ == "__main__":
    main()
