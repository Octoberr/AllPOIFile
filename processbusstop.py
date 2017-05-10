#coding:utf-8

import pymysql
import math
import numpy as np
import json


LIMIT=30
MAXNUMBER=10000
ZERO=0

class PRODATA:
    def connectSQL(self,sql):
        con=pymysql.connect("localhost","root","bwc123","bw",charset="utf8")
        cur=con.cursor()
        cur.execute(sql)
        res=cur.fetchall()
        cur.close()
        con.close()
        return res
    def getResList(self):
        sql="SELECT * FROM busstoppoi WHERE bdmaplat>'30.608336' AND bdmaplat<'30.727753' AND bdmaplng>'103.990825' AND bdmaplng<'104.173798'"
        # sql = "SELECT * FROM busstoppoi WHERE bdmaplat>'30.653016' AND bdmaplat<'30.667887' AND bdmaplng>'104.041627' AND bdmaplng<'104.067597'" #测试范围
        res=self.connectSQL(sql)
        orderList=list(res)
        return orderList

    def insert(self, sql):
        db = pymysql.connect("localhost", "root", "bwc123", "bw", charset="utf8")
        cur = db.cursor()
        try:
            cur.execute(sql)
            db.commit()
        except:
            fp = open('sqlerror.txt', 'w+')
            fp.write(sql.encode('utf-8')+'\n')
            print 'this data has a special string'
            fp.close()
            db.rollback()
        db.close()

    def getTheSameName(self,orderList):
        name=[]
        for element in orderList:
            name.append(element[0])
        newName=list(set(name))
        return newName
    def calcDist(self, LatA, LngA, LatB, LngB):
        ra = 6378.140  # 赤道半径 (km)
        rb = 6356.755  # 极半径 (km)
        flatten = (ra - rb) / ra  # 地球扁率
        radLatA = math.radians(LatA)
        radLngA = math.radians(LngA)
        radLatB = math.radians(LatB)
        radLngB = math.radians(LngB)
        pA = math.atan(rb / ra * math.tan(radLatA))
        pB = math.atan(rb / ra * math.tan(radLatB))
        xx = math.acos(math.sin(pA) * math.sin(pB) + math.cos(pA) * math.cos(pB) * math.cos(radLngA - radLngB))
        c1 = (math.sin(xx) - xx) * (math.sin(pA) + math.sin(pB)) ** 2 / math.cos(xx / 2) ** 2
        c2 = (math.sin(xx) + xx) * (math.sin(pA) - math.sin(pB)) ** 2 / math.sin(xx / 2) ** 2
        dr = flatten / 8 * (c1 - c2)
        distance = ra * (xx + dr) * 1000
        return distance


    def processdata(self,sameNameList):
        # print "sameList:",sameNameList
        disVec=np.zeros([len(sameNameList)-1],dtype=int)
        same=[]
        diff=[]
        newsameList=[]
        for i in range(1,len(sameNameList)):
            distance=self.calcDist(sameNameList[0][1],sameNameList[0][2],sameNameList[i][1],sameNameList[i][2])
            if distance <= LIMIT:
                disVec[i-1]=ZERO
            else:
                disVec[i-1]=MAXNUMBER
        samePoint = np.where(disVec == ZERO)
        if samePoint[0].shape[0] > 0:
            same.append(sameNameList[samePoint[0][0]+1])
        else:
            same.append(sameNameList[0])
        diffPoint = np.where(disVec == MAXNUMBER)
        for idx in diffPoint[0]:
            diff.append(sameNameList[idx+1])
        newsameList.append(same)
        newsameList.append(diff)
        # print "newsameList:",newsameList
        return newsameList




    def removeDuplicates(self,name,orderList):
        onlydata=[]
        for i in range(len(name)):
            sameName=[]
            for j in range(len(orderList)):
                if name[i]==orderList[j][0]:
                    sameName.append(orderList[j])#所有同名的都进入到sameName

            while len(sameName) >= 3:
                sameName = list(set(sameName))
                newSameName=self.processdata(sameName)
                onlydata.append(newSameName[0][0])
                sameName=newSameName[1]
                # print "重复点多的onlydata:", onlydata
            else:
                if len(sameName)==1:
                    onlydata.append(sameName[0])       #唯一的数据直接写入onlydata
                    # print "重复点只有一个的onlydata:", onlydata
                elif len(sameName)==2:
                    distance=self.calcDist(sameName[0][1],sameName[0][2],sameName[1][1],sameName[1][2])
                    if distance <= LIMIT:
                        onlydata.append(sameName[0])
                    else:
                        onlydata.append(sameName[0])
                        onlydata.append(sameName[1])
                    # print "重复点有两个的onlydata:", onlydata
                else:
                    continue
        return onlydata
    # for m in range(disVec.shape[0]):
    #     if disVec[m] <= LIMIT:
    #         disVec[m]=ZERO
    #     else:
    #         disVec[m]=MAXNUMBER
    # samePoint=np.where(disVec==ZERO)

    # diffPoin=np.where(disVec==MAXNUMBER)
    #

    def incodejs(self,rdData):
        jsonData = []
        for row in rdData:
            result = {}
            result['name'] = row[0]
            result['lat'] = row[1]
            result['lng'] = row[2]
            jsonData.append(result)
        jsondatar = json.dumps(jsonData, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        return jsondatar


def main():
    pd=PRODATA()
    orderList=pd.getResList()
    # print orderList
    # print len(orderList)
    name=pd.getTheSameName(orderList)
    # print name
    # print len(name)
    rdData=pd.removeDuplicates(name,orderList)
    # print rdData
    # print len(rdData)
    # jsondata=pd.incodejs(rdData)
    # fp=open('bus.json','w+')
    # fp.write(jsondata)
    # fp.close()
    for element in rdData:
        sql="INSERT INTO duplicatebusstoppoi(name,bdlat,bdlng) VALUES ('%s','%lf','%lf')"%(element[0],element[1],element[2])
        pd.insert(sql)
        print "insert success"

    print 'finish'
    # pd.removeDuplicates(orderList)





if __name__ == '__main__':
    main()