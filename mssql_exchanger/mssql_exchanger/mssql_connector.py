# -*- coding:utf-8 -*-

import pyodbc

class MSSQL_Connector:

    def __init__(self,driver,server,db,user,pwd):
        self.conn = pyodbc.connect(driver+';'+server+';'+db+';'+user+';'+pwd)
        self.conn.setencoding(encoding='utf-8')

        self.connUpdate = pyodbc.connect(driver+';'+server+';'+db+';'+user+';'+pwd)
        self.connUpdate.setencoding(encoding='utf-8')
        try:
            if (self.conn!=None):
                print("---INFO: mssql connection established.")
            else:
                raise "error"
        except:
            print("***ALERT: error with mssql connection") 

    def getPO(self):

        query = "exec getPO"
        if (query!=""):
            cursor = self.conn.cursor()
            res = cursor.execute(query)
            return res
        
    def getBrand(self):
        query = "exec getBrands"
        if (query!=""):
            return self.conn.cursor().execute(query)

    def getEdition(self):
        query = "exec getEditions"
        if (query!=""):
            return self.conn.cursor().execute(query)

    def setPO(self, entryNo, po_id):
        query = "update [Insertion Buf_ LATAM] set odooId=%s where [Entry No_]=%s"%(po_id,entryNo)
        print (query)
        if (query!=""):
            cursor= self.connUpdate.cursor()
            cursor.execute(query)
            cursor.commit()
            #cursor.close()


    def select(self,query,fetchOne=False):
        self.link.execute(query)
        
        # print('este es el resultado----------------->', self.link.fetchall())
        data = self.link.fetchall()
        if fetchOne:
            return self.link.fetchone()
        else:
            return data 
        
