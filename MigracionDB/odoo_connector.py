# -*- coding:utf-8 -*-

import xmlrpc.client as xmlrpclib
odoo_srv    = 'http://149.56.182.45:8069'
odoo_db     = 'acotese'
odoo_usr    = 'admin'
odoo_pw     = 'gui0d00#'

class Odoo_connector:

    def __init__(self, url=odoo_srv, db=odoo_db, usr=odoo_usr, pwd=odoo_pw):
        #connection parameters
        self.url = url
        self.db = db
        self.username = usr
        self.password = pwd
        #self.model=model
        #self.domain = domain

        try:
            #test connection
            self.common = xmlrpclib.ServerProxy('%s/xmlrpc/2/common' % self.url)
            print(self.common.version())

            #connection authentication
            self.uid = self.common.authenticate(self.db, self.username, self.password, {})
            print("uid:"+str(self.uid))

            #connection to api
            self.models=xmlrpclib.ServerProxy('%s/xmlrpc/2/object' % self.url)
	    #print ("*" * 45)
            #self.ids = self.models.execute_kw(self.db, self.uid, self.password,self.model, 'search',self.domain)
            print("---INFO: odoo connection established.")
        except:
            print("***ALERT: error with odoo connection")

    def search(self, model, domain):
        ids = self.models.execute_kw(self.db, self.uid, self.password,
            model, 'search', domain)    
        return ids

    def search_ramo_type(self, model, domain):
        ids = self.models.execute_kw(self.db, self.uid, self.password,
            model, 'search_read', domain, {'fields': ['type']} )    
        return ids    

    def create(self, model, values):
        print('esto resive para crear------------->',model,values)
        id = self.models.execute_kw(self.db, self.uid, self.password, model, 'create', values)
        return id

        
