# -*- coding:utf-8 -*-

import odoo_connector
import mssql_connector
import threading

record=[]
mssql=mssql_connector.MSSQL_Connector('Driver={SQL Server}',r'Server=.\SQLEXPRESS;','Database=Integration_HN;','user=ZetinaMedsys;','password=Z3t1naM3dsys;')


def BrandToOdoo():
    oc=odoo_connector.Odoo_connector()
    
    for row in mssql.getBrand():
        partner_ids = oc.search('res.partner', [[ ['code', '=', row.partner_code] ]] )
        if partner_ids:
            partner_id = partner_ids[0]
        else:
            continue
    
        rec = {
            'partner_id': partner_id,
            'name': row.name.strip(),
            'code': row.code.strip(),
        }

        values = []
        values.append(rec)
        print (values)
        oc.create('media.partner.brand', values)
        
def EditionToOdoo():
    oc=odoo_connector.Odoo_connector()
    
    for row in mssql.getEdition():
        partner_ids = oc.search('res.partner', [[ ['code', '=', row.partner_code] ]] )
        if partner_ids:
            partner_id = partner_ids[0]
        else:
            continue
        
        rec = {
            'partner_id': partner_id,
            'name': row.code.strip(),
            'code': row.code.strip(),
            'other_name': row.name.strip(),
            'type': row.type.strip()
        }
        values = []
        values.append(rec)
        print (values)
        oc.create('media.partner.media', values)

def POtoOdoo():
    oc=odoo_connector.Odoo_connector()
    
    for row in mssql.getPO():
        
        #partner_id
        sup_no = row.supplier_no.strip()
        partner_ids = oc.search('res.partner', [[ ['code', '=', sup_no] ]] )
        #print (sup_no)
        if partner_ids:
            partner_id = partner_ids[0]
        else:
            print ('supplier not found')
            continue
        
        #Edition
        ed = row.Edition.strip()
        #print (partner_id, ' /', ed)
        eds = oc.search('media.partner.media', [[ ['partner_id', '=', partner_id],['name', '=', ed] ]] )
        if eds:
            ed_id = eds[0]
        else:
            print('edition not found:', ed)
            continue

        #to_partner_id
        customer_no = row.customer_no.strip()
        print(customer_no.strip(), '*')
        partner_ids = oc.search('res.partner', [[ ['medsys_code', '=', customer_no.strip()] ]] )
        if partner_ids:
            to_partner_id = partner_ids[0]
        else:
            print ('customer not found')
            continue

        #brand     
        brand = str(row.brand).strip()
        brands = oc.search('media.partner.brand', [[ ['partner_id', '=', to_partner_id],['code', '=', brand] ]] )
        if brands:
            brand_id = brands[0]
        else:
            print('brand not found')
            continue
        
        reinvertible = False
        if row.reinvertible == 'Reinvertible':
            reinvertible = True

        rec = {
            'name': row.po_no,
            'partner_id': partner_id,
            'media_id': ed_id,
            'to_partner_id': to_partner_id,
            'brand_id': brand_id,
            'campaing_no': row.campaing_id,
            'po_no': row.po_no,
            'date_order': row.date_order + ' 0600',
            'state': 'purchase',
            'currency_id': row.currency_id,
            'currency_rate': row.currency_rate,
            'reinvertible' : reinvertible
        }

        values = []
        values.append(rec)
        print(values)
        po_id = oc.create('purchase.order', values)

        #Now, create line
        line = {
            'product_id': 2, # Cargos por facturar a medios
            'name': 'CARGOS POR FACTURAR A MEDIOS: '+ row.po_no,
            'date_planned': row.date_order + ' 0600',
            'product_qty': 1,
            'product_uom':1,
            'price_unit': row.amount,
            'order_id': po_id,
            'taxes_id': [(6, 0, [3] )]
        }
        values= []
        values.append(line)
        print ('line==> ', values)
        oc.create('purchase.order.line', values)

        #Now, update Integration
        mssql.setPO(row.entryNo, po_id)

    threading.Timer(300.0, POtoOdoo).start()
    

def main():
    
    #BrandToOdoo()
    #EditionToOdoo()
    POtoOdoo()
    
    
# ,'res.partner', [[['is_company', '=', True], ['supplier', '=', True]]]
    
    
    
        
    #oc=odoo_connector.Odoo_connector('http://mccann.catics.online','mccann','admin','1mpuls@','res.partner', [[['is_company', '=', True], ['supplier', '=', True]]])
    #l=len(oc.ids)+1
    
    #fields=['code', 'name', 'street', 'city', 'phone', 'mobile', 'vat','write_date', 'customer', 'supplier']
    #print (oc.ids)

    #for num in oc.ids: #range(1,l):
    #    try:
    #        record = oc.models.execute_kw(oc.db, oc.uid, oc.password,oc.model, 'read', [num], {'fields': fields})
            #queryBuilder(record)
            #print(str(num)+": "+str(record.id))
    #        for rec in record:
    #            print (rec)
            #print (record)
           
    #    except KeyboardInterrupt:
    #        print("***ALERT: interrupted by user")
    #        break
        #except:
        #    print(str(num)+": ***ALERT: error on load")
        #    continue

    """
    for key, value in record.items():
        print(key+" : "+str(type(record[key])))
    """
    print("exchange-update finished.")
    return
#}


def queryBuilder(record): 
#{
    cursor = mssql.conn.cursor()
    query=""

    if (record['customer']==True):
        query="INSERT INTO db1.dbo.Customer VALUES(\'"+str(record['id'])+"\',\'"+str(record['code'])+"\',\'"+str(record['name'])\
            +"\',\'"+str(record['street'])+"\',\'"+str(record['city'])+"\',\'"+str(record['phone'])+"\',\'"\
                +str(record['mobile'])+"\',\'"+str(record['vat'])+"\',\'"+str(record['write_date'])+"\')"

    if (record['supplier']==True): 
        query="INSERT INTO db1.dbo.Vendor VALUES(\'"+str(record['id'])+"\',\'"+str(record['code'])+"\',\'"+str(record['name'])\
            +"\',\'"+str(record['street'])+"\',\'"+str(record['city'])+"\',\'"+str(record['phone'])+"\',\'"\
                +str(record['mobile'])+"\',\'"+str(record['vat'])+"\',\'"+str(record['write_date'])+"\')"
    
    print(query)

    if (query!=""):
        cursor.execute(query)
        cursor.commit()
    #except:
    #    print("error on database write")

    return
#}

main()
