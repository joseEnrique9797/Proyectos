# -*- coding:utf-8 -*-

import odoo_connector
import mssql_connector
import threading
record=[]
mssql = mssql_connector.MSSQL_Connector('Driver={SQL Server}',r'Server=DESKTOP-3LDFV5D;','Database=SISCOR;','user=admin;','password=123cuatr0;')

oc=odoo_connector.Odoo_connector()


def get_date(date_string):
    if date_string == '00000000' or date_string == False or int(date_string[0:4])< 1900:
        date = False 
    else:
        # print('esta es la fecha ------------>', date_string)
        date = ("%s-%s-%s"%( date_string[0:4], date_string[4:6], date_string[6:8]  ))
    return date


def insert_insurance_table_value():
    types = mssql.setQuery("select * from SC_TABLAS_RELACIONADAS where Identificador_Tabla between 12 and 15;")
    values = []
    MARCA = oc.search('insurance.table', [[ ['name', '=', 'MARCA'] ]] )
    COLOR = oc.search('insurance.table', [[ ['name', '=', 'COLOR'] ]] )
    TIPO = oc.search('insurance.table', [[ ['name', '=', 'TIPO'] ]] )
    MODELO = oc.search('insurance.table', [[ ['name', '=', 'MODELO'] ]] )
    print('=======================>',MARCA,MODELO, COLOR, TIPO)
    # print(oi)
    for typ in types:
        if typ[0] == 12:
            validation = oc.search('insurance.table.value', [[ ['table_id', '=', MARCA[0] ] ,['name', '=', str(typ[2]).rstrip()] ]] )
            if not validation:
                values = {
                    'name': str(typ[2]).rstrip(),
                    'table_id':MARCA[0],
                }
                lines = []
                lines.append(values)
                oc.create('insurance.table.value',lines)
        if typ[0] == 13:
            validation = oc.search('insurance.table.value', [[ ['table_id', '=', MODELO[0] ], ['name', '=', str(typ[2]).rstrip()] ]] )
            if not validation:
                values = {
                    'name': str(typ[2]).rstrip(),
                    'table_id':MODELO[0],
                }
                lines = []
                lines.append(values)
                oc.create('insurance.table.value',lines)
            
        if typ[0] == 14:
            validation = oc.search('insurance.table.value', [[ ['table_id', '=', COLOR[0] ], ['name', '=', str(typ[2]).rstrip()] ]] )
            if not validation:
                values = {
                    'name': str(typ[2]).rstrip(),
                    'table_id':COLOR[0],
                }
                lines = []
                lines.append(values)
                oc.create('insurance.table.value',lines)
            
        if typ[0] == 15:
            validation = oc.search('insurance.table.value', [[ ['table_id', '=', TIPO[0] ],['name', '=', str(typ[2]).rstrip()] ]] )
            if not validation:
                values = {
                    'name': str(typ[2]).rstrip(),
                    'table_id':TIPO[0],
                }
                lines = []
                lines.append(values)
                oc.create('insurance.table.value',lines)
            
def insert_type_document():
    types = mssql.setQuery("SELECT * FROM SC_TABLA_DE_TIPOS_DOCUMENTOS;")
    for obj_type in types:
        exists = oc.search('insurance.type.document', [[ ['external_id', '=', obj_type[0]] ]] )
        if not exists:
            values = {
                'external_id': str(obj_type[0]),
                'name': str(obj_type[1]).rstrip(),
                'name_abbreviated':str(obj_type[2]).rstrip(),
            }
            lines = []
            lines.append(values)
            oc.create('insurance.type.document',lines)
        
def insert_client():
    client = mssql.setQuery("SELECT * FROM SC_TABLA_CLIENTES;")
    for obj_client in client:
        exists = oc.search('res.partner', [[ ['external_id', '=', obj_client[0]] ]] )
        if not exists:
            date_birth = get_date(obj_client[14])
            print('gender========++++++++++++++++++++==================',obj_client[13], str(obj_client[16]) if str(obj_client[16]) != '0' else False)
            gender = False
            if obj_client[13] == 'None':
                gender = 'O'
            if obj_client[13] == 'M':
                gender = 'M'
            if obj_client[13] == 'F':
                gender = 'F'
            values = {
                'external_id': str(obj_client[0]),
                'company_bool': False,
                'customer_rank':1,
                'name':str(obj_client[1]).rstrip(),
                'asoc_name':str(obj_client[2]).rstrip(),
                'company_type': 'company' if obj_client[18] == 'J' else 'person',
                'street':str(obj_client[3]).rstrip(),
                'street2':str(obj_client[4]).rstrip(),
                
                'fax':str(obj_client[10]).rstrip(),
                'house_phone':str(obj_client[11]).rstrip(),
                # 'contact':str(obj_client[2]),
                
                'contact_gender': gender, 
                'contact_date_birth':date_birth,
                'status_code':str(obj_client[15]).rstrip(),
                #revisar 0
                'tittle_code':str(obj_client[16]) if (str(obj_client[16]) != '0') and (int(obj_client[16]) < 26)  else False,
                'exc':True if obj_client[19] == 'S' else False,
                'langg':str(obj_client[20]).rstrip(),
                'id_num':str(obj_client[23]).rstrip(),
                'id_passport':str(obj_client[24]).rstrip(),
                # 'giro':str(obj_client[2]),

                'trate':str(obj_client[17]).rstrip(),
                'juridic':str(obj_client[18]).rstrip(),
                'id_type':str(obj_client[22]).rstrip(),
                'gestor':str(obj_client[25]).rstrip(),
                'financial_group':str(obj_client[26]).rstrip(),
                'civil_state':str(obj_client[28]).rstrip(),
                'ocupation':str(obj_client[29]).rstrip(),
                'level_school':str(obj_client[30]).rstrip(),

            }
            lines = []
            lines.append(values)
            oc.create('res.partner',lines)

def insert_company():
    company = mssql.setQuery("SELECT * FROM SC_TABLA_COMPANIAS;")
    
    for obj_company in company:
        exists = oc.search('res.partner', [[ ['external_id', '=', obj_company[0]],['company_bool', '=', True] ]] )
        if not exists:
            agent = oc.search('res.users', [[ ['external_id', '=', obj_company[6]],['company_bool', '=', False] ]] ) 
            values = {
                'external_id': str(obj_company[0]),
                'name': str(obj_company[1]).rstrip(),
                'street':str(obj_company[3]).rstrip(),
                'street2': obj_company[4].rstrip(),
                'phone': obj_company[5],
                'agent_id': agent[0] if agent else False,
                'company_bool': True, 
                'company_type': 'company'
            }
            lines = []
            lines.append(values)
            oc.create('res.partner',lines)

def insert_agent():
    agent = mssql.setQuery("SELECT * FROM SC_TABLA_AGENTES;")
    for obj_agent in agent:
        exists = oc.search('res.users', [[ ['external_id', '=', obj_agent[0]],['agent', '=', True] ]] )
        if not exists:
            values = {
                'external_id': str(obj_agent[0]),
                'name': str(obj_agent[1]).rstrip(),
                'login': str(obj_agent[1]).rstrip(),
                'street':str(obj_agent[2]).rstrip(),
                'street2': obj_agent[4].rstrip(),
                'phone': obj_agent[5],
                'agent': True,
            }
            lines = []
            lines.append(values)
            oc.create('res.users',lines)

def insert_ramos():
    ramos = mssql.setQuery("SELECT * FROM SC_TABLA_DE_RAMOS;")
    for obj_ramos in ramos:
        exists = oc.search('insurance.ramo', [[ ['external_id', '=', obj_ramos[0]] ]] )
        if not exists:
            values = {
                'external_id': str(obj_ramos[0]),
                'name': str(obj_ramos[1]).rstrip().rstrip(),
                'name_abbreviated':str(obj_ramos[2]).rstrip(),
                'tax': True if obj_ramos[3] == 'S' else False,

                'zero_premium': True if obj_ramos[4] == 'S' else False,
                'multincisos': True if obj_ramos[6] == 'S' else False,
                
                'annual_payments' : obj_ramos[8],
                'automatic_renewal':obj_ramos[7]
            }
            lines = []
            lines.append(values)
            oc.create('insurance.ramo',lines)

def insert_emissions():
    emissions = mssql.setQuery("SELECT * FROM SC_EMISIONES WHERE insert_data = 'False';")

    for obj_emission in emissions:
        # exists = oc.search('insurance.emissions', [[ ['external_id', '=', obj_emission[0]] ]] )
        # if not exists:
        # print('ya fue insertado=====================>',obj_emission[111],obj_emission)
        
        ramo_id = oc.search('insurance.ramo', [[ ['external_id', '=', obj_emission[2]] ]] ) 
        ramo_type = oc.search_ramo_type('insurance.ramo', [[ ['external_id', '=', obj_emission[2]] ]] ) 
        print('este es el ramo===============', ramo_type[0]['type'])
        # print(pii)
        client_id = oc.search('res.partner', [[ ['external_id', '=', obj_emission[0]],['company_bool', '=', False] ]] ) 
        company_id = oc.search('res.partner', [[ ['external_id', '=', obj_emission[1]], ['company_bool', '=', True] ]] ) 
        type_id = oc.search('insurance.type.document', [[ ['external_id', '=', obj_emission[3]] ]] ) 
        
        date_init = get_date(obj_emission[13])
        date_end = get_date(obj_emission[14])
        date_issue_company = get_date(obj_emission[15])
        date_pay = get_date(obj_emission[16])
        date_issue_correduria = get_date(obj_emission[17])
        date_recording = get_date(obj_emission[18]) 
        values = {
            'external_id':obj_emission[0],
            'ramo_id': ramo_id[0] if ramo_id else False,
            
            'client_id':client_id[0] if client_id else False,
            'company_id': company_id[0] if company_id else False,
            'type_document_id': type_id[0] if type_id else False,
            
            'document_number': str(obj_emission[4]).rstrip(),
            'year_document': str(obj_emission[5]).rstrip(),
            'date_init': date_init,
            'date_end': date_end,

            'date_issue_company': date_issue_company,
            'date_pay':date_pay,

            'date_issue_correduria': date_issue_correduria,
            'date_recording': date_recording
        }
        lines = []
        lines.append(values)
        emission_id = oc.create('insurance.emissions', lines)
        print('este es el ramo===============', ramo_type)
        # print(pii)
        if ramo_type[0]['type'] =='vehicle':
            insert_vehicle(obj_emission[0] ,str(obj_emission[4]).rstrip(), obj_emission[2], obj_emission[1], (obj_emission[5]),emission_id)
        else:
            insert_insured(str(obj_emission[4]).rstrip(), obj_emission[2], obj_emission[1], (obj_emission[5]),emission_id)
        # print('payment===================>',obj_emission[4], obj_emission[2], obj_emission[1], (obj_emission[5]) ,emission_id,obj_emission[0])
        insert_payments(obj_emission[4], obj_emission[2], obj_emission[1], (obj_emission[5]) ,emission_id,obj_emission[0],obj_emission[6])
        # mssql.update("UPDATE SC_EMISIONES SET insert_data='True' WHERE A_Codigo_de_Cliente= %s and A_codigo_de_compania = %s and A_codigo_de_ramo = %s and A_numero_de_Documento = '%s' and A_anio_de_Documento = '%s'" %(obj_emission[0],obj_emission[1], obj_emission[2],obj_emission[4],obj_emission[5]))
        
        # print('ESTE ES LA DATA------>',) 

def insert_payments(document_number,ramo_id,company_id,year_document,emission_id,client_id, number_emision):
    payments = mssql.setQuery("select * from SC_A_CUENTAS_POR_COBRAR WHERE A_codigo_de_cliente = %s and A_codigo_de_compania = %s and A_Codigo_de_Ramo = %s and A_numero_de_Documento = '%s' and A_Anio_del_Documento = '%s' and A_Numero_Correlativo_de_Emision = %s;" %(client_id,company_id,ramo_id,document_number,year_document,number_emision))
    for obj_payment in payments:
        # print('aqio compara------+++++++++++++++++++++++++++', obj_payment[10])
        if obj_payment[10] == None or obj_payment[10] == '':
            state = obj_payment[10] 
            
        else:
            # print('esta vacio=========================')
            state = 'D'

        # print('estado a incertar=====+++++++++++++++++++++++++++++++', state)
        values = {
            'emission_id': emission_id,
            'receive_num': str(obj_payment[7]).rstrip(),
            'receive_count':obj_payment[8] if obj_payment[8] else False,
            'state': state,
            'due_date' :get_date(obj_payment[12]) if obj_payment[12] else False,
            'net_prima':obj_payment[14] if obj_payment[14] else False,
            'net_saving':obj_payment[15] if obj_payment[15] else False,
            'cogs':obj_payment[18] if obj_payment[18] else False,
            'tax':obj_payment[19] if obj_payment[19] else False,
            'comission': obj_payment[36] if obj_payment[36] else False,
            'bank_code':obj_payment[43] if obj_payment[43] else False,
            'payment_doc_number': obj_payment[44] if obj_payment[44] else False,
            'poliza':obj_payment[47] if obj_payment[47] else False,
            'invoice_num':obj_payment[50] if obj_payment[50] else False,
            'planilla_num':obj_payment[51] if obj_payment[51] else False,
        }
        lines = []
        lines.append(values)
        new_payment = oc.create('insurance.payment',lines)
        # print('inserto un payment----------------->', emission_id)

def insert_product():
    products = mssql.setQuery("SELECT * FROM SC_Productos;")
    for obj_product in products:
        exists = oc.search('insurance.product', [[ ['external_id', '=', obj_product[0]] ]] )
        if not exists:
            ramo_id = oc.search('insurance.ramo', [[ ['external_id', '=', obj_product[2]] ]] ) 
            company_id = oc.search('res.partner', [[ ['external_id', '=', obj_product[1]],['company_bool', '=', True] ]] ) 

            values = {
                'external_id': str(obj_product[0]),
                'name': str(obj_product[4]).rstrip(),
                'company_id':company_id[0] if company_id else False,
                'ramo_id': ramo_id[0] if ramo_id else False,
                'currency_id': 44 if obj_product[3] == 1 else 2 if obj_product[3] == 2 else False,
            }
            lines = []
            lines.append(values)
            oc.create('insurance.product',lines)

def insert_group():
    groups = mssql.setQuery("SELECT * FROM SC_GRUPOS;")
    for obj_group in groups:
        exists = oc.search('insurance.group', [[ ['external_id', '=', obj_group[0]] ]] )
        if not exists:
            values = {
                'external_id': str(obj_group[0]),
                'name': str(obj_group[1]).rstrip(),
                'address1':str(obj_group[2]).rstrip(),
                'address2': str(obj_group[3]).rstrip(),
                'phone': str(obj_group[4]),
                'contact': str(obj_group[6]),
            }
            lines = []
            lines.append(values)
            oc.create('insurance.group',lines)

def insert_ramo_cover():
    groups = mssql.setQuery("SELECT * FROM SC_TABLA_DE_COBERTURAS;")
    for obj_ramo_cover in groups:
        exists = oc.search('insurance.ramo.cover', [[ ['external_id', '=', obj_ramo_cover[0]] ]] )
        print('1===========================')
        if not exists:
            print('2===========================')
            ramo_id = oc.search('insurance.ramo', [[ ['external_id', '=', obj_ramo_cover[1]] ]] )
            print('3===========================')
            values = {
                'external_id': str(obj_ramo_cover[0]),
                'name': str(obj_ramo_cover[2]).rstrip(),
                'ramo_id':ramo_id[0] if ramo_id else False,
            }
            lines = []
            lines.append(values)
            oc.create('insurance.ramo.cover',lines)

def insert_insured_ramo_cover():
    groups = mssql.setQuery("SELECT * FROM SC_TABLA_BIENES_RIESGOS;")
    for obj_ramo_cover in groups:
        exists = oc.search('insurance.ramo.cover', [[ ['external_id', '=', obj_ramo_cover[0]] ]] )
        if not exists:
            ramo_id = oc.search('insurrance.ramo', [[ ['external_id', '=', obj_ramo_cover[2]] ]] )
            values = {
                'external_id': (obj_ramo_cover[1]),
                'name': str(obj_ramo_cover[2]).rstrip(),
                'ramo_id':ramo_id[0] if ramo_id else False,
            }
            lines = []
            lines.append(values)
            oc.create('insurance.ramo.cover',lines)

def insert_affiliation_services():
    services = mssql.setQuery("SELECT * FROM SC_TABLA_Servicios_Afiliacion;")
    
    for obj_services in services:
        compute_external_id = (1000 * int(obj_services[0])) + int(obj_services[1])
        exists = oc.search('insurance.affiliation.services', [[ ['external_id', '=', compute_external_id], ['service_type','=','services'] ]] )
        if not exists:
            ramo_id = oc.search('insurance.ramo', [[ ['external_id', '=', obj_services[0]] ]] )
            compute_external_id = (1000 * int(obj_services[0])) + int(obj_services[1])
            
            values = {
                'external_id': compute_external_id,
                'name': str(obj_services[2]).rstrip(),
                'ramo_id':ramo_id[0] if ramo_id else False,
                'acum_sum':str(obj_services[4]).rstrip(),
                'pct_deposit_prima':str(obj_services[5]),
                'prima_cero':True if obj_services[6] == 'S' else False,
                'isv':True if obj_services[7] == 'S' else False,
                'invoice_column':str(obj_services[8]).rstrip(),
                'service_type':'services',
            }
            lines = []
            lines.append(values)
            oc.create('insurance.affiliation.services',lines)

def insert_assets_risks():
    services = mssql.setQuery("SELECT * FROM SC_TABLA_BIENES_RIESGOS;")
    for obj_services in services:
        compute_external_id = (1000 * int(obj_services[0])) + int(obj_services[1])
        exists = oc.search('insurance.affiliation.services', [[ ['external_id', '=',compute_external_id], ['service_type', '=','covers'] ]] )
        if not exists:
            ramo_id = oc.search('insurance.ramo', [[ ['external_id', '=', obj_services[0]] ]] )
            
            values = {
                'external_id': compute_external_id,
                'name': str(obj_services[2]).rstrip(),
                'ramo_id':ramo_id[0] if ramo_id else False,
                'acum_sum':str(obj_services[4]) if obj_services[4] else False,
                'pct_deposit_prima':str(obj_services[5]),
                'prima_cero':True if obj_services[6] == 'S' else False,
                'isv':True if obj_services[7] == 'S' else False,
                # 'invoice_column':str(obj_services[8]),
                'service_type':'covers',
            }
            lines = []
            lines.append(values)
            oc.create('insurance.affiliation.services',lines)

def insert_insured(document_number,ramo_id,company_id,year_document,emission_id):
    insured = mssql.setQuery("SELECT * FROM SC_A_INCISO_PERSONAS WHERE A_Numero_Documento= '%s' and A_Codigo_de_Ramo =%s and A_Codigo_de_Compania = %s and A_Anio_del_Documentos = %s;" %(document_number,ramo_id,company_id,year_document))
    
    for obj_insured in insured:
        partner_id = oc.search('res.partner', [[ ['company_bool', '=',False], ['name', '=',str(obj_insured[11])] ]] )
        values = {
            'emission_id':emission_id,
            'certified': str(obj_insured[6]).rstrip(),
            'cia_certified': str(obj_insured[7]).rstrip(),
            'cia_sent_date':get_date(obj_insured[20]),
            'partner_id':partner_id[0] if partner_id else False,
        }
        lines = []
        lines.append(values)
        insured_id = oc.create('insurance.insured',lines)
        # print('++++++++++++++++++++++++++++++++++++++++++++++++++++++++','documento emision =======>', document_number , 'documento insured', obj_insured[4])
        # print(poj)
        
        insert_insured_cover(obj_insured[4],obj_insured[2],obj_insured[1],obj_insured[5],insured_id,obj_insured[6],obj_insured[7], obj_insured[0])


def insert_vehicle(client_id,document_number,ramo_id,company_id,year_document,emission_id):
    print('ejecuta query================1111111111111======================',document_number,ramo_id,company_id,year_document, )
    
    vehicle = mssql.setQuery("SELECT * FROM SC_A_inciso_vehiculos WHERE A_Codigo_de_Cliente = %s and A_Numero_de_Documento= '%s' and A_Codigo_de_Ramo =%s and A_Codigo_de_Compania = %s and A_Anio_del_Documento = %s;" %(client_id,document_number,ramo_id,company_id,year_document))
    
    for obj_vehicle in vehicle:
        
        MARCA = mssql.setQuery("select * from SC_TABLAS_RELACIONADAS where identificador_Tabla = 12 and Codigo_Tabla_Relacionada = %s;" %(obj_vehicle[17]))
        MODEL = mssql.setQuery("select * from SC_TABLAS_RELACIONADAS where identificador_Tabla = 13 and Codigo_Tabla_Relacionada = %s;" %(obj_vehicle[20]))
        COLOR = mssql.setQuery("select * from SC_TABLAS_RELACIONADAS where identificador_Tabla = 14 and Codigo_Tabla_Relacionada = %s;" %(obj_vehicle[24]))
        TIPO = mssql.setQuery("select * from SC_TABLAS_RELACIONADAS where identificador_Tabla = 15 and Codigo_Tabla_Relacionada = %s;" %(obj_vehicle[25]))
        print('esta es la marcas ================',MARCA,MODEL,COLOR,TIPO)
        # print(op)
        partner_id = oc.search('res.partner', [[ ['company_bool', '=',False], ['name', '=',str(obj_vehicle[11])] ]] )
        MARCA_o = oc.search('insurance.table.value', [[ ['name', '=', str(MARCA[0][2]).rstrip()] ]] )
        MODEL_o = oc.search('insurance.table.value', [[ ['name', '=', str(MODEL[0][2]).rstrip()] ]] )
        COLOR_o = oc.search('insurance.table.value', [[ ['name', '=', str(COLOR[0][2]).rstrip()] ]] )
        TIPO_o = oc.search('insurance.table.value', [[ ['name', '=', str(TIPO[0][2]).rstrip()] ]] )
        
        print('esta es la modelo ================',str(MODEL[0][2]).rstrip())
        
        values = {
            'emission_id':emission_id,
            'certified': str(obj_vehicle[6]).rstrip(),
            'cia_certified': str(obj_vehicle[7]).rstrip(),
            # 'cia_sent_date':get_date(obj_vehicle[20]),
            'partner_id':partner_id[0] if partner_id else False,

            'vehicle_value': obj_vehicle[8] or 0,
            
            'vehicle_mark_id': MARCA_o[0] if MARCA_o else False,
            'vehicle_model_id':MODEL_o[0] if MODEL_o else False,
            'vehicle_color_id':COLOR_o[0] if COLOR_o else False,
            'vehicle_type_id':TIPO_o[0] if TIPO_o else False,
            
            'vehicle_year':obj_vehicle[18],
            'vehicle_capacity':obj_vehicle[27],
            'vehicle_chasis':obj_vehicle[21],
            'vehicle_motor':obj_vehicle[22],
            'vehicle_placa':obj_vehicle[23],
            # 'vehicle_date':
        }
        lines = []
        lines.append(values)
        insured_id = oc.create('insurance.insured',lines)

        insert_insured_cover_vehicle(obj_vehicle[4],obj_vehicle[2],obj_vehicle[1],obj_vehicle[5],insured_id,obj_vehicle[6],obj_vehicle[7], obj_vehicle[0])
        # print('++++++++++++++++++++++++++++++++++++++++++++++++++++++++','documento emision =======>', document_number , 'documento insured', obj_insured[4])
        # print(poj)
        
        # insert_insured_cover(obj_insured[4],obj_insured[2],obj_insured[1],obj_insured[5],insured_id,obj_insured[6],obj_insured[7], obj_insured[0])
   
def insert_insured_cover_vehicle(document_number,ramo_id,company_id,year_document,insured_id,cer,inc, client):
    insured_cover = mssql.setQuery("SELECT * FROM SC_A_RIEGOS_VEHICULOS WHERE A_Numero_de_Documento= '%s' and A_Codigo_de_Ramo =%s and A_Codigo_de_Compania = %s and A_Anio_del_Documento = %s and A_Numero_Correlativo_de_Emision = %s and A_Certificado = %s and A_Codigo_de_Cliente = %s;" %(document_number,ramo_id,company_id,year_document,cer,inc,client))
    
    for obj_insured in insured_cover:
        
        # insured = mssql.setQuery("SELECT  * FROM SC_TABLA_BIENES_RIESGOS WHERE Codigo_de_Riesgo_Tabla_riesgos = %s limit=1;" %obj_insured[8])
        affiliation_service = oc.search('insurance.affiliation.services', [[ ['external_id', '=', 1000 * ramo_id + obj_insured[8] ], ['service_type','=','covers'] ]] )
        # print('este es el external id que===============================', affiliation_service, 1000 * ramo_id + obj_insured[8])
        # print('con esto busca============', document_number,ramo_id,company_id,year_document,cer,inc)
        # print(qw)
        # print('este objeto 16==============================',obj_insured)
        values = {
            'insured_id': insured_id,
            'insured_sum':obj_insured[9] or 0,
            'insured_prima':obj_insured[10] or 0,
            '_A_Suma_Asegurada_de_Renovacion':obj_insured[12] or 0,
            '_A_Prima_de_Renovacion':obj_insured[13] or 0,
            '_A_Tasa_de_Renovacion_Porcentaje':obj_insured[14] or 0,
            'insured_desc':obj_insured[15] if obj_insured[15] != None else False,
            'insured_desc_pct':obj_insured[16] if obj_insured[16] != None else 0,
            '_A_Accion_de_Endosos':obj_insured[17] or False,
            'cover_id':affiliation_service[0] if affiliation_service else False ,

            'deducible_pct':obj_insured[15] or 0,
            'deducible_aply':obj_insured[16] or 0,
            'deducible_min':obj_insured[17] or 0,
            'coaseg_pct':obj_insured[18] or 0,
            'coaseg_apply':obj_insured[19] or 0,
        }
        lines = []
        lines.append(values)
        # print('insurance.insured.cover------------------------>',values)
        oc.create('insurance.insured.cover',lines)


def insert_insured_cover(document_number,ramo_id,company_id,year_document,insured_id,cer,inc, client):
    insured_cover = mssql.setQuery("SELECT * FROM SC_A_RIEGOS_PERSONAS WHERE A_Numero_de_Documento= '%s' and A_Codigo_de_Ramo =%s and A_Codigo_de_Compania = %s and A_Anio_del_Documento = %s and A_Numero_Correlativo_de_Emision = %s and A_Certificado = %s and A_Codigo_de_Cliente = %s;" %(document_number,ramo_id,company_id,year_document,cer,inc,client))
    insured_cover_services = mssql.setQuery("SELECT * FROM SC_A_Servicios_Afiliados_PERSONAS WHERE A_Numero_de_Documento= '%s' and A_Codigo_de_Ramo =%s and A_Codigo_de_Compania = %s and A_Anio_del_Documento = %s and A_Numero_Correlativo_de_Emision = %s and A_Certificado = %s and A_Codigo_de_Cliente = %s;" %(document_number,ramo_id,company_id,year_document,cer,inc,client))
    for obj_insured in insured_cover:
        
       
        # insured = mssql.setQuery("SELECT  * FROM SC_TABLA_BIENES_RIESGOS WHERE Codigo_de_Riesgo_Tabla_riesgos = %s limit=1;" %obj_insured[8])
        affiliation_service = oc.search('insurance.affiliation.services', [[ ['external_id', '=', 1000 * ramo_id + obj_insured[8] ], ['service_type','=','covers'] ]] )
        # print('este es el external id que===============================', affiliation_service, 1000 * ramo_id + obj_insured[8])
        # print('con esto busca============', document_number,ramo_id,company_id,year_document,cer,inc)
        # print(qw)
        # print('este objeto 16==============================',obj_insured)
        values = {
            'insured_id': insured_id,
            'insured_sum':obj_insured[9] or 0,
            'insured_prima':obj_insured[10] or 0,
            '_A_Suma_Asegurada_de_Renovacion':obj_insured[12] or 0,
            '_A_Prima_de_Renovacion':obj_insured[13] or 0,
            '_A_Tasa_de_Renovacion_Porcentaje':obj_insured[14] or 0,
            'insured_desc':obj_insured[15] if obj_insured[15] != None else False,
            'insured_desc_pct':obj_insured[16] if obj_insured[16] != None else 0,
            '_A_Accion_de_Endosos':obj_insured[17] if len(obj_insured) >= 17 else 0,
            'cover_id':affiliation_service[0] if affiliation_service else False 
        }
        lines = []
        lines.append(values)
        # print('insurance.insured.cover------------------------>',values)
        oc.create('insurance.insured.cover',lines)
    
    # print(ok)
    for obj_insured in insured_cover_services:
        affiliation_service = oc.search('insurance.affiliation.services', [[ ['external_id', '=', 1000 * ramo_id + obj_insured[8] ], ['service_type','=','services'] ]] )
        # print('este es el external id que===============================', affiliation_service, 1000 * ramo_id + obj_insured[8])
        # print('con esto busca============',obj_insured[15])
        # print(qw)
        values = {
            'insured_service_id': insured_id,
            'insured_sum':obj_insured[9] or 0,
            # 'insured_prima':obj_insured[10],
            '_A_Suma_Asegurada_de_Renovacion':obj_insured[12] or 0,
            '_A_Prima_de_Renovacion':obj_insured[13] or 0,
            '_A_Tasa_de_Renovacion_Porcentaje':obj_insured[14] or 0,
            # 'insured_desc':obj_insured[15] if obj_insured[15] != str else False,
            # 'insured_desc_pct':obj_insured[16] if len(obj_insured) > 16 else 0,
            '_A_Accion_de_Endosos':obj_insured[15],
            'cover_id':affiliation_service[0] if affiliation_service else False 
        }
        lines = []
        lines.append(values)
        oc.create('insurance.insured.cover',lines)
        print(' creo '*50, insured_id )
def action_insert_data_odoo():
    #insert_type_document()
    #insert_agent()
    #insert_client()
    #insert_company()
    #insert_ramo_cover()
    #insert_product()
    # insert_group()
    # insert_affiliation_services()
    #insert_ramos()
    insert_emissions()
    #insert_assets_risks()
    # insert_insurance_table_value()
    # new insert
    

action_insert_data_odoo()