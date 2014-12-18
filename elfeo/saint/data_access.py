#from ..conf import master_config as mc
from ..conf import local_config as lc


#from ..lib import logger_lib as ll

from collections import Counter
import sys
import traceback
import json
import time
#import sqlanydb
#import mysql.connector
import pyodbc
import math
import datetime
import calendar

MODEL = 'SAINT'
VERSION = 1.0



class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)

        return super(DecimalEncoder, self).default(o)



def make_connection(config_db):
    try:
        cnx = pyodbc.connect('DRIVER='+config_db['driver']+';SERVER='+config_db['server']+';DATABASE='+config_db['db']+';UID='+config_db['user']+';PWD='+config_db['password'])
        cursor = cnx.cursor()
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        dt_ex = traceback.extract_tb(exc_traceback)
        x = e.args
        
        print("ERROR FATAL EN "+str(dt_ex[0][0])+" (make_connection - line "+str(dt_ex[0][1])+") ["+str(x)+"]")
        return ("ERROR FATAL EN "+str(dt_ex[0][0])+" (make_connection - line "+str(dt_ex[0][1])+") ["+str(x)+"]","")
        
    return (cursor,cnx)

def close_connection(cursor):
    try:
        cursor.close()
        #logging.debug('DATABASE CONNECTION CLOSED')

    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        dt_ex = traceback.extract_tb(exc_traceback)
        x = e.args
        
        print("ERROR FATAL EN "+str(dt_ex[0][0])+" (close_connection - line "+str(dt_ex[0][1])+") ["+str(x)+"]")
        return ("ERROR FATAL EN "+str(dt_ex[0][0])+" (close_connection - line "+str(dt_ex[0][1])+") ["+str(x)+"]","")

        




def get_params(configuration,booli=False):

    if booli:
        if configuration['split']:
            configuration['thearray'] = date_splitter(configuration['time_init'],configuration['time_stop'])
            configuration['split'] = False
    else:
        configuration["time_load"] = datetime.datetime.now() - datetime.timedelta(minutes=15)
    return configuration



def set_params(configuration,booli=False):
    #print('entre aqui')
    if not booli:
        configuration["time_init"]=configuration["time_load"]
    return configuration
    

def date_splitter(inicio, final):

    result = []

    result.append(inicio)
    pivote = inicio + datetime.timedelta(days=1)


    while pivote < final:

        #print(pivote)
        result.append(pivote)
        pivote = pivote + datetime.timedelta(days=1)

    result.append(final)
    return result



def should_iterate(configuration):

    #ASUMO QUE AL ARRANQUE SIEMPRE HAY DOS VAINAS
    if len(configuration['thearray']) < 2:
        configuration['split'] = True
        return (configuration,False)

    configuration['time_init'] = configuration['thearray'].pop(0)
    configuration['time_stop'] = configuration['thearray'][0]

    return(configuration,len(configuration['thearray']) > 1)







def get_products(params,top=False):
    try:

        #print("AQUI")

        (cursor,cnx)=make_connection(lc.dbconfig)
        if isinstance(cursor,str):
            return cursor
        #print("salio??")

        #print(cursor)
        fakinresult = []
        
        if top:
            #aux = datetime.datetime.now()
            datestring = "(fechaT >= \'" + params["time_init"].strftime("%Y-%m-%d %H:%M:%S") + "\' AND fechaT < \'"  + params["time_stop"].strftime("%Y-%m-%d %H:%M:%S") + "\')"

        else:
            datestring = "fechaT >= \'" + params["time_init"].strftime("%Y-%m-%d %H:%M:%S") + "\'"


        query = """ SELECT DISTINCT b.codItem as product \
            FROM safact a , saitemfac b \
            WHERE a.tipoFac='A' AND  b.codItem IS NOT NULL AND b.codItem!='' \
            AND a.signo=1 AND a.numeroD=b.numeroD AND """ + datestring + """ """
       
        cursor.execute(query)
            
        parabuscar = []
        products = cursor.fetchall()
        #print(invoices)
        if len(products) > 0:

            a = []
            for row in products:

                a.append(str(getattr(row,'product')))

            query = '(' + ', '.join("'{0}'".format(w) for w in a) + ')'
            q0_select = ['code','description','price']

            query = """ SELECT CodProd AS code, Descrip AS description, precio1 AS price \
            FROM saprod WHERE  CodProd IN """ + query

        
            cursor.execute(query)        
            rows = cursor.fetchall()
            #print(rows)
            #print(json.dump(rows))
            if len(rows) > 0:

                #print("YISUS!!")
                for row in rows:             
                    product = {}
                    #print("SUCH PRODUCTS")
                    for i in range(len(q0_select)):
                        
                        product[q0_select[i]] = getattr(row,q0_select[i])
                    
                        product['code']= str(getattr(row,'code'))
                        product['description'] = str(getattr(row,'description'))
                        product['price'] = float(getattr(row,'price'))
                        product['_id']=lc.params['local']+'_'+product['code']
                        product['local']=lc.params['local']
                
                    fakinresult.append(product)
            
        return fakinresult
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        dt_ex = traceback.extract_tb(exc_traceback)
        x = e.args
        print('FATAL ERROR ON '+str(dt_ex[0][0])+' (get_products - line '+str(dt_ex[0][1])+') ['+str(x)+']')
        return 'FATAL ERROR ON '+str(dt_ex[0][0])+' (get_products - line '+str(dt_ex[0][1])+') ['+str(x)+']'

def get_clients(params,top=False):  
    try:


        (cursor,cnx)=make_connection(lc.dbconfig)
        if isinstance(cursor,str):
            return cursor
        fakinresult = []

        if top:
            #aux = datetime.datetime.now()
            datestring = "(a.fechaT >= \'" + params["time_init"].strftime("%Y-%m-%d %H:%M:%S") + "\' AND a.fechaT < \'"  + params["time_stop"].strftime("%Y-%m-%d %H:%M:%S") + "\')"

        else:
            datestring = "a.fechaT >= \'" + params["time_init"].strftime("%Y-%m-%d %H:%M:%S") + "\'"


        query = """ SELECT DISTINCT a.id3 AS client \
            FROM safact a \
            WHERE a.tipoFac='A' AND a.signo=1 AND """ + datestring

       
        cursor.execute(query)
            
        parabuscar = []
        clients = cursor.fetchall()
        #print(invoices)
        if len(clients) > 0:

            a = []
            for row in clients:

                a.append(str(getattr(row,'client')))

            query = '(' + ', '.join("'{0}'".format(w) for w in a) + ')'
            q0_select = ['_id','name','address','phones']

            query = """ SELECT codClie as _id, Descrip as name, direc1 as address, telef as phones \
                FROM saclie \
                WHERE codClie IN """ + query

            cursor.execute(query)

            rows = cursor.fetchall()
            if len(rows) > 0:

                for row in rows:
                    client = {}

                    for i in range(len(q0_select)):
                        element = str(getattr(row,q0_select[i]))
                        if element != '' and element != None:
                            if q0_select[i] == 'phones':
                                client[q0_select[i]] = [str(getattr(row,q0_select[i]))]
                            else:
                                client[q0_select[i]] = str(getattr(row,q0_select[i]))
                    
                    for name in lc.params['no_id']:

                        if '_id' in client.keys() and client['_id']==name:
                            client['_id']=lc.params['local']+'_NOID'

                    if '_id' in client.keys():
                        fakinresult.append(client)

        return fakinresult


    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        dt_ex = traceback.extract_tb(exc_traceback)
        x = e.args
        print('FATAL ERROR ON '+str(dt_ex[0][0])+' (get_clients - line '+str(dt_ex[0][1])+') ['+str(x)+']')
        return 'FATAL ERROR ON '+str(dt_ex[0][0])+' (get_clients - line '+str(dt_ex[0][1])+') ['+str(x)+']'

    
    


def byte_encoder(element):

    if isinstance(element,bytes):
        return element.decode('utf-8')

    return element


def product_counter(a1,a2,x):

    i = 0
    result = 0

    for el in a1:

        if el == x:
            result += a2[i]
        i += 1
    return result



def get_indicator(params,top=False):
    if top:
        return params['time_init'].strftime("%Y-%m-%d %H:%M:%S")+"//"+params['thearray'][-1].strftime("%Y-%m-%d %H:%M:%S")
    else:
        return params['time_init'].strftime("%Y-%m-%d %H:%M:%S")

def get_invoices(params,top=False):
    
    
    try:
        
        #print('guat?')
        (cursor,cnx)=make_connection(lc.dbconfig)
        if isinstance(cursor,str):
            return cursor
        fakinresult = []  
        # SE AGREGO EL PEO DEL DESCUENTO, PERO NO SE ESTA DISCRIMINANDO ESTE TIPO DE FACTURAS SOBRE LAS DEMAS
        # SE AGREGO LA CANTIDAD DE SAINT PARA DISCRIMINAR UNITARIAS  
          
        if top:
            #aux = datetime.datetime.now()
            datestring = "(a.fechaT >= \'" + params["time_init"].strftime("%Y-%m-%d %H:%M:%S") + "\' AND a.fechaT < \'"  + params["time_stop"].strftime("%Y-%m-%d %H:%M:%S") + "\')"

        else:
            datestring = "a.fechaT >= \'" + params["time_init"].strftime("%Y-%m-%d %H:%M:%S") + "\'"

        #print("over here")
        
        query= """ SELECT a.Numerod as number, a.fechaT as date, a.id3 as client, (a.totalPRD) as subtotal, a.descto1 as discount, (a.contado - a.contado/1.12) as tax,  a.contado as total, b.codItem as product, count(b.codItem) as quantity, sum(b.cantidad) as qtPerItem, sum(b.totalItem) as tot \
            FROM safact a , saitemfac b \
            WHERE a.tipoFac='A' AND  b.codItem IS NOT NULL AND b.codItem!='' \
            AND a.signo=1 AND a.numeroD=b.numeroD AND """ + datestring + """ \
            GROUP by a.Numerod, a.fechaT, a.id3, a.totalPRD , a.monto, a.mtoTax, a.contado, a.descto1, b.codItem \
            ORDER BY a.Numerod """

        #print(query)

        #print(query)
        cursor.execute(query)
        #print("pero que coño")        
        rows = cursor.fetchall()
        #print('guat??')
        if len(rows) > 0:

            #print('guat???')
            firstrow=rows[0]
            invoice = {}
                
            invoice['number']= str(getattr(firstrow,'number'))
            invoice['date']= calendar.timegm(getattr(firstrow,'date').utctimetuple())
            invoice['client']= str(getattr(firstrow,'client'))
            if invoice['client'] in lc.params['no_id'] :
                        invoice['client']= lc.params['local']+'_NOID'
            invoice['subtotal']= float(getattr(firstrow,'subtotal'))
            invoice['discount']= float(getattr(firstrow,'discount'))
            invoice['tax']= float(getattr(firstrow,'tax'))
            invoice['total']= float(getattr(firstrow,'total'))
            invoice['local']=lc.params['local']
        

        
            product_id = str(getattr(firstrow,'product'))
            # IF CHIMBO PARA LOS HELADOS
            #print(product_id)
            if lc.params['local'] == "Froyo01" and product_id == '01':
                invoice['products']= [ {'pr':lc.params['local']+"_"+product_id,'qt':int(getattr(firstrow,'quantity')),'tot':float(getattr(firstrow,'tot'))} ]
            else:
                rqt = int(getattr(firstrow,'qtPerItem'))
                #print("CARLOS POR FAVOR MIRA ESTO -----------------------------",rqt)
                invoice['products']= [ {'pr':lc.params['local']+"_"+product_id,'qt':int(rqt),'tot':float(getattr(firstrow,'tot'))} ]

            num_fact=invoice['number']
            
            rows.pop(0)


            for element in rows:

               
                if num_fact==str(getattr(element,'number')):
                    product_id = str(getattr(element,'product'))
                    # IF CHIMBO PARA LOS HELADOS
                    if lc.params['local'] == "Froyo01" and product_id == '01':
                        invoice['products']= invoice['products'] + [ {'pr':lc.params['local']+"_"+product_id,'qt':int(getattr(element,'quantity')),'tot':float(getattr(element,'tot'))} ]
                    else:
                        rqt = int(getattr(element,'qtPerItem'))
                        invoice['products']= invoice['products'] + [ {'pr':lc.params['local']+"_"+product_id,'qt':int(rqt),'tot':float(getattr(element,'tot'))} ]

                    
                else:
                    
                    fakinresult.append(invoice)
                    
                    num_fact=str(getattr(element,'number'))
                    invoice={}
                    invoice['number']= str(getattr(element,'number'))
                    invoice['date']= calendar.timegm(getattr(element,'date').utctimetuple())
                    invoice['client']= str(getattr(element,'client'))
                    if invoice['client'] in lc.params['no_id'] :
                        invoice['client']= lc.params['local']+'_NOID'
                    
                    invoice['subtotal']= float(getattr(element,'subtotal'))
                    invoice['discount']= float(getattr(element,'discount'))
                    invoice['tax']= float(getattr(element,'tax'))
                    invoice['total']= float(getattr(element,'total'))
                    invoice['local']=lc.params['local']

                    product_id = str(getattr(element,'product'))
                    # IF CHIMBO PARA LOS HELADOS
                    if lc.params['local'] == "Froyo01" and product_id == '01':
                        invoice['products']= [ {'pr':lc.params['local']+"_"+product_id,'qt':int(getattr(element,'quantity')),'tot':float(getattr(element,'tot'))} ]
                    else:
                        rqt = getattr(element,'qtPerItem')
                        invoice['products']= [ {'pr':lc.params['local']+"_"+product_id,'qt':int(rqt),'tot':float(getattr(element,'tot'))} ]
                    
                          
            fakinresult.append(invoice)
            
            
        return fakinresult
            
        
    except Exception as e:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        dt_ex = traceback.extract_tb(exc_traceback)
        x = e.args
        print('FATAL ERROR ON '+str(dt_ex[0][0])+' (get_invoices - line '+str(dt_ex[0][1])+') ['+str(x)+']')
        return 'FATAL ERROR ON '+str(dt_ex[0][0])+' (get_invoices - line '+str(dt_ex[0][1])+') ['+str(x)+']'

def get_del_invoices(params,top=False):
    
    try:


        (cursor,cnx)=make_connection(lc.dbconfig)
        if isinstance(cursor,str):
            return cursor
        fakinresult = []


        if top:
            datestring = "(a.fechaT >= \'" + params["time_init"].strftime("%Y-%m-%d %H:%M:%S") + "\' AND a.fechaT < \'"  + params["time_stop"].strftime("%Y-%m-%d %H:%M:%S") + "\')"
        else:
            datestring = "a.fechaT >= \'" + params["time_init"].strftime("%Y-%m-%d %H:%M:%S") + "\'"

        query = """ SELECT a.NumeroD as number, a.NumeroR as reference \
            FROM safact a \
            WHERE a.tipoFac='A' AND a.signo=1 AND a.numeroR != '' AND """ + datestring 
        
        cursor.execute(query)
        rows = cursor.fetchall()

        fail = []
        if len(rows) > 0 :

            
            deletions = []
            origins = []

            for row in rows:

                deletions.append(str(getattr(row,'reference')))
                origins.append(str(getattr(row,'number')))

            queryD = '(' + ', '.join("'{0}'".format(w) for w in deletions) + ')'
            queryO = '(' + ', '.join("'{0}'".format(w) for w in origins) + ')'


            query = """ SELECT a.Numerod as number, a.NumeroR as deleted, a.fechaT as date, a.id3 as client, a.Monto as subtotal, a.mtoTax as tax, a.credito as total, b.codItem as product, count(b.codItem) as quantity, sum(b.cantidad) as qtPerItem, sum(b.totalItem) as tot \
                FROM safact a, saitemfac b \
                WHERE a.numeroR IN """ + queryO + """ AND a.numeroD IN """ + queryD + """ AND a.TipoFac='B' \
                AND b.codItem IS NOT NULL AND b.codItem!='' AND b.tipoFac='B' AND b.totalItem>0 AND a.numeroD=b.numeroD \
                GROUP BY a.Numerod, a.numeroR, a.fechaT, a.id3, a.monto, a.mtoTax, a.credito, b.codItem \
                ORDER BY a.Numerod """

            cursor.execute(query)
            rows = cursor.fetchall()

            if len(rows) > 0:

            #print('guat???')
                firstrow=rows[0]
                invoice = {}
                    
                invoice['number']= str(getattr(firstrow,'number'))
                invoice['deleted']= str(getattr(firstrow,'deleted'))
                invoice['date']= calendar.timegm(getattr(firstrow,'date').utctimetuple())
                invoice['client']= str(getattr(firstrow,'client'))
                if invoice['client'] in lc.params['no_id'] :
                            invoice['client']= lc.params['local']+'_NOID'
                invoice['subtotal']= float(getattr(firstrow,'subtotal'))
                invoice['tax']= float(getattr(firstrow,'tax'))
                invoice['total']= float(getattr(firstrow,'total'))
                invoice['local']=lc.params['local']
        

        
                product_id = str(getattr(firstrow,'product'))
                # IF CHIMBO PARA LOS HELADOS
                #print(product_id)
                if lc.params['local'] == "Froyo01" and product_id == '01':
                    invoice['products']= [ {'pr':lc.params['local']+"_"+product_id,'qt':int(getattr(firstrow,'quantity')),'tot':float(getattr(firstrow,'tot'))} ]
                else:
                    rqt = int(getattr(firstrow,'qtPerItem'))
                    #print("CARLOS POR FAVOR MIRA ESTO -----------------------------",rqt)
                    invoice['products']= [ {'pr':lc.params['local']+"_"+product_id,'qt':int(rqt),'tot':float(getattr(firstrow,'tot'))} ]

                num_fact=invoice['number']
                
                rows.pop(0)


                for element in rows:

               
                    if num_fact==str(getattr(element,'number')):
                        product_id = str(getattr(element,'product'))
                        # IF CHIMBO PARA LOS HELADOS
                        if lc.params['local'] == "Froyo01" and product_id == '01':
                            invoice['products']= invoice['products'] + [ {'pr':lc.params['local']+"_"+product_id,'qt':int(getattr(element,'quantity')),'tot':float(getattr(element,'tot'))} ]
                        else:
                            rqt = int(getattr(element,'qtPerItem'))
                            invoice['products']= invoice['products'] + [ {'pr':lc.params['local']+"_"+product_id,'qt':rqt,'tot':float(getattr(element,'tot'))} ]

                        
                    else:
                        
                        fakinresult.append(invoice)
                        
                        num_fact=str(getattr(element,'number'))
                        invoice={}
                        invoice['number']= str(getattr(element,'number'))
                        invoice['deleted']= str(getattr(element,'deleted'))
                        invoice['date']= calendar.timegm(getattr(element,'date').utctimetuple())
                        invoice['client']= str(getattr(element,'client'))
                        if invoice['client'] in lc.params['no_id'] :
                            invoice['client']= lc.params['local']+'_NOID'
                        
                        invoice['subtotal']= float(getattr(element,'subtotal'))
                        invoice['tax']= float(getattr(element,'tax'))
                        invoice['total']= float(getattr(element,'total'))
                        invoice['local']=lc.params['local']

                        product_id = str(getattr(element,'product'))
                        # IF CHIMBO PARA LOS HELADOS
                        if lc.params['local'] == "Froyo01" and product_id == '01':
                            invoice['products']= [ {'pr':lc.params['local']+"_"+product_id,'qt':int(getattr(element,'quantity')),'tot':float(getattr(element,'tot'))} ]
                        else:
                            rqt = getattr(element,'qtPerItem')
                            invoice['products']= [ {'pr':lc.params['local']+"_"+product_id,'qt':int(rqt),'tot':float(getattr(element,'tot'))} ]
                        
                          
            fakinresult.append(invoice)
            
            
        return fakinresult

            
    except Exception as e:
        #logging.error("SOMETHING WENT WRONG, ON data_access_get_delinvoices : [%s]",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        dt_ex = traceback.extract_tb(exc_traceback)
        x = e.args
        print('FATAL ERROR ON '+str(dt_ex[0][0])+' (get_del_invoices - line '+str(dt_ex[0][1])+') ['+str(x)+']')
        return 'FATAL ERROR ON '+str(dt_ex[0][0])+' (get_del_invoices - line '+str(dt_ex[0][1])+') ['+str(x)+']'
