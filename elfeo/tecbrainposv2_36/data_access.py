#from ..conf import master_config as mc
from ..conf import local_config as lc


#from ..lib import logger_lib as ll

from collections import Counter
import sys
import traceback
import json
import time
#import sqlanydb
import mysql.connector
import math
import datetime
import calendar

MODEL = 'TBGOLD 2.6'
VERSION = 1.0



class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)

        return super(DecimalEncoder, self).default(o)



def make_connection(config_db):
    try:

        #logging.debug("DATABASE CONNECTION OPENED")        
        cnx = mysql.connector.connect(user=config_db['user'], password=config_db['password'], database=config_db['db'], host=config_db['server'])
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
        
        print("ERROR FATAL EN "+str(dt_ex[0][0])+" (make_connection - line "+str(dt_ex[0][1])+") ["+str(x)+"]")
        return ("ERROR FATAL EN "+str(dt_ex[0][0])+" (make_connection - line "+str(dt_ex[0][1])+") ["+str(x)+"]","")     




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

        print(pivote)
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
            datestring = "(b.datenew >= \'" + params["time_init"].strftime("%Y-%m-%d %H:%M:%S") + "\' AND b.datenew < \'"  + params["time_stop"].strftime("%Y-%m-%d %H:%M:%S") + "\')"

        else:
            datestring = "b.datenew >= \'" + params["time_init"].strftime("%Y-%m-%d %H:%M:%S") + "\'"

        #print("over here")
        query = """ SELECT DISTINCT e.reference, e.name, e.pricesell \
            FROM """+lc.dbconfig['db']+""".tickets a, """+lc.dbconfig['db']+""".receipts b, """+lc.dbconfig['db']+""".taxlines c, """+lc.dbconfig['db']+""".ticketlines d, """+lc.dbconfig['db']+""".products e, """+lc.dbconfig['db']+""".payments f \
            WHERE a.id=b.id AND a.id=c.receipt AND (a.status=0 or a.status=1) AND a.tickettype=0 AND a.id=d.ticket AND a.id=f.receipt AND d.product = e.id AND """ + datestring + """ """


        #print(query)
        cursor.execute(query)        
        rows = cursor.fetchall()
        #print(rows)
        #print(json.dump(rows))
        if len(rows) > 0:

          
            for element in rows:             

                auxiliar=[byte_encoder(x) for x in element]
                product = {}
        
                product['code'] = auxiliar[0]
                product['description'] = auxiliar[1]
                product['price'] = float(auxiliar[2])
                product['_id']=lc.params['local']+'_'+str(product['code'])
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
            datestring = "(b.datenew >= \'" + params["time_init"].strftime("%Y-%m-%d %H:%M:%S") + "\' AND b.datenew < \'"  + params["time_stop"].strftime("%Y-%m-%d %H:%M:%S") + "\')"

        else:
            datestring = "b.datenew >= \'" + params["time_init"].strftime("%Y-%m-%d %H:%M:%S") + "\'"

        query = """ SELECT DISTINCT a.customer, a.customertaxid, a.customername  \
            FROM """+lc.dbconfig['db']+""".tickets a, """+lc.dbconfig['db']+""".receipts b, """+lc.dbconfig['db']+""".taxlines c, """+lc.dbconfig['db']+""".ticketlines d, """+lc.dbconfig['db']+""".products e, """+lc.dbconfig['db']+""".payments f \
            WHERE a.id=b.id AND a.id=c.receipt AND (a.status=0 or a.status=1) AND a.tickettype=0 AND a.id=d.ticket AND a.id=f.receipt AND d.product = e.id AND """ + datestring + """  """

        cursor.execute(query)
            
        parabuscar = []
        clients = cursor.fetchall()
        #print(invoices)
        if len(clients) > 0:

            for element in clients:

                auxiliar=[byte_encoder(x) for x in element]

                client = {}
                if auxiliar[0] == None:
                    client['_id'] = auxiliar[1]
                    client['name'] = auxiliar[2]
                else:
                    client['_id'] = auxiliar[0]
                    parabuscar.append(auxiliar[0])

                fakinresult.append(client)

            query = '(' + ', '.join("'{0}'".format(w) for w in parabuscar) + ')'

            query = """SELECT id, taxid, name, firstname + lastname ,address + address2, phone, email  
                FROM customers where id in """+query

            cursor.execute(query)        
            rows = cursor.fetchall()

            thedict = {}
            for element in rows:

                auxiliar=[byte_encoder(x) for x in element]
                thedict[auxiliar[0]] = {}
                thedict[auxiliar[0]]['id'] = auxiliar[1]
                if auxiliar[3] != None:
                    thedict[auxiliar[0]]['name'] = auxiliar[3]
                else:
                    if auxiliar[2] == None:
                        thedict[auxiliar[0]]['name'] = ''
                    else:    
                        thedict[auxiliar[0]]['name'] = auxiliar[2]
                if auxiliar[4] != None:
                    thedict[auxiliar[0]]['address'] = auxiliar[4]

                if auxiliar[5] != None:
                    thedict[auxiliar[0]]['phones'] = [auxiliar[5]]

                if auxiliar[6] != None:
                    thedict[auxiliar[0]]['email'] = [auxiliar[6]]

            i = 0
            for elementum in fakinresult:

                if elementum['_id'] in thedict.keys():

                    hold = elementum['_id']
                    
                    #print(fakinresult[i])
                    #print("---")
                    #print(thedict[])
                    fakinresult[i]['_id'] = thedict[hold]['id']
                    #fakinresult[i]['_id'] = 
                    #print(fakinresult[i])
                    #print("*****")
                    #print("&&&&&&")
                    #print(fakinresult[i]['_id'])
                    #fakinresult[i]['hohoho'] = 32
                    #print(fakinresult[i])
                    #print(elementum['_id'])
                    #print(thedict[elementum['_id']])
                    fakinresult[i]['name'] = thedict[hold]['name']
                    if 'address' in thedict[hold].keys():
                        fakinresult[i]['address'] = thedict[hold]['address']
                    if 'phones' in thedict[hold].keys():
                        fakinresult[i]['phones'] = thedict[hold]['phones']
                    if 'email' in thedict[hold].keys():
                        fakinresult[i]['email'] = thedict[hold]['email']

                i += 1
    

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
            datestring = "(b.datenew >= \'" + params["time_init"].strftime("%Y-%m-%d %H:%M:%S") + "\' AND b.datenew < \'"  + params["time_stop"].strftime("%Y-%m-%d %H:%M:%S") + "\')"

        else:
            datestring = "b.datenew >= \'" + params["time_init"].strftime("%Y-%m-%d %H:%M:%S") + "\'"

        #print("over here")
        query = """ SELECT group_concat(d.price) , group_concat(d.units), group_concat(e.reference), a.ticketid, b.datenew, a.customer, a.customertaxid, a.customername,  c.amount as tax, c.base as subtotal, f.total as total, group_concat(d.discount*d.price) \
            FROM """+lc.dbconfig['db']+""".tickets a, """+lc.dbconfig['db']+""".receipts b, """+lc.dbconfig['db']+""".taxlines c, """+lc.dbconfig['db']+""".ticketlines d, """+lc.dbconfig['db']+""".products e, """+lc.dbconfig['db']+""".payments f \
            WHERE a.id=b.id AND a.id=c.receipt AND (a.status=0 or a.status=1) AND a.tickettype=0 AND a.id=d.ticket AND a.id=f.receipt AND d.product = e.id AND """ + datestring + """ \
            GROUP BY a.ticketid, b.datenew, a.customer, a.customertaxid, a.customername,  c.amount, c.base, f.total \
            ORDER BY b.datenew ASC """

        #print(query)

        #print(query)
        cursor.execute(query)
        #print("pero que coño")        
        rows = cursor.fetchall()
        #print('guat??')
        if len(rows) > 0:

            #print('guat???')
            parabuscar = []
            for element in rows:

                auxiliar=[byte_encoder(x) for x in element]

                if str(auxiliar[3]) not in lc.params['no_invoices']:
                
                    refs = auxiliar[2].split(",")
                    n = Counter(refs)
                    naux = n.most_common()
                    

                    invoice = {}
                    #print(element)
                    aux_date= auxiliar[4]

                    invoice['number']= str(auxiliar[3])
                    invoice['date']= calendar.timegm(aux_date.utctimetuple())
                    #print(invoice['date'])
                
                    


                    if auxiliar[5] == None:
                        invoice['client']= auxiliar[6]
                    else:
                        invoice['client'] = auxiliar[5]
                        parabuscar.append(auxiliar[5])

        
                    invoice['local']=lc.params['local']


                    thediscount = 0.0
                    invoice['products'] = []
                    i=0

                    
                    units = [int(x) for x in auxiliar[1].split(",")]
                    prices = [float(x) for x in auxiliar[0].split(",")]
                    discounts = [float(x) for x in auxiliar[11].split(",")]

                    for productinios in naux:

                        pr = {}
                        pr['pr'] = lc.params['local']+'_'+productinios[0]
                        pr['qt'] = product_counter(refs,units,productinios[0])
                        pr['disc'] = product_counter(refs,discounts,productinios[0])
                        pr['tot'] = product_counter(refs,prices,productinios[0])
                        pr['tot'] = pr['tot'] - pr['disc']

                        thediscount += pr['disc']
                        invoice['products'].append(pr)
                        i += 1
                            

                    invoice['subtotal']= float(auxiliar[9]) + thediscount
                    invoice['discount']= thediscount
                    invoice['tax']= float(auxiliar[8])
                    invoice['total'] = float(auxiliar[10])
                    
                    fakinresult.append(invoice)


            if len(fakinresult) > 0:
                query = '(' + ', '.join("'{0}'".format(w) for w in parabuscar) + ')'

                query = """SELECT id, taxid
                    FROM customers where id in """+query

                cursor.execute(query)        
                rows = cursor.fetchall()

                thedict = {}
                for element in rows:

                    auxiliar=[byte_encoder(x) for x in element]
                    thedict[auxiliar[0]] = auxiliar[1]



                #print(thedict)

                i = 0
                for element in fakinresult:

                    if element['client'] in thedict.keys():

                        fakinresult[i]['client'] = thedict[element['client']]

                    i += 1

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
            #aux = datetime.datetime.now()
            datestring = "(b.datenew >= \'" + params["time_init"].strftime("%Y-%m-%d %H:%M:%S") + "\' AND b.datenew < \'"  + params["time_stop"].strftime("%Y-%m-%d %H:%M:%S") + "\')"

        else:
            datestring = "b.datenew >= \'" + params["time_init"].strftime("%Y-%m-%d %H:%M:%S") + "\'"

        #print("over here")
        query = """ SELECT group_concat(d.price) , group_concat(d.units), group_concat(e.reference), a.ticketid, b.datenew, a.customer, a.customertaxid, a.customername,  c.amount as tax, c.base as subtotal, f.total as total, group_concat(d.discount*d.price), g.idventa \
            FROM """+lc.dbconfig['db']+""".tickets a, """+lc.dbconfig['db']+""".receipts b, """+lc.dbconfig['db']+""".taxlines c, """+lc.dbconfig['db']+""".ticketlines d, """+lc.dbconfig['db']+""".products e, """+lc.dbconfig['db']+""".payments f, """+lc.dbconfig['db']+""".ventasxdevoluciones g \
            WHERE a.id=b.id AND a.id=c.receipt AND (a.status=0 or a.status=1) AND a.tickettype=1 AND a.id=d.ticket AND a.id=f.receipt AND d.product = e.id AND a.id=g.idDevolucion AND """ + datestring + """ \
            GROUP BY a.ticketid, b.datenew, a.customer, a.customertaxid, a.customername,  c.amount, c.base, f.total, g.idventa \
            ORDER BY b.datenew ASC """
        
        
        cursor.execute(query)
        rows = cursor.fetchall()

        fail = []
        if len(rows) > 0 :

            thefuckingrefs = []
            for element in rows:
            #print(query)

                auxiliar=[byte_encoder(x) for x in element]
                n = Counter(auxiliar[2])
                naux = n.most_common()

                invoice = {}
                #print(element)
                aux_date= auxiliar[4]

                invoice['number']= str(auxiliar[3])
                invoice['date']= calendar.timegm(aux_date.utctimetuple())
                #print(invoice['date'])
            
            
                invoice['local']=lc.params['local']

                invoice['products'] = []
                i=0

                refs = auxiliar[2].split(",")
                units = [int(x) for x in auxiliar[1].split(",")]
                prices = [float(x) for x in auxiliar[0].split(",")]
                discounts = [float(x) for x in auxiliar[11].split(",")]

                for productinios in naux:

                    pr = {}
                    pr['pr'] = lc.params['local']+'_'+productinios[0]
                    pr['qt'] = product_counter(refs,units,productinios[0])
                    auxdisc = product_counter(refs,discounts,productinios[0])
                    pr['tot'] = product_counter(refs,prices,productinios[0])
                    pr['tot'] = pr['tot'] - auxdisc

                    
                    invoice['products'].append(pr)
                    i += 1


                invoice['subtotal']= (float(auxiliar[9])) * -1
                #invoice['discount']= thediscount
                invoice['tax']= float(auxiliar[8]) * -1
                invoice['total'] = float(auxiliar[10]) * -1
                invoice['deleted'] = auxiliar[12]
                thefuckingrefs.append(auxiliar[12])
                
                fakinresult.append(invoice)
            #print(thefuckingrefs)

            query = '(' + ', '.join("'{0}'".format(w) for w in thefuckingrefs) + ')'

            query = """SELECT id, ticketid  
                FROM tickets WHERE tickeTtype = 0 AND id IN """+query

            cursor.execute(query)        
            rows = cursor.fetchall()

            #print("YISUS")
            thedict = {}
            for element in rows:
                auxiliar=[byte_encoder(x) for x in element]
                thedict[auxiliar[0]] = auxiliar[1]



            #print(thedict)

            i = 0
            for element in fakinresult:

                if element['deleted'] in thedict.keys():

                    fakinresult[i]['deleted'] = thedict[element['deleted']]

                i += 1


                


                
        
        return fakinresult
    except Exception as e:
        #logging.error("SOMETHING WENT WRONG, ON data_access_get_delinvoices : [%s]",e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        dt_ex = traceback.extract_tb(exc_traceback)
        x = e.args
        print('FATAL ERROR ON '+str(dt_ex[0][0])+' (get_del_invoices - line '+str(dt_ex[0][1])+') ['+str(x)+']')
        return 'FATAL ERROR ON '+str(dt_ex[0][0])+' (get_del_invoices - line '+str(dt_ex[0][1])+') ['+str(x)+']'
