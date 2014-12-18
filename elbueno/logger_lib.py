import logging #USO DEL SISTEMA DE LOGGING
import logging.handlers #CREAR LOGGER CUSTOM
import urllib.request #ENVIAR LAS PETICIONES
import urllib.error #MANEJAR LOS ERRORES
import sys #EVALUAR SI LA VAINA ESTA FROZEN O NO
import os #UTILIZAR PATHS DEL SISTEMA OPERATIVO
import decimal #PARA ENVIAR OBJETOS JSON
import json #PARA ENVIAR OBJETOS JSON
from ..conf import static_config as gs #CONFIGURACIONES "ESTATICAS" DEL PROYECTO
from ..conf import local_config as lc
import time
import datetime
import sqlite3
import  __main__ #UTILIZAR PATHS DEL SISTEMA OPERATVIO





def check_event(e,q):

    if e.is_set():
        e.clear()
        q.put("K")

def db_check_table(con):

    try:
        c = con.cursor()
        c.execute('''CREATE TABLE IF NOT EXISTS objects 
                (id text primary key, last_try text, object text, try integer, type text)''')
        con.commit()
        return 0
    except Exception as e:
        x = e.args
        return "ERROR CHECKING LOADER TABLE ["+ x[0] + "]"


def db_create_table(con):

    bd = con.cursor()
    bd.execute('''CREATE TABLE objects
             (id text primary key, last_try text, object text, try integer, type text)''')

    con.commit()


def db_connect(route):

    try:
        conn = sqlite3.connect(route,15)

        check = db_check_table(conn)
        if isinstance(check,str):
            return check

        return conn

    except Exception as e:

        x = e.args
        return "ERROR CONNECTING TO LOADER DATABASE ["+ x[0] + "]"




def db_insert(con,oid,objectt,typee):

    try:

        c = con.cursor()
        last_try = datetime.datetime.now()
        c.execute('''INSERT INTO objects
                    (id,last_try, object, try, type) VALUES (?,?,?,?,?)''',(oid,last_try,objectt,0,typee))
        con.commit()
        return 0

    except sqlite3.IntegrityError as e:
        
        return 0

    except Exception as e:

        x = e.args
        return "LOADER DATABASE PROBLEM ["+ x[0] +"]"
        










# REDES, AUXILIAR
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)

        return super(DecimalEncoder, self).default(o)


# REDES, SEND WITH FUCKING RECOVERY


# REDES, ENVIAR OBJETO SIN PARARLE BOLAS A LA RESPUESTA
def sender(url,port,endpoint,jsonS,headersD):
    try:
        if not jsonS == None:
            post_data =jsonS.encode('utf-8')
            headers = { 'Content-type': "application/json",
                        'Accept': "application/json"}

            #print('hey'+str(headersD))
            headers.update(headersD)
            #print('double mother flapping hey')
            ri = 0
            max_ri = 3
            while(True):

                try:
                    request = urllib.request.Request(url+':'+port+endpoint, data=post_data, headers=headers)
                    body = urllib.request.urlopen(request)
                    response = 0
                    break
                except urllib.error.HTTPError as e: 
                    if e.getcode() == 403:
                        logging.debug("COULD NOT SEND OBJECT, CODE: 403, REASON: FORBIDDEN")
                        response = e.getcode()
                        break
                    else:
                        logging.debug("COULD NOT SEND OBJECT, CODE: %s REASON: %s, BODY: %s",e.getcode(),e.reason,e.read().decode('utf8','ignore'))
                        response = e.getcode()
                        break
                except urllib.error.URLError as e: 
                    logging.error("COULD NOT SEND OBJECT, CONNECTION PROBLEM: [%s]",e.reason)
                    if ri == max_ri:
                        response = 600
                        break
                    else:
                        ri += 1
                except Exception as e:
                    logging.error('THERE IS SOMETHING WRONG ON (sender_inner): [%s]',e)
                    response = 601
                    break
        else:
            logging.debug("THERE WAS NOTHING TO SEND IN THIS REQUEST")
            response = 0
    
    except Exception as e:
        logging.error('THERE IS SOMETHING WRONG ON (sender): [%s]',e)
        response = 601
        
    
    return response


# REDES, ENVIAR OBJETO Y PARARLE BOLAS AL CONTENIDO
def senderBody(url,port,endpoint,jsonS):
    try:
        if not jsonS == None:
            post_data =jsonS.encode('utf-8')
            headers = { 'Content-type': "application/json",
                        'Accept': "application/json"}

            ri = 0
            max_ri = 3
            while(True):

                try:
                    request = urllib.request.Request(url+':'+port+endpoint, data=post_data, headers=headers)
                    body = urllib.request.urlopen(request)
                    str_response = body.readall().decode('utf-8')

                    if str_response == "":
                        response = 0
                    else:
                        response =  json.loads(str_response)
                    break
                except urllib.error.HTTPError as e: 
                    if e.getcode() == 403:
                        logging.debug("COULD NOT SEND OBJECT, CODE: 403, REASON: FORBIDDEN")
                        response = e.getcode()
                        break
                    else:
                        logging.debug("COULD NOT SEND OBJECT, CODE: %s REASON: %s, BODY: %s",e.getcode(),e.reason,e.read().decode('utf8','ignore'))
                        response = e.getcode()
                        break
                except urllib.error.URLError as e: 
                    logging.error("COULD NOT SEND OBJECT, CONNECTION PROBLEM: [%s]",e.reason)
                    if ri == max_ri:
                        response = 600
                        break
                    else:
                        ri += 1
                except Exception as e:
                    logging.error('THERE IS SOMETHING WRONG ON (sender_inner): [%s]',e)
                    response = 601
                    break
        else:
            logging.debug("THERE WAS NOTHING TO SEND IN THIS REQUEST")
            response = 0
    
    except Exception as e:
        logging.error('THERE IS SOMETHING WRONG ON (sender): [%s]',e)
        response = 601
        
    
    return response


# REDES, HACER LOGIN
def login(user,passw,id_local):

    try:
        authOb = {}
        authOb['email'] = user
        authOb['password'] = passw

        package = json.dumps(authOb,cls=DecimalEncoder)
        #print(package)
        obj = senderBody(gs.routes["url"],gs.routes["port"],gs.routes["login"],package)

        if isinstance(obj,int):
            logging.error("COULD NOT CONNECT TO API, AUTH FAILURE, CODE [%s] ",obj)
            authOb = -1
        else:
            authOb = {}
            authOb['X-COIN'] = obj['token'] 
            authOb['ID'] = id_local
            
            
        return authOb

    except Exception as e:

        logging.error('SOMETHING WENT REALLY WRONG ON (login) : [%s]',e)
        authOb = -1
        return authOb



# REDES, CODIGOS DE SALIDA RECUPERABLES
def recover(code):
    try:
        if code > 0 and code != 422:
            return False
        return True
    except Exception as e:
        logging.error('THERE IS SOMETHING WRONG ON (recover): [%s]',e)
        return False


# UTILIDADES, OBTENER PATH DEL SCRIPT
def getRealPath():

    try:
        if hasattr(sys, 'frozen'):    
            aux = sys.executable.split("\\")
            aux.pop()
            ster = "\\".join(aux)
        else:
            ster=os.path.dirname(os.path.abspath(getattr(__main__,'__file__','__main__.py')))

        return ster
    except Exception as e:
        logging.error('THERE IS SOMETHING WRONG ON (getRealPath): [%s]',e)
        return 'Holy Shit!!'


# LOGGING, DEFINIR LOGS DE ACUERDO A LOS PARAMETROS PARA LA INSTANCIA EN EJECUCION
def setLogs(params):

    try:
        ster = getRealPath()
        formatStr = '[%(asctime)s # %(lineno)d # %(levelname)s] %(message)s'
            
        # SE CREA EL HANDLER QUE ESCRIBIRA LA INFORMACION DE FUNCIONAMIENTO DEL PROGRAMA, Y 
        # DE SER EL CASO LOS MENSAJES DE DEBUG
        size = int(params['log_size']) * 1048576
        fh = logging.handlers.RotatingFileHandler(ster + '\\' + params['log_file'], maxBytes=size, backupCount=7)
        
        # SE CREA EL HANDLER QUE ENVIARA MENSAJES AL API, SIEMPRE A PARTIR DEL NIVEL INFO PARA NO SOBRECARGAR LA RED
        rh = RESTHandler(params["url"],params["port"],params["errors"])
        rh.setLevel(logging.INFO)

        # SE CREA EL HANDLER QUE ESCRIBIRA EN UN ARCHIVO ROTATORIO CUALQUIER LOG DE ERRORES
        efh = logging.handlers.RotatingFileHandler(ster +'\\'+ params['log_error_file'], maxBytes=size, backupCount=7)
        efh.setLevel(logging.ERROR)    

        logging.basicConfig(level='DEBUG',handlers=[fh,rh,efh],format=formatStr)
    except Exception as e:
        logging.error('THERE IS SOMETHING WRONG ON (setLogs): [%s]',e)
        


def test_connection():
    
    try:       
        ri = 0
        max_ri = 3
        while(True):
            try:
                request = urllib.request.Request(gs.routes["url"]+':'+gs.routes["port"]+gs.routes["test"])
                body = urllib.request.urlopen(request)
                response = 0
                break
            except urllib.error.URLError as e:
                logging.error("COULD NOT SEND OBJECT, CONNECTION PROBLEM: [%s]",e.reason)
                if ri == max_ri:
                    response = 600
                    break
                else:
                    ri += 1
            except Exception as e:
                logging.error('THERE IS SOMETHING WRONG ON (test_connection_inner): [%s]',e)
                response = 601
                break
    except Exception as e:
        logging.error('THERE IS SOMETHING WRONG ON (test_connection): [%s]',e)
        response = 601
        
    
    return response  






# LOGGING, ENVIAR LOS A TRAVES DE LA RED
class RESTHandler(logging.Handler):
    """
    A handler class which sends log strings to a wx object
    """
    def __init__(self,host,port,url):
        """
        Initialize the handler
        @param wxDest: the destination object to post the event to 
        @type wxDest: wx.Window
        """
        logging.Handler.__init__(self)
        self.host = host
        self.port = port
        self.url = url

    def flush(self):
        """ 
        does nothing for this def
        """


    def emit(self, record):
        """
        Emit a record.


        """
        try:
            data = {'level': record.levelname, 'message': record.getMessage(), "date":record.asctime, "local":lc.params['local']}
            jsonS = json.dumps(data, cls=DecimalEncoder)
            headers = {'Content-type': 'application/json', 'Accept': 'application/json'}
            headersD = {'X-COIN':'h4kun4m4t4t4k4p15c4bul','ID':lc.params['local']}

            if hasattr(record, 'headerinteraction'):
                headersD['X-TAGS'] = record.headerinteraction
                headersD['X-COIN'] = record.coin

            headers.update(headersD)
            post_data = jsonS.encode('utf-8')
            #print(jsonS)
            request = urllib.request.Request(self.host+':'+self.port+self.url, data=post_data, headers=headers)
            body = urllib.request.urlopen(request)
        except Exception as e:
            #print(e)
            ster = getRealPath()
            f = open(ster + '\\' + 'logging' +'.dead', 'a')
            f.write(time.strftime('%d/%m/%Y %H:%M:%S')+' # CONNECTION ERROR # '+self.host+':'+self.port+self.url+'\n')
            f.close()
