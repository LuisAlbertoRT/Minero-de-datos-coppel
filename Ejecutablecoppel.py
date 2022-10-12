import requests
import json
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import pandas as pd
import numpy as np 
import json
from bs4 import BeautifulSoup
from bs4 import BeautifulStoneSoup
from pandas import json_normalize
from datetime import datetime
import requests
import time
from feedsearch import search
import mysql.connector
from mysql.connector import Error
from multiprocessing import Event
from sqlite3 import connect

def create_server_connection(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password
        )
        print("MySQL Database connection successful")
    except Error as err:
        print(f"Error: '{err}'")

    return connection

def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print("Database created successfully")
    except Error as err:
        print(f"Error: '{err}'")


def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query successful")
    except Error as err:
        print(f"Error: '{err}'")

ArticulosElektra=pd.read_excel("Categoriasmercado.xlsx",sheet_name="Categorias Elektra")



connection = create_server_connection("localhost","root","123456789")
cursor = connection.cursor()
execute_query(connection, "USE COPPEL;") 



for x in range(0,len(ArticulosElektra["Nombre"])):
    try:    
        Consulta=ArticulosElektra["Nombre"][x]
        url="https://www.coppel.com/SearchDisplay?categoryId=&storeId=10151&catalogId=10051&langId=-5&sType=SimpleSearch&resultCatEntryType=2&showResultsPage=true&searchSource=Q&pageView=&pageGroup=Search&beginIndex=0&pageSize=72&searchTerm={}&authToken=900975871%252CGYh9gW2viVqSS1gydydblK%252Fb0FKnNevi1z28YA40hmo%253D".format(Consulta)

        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')
        results = soup.find_all('div', {'class':"product_listing_container"})
        items = []
        for x in results:
            items.append([x.text])

        texto=''.join(items[0])

        texto=texto.replace('\n', '')
        texto=texto.replace('\t', '')
        texto=texto.replace("Comparar",'')
        a=texto.split("\r")
        df11 = pd.DataFrame(a)
        df11=df11.dropna()

        Producto=[]
        Preciooroginal=[]
        Precioventa=[]
        Abono=[]
        Entrega=[]

        for x in range(0,len(df11[0])):
            if "contado" in df11[0][x]:
                Producto.append(df11[0][x].strip())
                if "$" in df11[0][x+1] and "$" in df11[0][x+3]:
                    Preciooroginal.append(df11[0][x+1])
                    Precioventa.append(df11[0][x+3])
                    if "quincenal" in df11[0][x+7]:
                        Abono.append(df11[0][x+7])
                        if "envío" in df11[0][x+8].lower() and "entrega" in df11[0][x+8].lower():
                            Entrega.append("En tienda y domicilio")
                        elif "envío" in df11[0][x+8].lower():
                            Entrega.append("Envio")
                        elif  "entrega" in df11[0][x+8]:
                            Entrega.append("En tienda")
                        else:
                            Entrega.append("Null") 
                    else:
                        Abono.append("Null")
                        if "envío" in df11[0][x+8].lower() and "entrega" in df11[0][x+8].lower():
                            Entrega.append("En tienda y domicilio")
                        elif "envío" in df11[0][x+8].lower():
                            Entrega.append("Envio")
                        elif  "entrega" in df11[0][x+8].lower():
                            Entrega.append("En tienda")
                        else:
                            Entrega.append("Null") 
                else:
                    Preciooroginal.append("Null")
                    Precioventa.append(df11[0][x+1])
                    if "quincenal" in df11[0][x+5]:
                        Abono.append(df11[0][x+5])
                        if "envío" in df11[0][x+6].lower() and "entrega" in df11[0][x+6].lower():
                            Entrega.append("En tienda y domicilio")
                        elif "envío" in df11[0][x+6].lower():
                            Entrega.append("Envio")
                        elif  "entrega" in df11[0][x+6].lower():
                            Entrega.append("En tienda")
                        else:
                            Entrega.append("Null") 
                    else:
                        Abono.append("Null")
                        if "envío" in df11[0][x+6].lower() and "entrega" in df11[0][x+6].lower():
                            Entrega.append("En tienda y domicilio")
                        elif "envío" in df11[0][x+6].lower():
                            Entrega.append("Envio")
                        elif  "entrega" in df11[0][x+6].lower():
                            Entrega.append("En tienda")
                        else:
                            Entrega.append("Null") 

        Top=list(range(1,len(Producto)+1))
        daataa = pd.DataFrame(list(zip(Top,Producto,Preciooroginal,Precioventa,Abono,Entrega)),
                    columns =['Top','Producto', 'Precio original','Precio de venta', 'Abono', 'Entrega'])

        for x in range(0,len(daataa["Producto"])):
            aa=daataa["Producto"][x].replace('Precio contado:', '')
            aa=aa.replace('Oferta', '')
            aa=aa.replace('Exclusivo en línea', '')
            aa=aa.replace('Contenido de Información rápida', '')
            aa=aa.replace('Descuento', '')
            daataa["Producto"][x]=aa

        daataa["Tiempo"]=str(datetime.now())
        daataa["Categoria"]=Consulta
        
        for y in range(0,len(daataa["Top"])):
            cursor = connection.cursor()
            cursor.execute("""INSERT INTO COPPEL1
            (Top	,
            Producto ,
            Preciooriginal ,
            Preciodeventa	,
            Abono	,
            EntregaTiempo	,
            Categoria ) 
            VALUES (%s, %s,%s,%s,%s,%s, %s) """, (str(daataa.iloc[y,0]),
            str(daataa.iloc[y,1]),
            str(daataa.iloc[y,2]),
            str(daataa.iloc[y,3]),
            str(daataa.iloc[y,4]),
            str(daataa.iloc[y,5]),
            str(daataa.iloc[y,6])))
            connection.commit()
            print(str(daataa.iloc[y,1]))
        time.sleep(20)
    except:
        print("No se pudo ejeutar la consulta {}".format(ArticulosElektra["Nombre"][x]))
        time.sleep(20)
