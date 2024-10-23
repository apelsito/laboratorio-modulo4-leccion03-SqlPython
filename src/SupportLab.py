# importamos las librerías con las que vamos a trabajar

# Trabajar con bases de datos y python
# -----------------------------------------------------------------------
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors
from tqdm import tqdm


# Trabajar con DataFrames
# -----------------------------------------------------------------------
import pandas as pd
from geopy.geocoders import Nominatim

def obtain_sky_state(df_to_modify,df_to_dict):
    diccionario = {}
    datos = df_to_dict.groupby("estado_cielo")["index"].first()
    indices_cielo = list(datos.index)
    lista_estados = list(datos.values)
    for i in range(0, len(indices_cielo)):
        diccionario[indices_cielo[i]] = lista_estados[i]    
    
    #Ponemos cada valor según el diccionario
    df_to_modify["indice_estado_cielo"] = df_to_modify["cielo"].map(diccionario)
    
    df_to_modify = df_to_modify[["fecha","cielo",
                                 "indice_estado_cielo",
                                 "temp._(°c)",
                                 "sen._térmica_(°c)",
                                 "racha_máx._(km/h)",
                                 "precipitación_(mm)",
                                 "nieve_(mm)",
                                 "humedad_relativa_(%)",
                                 "prob._precip.__(%)",
                                 "prob._de_nieve_(%)",
                                 "prob._de_tormenta_(%)",
                                 "avisos",
                                 "dirección_viento",
                                 "velocidad_del_viento",
                                 "localizacion_id"]]
    return df_to_modify

def obtain_date_index(df_to_modify,df_to_dict):
    diccionario = {}
    datos = df_to_dict.groupby("fecha")["index"].first()
    indices_cielo = list(datos.index)
    lista_estados = list(datos.values)
    for i in range(0, len(indices_cielo)):
        diccionario[indices_cielo[i]] = lista_estados[i]    
    
    #Ponemos cada valor según el diccionario
    df_to_modify["indice_fecha"] = df_to_modify["fecha"].map(diccionario)
    
    df_to_modify = df_to_modify[["fecha",
                                 "indice_fecha",
                                 "cielo",
                                 "indice_estado_cielo",
                                 "temp._(°c)",
                                 "sen._térmica_(°c)",
                                 "racha_máx._(km/h)",
                                 "precipitación_(mm)",
                                 "nieve_(mm)",
                                 "humedad_relativa_(%)",
                                 "prob._precip.__(%)",
                                 "prob._de_nieve_(%)",
                                 "prob._de_tormenta_(%)",
                                 "avisos",
                                 "dirección_viento",
                                 "velocidad_del_viento",
                                 "localizacion_id"]]
    return df_to_modify

def obtain_municipio_index(df_to_modify,df_to_dict):
    diccionario = {}
    datos = df_to_dict.groupby("municipio")["index"].first()
    indices_cielo = list(datos.index)
    lista_estados = list(datos.values)
    for i in range(0, len(indices_cielo)):
        diccionario[indices_cielo[i]] = lista_estados[i]    
    
    #Ponemos cada valor según el diccionario
    df_to_modify["indice_municipio"] = df_to_modify["localizacion"].map(diccionario)
    
    return df_to_modify

def geopy_search(municipios_to_search,df_municipios_to_change):
    geolocator = Nominatim(user_agent="lab-geolocation-GRO") #User agent debe ser lo que quieras
    municipios_geolocalizados = []
    for i in tqdm(range(0,len(municipios_to_search))):
        location = geolocator.geocode(f"{municipios_to_search[i]},Comunidad de Madrid, Spain")
        diccionario = {
        "municipio sucio": municipios_to_search[i],
        "latitud" : location.latitude,
        "longitud" : location.longitude,
        "nombre municipio" : location.address.split(",")[0]
        }
        municipios_geolocalizados.append(diccionario)

    df_municipios = pd.DataFrame(municipios_geolocalizados)
    df_municipios_to_change = pd.concat([df_municipios_to_change,df_municipios],axis=0)
    return df_municipios_to_change
