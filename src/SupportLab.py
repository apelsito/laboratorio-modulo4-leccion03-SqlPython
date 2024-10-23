# importamos las librerías con las que vamos a trabajar

# Trabajar con bases de datos y python
# -----------------------------------------------------------------------
import psycopg2
from psycopg2 import OperationalError, errorcodes, errors


# Trabajar con DataFrames
# -----------------------------------------------------------------------
import pandas as pd


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