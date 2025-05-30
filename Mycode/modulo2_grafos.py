import sys
import os
import matplotlib.pyplot as plt

#irname(os.pa Asegura que el directorio del módulo 3 esté en el path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from modulo3_deteccion import (
    transacciones_aprobadas_no_ejecutadas,
    transacciones_duplicadas_o_latencia_anormal,
    registrosIndeseables,
    registrosFallidosYSusCausas
)

inconsistentes = transacciones_aprobadas_no_ejecutadas()
transDuplicadasLacenciaAnormal = transacciones_duplicadas_o_latencia_anormal()
registrosIndeseables = registrosIndeseables()
registrosFallidosYSusCausas = registrosFallidosYSusCausas()

