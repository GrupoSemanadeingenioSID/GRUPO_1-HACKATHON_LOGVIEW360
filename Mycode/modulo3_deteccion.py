import pandas as pd

logs_df = pd.read_csv("Mycode/logs/logs_unificados.csv", parse_dates=['timestamp'])
logs_df_secucheck= pd.read_json("Mycode/logs/logs_SecuCheck.json")
logs_df_esb = pd.read_csv("Mycode/logs/logs_MidFlow_ESB.csv", parse_dates=['timestamp'])
logs_df_core = pd.read_csv("Mycode/logs/logs_CoreBank.csv", parse_dates=['timestamp'])



''' Transacciones aprobadas en SecuCheck pero no ejecutadas en CoreBank '''
def transacciones_aprobadas_no_ejecutadas():
    # Transacciones aprobadas en SecuCheck
    aprobadas_secu = logs_df[
        (logs_df['resultado_validación'].astype(str).str.lower() == 'aprobada')
    ]['transaction_id'].unique()
    # Transacciones ejecutadas en CoreBank
    ejecutadas_core = logs_df[
        (logs_df['estado'].astype(str).str.lower() == 'completada')
    ]['transaction_id'].unique()
    inconsistentes = set(aprobadas_secu) - set(ejecutadas_core) # Transacciones aprobadas en SecuCheck pero que no esten ejecutadas en CoreBank
    print("Transacciones aprobadas en SecuCheck pero NO ejecutadas en CoreBank:")
    if inconsistentes:
        # Mostrar todos los datos relevantes de esas transacciones
        df_inconsistentes = logs_df[logs_df['transaction_id'].isin(inconsistentes)]
        print(df_inconsistentes[['transaction_id', 'timestamp', 'user_id', 'usuario', 'ip', 'modulo', 'resultado_validación', 'estado']].drop_duplicates())
    print("No se encontraron inconsistencias.")
    return inconsistentes
'''Transacciones duplicadas o con latencia anormal'''

def transacciones_duplicadas_o_latencia_anormal():
    duplicadas = logs_df_secucheck[logs_df_secucheck.duplicated(subset=['timestamp'], keep=False)]
    print("\nTransacciones duplicadas (mismo transaction_id):")
    if not duplicadas.empty:
        print(duplicadas[['transaction_id', 'timestamp', 'modulo', 'usuario', 'ip', 'valor']])
    else:
        print("No se encontraron transacciones duplicadas.")

    # Analizar latencias anormales usando latency_ms
    if 'latency_ms' in logs_df.columns:
        latencias = logs_df['latency_ms'].dropna()
        if len(latencias) == 0:
            print("No hay datos de latency_ms para analizar.")
            return
        media = latencias.mean()
        std = latencias.std()
        umbral = media + std
        anormales = logs_df[logs_df['latency_ms'] > umbral].sort_values(by='latency_ms', ascending=False)
        print(f"\nLatencias anormales en latency_ms (mayores a media + 2*std = {umbral:.2f} ms):")
        if not anormales.empty:
            print(anormales[['transaction_id', 'timestamp', 'modulo', 'latency_ms', 'user_id', 'ip']])
        else:
            print("No se encontraron latencias anormales en latency_ms.")
    else:
        print("No existe la columna latency_ms en el archivo.")



''' Registros indeseables en logs de ESB y sus relaciones con CoreBank.'''
def registrosIndeseables():
    indeseables = logs_df_esb[
        (logs_df_esb['nivel_log'].astype(str).str.lower() == 'rechazada') |
        (logs_df_esb['nivel_log'].astype(str).str.lower() == 'error') |
        (logs_df_esb['nivel_log'].astype(str).str.lower() == 'fallida')
    ]
    
    print("\nRegistros que no deberian estar en logs:")
    if not indeseables.empty:
        print(indeseables[['transaction_id', 'timestamp', 'modulo', 'user_id', 'ip_address', 'nivel_log']])
        
        buscador = logs_df_core[logs_df_core['transaction_id'].isin(indeseables['transaction_id'])]
        if not buscador.empty:
            print("\nRegistros indeseables en CoreBank:")
            print(buscador[['transaction_id', 'timestamp', 'canal', 'estado', 'usuario', 'ip']])
        else:
            print("No se encontraron registros relacionados en CoreBank.")
        
    else:
        print("No se encontraron registros irregulares en los logs.")


''' Registros fallidos y sus causas en logs de SecuCheck.'''
def registrosFallidosYSusCausas():
    fallidos = logs_df_secucheck[
        (logs_df_secucheck['resultado_verificacion'].astype(str).str.lower() == 'rechazada') |
        (logs_df_secucheck['resultado_verificacion'].astype(str).str.lower() == 'error') |
        (logs_df_secucheck['resultado_verificacion'].astype(str).str.lower() == 'fallida')
    ]
    
    print("\nRegistros fallidos y sus causas:")
    if not fallidos.empty:
        print(fallidos[['transaction_id', 'timestamp', 'modulo', 'user_id', 'ip_address', 'motivo_fallo']])
    else:
        print("No se encontraron registros fallidos.")

if __name__ == "__main__":
    transacciones_aprobadas_no_ejecutadas()
    transacciones_duplicadas_o_latencia_anormal()
    registrosIndeseables()
    registrosFallidosYSusCausas()