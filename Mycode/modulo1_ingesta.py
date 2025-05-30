import pandas as pd
import json
import re

midflow_csv = "Mycode/logs/logs_MidFlow_ESB.csv"
secucheck_json = "Mycode/logs/logs_SecuCheck.json"
corebank_log = "Mycode/logs/logs_CoreBank.log"

# Parser para logs CoreBank
def parse_corebank_line(line):
    # Expresión para extraer los campos del log de CoreBank
    regex = (
        r'(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) '
        r'INFO \[(?P<canal>\w+)\] '
        r'(?P<usuario>user\d+)@(?P<ip>[\d\.]+) '
        r'Transacción ejecutada \(transaction: (?P<transaction_id>[^,]+), '
        r'tipo: (?P<tipo>[^,]+), '
        r'cuenta: (?P<cuenta>[^,]+), '
        r'estado: (?P<estado>[^,]+), '
        r'valor: (?P<valor>[\d\.]+)\)'
    )
    match = re.match(regex, line)
    # Si la línea coincide con el patrón, extrae los datos
    if match:
        d = match.groupdict()
        d['estado'] = d['estado'].lower().strip()
        d['usuario'] = d['usuario'].lower().strip()
        d['ip'] = d['ip'].strip()
        d['canal'] = d['canal'].lower().strip()
        d['tipo'] = d['tipo'].lower().strip()
        d['cuenta'] = d['cuenta'].lower().strip()
        d['valor'] = float(d['valor'])
        return d
    return None

# Procesa el logs_CoreBank.log
corebank_logs = []
with open(corebank_log, encoding='utf-8') as f:
    for line in f:
        parsed = parse_corebank_line(line)
        if parsed:
            corebank_logs.append(parsed)
corebank_df = pd.DataFrame(corebank_logs)

# Procesa ellogs_MidFlow_ESB.csv
midflow_df = pd.read_csv(midflow_csv)
midflow_df = midflow_df.rename(columns={
    'user': 'usuario',
    'status': 'estado',
    'ip_address': 'ip',
    'time': 'timestamp',
    'channel': 'canal'
})
for col in ['estado', 'usuario', 'ip', 'canal']:
    if col in midflow_df.columns:
        midflow_df[col] = midflow_df[col].astype(str).str.lower().str.strip()

# Procesa el logs_SecuCheck.json
with open(secucheck_json, 'r', encoding='utf-8') as f:
    secu_data = json.load(f)
secu_df = pd.json_normalize(secu_data)
secu_df = secu_df.rename(columns={
    'user': 'usuario',
    'status': 'estado',
    'ip_address': 'ip',
    'time': 'timestamp',
    'channel': 'canal'
})
for col in ['estado', 'usuario', 'ip', 'canal']:
    if col in secu_df.columns:
        secu_df[col] = secu_df[col].astype(str).str.lower().str.strip()

'''
Unificamos los data frames con el fin de tener un único DataFrame con los logs de las tres fuentes.
'''
logs_df = pd.concat([corebank_df, midflow_df, secu_df], ignore_index=True)

logs_df = logs_df.sort_values(['transaction_id', 'timestamp']) #Se organiza los logs unificados por transacción y timestamp

# logs_df ya estandarizado y unificado se convierte en un archivo CSV
logs_df.to_csv("Mycode/logs/logs_unificados.csv", index=False)

# impresion via consola de los primeros registros 
print("Datos unificados y preprocesados:")
print(logs_df)