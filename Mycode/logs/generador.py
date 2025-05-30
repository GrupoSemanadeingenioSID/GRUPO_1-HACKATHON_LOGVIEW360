import pandas as pd
import re

corebank_log = "Mycode/logs/logs_CoreBank.log"
corebank_csv = "Mycode/logs/logs_CoreBank.csv"

def parse_corebank_line(line):
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

# Procesar el log y guardar como CSV
corebank_logs = []
with open(corebank_log, encoding='utf-8') as f:
    for line in f:
        parsed = parse_corebank_line(line)
        if parsed:
            corebank_logs.append(parsed)

corebank_df = pd.DataFrame(corebank_logs)
corebank_df.to_csv(corebank_csv, index=False)
print(f"Archivo CSV generado: {corebank_csv}")