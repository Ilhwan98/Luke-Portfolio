import pandas as pd
import os


change_path = "C:/Users/E80-1798/Desktop/Payment Data Change"
fin_path = "C:/Users/E80-1798/Desktop/Payment Fin"

for files in os.listdir(change_path):
    if files.endswith(".csv"):
        if "Caseology" in files:
            if "MX" in files:
                destination_file = os.path.join(fin_path, "MX.csv")
            else:
                destination_file = os.path.join(fin_path, "caseology.csv")
        elif "Cyrill" in files:
            if "MX" in files:
                destination_file = os.path.join(fin_path, "MX.csv")
            else:
                destination_file = os.path.join(fin_path, "cyrill.csv")
        elif "PowerArc" in files:
            destination_file = os.path.join(fin_path, "arctech.csv")
        elif "ArcTech" in files:
            destination_file = os.path.join(fin_path, "arctech.csv")
        elif "Legato" in files:
            destination_file = os.path.join(fin_path, "legato.csv")
        elif "Spigen_US" in files:
            if "NARF" in files:
                if "MX" in files:
                    destination_file = os.path.join(fin_path, "MX.csv")
                elif "BR" in files:
                    destination_file = os.path.join(fin_path, "BR.csv")
            else:
                destination_file = os.path.join(fin_path, "Spigen.csv")
        elif "Spigen_Narf" in files:
            if "MX" in files:
                    destination_file = os.path.join(fin_path, "MX.csv")
            elif "BR" in files:
                    destination_file = os.path.join(fin_path, "BR.csv")
        elif "Spigen" in files:
            if "MX" in files:
                destination_file = os.path.join(fin_path, "MX.csv")
            else:
                destination_file = os.path.join(fin_path, "Spigen.csv")

        if 'destination_file' in locals():
            try:
                df = pd.read_csv(os.path.join(change_path, files), header=None, skiprows=1)
                if os.path.isfile(destination_file):
                    df.to_csv(destination_file, mode='a', index=False, header=False)
                else:
                    df.to_csv(destination_file, index=False)
            except pd.errors.EmptyDataError:
                print(f"Skipping empty file: {files}")


for cfiles in os.listdir(fin_path):
    file_path = os.path.join(fin_path, cfiles)
    df = pd.read_csv(file_path)
    df.columns = ['date/time', 'type', 'sku', 'description', 'quantity', 'marketplace',
                  'fulfillment', 'order city', 'order state', 'order postal', 'product sales',
                  'product sales tax', 'marketplace withheld tax', 'selling fees', 'fba fees',
                  'other transaction fees', 'other', 'total']
    if "MX" in cfiles:
        ttype = df['type']
        desc = df['description']
        ttype_replace = {
            'Pedido': 'Order',
            'Reembolso': 'Refund',
            'Ajuste': 'Adjustment',
            'Tarifa de inventario FBA': 'FBA Inventory Fee',
            'Tarifa de servicio': 'Service Fee',
            'Reembolso por reintegro': 'Chargeback Refund',
            'Trasferir': 'Transfer'
        }
        desc_replace = {
            'Reembolso de inventario de LogÃ­stica de Amazon (DevoluciÃ³n de cliente)': 'FBA Inventory Reimbursement - Customer Return',
            'A la cuenta que finaliza en:': lambda value: 'To account ending with: ' + value[-3:],
            'SuscripciÃ³n': 'Subscription',
            'Ajuste de comisiÃ³n': 'Commission Adjustment',
            'Impuesto sobre la Renta retenido': 'Withheld Income Tax',
            'Costo de la publicidad': 'Advertising Cost',
            'Solicitud de retiro de inventario de LogÃ­stica de Amazon: tarifa por baja (eliminaciÃ³n)': 'FBA Inventory Removal Request - Deletion Fee',
            'Tarifa por almacenamiento prolongado': 'FBA Long-Term Storage Fee',
            'Tarifas de almacenamiento de LogÃ­stica de Amazon': 'FBA storage fees',
            'Reembolso de inventario de LogÃ­stica de Amazon (DaÃ±ado en el almacÃ©n)': 'FBA Inventory Refund (Damaged in Warehouse)'
        }
        
        for i, value in enumerate(ttype):
            for key, replacement in ttype_replace.items():
                if key in str(value):
                    if callable(replacement):
                        df.at[i, 'type'] = replacement(str(value))
                    else:
                        df.at[i, 'type'] = str(value).replace(key, replacement)

        for i, value in enumerate(desc):
            for key, replacement in desc_replace.items():
                if key in str(value):
                    if callable(replacement):
                        df.at[i, 'description'] = replacement(str(value))
                    else:
                        df.at[i, 'description'] = str(value).replace(key, replacement)

for cfiles in os.listdir(fin_path):
    file_path = os.path.join(fin_path, cfiles)
    try:
        df = pd.read_csv(file_path)
        if len(df.columns) != 18:
            print(f"Incorrect number of columns in file: {file_path}")
        # ... rest of your code ...
    except pd.errors.ParserError:
        print(f"Error parsing CSV file: {file_path}")

    df.to_csv(file_path, index=False)
print("Done")
