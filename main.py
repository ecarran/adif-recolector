import requests
import time
import os
import pytz
import gspread
import threading
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

# --- CONFIGURACIÓN ---
ZONA_HORARIA = pytz.timezone("Europe/Madrid")
GOOGLE_JSON = "service_account.json" 
SPREADSHEET_NAME = "Renfe_Dataset_Live" 
BASE_URL = "https://tiempo-real.largorecorrido.renfe.com"
HEADERS = {'Referer': BASE_URL + '/', 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}

app = FastAPI()

# --- NUEVA CONEXIÓN GLOBAL PERSISTENTE ---
print("🔐 Inicializando conexión persistente con Google Sheets...")
try:
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_global = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_JSON, scope)
    client_global = gspread.authorize(creds_global)
    SHEET_GLOBAL = client_global.open(SPREADSHEET_NAME).get_worksheet(0)
    print("✅ Conexión con Google Sheets establecida y lista.")
except Exception as e:
    print(f"❌ Error al conectar con Sheets al inicio: {e}")
    client_global = None
    SHEET_GLOBAL = None

# TABLA XREF (Cross-Reference): Mapeo de Nodos Comerciales Renfe -> Adif
XREF_COMERCIAL = {
    '03216': 'Madrid-Chamartín',
    '02003': 'Madrid-Pta.Atocha',
    '04307': 'Barcelona-Sants',
    '08223': 'A Coruña',
    '05000': 'Almería',
    '03100': 'Alcázar de San Juan'
}

# --- FUNCIONES EXACTAS DE TU ADIF.PY ---

def obtener_nombre_producto(cod):
    mapping = {'2': 'AVE', '10': 'AVLO', '11': 'ALVIA', '16': 'AVANT', '3': 'IC', '28': 'MD'}
    return mapping.get(str(cod), f"OTRO({cod})")

def procesar_fecha_hora_completa(valor, retraso_min=0):
    if not valor or str(valor) in ['0', 'N/D']: return "N/D", "N/D"
    try:
        # 1. El valor de la API es la llegada ESTIMADA
        if 'T' in str(valor):
            dt_prev = datetime.fromisoformat(str(valor).replace('Z', ''))
        else:
            v = str(valor).zfill(4)
            dt_prev = datetime.now(ZONA_HORARIA).replace(hour=int(v[:2]), minute=int(v[2:4]), second=0, microsecond=0)
        
        # 2. La PLANIFICADA original se calcula RESTANDO el retraso
        dt_plan = dt_prev - timedelta(minutes=int(retraso_min))
        
        # Devolvemos (h_plan, h_prev)
        return dt_plan.strftime("%Y-%m-%d %H:%M"), dt_prev.strftime("%Y-%m-%d %H:%M")
    except: 
        return "N/D", "N/D"

def mapear_serie(mat):
    m = str(mat)
    if m.startswith('103'): return 'S-103 (Siemens)'
    if m.startswith(('102', '112')): return 'S-112 (Pato)'
    if m.startswith('106'): return 'S-106 (Avril)'
    if m.startswith(('130', '730')): return 'S-130 (Patito)'
    if m.startswith(('120', '121')): return 'S-120/121 (CAF)'
    if m.startswith('100'): return 'S-100 (Alstom)'
    return f'Serie {m[:3]}' if len(m) >= 3 else "N/D"

def resolver_estacion(cod_original, dicc_adif):
    if not cod_original or str(cod_original) == 'None': return "N/D"
    cod_str = str(cod_original)
    if cod_str in XREF_COMERCIAL: return XREF_COMERCIAL[cod_str]
    if cod_str in dicc_adif: return dicc_adif[cod_str]
    if cod_str.lstrip('0') in dicc_adif: return dicc_adif[cod_str.lstrip('0')]
    return f"N/D ({cod_str})"

# --- LÓGICA DE EXTRACCIÓN Y ESCRITURA ---

def ejecutar_extraccion():
    global SHEET_GLOBAL, client_global
    
    # Si la conexión falló al arrancar, la reintentamos aquí
    if SHEET_GLOBAL is None:
        print("⚠️ Hoja no conectada. Reintentando conexión inicial...")
        try:
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_JSON, scope)
            client_global = gspread.authorize(creds)
            SHEET_GLOBAL = client_global.open(SPREADSHEET_NAME).get_worksheet(0)
        except Exception as e:
            print(f"⛔ Error en Sheets al reconectar: {e}")
            return

    try:
        # 1. CARGA DE ESTACIONES
        res_est = requests.get(f"{BASE_URL}/data/estaciones.geojson?v=1", headers=HEADERS).json()
        features = res_est.get('features', [])
        
        dicc_est = {}
        if features:
            props = features[0]['properties']
            c_id = next((k for k in props if 'COD' in k.upper() or 'ID' in k.upper()), 'codigo')
            c_nom = next((k for k in props if 'NOM' in k.upper()), 'nombre')
            dicc_est = {str(f['properties'][c_id]): f['properties'][c_nom] for f in features}

        # 2. OBTENCIÓN DE TRÁFICO
        res_flota = requests.get(f"{BASE_URL}/renfe-visor/flotaLD.json?v={int(time.time())}", headers=HEADERS).json()
        trenes = res_flota.get('trenes', [])

        # 3. PROCESAMIENTO
        ahora = datetime.now(ZONA_HORARIA)
        timestamp_captura = ahora.strftime("%Y-%m-%d %H:%M:%S")
        
        nuevos_registros = []

        for t in trenes:
            if str(t.get('codProduct')) in ['2', '10', '11', '16']:
                ret = int(t.get('ultRetraso', 0))
                prod = obtener_nombre_producto(t.get('codProduct'))
                h_plan_completa, h_prev_completa = procesar_fecha_hora_completa(t.get('horaLlegadaSigEst'), ret)
                
                ori = resolver_estacion(t.get('codOrigen'), dicc_est)
                des = resolver_estacion(t.get('codDestino'), dicc_est)
                ant = resolver_estacion(t.get('codEstAnt'), dicc_est)
                sig = resolver_estacion(t.get('codEstSig'), dicc_est)
                
                # --- Firma sintética anclada ---
                cod_comercial = t.get('codComercial', 'N/D')
                fecha_servicio = t.get('fecSalida', ahora.strftime("%Y-%m-%d"))
                firma_unica = f"{cod_comercial}_{fecha_servicio}"
                
                fila_dict = {
                    'timestamp': timestamp_captura,
                    'cod_tren_unico': firma_unica,
                    'producto': prod,
                    'id_tren': cod_comercial,
                    'matricula': t.get('mat', 'N/D'),
                    'serie': mapear_serie(t.get('mat', '')),
                    'corredor': t.get('desCorridor', 'N/D'),
                    'origen': ori,
                    'destino': des,
                    'actual_desde': ant,
                    'actual_hacia': sig,
                    'h_plan': h_plan_completa,
                    'h_prev': h_prev_completa,
                    'retraso': ret,
                    'lat': t.get('latitud', 'N/D'),
                    'lon': t.get('longitud', 'N/D')
                }
                
                nuevos_registros.append(list(fila_dict.values()))

        # 4. GUARDADO EN GOOGLE SHEETS (Conexión persistente)
        if nuevos_registros:
            try:
                SHEET_GLOBAL.append_rows(nuevos_registros)
                print(f"✅ DATASET ACTUALIZADO: {len(nuevos_registros)} registros inyectados en {SPREADSHEET_NAME}")
            except Exception as e_sheet:
                # Si la sesión caducó, gspread intentará loguearse de nuevo
                print(f"🔄 Posible sesión caducada, reconectando... ({e_sheet})")
                client_global.login() 
                SHEET_GLOBAL = client_global.open(SPREADSHEET_NAME).get_worksheet(0)
                SHEET_GLOBAL.append_rows(nuevos_registros)
                print(f"✅ DATASET ACTUALIZADO (tras reconexión automática)")
                
    except Exception as e:
        print(f"❌ Error en la extracción: {e}")

# --- RUTAS API (Para tu Cron-Job) ---

@app.get("/")
def home():
    return {"status": "online", "msg": "Motor Renfe-Sheets Operativo"}

@app.get("/ping")
def ping():
    return {"status": "alive", "timestamp": datetime.now(ZONA_HORARIA).isoformat()}

@app.get("/recolectar")
def recolectar():
    threading.Thread(target=ejecutar_extraccion).start()
    return {"status": "started", "msg": "Extracción iniciada en background"}

# Permitir testeo local / en Colab
if __name__ == '__main__':
    ejecutar_extraccion()


