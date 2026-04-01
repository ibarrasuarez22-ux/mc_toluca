# ==============================================================================
# 🏛️ PROYECTO SITS TOLUCA - MOTOR DE INTELIGENCIA TERRITORIAL (ETL FASE 1)
# ==============================================================================
# OBJETIVO: Limpieza profunda de geometrías, resolución de duplicados y 
#           creación de la base de datos cruda unificada.
# VERSIÓN: 200.0 (ESTRUCTURA MAESTRA CATEMACO + FIX TOLUCA)
# ==============================================================================

import pandas as pd
import geopandas as gpd
import os
import numpy as np
import warnings
import re
import time
from shapely.geometry import Point

# ------------------------------------------------------------------------------
# 1. PROTOCOLO DE SEGURIDAD Y RUTAS
# ------------------------------------------------------------------------------
warnings.filterwarnings('ignore')

BASE_DIR = "/Users/robertoibarrasuarez/Desktop/SIT_EDOMEX"
DATA_DIR = os.path.join(BASE_DIR, "data", "shp")
OUT_DIR  = os.path.join(BASE_DIR, "output")

if not os.path.exists(OUT_DIR): os.makedirs(OUT_DIR)

PATH_CSV_RUR = os.path.join(DATA_DIR, "resumen_rural.csv")
PATH_ITER_20 = os.path.join(DATA_DIR, "iter_mexico_2020.csv")
PATH_SHP_URB = os.path.join(DATA_DIR, "15m.shp")

TASA_AJUSTE = 1.0003639 

print("====================================================================")
print("🏛️ MOTOR SITS TOLUCA - FASE 1: LIMPIEZA Y GENERACIÓN DE BASE CRUDA")
print(f"📅 PROCESO: {time.strftime('%Y-%m-%d %H:%M:%S')}")
print("====================================================================")

# ------------------------------------------------------------------------------
# 2. MOTOR DE MAPEO (ESTANDARIZACIÓN DE VARIABLES)
# ------------------------------------------------------------------------------
def mapear_variables_final(df):
    """Estandariza columnas y limpia ruido estadístico (asteriscos)."""
    df.columns = [str(c).upper().strip() for c in df.columns]
    
    if 'PCON_DISC' in df.columns: df = df.rename(columns={'PCON_DISC': 'POB_DISCAPACITADA'})
    elif 'PCDISC' in df.columns: df = df.rename(columns={'PCDISC': 'POB_DISCAPACITADA'})
    elif 'PCADD' in df.columns: df = df.rename(columns={'PCADD': 'POB_DISCAPACITADA'})
    
    m = {
        'POBFEM': 'MUJERES', 
        'POBMAS': 'HOMBRES', 
        'HOGJEF_F': 'JEFAS_FAMILIA', 
        'P3YM_HLI': 'POB_INDIGENA',
        'POB0_14': 'NINOS_0_14',
        'P_15A17': 'JOVENES_15_17_TOT',
        'P_15A17_F': 'JOVENES_15_17_FEM',
        'P_15A17_M': 'JOVENES_15_17_MAS',
        'P_60YMAS': 'ADULTOS_MAYORES', 
        'VPH_PISOTI': 'POBREZA_VIVIENDA',
        'VPH_PISOT': 'POBREZA_VIVIENDA',
        'PSINDER': 'CARENCIA_SALUD',      
        'P15YM_SE': 'CARENCIA_EDU_SE',    
        'P15YM_AN': 'CARENCIA_EDU_AN',    
        'TVIVPARH': 'TVIVPARH',
        'TVIVPAR': 'TVIVPARH'
    }
    
    for old, new in m.items():
        if old in df.columns and new not in df.columns:
            df = df.rename(columns={old: new})
            
    # Construcción de Niños 0-14 si no existe
    if 'NINOS_0_14' not in df.columns:
        ninos_cols = [c for c in ['P_0A2', 'P_3A5', 'P_6A11', 'P_12A14'] if c in df.columns]
        df['NINOS_0_14'] = 0.0
    else:
        ninos_cols = ['NINOS_0_14']
            
    cols_interes = [
        'POBTOT', 'MUJERES', 'HOMBRES', 'POB_DISCAPACITADA', 
        'JOVENES_15_17_TOT', 'JOVENES_15_17_FEM', 'JOVENES_15_17_MAS',
        'ADULTOS_MAYORES', 'CARENCIA_SALUD', 'CARENCIA_EDU_SE', 'CARENCIA_EDU_AN',
        'POBREZA_VIVIENDA', 'TVIVPARH'
    ] + ninos_cols
    
    for c in cols_interes:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(r'[^\d.]', '', regex=True), errors='coerce').fillna(0).astype(float)
        else:
            df[c] = 0.0
            
    if ninos_cols != ['NINOS_0_14']:
        df['NINOS_0_14'] = df[ninos_cols].sum(axis=1)
            
    df['CARENCIA_EDU'] = df[['CARENCIA_EDU_SE', 'CARENCIA_EDU_AN']].max(axis=1)
    
    if 'POB_DISCAPACITADA' in df.columns:
        df['DISCAPACIDAD_FEM'] = (df['POB_DISCAPACITADA'] * 0.52).round(0)
        df['DISCAPACIDAD_MAS'] = (df['POB_DISCAPACITADA'] * 0.48).round(0)

    return df.loc[:, ~df.columns.duplicated()]

# ------------------------------------------------------------------------------
# 3. CONVERTIDOR DE COORDENADAS RURALES (DMS A DECIMAL)
# ------------------------------------------------------------------------------
def dms_a_decimal_final(c):
    """Convierte coordenadas Grados-Minutos-Segundos a Lat/Lon Decimales"""
    try:
        if pd.isna(c) or str(c).strip() == "": return None
        s = str(c).replace('Â', '').replace('"', ' ').replace("'", " ").replace("°", " ")
        nums = re.findall(r"[-+]?\d*\.\d+|\d+", s)
        if len(nums) < 3: return None
        dec = float(nums[0]) + (float(nums[1])/60) + (float(nums[2])/3600)
        return -dec if any(x in s.upper() for x in ["W", "O", "S"]) else dec
    except: return None

# ------------------------------------------------------------------------------
# 4. CÁLCULO DE INDICADORES (MÉTODO CATEMACO)
# ------------------------------------------------------------------------------
def calcular_indicadores(gdf):
    if gdf.empty: return gdf
    if 'POBTOT' not in gdf.columns:
        print("   ❌ ERROR CRÍTICO: La columna 'POBTOT' no existe. El cruce con el censo falló.")
        return gdf
    
    gdf['POBTOT'] = (gdf['POBTOT'] * TASA_AJUSTE).round(0)
    p = gdf['POBTOT'].values
    
    salud = gdf['CARENCIA_SALUD'].fillna(0)
    edu = gdf['CARENCIA_EDU'].fillna(0)
    
    # Fórmula Catemaco para Pobreza Extrema
    gdf['POB_POBREZA_EXTREMA'] = (salud * 0.45 + edu * 0.55).clip(0, p)
    
    mask_cero = (gdf['POB_POBREZA_EXTREMA'] == 0) & (gdf['POBTOT'] > 0)
    if mask_cero.any():
        gdf.loc[mask_cero, 'POB_POBREZA_EXTREMA'] = gdf.loc[mask_cero, 'POBTOT'] * 0.15

    gdf['POB_POBREZA_EXTREMA'] = gdf['POB_POBREZA_EXTREMA'].round(0)
    gdf['P25_TOT'] = gdf['POBTOT'] * 1.07
    return gdf

# ------------------------------------------------------------------------------
# 5. MOTOR URBANO (LIMPIEZA DE GEOMETRÍAS Y CRUCE POR MANZANA)
# ------------------------------------------------------------------------------
def procesar_urbano_manzana():
    print(f"\n🔄 PROCESANDO: Urbano Toluca (Limpieza de Geometrías y Cruce)")
    
    if not os.path.exists(PATH_SHP_URB):
        print(f"   ❌ ERROR CRÍTICO: No se encuentra {PATH_SHP_URB}")
        return None
        
    gdf = gpd.read_file(PATH_SHP_URB)
    
    # 1. FIX COLUMNAS (Mantiene geometry intacto)
    gdf.columns = [c.upper() if c.lower() != 'geometry' else 'geometry' for c in gdf.columns]
    if 'GEOMETRY' in gdf.columns:
        gdf = gdf.rename(columns={'GEOMETRY': 'geometry'}).set_geometry('geometry')
    
    # 2. FILTRO TOLUCA (106)
    if 'CVE_MUN' in gdf.columns:
        gdf = gdf[gdf['CVE_MUN'] == '106'].copy()
    elif 'CVEGEO' in gdf.columns:
        gdf = gdf[gdf['CVEGEO'].str.startswith('15106')].copy()

    # 3. FIX CRÍTICO DE DUPLICADOS PARA APP.PY
    if 'CVEGEO' in gdf.columns:
        inicial = len(gdf)
        gdf = gdf.dissolve(by='CVEGEO', as_index=False)
        gdf = gdf.drop_duplicates(subset=['CVEGEO'])
        final = len(gdf)
        print(f"   🔧 Limpieza de Polígonos: {inicial} -> {final} (Duplicados eliminados)")

    # 4. DATOS CENSO (Cruce exacto por Manzana)
    if os.path.exists(PATH_ITER_20):
        try:
            try: 
                it = pd.read_csv(PATH_ITER_20, dtype=str, encoding='utf-8')
            except UnicodeDecodeError: 
                it = pd.read_csv(PATH_ITER_20, dtype=str, encoding='latin-1')
            
            # 🔥 EL EXORCISTA: Limpiador de caracteres fantasma BOM (\ufeff)
            it.columns = [c.replace('\ufeff', '').replace('\xef\xbb\xbf', '').strip().upper() for c in it.columns]
            
            if 'ENTIDAD' not in it.columns:
                if 'CVE_ENT' in it.columns:  
                    it = it.rename(columns={'CVE_ENT': 'ENTIDAD', 'CVE_MUN': 'MUN', 'CVE_LOC': 'LOC'})
                else:
                    print(f"   ❌ ERROR FATAL: No se encontró la columna 'ENTIDAD'. Columnas: {list(it.columns[:5])}")
                    return None

            it = it[(it['ENTIDAD'] == '15') & (it['MUN'] == '106') & (it['MZA'] != '000')].copy()
            it['CVEGEO'] = it['ENTIDAD'].str.zfill(2) + it['MUN'].str.zfill(3) + it['LOC'].str.zfill(4) + it['AGEB'].str.zfill(4) + it['MZA'].str.zfill(3)
            
            it = mapear_variables_final(it)
            gdf = gdf.merge(it, on='CVEGEO', how='left')
            
            cols_num = [c for c in it.columns if c != 'CVEGEO']
            for c in cols_num:
                if c in gdf.columns:
                    gdf[c] = gdf[c].fillna(0)
                    
            print(f"   ✅ Censo Urbano cruzado exitosamente.")
        except Exception as e:
            print(f"   ⚠️ Error leyendo/cruzando ITER: {e}")
            return None

    # 5. CÁLCULO DE INDICADORES FINALES
    gdf = calcular_indicadores(gdf)
    gdf['TIPO'] = 'Urbano'
    
    # 6. GUARDAR BASE CRUDA
    gdf = gdf.to_crs(epsg=4326) 
    out_file = os.path.join(OUT_DIR, "sits_capa_urbana_cruda.geojson")
    if os.path.exists(out_file): os.remove(out_file) 
    
    gdf.to_file(out_file, driver='GeoJSON')
    print(f"   ✅ ARCHIVO GENERADO: sits_capa_urbana_cruda.geojson | Pob Total: {gdf.get('POBTOT', pd.Series([0])).sum():,.0f}")
    return gdf

# ------------------------------------------------------------------------------
# 6. MOTOR RURAL (IDÉNTICO AL ORIGINAL)
# ------------------------------------------------------------------------------
def procesar_rural():
    if not os.path.exists(PATH_CSV_RUR): 
        print(f"\n🔄 OMITIENDO RURAL: No se detectó {PATH_CSV_RUR}")
        return None
        
    print(f"\n🔄 PROCESANDO: Rural")
    try: df = pd.read_csv(PATH_CSV_RUR, skiprows=1, encoding='latin-1', dtype=str)
    except: df = pd.read_csv(PATH_CSV_RUR, skiprows=1, encoding='utf-8', dtype=str)
        
    df = mapear_variables_final(df)
    if 'LOC' in df.columns:
        df = df[~df['LOC'].astype(str).str.zfill(4).isin(['0000', '0001'])].copy()
    
    if 'LATITUD' in df.columns and 'LONGITUD' in df.columns:
        df['LAT_D'] = df['LATITUD'].apply(dms_a_decimal_final)
        df['LON_D'] = df['LONGITUD'].apply(dms_a_decimal_final)
        df = df.dropna(subset=['LAT_D', 'LON_D'])
        
        gdf = gpd.GeoDataFrame(df, geometry=[Point(xy) for xy in zip(df['LON_D'], df['LAT_D'])], crs="EPSG:4326")
        gdf = calcular_indicadores(gdf)
        
        if 'CVEGEO' in gdf.columns: gdf = gdf.drop_duplicates(subset=['CVEGEO'])
        elif 'LOC' in gdf.columns: gdf = gdf.drop_duplicates(subset=['LOC'])

        gdf['TIPO'] = 'Rural'
        
        out_file = os.path.join(OUT_DIR, "sits_capa_rural_cruda.geojson")
        if os.path.exists(out_file): os.remove(out_file)
            
        gdf.to_file(out_file, driver='GeoJSON')
        print(f"   ✅ ARCHIVO GENERADO: sits_capa_rural_cruda.geojson | Pob: {gdf['POBTOT'].sum():,.0f}")
        return gdf
    return None

# ------------------------------------------------------------------------------
# 7. EJECUCIÓN
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    gr = procesar_rural()
    gu = procesar_urbano_manzana()
    
    lista = []
    if gr is not None: lista.append(pd.DataFrame(gr).drop(columns='geometry', errors='ignore'))
    if gu is not None: 
        df_u = pd.DataFrame(gu)
        if 'geometry' in df_u.columns: df_u = df_u.drop(columns='geometry')
        lista.append(df_u)
    
    if lista:
        db = pd.concat(lista, ignore_index=True)
        if 'CVEGEO' in db.columns: db = db.drop_duplicates(subset=['CVEGEO'])
            
        path_final = os.path.join(OUT_DIR, "sits_base_cruda_completa.csv")
        db.to_csv(path_final, index=False, encoding='utf-8-sig')
        
        print(f"\n📊 FASE 1 FINALIZADA - BASES CRUDAS LIMPIAS")
        print(f"   Ahora debes correr 'normalizar_fuentes.py' para la Fase 2 (Calibración).")
    else:
        print("\n❌ Error: No se generaron datos. El proceso fue abortado.")