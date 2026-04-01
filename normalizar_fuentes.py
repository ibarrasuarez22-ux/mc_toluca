# ==============================================================================
# 🦅 PROYECTO SITS TOLUCA - MOTOR DE INTELIGENCIA TERRITORIAL (ETL FASE 2)
# ==============================================================================
# AUTOR: MTRO. ROBERTO IBARRA SUAREZ
# OBRA: SISTEMA DE INTELIGENCIA TERRITORIAL Y SOCIAL (SITS) - VERSIÓN 2025
# REGISTRO INDAUTOR: 03-2026-010814271100-01 (13-Feb-2026)
# ENFOQUE: MULTIDIMENSIONAL + EVOLUCIÓN HISTÓRICA MOVIMIENTO CIUDADANO (2012-2024)
# ==============================================================================

import pandas as pd
import geopandas as gpd
import os
import numpy as np
import warnings

warnings.filterwarnings('ignore')

# ------------------------------------------------------------------------------
# 1. CONFIGURACIÓN DE RUTAS
# ------------------------------------------------------------------------------
BASE_DIR = "/Users/robertoibarrasuarez/Desktop/SIT_EDOMEX"
OUT_DIR  = os.path.join(BASE_DIR, "output")

PATH_CRUDA = os.path.join(OUT_DIR, "sits_capa_urbana_cruda.geojson")
PATH_FINAL_MZA = os.path.join(OUT_DIR, "sits_capa_urbana.geojson")
PATH_FINAL_ELEC = os.path.join(OUT_DIR, "sits_capa_maestra_electoral.geojson")
PATH_TABLA_ELEC = os.path.join(OUT_DIR, "sits_tabla_electoral.csv") # 🔥 NUEVO: TABLA PURA

# Rutas de los 5 archivos electorales CSV
ARCHIVOS_ELEC = {
    '2012': os.path.join(BASE_DIR, 'ELECTORAL_2012.csv'),
    '2015': os.path.join(BASE_DIR, 'ELECTORAL_2015.csv'),
    '2018': os.path.join(BASE_DIR, 'ELECTORAL_2018.csv'),
    '2021': os.path.join(BASE_DIR, 'ELECTORAL_2021.csv'),
    '2024': os.path.join(BASE_DIR, 'ELECTORAL_2024.csv')
}

def cargar_csv_electoral(ruta):
    """Carga CSV intentando distintas codificaciones para evitar errores de caracteres"""
    try: 
        return pd.read_csv(ruta, encoding='utf-8')
    except: 
        return pd.read_csv(ruta, encoding='latin-1')

def normalizar_base_toluca():
    print("====================================================================")
    print("🦅 MOTOR SITS TOLUCA - FASE 2: INTELIGENCIA SOCIAL Y NARANJA (MC)")
    print("====================================================================")
    
    if not os.path.exists(PATH_CRUDA):
        print(f"❌ ERROR: No se encontró la base cruda. Ejecuta 'generar_datos_final.py' primero.")
        return

    # --------------------------------------------------------------------------
    # 1. CARGA SOCIAL Y CÁLCULO DE ÍNDICES (INTACTO)
    # --------------------------------------------------------------------------
    print("🔄 Cargando microdatos sociales y restaurando índices...")
    gdf = gpd.read_file(PATH_CRUDA)

    # Convertir todas las columnas necesarias a números para evitar errores matemáticos
    cols_num = ['POBTOT', 'TVIVPARH', 'CARENCIA_SALUD', 'CARENCIA_EDU', 'POBREZA_VIVIENDA', 'CARENCIA_SERV', 
                'POB_POBREZA_EXTREMA', 'MUJERES', 'HOMBRES', 'POB_DISCAPACITADA', 'NINOS_0_14', 
                'JOVENES_15_17_TOT', 'ADULTOS_MAYORES', 'JEFAS_FAMILIA']
    
    for c in cols_num:
        if c in gdf.columns: 
            gdf[c] = pd.to_numeric(gdf[c], errors='coerce').fillna(0.0)
        else: 
            gdf[c] = 0.0

    if 'JEFAS_FAMILIA' not in gdf.columns or gdf['JEFAS_FAMILIA'].sum() == 0:
        gdf['JEFAS_FAMILIA'] = (gdf['MUJERES'] * 0.30).round(0)

    # CÁLCULO DE ÍNDICES SOCIALES
    pob_segura = gdf['POBTOT'].replace(0, 1.0)
    viv_segura = gdf['TVIVPARH'].replace(0, 1.0)

    gdf['IND_SALUD'] = (gdf['CARENCIA_SALUD'] / pob_segura).clip(0, 1)
    gdf['IND_EDU']   = (gdf['CARENCIA_EDU'] / pob_segura).clip(0, 1)
    gdf['IND_VIV']   = (gdf['POBREZA_VIVIENDA'] / viv_segura).clip(0, 1)
    
    if 'CARENCIA_SERV' in gdf.columns:
        gdf['IND_SERV'] = (gdf['CARENCIA_SERV'] / viv_segura).clip(0, 1)
    else:
        gdf['IND_SERV'] = gdf['IND_VIV']

    gdf['IND_POBREZA_EXTREMA'] = (gdf['POB_POBREZA_EXTREMA'] / pob_segura).clip(0, 1)
    
    # Índice Multidimensional para el mapa general
    gdf['SITS_INDEX'] = (gdf['IND_POBREZA_EXTREMA']*0.4 + gdf['IND_SERV']*0.3 + gdf['IND_SALUD']*0.2 + gdf['IND_EDU']*0.1).clip(0, 1)
    
    # Proyecciones Demográficas a 2026 (Tasa CONAPO)
    cols_demo = ['POBTOT', 'MUJERES', 'JEFAS_FAMILIA', 'NINOS_0_14', 'ADULTOS_MAYORES', 'JOVENES_15_17_TOT', 'POB_DISCAPACITADA']
    for c in cols_demo:
        gdf[c + '_2020'] = gdf[c]
        gdf[c + '_2026'] = (gdf[c] * 1.074).round(0)
        
    gdf['POB_POBREZA_EXTREMA_2020'] = gdf['POB_POBREZA_EXTREMA']
    gdf['POB_POBREZA_EXTREMA_2026'] = (gdf['POB_POBREZA_EXTREMA'] * 1.074).round(0)

    if 'SECCION' not in gdf.columns:
        gdf['SECCION'] = gdf['CVEGEO'].str[5:9].str.lstrip('0')
        
    gdf['SECCION'] = gdf['SECCION'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

    # --------------------------------------------------------------------------
    # 2. PROCESAMIENTO ELECTORAL MC (BLINDADO CONTRA CEROS Y COMAS)
    # --------------------------------------------------------------------------
    print("🗳️ Procesando Histórico Electoral de Movimiento Ciudadano...")
    
    # 🔥 BLINDAJE: Extraer TODAS las secciones directamente de los CSV primero
    secciones_electorales = set()
    for ruta in ARCHIVOS_ELEC.values():
        if os.path.exists(ruta):
            dt = cargar_csv_electoral(ruta)
            dt.columns = [c.upper().strip().replace('SECCIÓN', 'SECCION') for c in dt.columns]
            if 'SECCION' in dt.columns:
                secciones_electorales.update(dt['SECCION'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip().unique())
                
    # Fusionar las secciones del mapa y las de los CSV para no perder a nadie
    todas_las_secciones = list(secciones_electorales.union(set(gdf['SECCION'].unique())))
    df_elec = pd.DataFrame({'SECCION': todas_las_secciones})
    
    for anio, ruta in ARCHIVOS_ELEC.items():
        if os.path.exists(ruta):
            df_temp = cargar_csv_electoral(ruta)
            df_temp.columns = [c.upper().strip().replace('SECCIÓN', 'SECCION') for c in df_temp.columns]
            
            if 'SECCION' in df_temp.columns:
                df_temp['SECCION'] = df_temp['SECCION'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
                
                # Buscar las columnas de MC y Total de votos sin importar espacios
                col_mc = next((c for c in df_temp.columns if c in ['MC', 'M C']), None)
                col_tot = next((c for c in df_temp.columns if c in ['TOTAL_VOTOS', 'TOTAL', 'TOTAL VOTOS', 'NUM_VOTOS_VALIDOS']), None)
                
                if col_mc and col_tot:
                    # 🔥 BLINDAJE CONTRA COMAS (1,000 -> 1000)
                    if df_temp[col_mc].dtype == object:
                        df_temp[col_mc] = df_temp[col_mc].astype(str).str.replace(',', '', regex=False)
                    if df_temp[col_tot].dtype == object:
                        df_temp[col_tot] = df_temp[col_tot].astype(str).str.replace(',', '', regex=False)
                        
                    df_temp[f'MC_{anio}'] = pd.to_numeric(df_temp[col_mc], errors='coerce').fillna(0)
                    df_temp[f'TOT_VOTOS_{anio}'] = pd.to_numeric(df_temp[col_tot], errors='coerce').fillna(1)
                    
                    df_temp[f'%_MC_{anio}'] = (df_temp[f'MC_{anio}'] / df_temp[f'TOT_VOTOS_{anio}'].replace(0, 1)).clip(0, 1)
                    
                    resumen_anio = df_temp.groupby('SECCION')[[f'MC_{anio}', f'TOT_VOTOS_{anio}', f'%_MC_{anio}']].sum().reset_index()
                    df_elec = df_elec.merge(resumen_anio, on='SECCION', how='left')
        else:
            print(f"   ⚠️ Archivo electoral no encontrado: {ruta}")

    # Limpiar nulos de toda la tabla cruzada
    cols_elec = [c for c in df_elec.columns if c != 'SECCION']
    df_elec[cols_elec] = df_elec[cols_elec].fillna(0)

    # 3. INTELIGENCIA ESTRATÉGICA MC
    print("📈 Calculando Crecimientos y Bastiones Naranjas...")
    if '%_MC_2024' in df_elec.columns and '%_MC_2021' in df_elec.columns:
        df_elec['CRECIMIENTO_MC_21_24'] = df_elec['%_MC_2024'] - df_elec['%_MC_2021']
    else:
        df_elec['CRECIMIENTO_MC_21_24'] = 0.0

    # Bastiones = Top 20% secciones más fuertes de MC
    if '%_MC_2024' in df_elec.columns:
        umbral_bastion = df_elec['%_MC_2024'].quantile(0.80)
        df_elec['BASTION_MC'] = np.where(df_elec['%_MC_2024'] >= umbral_bastion, 'BASTIÓN NARANJA', 'REGULAR')
    else:
        df_elec['BASTION_MC'] = 'SIN DATOS'

    # --------------------------------------------------------------------------
    # 4. CRUCE FINAL Y EXPORTACIÓN DOBLE
    # --------------------------------------------------------------------------
    print("🤝 Fusionando Inteligencia Social y Política...")
    df_soc_sec = gdf.groupby('SECCION').agg({
        'POBTOT_2026': 'sum',
        'POB_POBREZA_EXTREMA_2026': 'sum'
    }).reset_index()
    df_soc_sec['TASA_POBREZA_EXTREMA'] = (df_soc_sec['POB_POBREZA_EXTREMA_2026'] / df_soc_sec['POBTOT_2026'].replace(0,1)).clip(0,1)

    # Pegar la información social a la tabla electoral maestra
    df_elec = df_elec.merge(df_soc_sec, on='SECCION', how='left').fillna(0)
    
    # 🔥 EXPORTACIÓN 1: Tabla Pura (Sin geometría, garantiza que los votos estén intactos para App.py)
    df_elec.to_csv(PATH_TABLA_ELEC, index=False)

    # 🔥 EXPORTACIÓN 2: Mapa Electoral (Solo para visualización)
    gdf_sec = gdf.dissolve(by='SECCION', as_index=False)
    gdf_sec = gdf_sec[['SECCION', 'geometry']].merge(df_elec, on='SECCION', how='inner')

    print("💾 Guardando bases de datos de alta precisión...")
    gdf = gdf.to_crs(epsg=4326)
    gdf.to_file(PATH_FINAL_MZA, driver='GeoJSON')
    
    if not gdf_sec.empty:
        gdf_sec = gdf_sec.to_crs(epsg=4326)
        gdf_sec.to_file(PATH_FINAL_ELEC, driver='GeoJSON')

    print(f"✅ ÉXITO: Base Electoral y Base Social creadas correctamente.")
    print(f"   Titular: Roberto Ibarra Suarez | INDAUTOR: 03-2026-010814271100-01")

if __name__ == "__main__":
    normalizar_base_toluca()