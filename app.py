# ==============================================================================
# 🏛️ PROYECTO SITS - TOLUCA 2026 (INTELIGENCIA TERRITORIAL)
# ==============================================================================
# SISTEMA: SITS MANAGER - TABLERO MULTIDIMENSIONAL CALIBRADO
# VERSIÓN: 3500.0 (UI NARANJA TENUE Y SELLO INDAUTOR + TAB 3 VERTICAL)
# ==============================================================================

import streamlit as st
import geopandas as gpd
import pandas as pd
import plotly.express as px
import os

# ------------------------------------------------------------------------------
# 1. CONFIGURACIÓN DE PÁGINA Y CSS (NUEVO DISEÑO)
# ------------------------------------------------------------------------------
st.set_page_config(layout="wide", page_title="SITS Toluca 2026", page_icon="🦅", initial_sidebar_state="expanded")

st.markdown("""
<style>
    /* Fondo principal claro */
    .main { background-color: #f8f9fa; }
    
    /* Barra lateral Naranja Tenue con texto oscuro para contraste */
    [data-testid="stSidebar"] { 
        background-color: #FFF3E0; /* Naranja muy clarito */
        color: #2c3e50; 
    }
    [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3, [data-testid="stSidebar"] p, [data-testid="stSidebar"] label {
        color: #2c3e50 !important;
    }
    
    /* Paleta Naranja para los KPIs */
    .stMetric { background-color: #ffffff; padding: 20px; border-radius: 10px; box-shadow: 0 4px 10px rgba(0,0,0,0.05); border-left: 6px solid #ff9800; }
    .section-title { font-size: 28px; font-weight: 900; color: #1e272e; margin-top: 20px; margin-bottom: 15px; border-bottom: 3px solid #ff9800; padding-bottom: 10px; }
    .sub-title { font-size: 20px; font-weight: 700; color: #34495e; margin-top: 30px; margin-bottom: 15px; }
    
    /* Estilo para la leyenda legal */
    .legal-legend {
        font-size: 11px;
        color: #546E7A;
        text-align: center;
        margin-top: 30px;
        padding-top: 15px;
        border-top: 1px solid #CFD8DC;
        line-height: 1.4;
    }
</style>
""", unsafe_allow_html=True)

st.title("🦅 SITS Toluca: War Room de Desarrollo Social")
st.markdown("---")

# ------------------------------------------------------------------------------
# 2. CARGA DE BASE MAESTRA NORMALIZADA
# ------------------------------------------------------------------------------
@st.cache_data
def cargar_datos():
    f_urb = "output/sits_capa_urbana.geojson"
    if os.path.exists(f_urb):
        u = gpd.read_file(f_urb)
        u['CVE_AGEB'] = u['CVEGEO'].str[:-3] 
        return u
    return gpd.GeoDataFrame()

# 🔥 AGREGADO: CARGADORES ELECTORALES PARA LA PESTAÑA 3
@st.cache_data
def cargar_datos_elec_mapa():
    f_elec = "output/sits_capa_maestra_electoral.geojson"
    if os.path.exists(f_elec): 
        return gpd.read_file(f_elec)
    return gpd.GeoDataFrame()

@st.cache_data
def cargar_tabla_electoral():
    f_tab = "output/sits_tabla_electoral.csv"
    if os.path.exists(f_tab): 
        return pd.read_csv(f_tab)
    return pd.DataFrame()

df_zona = cargar_datos()
df_elec = cargar_datos_elec_mapa()
df_tabla_pura = cargar_tabla_electoral()

if df_zona.empty:
    st.error("🚨 ERROR: No se encuentra la base normalizada. Corre la Fase 1 y Fase 2 primero.")
    st.stop()

# ------------------------------------------------------------------------------
# 3. CONTROLES LATERALES (FILTROS Y BRANDING)
# ------------------------------------------------------------------------------
with st.sidebar:
    # 🔥 AQUÍ VA TU LOGO: Cambia "logo.png" por el nombre de tu archivo o pon una URL.
    try:
        st.image("logo.png", width=180) 
    except:
        # Si no encuentra tu archivo local, pone un recuadro de aviso momentáneo
        st.info("Coloca tu archivo 'logo.png' en la carpeta del proyecto para que aparezca aquí.")
        
    st.header("🎛️ Filtros de Inteligencia")
    
    st.markdown("**🗓️ Escenario Temporal:**")
    anio_str = st.radio("Selecciona el año:", ["2026 (Proyección CONAPO)", "2020 (Censo INEGI)"])
    sufijo_anio = "_2026" if "2026" in anio_str else "_2020"
    
    st.markdown("---")
    agebs = sorted(df_zona['CVE_AGEB'].unique())
    sel_ageb = st.selectbox("🏘️ Focalizar por AGEB:", ["TODOS"] + agebs)
    if sel_ageb != "TODOS":
        df_zona = df_zona[df_zona['CVE_AGEB'] == sel_ageb]

    st.markdown("---")
    st.markdown("**🗺️ Dimensión a Analizar:**")
    dict_inds = {
        "SITS_INDEX": "🔥 Pobreza Multidimensional General (CONEVAL)",
        "IND_POBREZA_EXTREMA": "🔴 Pobreza Extrema (11.6% Calibrado)",
        "IND_SALUD": "🏥 Carencia: Acceso a Salud",
        "IND_EDU": "🎓 Carencia: Rezago Educativo",
        "IND_VIV": "🏠 Carencia: Calidad de Vivienda",
        "IND_SERV": "🚰 Carencia: Servicios Básicos"
    }
    carencia_key = st.radio("Selecciona la Capa:", list(dict_inds.keys()), format_func=lambda x: dict_inds[x])
    
    st.markdown("---")
    st.markdown("**👥 Cruzar con Grupo Demográfico:**")
    grupos_pob = {
        "Población Total": "POBTOT", 
        "Mujeres": "MUJERES", 
        "Madres Jefas de Familia": "JEFAS_FAMILIA", 
        "Niños (0-14 años)": "NINOS_0_14", 
        "Adultos Mayores (60+)": "ADULTOS_MAYORES",
        "Jóvenes (15-17 años)": "JOVENES_15_17_TOT", 
        "Personas con Discapacidad": "POB_DISCAPACITADA" 
    }
    sel_g = st.selectbox("Segmento Vulnerable:", list(grupos_pob.keys()))
    col_g = grupos_pob[sel_g] + sufijo_anio

    # 🔥 SELLO LEGAL INDAUTOR
    st.markdown("""
        <div class="legal-legend">
            <strong>© 2026 Mtro. Roberto Ibarra Suarez. Todos los derechos reservados.</strong><br>
            SISTEMA DE INTELIGENCIA TERRITORIAL Y SOCIAL (SITS) - VERSIÓN 2025<br>
            Registro Público del Derecho de Autor (INDAUTOR)<br>
            <strong>Núm. Registro: 03-2026-010814271100-01</strong>
        </div>
    """, unsafe_allow_html=True)

# ------------------------------------------------------------------------------
# 4. MOTOR MATEMÁTICO EN TIEMPO REAL
# ------------------------------------------------------------------------------
df_zona['Poblacion_Activa'] = pd.to_numeric(df_zona[col_g], errors='coerce').fillna(0)
df_zona['Tasa_Carencia'] = pd.to_numeric(df_zona[carencia_key], errors='coerce').fillna(0)
df_zona['Afectados_Activos'] = (df_zona['Poblacion_Activa'] * df_zona['Tasa_Carencia']).round(0)

LAT_TOLUCA = 19.2891
LON_TOLUCA = -99.6534

# ------------------------------------------------------------------------------
# 5. PESTAÑAS Y VISUALIZACIÓN
# ------------------------------------------------------------------------------
tab1, tab2, tab3 = st.tabs(["🗺️ MAPA MULTIDIMENSIONAL", "📊 PADRÓN Y ESTADÍSTICA", "🗳️ INTELIGENCIA ELECTORAL (MC)"])

with tab1:
    st.markdown(f"<div class='section-title'>Focalización Territorial: {dict_inds[carencia_key]} ({anio_str[:4]})</div>", unsafe_allow_html=True)
    st.info("💡 **Consejo Táctico:** El mapa utiliza una paleta naranja tenue para facilitar la lectura. Las áreas casi transparentes no tienen problemas, mientras que el naranja sólido señala focos rojos de atención prioritaria.")
    
    k1, k2, k3 = st.columns(3)
    k1.metric("Manzanas Analizadas", f"{len(df_zona):,}")
    k2.metric(f"Total Población ({sel_g})", f"{int(df_zona['Poblacion_Activa'].sum()):,}")
    k3.metric(f"Afectados Detectados", f"{int(df_zona['Afectados_Activos'].sum()):,}", delta_color="inverse")

    st.markdown("<div class='sub-title'>Visor Geográfico por Manzana</div>", unsafe_allow_html=True)
    
    fig_p = px.choropleth_mapbox(
        df_zona, 
        geojson=df_zona.geometry, 
        locations=df_zona.index,
        color=carencia_key, 
        color_continuous_scale="Oranges", 
        range_color=(0, df_zona[carencia_key].max() if df_zona[carencia_key].max() > 0 else 1),
        mapbox_style="carto-positron", 
        zoom=11.5, 
        center={"lat": LAT_TOLUCA, "lon": LON_TOLUCA},
        opacity=0.80, 
        hover_name="CVEGEO", 
        hover_data={'Poblacion_Activa': True, 'Afectados_Activos': True, carencia_key: False}
    )
    fig_p.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=650)
    st.plotly_chart(fig_p, use_container_width=True)

with tab2:
    st.markdown(f"<div class='section-title'>Diagnóstico Social y Demográfico</div>", unsafe_allow_html=True)
    
    pob_total = df_zona['Poblacion_Activa'].sum()
    afectados_total = df_zona['Afectados_Activos'].sum()
    
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("<div class='sub-title'>Proporción de Afectación</div>", unsafe_allow_html=True)
        if pob_total > 0:
            fig_pie = px.pie(
                names=[f"Afectados ({sel_g})", "Población Sin Carencia"], 
                values=[afectados_total, max(0, pob_total - afectados_total)], 
                color_discrete_sequence=['#ff9800', '#2e7d32'], 
                hole=0.4
            )
            st.plotly_chart(fig_pie, use_container_width=True)
            
    with c2:
        st.markdown(f"<div class='sub-title'>Top 10 Manzanas Críticas</div>", unsafe_allow_html=True)
        top10 = df_zona.sort_values('Afectados_Activos', ascending=False).head(10)
        if not top10.empty and top10['Afectados_Activos'].sum() > 0:
            fig_bar = px.bar(
                top10, 
                x='Afectados_Activos', 
                y='CVEGEO', 
                orientation='h', 
                color='Afectados_Activos', 
                color_continuous_scale='Oranges'
            )
            fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_bar, use_container_width=True)
            
    st.markdown("---")
    st.markdown(f"<div class='sub-title'>📋 Padrón Táctico de Intervención ({sel_g})</div>", unsafe_allow_html=True)
    st.write("Identifica las manzanas con el mayor volumen de personas que requieren atención inmediata.")
    
    df_tabla = df_zona.sort_values('Afectados_Activos', ascending=False).head(150).reset_index(drop=True)
    df_tabla['STREET_VIEW'] = df_tabla['geometry'].apply(lambda geom: f"https://www.google.com/maps/@?api=1&map_action=pano&viewpoint={geom.centroid.y},{geom.centroid.x}")
    
    cols_mostrar = ['CVEGEO', 'Poblacion_Activa', 'Afectados_Activos', carencia_key, 'STREET_VIEW']
    
    st.data_editor(
        df_tabla[cols_mostrar],
        column_config={
            "CVEGEO": "Manzana (Clave INEGI)",
            "Poblacion_Activa": st.column_config.NumberColumn(f"Total {sel_g}", format="%d"),
            "Afectados_Activos": st.column_config.NumberColumn("Habitantes Afectados", format="%d"),
            carencia_key: st.column_config.ProgressColumn("Intensidad de Carencia", min_value=0, max_value=1, format="%.2f"),
            "STREET_VIEW": st.column_config.LinkColumn("👁️ Ver Entorno (Google)")
        },
        hide_index=True, 
        use_container_width=True, 
        height=500
    )

# ==============================================================================
# 🔥 NUEVA PESTAÑA 3: INTELIGENCIA ELECTORAL (EXPANDIDA POR RENGLONES)
# ==============================================================================
with tab3:
    st.markdown(f"<div class='section-title'>🗳️ Movimiento Ciudadano (MC): Evolución y Bastiones</div>", unsafe_allow_html=True)
    
    if df_tabla_pura.empty:
        st.warning("⚠️ No se encontró la base electoral. Ejecuta la Fase 2 en normalizar_fuentes.py")
    else:
        st.info("💡 **Guía de Uso:** Esta sección analiza el comportamiento histórico y territorial del partido. Todos los gráficos operan a nivel 'Sección Electoral' para detectar fortalezas territoriales y medir la penetración del voto.")

        # ---- RENGLÓN 1: MAPA ESTRATÉGICO ----
        st.markdown("<div class='sub-title'>🗺️ Mapa Estratégico de Fuerza Naranja</div>", unsafe_allow_html=True)
        st.write("Visualiza la distribución del voto a nivel de sección. Utiliza el menú desplegable para alternar entre la fuerza electoral actual (2024), el crecimiento comparativo (2021 vs 2024), o el volumen absoluto de votos conseguidos.")
        
        mapa_dict = {
            "%_MC_2024": "Fuerza Electoral MC 2024 (%)",
            "CRECIMIENTO_MC_21_24": "Crecimiento MC 2021 vs 2024 (+/- %)",
            "MC_2024": "Volumen de Votos MC 2024 (Total)"
        }
        var_e = st.selectbox("🎯 Capa a proyectar en el mapa:", list(mapa_dict.keys()), format_func=lambda x: mapa_dict[x])
        
        if not df_elec.empty:
            fig_e = px.choropleth_mapbox(
                df_elec, 
                geojson=df_elec.geometry, 
                locations=df_elec.index, 
                color=var_e, 
                color_continuous_scale="Oranges" if "CRECIMIENTO" not in var_e else "RdYlGn", 
                mapbox_style="carto-positron", 
                zoom=11.5, 
                center={"lat": LAT_TOLUCA, "lon": LON_TOLUCA}, 
                opacity=0.75, 
                hover_name="SECCION",
                hover_data={"BASTION_MC": True, "MC_2024": True, "TASA_POBREZA_EXTREMA": ":.1%"}
            )
            fig_e.update_layout(margin={"r":0,"t":0,"l":0,"b":0}, height=600)
            st.plotly_chart(fig_e, use_container_width=True)
        else:
            st.warning("⚠️ No se pudo generar el mapa por falta de geometrías de sección, pero los datos duros están abajo.")

        st.markdown("---")

        # ---- RENGLÓN 2: GRÁFICO HISTÓRICO ----
        st.markdown("<div class='sub-title'>📈 Evolución Histórica Toluca (2012 - 2024)</div>", unsafe_allow_html=True)
        st.write("Mide la cantidad total de votos obtenidos por Movimiento Ciudadano en cada elección constitucional. Útil para determinar si la estructura electoral y la preferencia ciudadana sostienen una tendencia al alza.")
        
        historico = []
        for anio in ['2012', '2015', '2018', '2021', '2024']:
            if f'MC_{anio}' in df_tabla_pura.columns: 
                historico.append({'Año': anio, 'Votos Totales MC': df_tabla_pura[f'MC_{anio}'].sum()})
        
        if historico:
            df_hist = pd.DataFrame(historico)
            fig_line = px.line(df_hist, x='Año', y='Votos Totales MC', markers=True, color_discrete_sequence=['#ff9800'])
            fig_line.update_layout(height=400, margin={"r":0,"t":10,"l":0,"b":0})
            st.plotly_chart(fig_line, use_container_width=True)

        st.markdown("---")

        # ---- RENGLÓN 3: DESCARGA DE BASTIONES ----
        st.markdown("<div class='sub-title'>📋 Directorio Descargable de Bastiones Naranjas</div>", unsafe_allow_html=True)
        st.write("Tabla maestra con todas las secciones electorales catalogadas como Bastión por su alta rentabilidad en 2024. Puedes exportar esta tabla a Excel/CSV para entregar a coordinadores territoriales.")

        if 'BASTION_MC' in df_tabla_pura.columns:
            bastiones = df_tabla_pura[df_tabla_pura['BASTION_MC'] == 'BASTIÓN NARANJA'].copy()
            
            # Formatear la tabla para que se vea bonita
            cols_ver = ['SECCION', 'MC_2024', '%_MC_2024', 'CRECIMIENTO_MC_21_24', 'POBTOT_2026', 'TASA_POBREZA_EXTREMA']
            cols_disp = [c for c in cols_ver if c in bastiones.columns]
            
            tabla_export = bastiones[cols_disp].sort_values(by='%_MC_2024', ascending=False)
            if '%_MC_2024' in tabla_export.columns: 
                tabla_export['%_MC_2024'] = (tabla_export['%_MC_2024']*100).round(2).astype(str) + '%'
            if 'CRECIMIENTO_MC_21_24' in tabla_export.columns: 
                tabla_export['CRECIMIENTO_MC_21_24'] = (tabla_export['CRECIMIENTO_MC_21_24']*100).round(2).astype(str) + '%'
            if 'TASA_POBREZA_EXTREMA' in tabla_export.columns: 
                tabla_export['TASA_POBREZA_EXTREMA'] = (tabla_export['TASA_POBREZA_EXTREMA']*100).round(2).astype(str) + '%'
            
            st.dataframe(tabla_export, use_container_width=True)
            
            # Botón de Descarga
            csv_export = tabla_export.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Descargar Directorio de Bastiones (CSV)",
                data=csv_export,
                file_name="bastiones_mc_toluca.csv",
                mime="text/csv",
            )

        st.markdown("---")

        # ---- RENGLÓN 4: SCATTER DE CRUCE SOCIAL ----
        st.markdown("<div class='sub-title'>📉 Cruce Social: Pobreza vs Voto Naranja</div>", unsafe_allow_html=True)
        st.write("Correlación directa entre la Tasa de Pobreza Extrema (Eje X) y el nivel de votos conseguidos (Eje Y). Si las burbujas suben del lado izquierdo, el partido es fuerte en zonas no vulnerables; si suben del lado derecho, el partido penetró en los sectores de mayor pobreza.")
        
        if '%_MC_2024' in df_tabla_pura.columns:
            fig_scat = px.scatter(
                df_tabla_pura, x="TASA_POBREZA_EXTREMA", y="%_MC_2024", 
                hover_name="SECCION", size="POBTOT_2026", color="BASTION_MC",
                color_discrete_map={"BASTIÓN NARANJA": "#ff9800", "REGULAR": "#CFD8DC"}
            )
            fig_scat.update_layout(height=400, margin={"r":0,"t":10,"l":0,"b":0})
            st.plotly_chart(fig_scat, use_container_width=True)

        st.markdown("---")

        # ---- RENGLÓN 5: ANÁLISIS AUTOMATIZADO ----
        st.markdown("<div class='sub-title'>🧠 Análisis Estratégico de Resultados</div>", unsafe_allow_html=True)
        total_bastiones = len(df_tabla_pura[df_tabla_pura['BASTION_MC'] == 'BASTIÓN NARANJA']) if 'BASTION_MC' in df_tabla_pura.columns else 0
        
        crecimiento_promedio = 0
        if 'CRECIMIENTO_MC_21_24' in df_tabla_pura.columns: 
            crecimiento_promedio = df_tabla_pura['CRECIMIENTO_MC_21_24'].mean() * 100
            
        try: 
            seccion_top = df_tabla_pura.loc[df_tabla_pura['%_MC_2024'].idxmax()]['SECCION']
        except: 
            seccion_top = "N/D"

        st.success(f"""
        **Resumen Automatizado de Inteligencia Política:**
        * **Concentración de Fuerza:** Se han detectado **{total_bastiones} secciones** catalogadas como *Bastiones Naranjas*. Estas representan el Top 20% de mayor rentabilidad electoral para el partido en 2024. La sección más fuerte es la **Sección {seccion_top}**.
        * **Dinámica Reciente:** Al comparar la elección de 2021 frente a 2024, el partido presentó una variación promedio por sección del **{crecimiento_promedio:.2f}%** en su captación de votos totales.
        * **Táctica Territorial:** Utiliza el archivo descargado para diseñar rutas de trabajo focalizadas y proteger la estructura en las secciones prioritarias.
        """)