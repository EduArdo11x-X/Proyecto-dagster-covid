import pandas as pd
import dagster as dg
from datetime import datetime
from io import StringIO
import requests

# Países de interés
PERU_ECUADOR = ["Peru", "Ecuador"]

# ---------------------------------------------------------
# Asset: Leer datos desde URL canónica
# ---------------------------------------------------------
@dg.asset(
    automation_condition=dg.AutomationCondition.eager(),
    description="Descarga el dataset completo desde OWID (sin transformar)."
)
def leer_datos() -> pd.DataFrame:
    # ✅ URL corregida (sin espacios extra)
    URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
    
    try:
        response = requests.get(URL, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        raise Exception(f"❌ Error al descargar datos: {e}")

    try:
        df = pd.read_csv(
            StringIO(response.text),
            usecols=["location", "date", "new_cases", "people_vaccinated", "population"]
        )
    except KeyError as e:
        raise Exception(f"❌ Columna faltante en CSV: {e}")

    # ✅ Convertir tipos
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["location"] = df["location"].astype(str).str.strip().str.title()

    return df


# ---------------------------------------------------------
# Asset Check: Validaciones en datos de entrada
# ---------------------------------------------------------
@dg.asset_check(asset=leer_datos)
def chequeos_entrada(leer_datos: pd.DataFrame) -> dg.AssetCheckResult:
    rules = []

    # Verificar columnas esenciales
    required_cols = ["location", "date", "new_cases", "population"]
    for col in required_cols:
        exists = col in leer_datos.columns
        rules.append({
            "rule": f"{col} presente",
            "passed": exists
        })

    # Verificar países
    if "location" in leer_datos.columns:
        locations = leer_datos["location"].astype(str).str.strip().str.title()
        paises_encontrados = locations.isin(PERU_ECUADOR).any()
        rules.append({
            "rule": "datos de Perú o Ecuador presentes",
            "passed": bool(paises_encontrados)
        })
    else:
        rules.append({
            "rule": "datos de Perú o Ecuador presentes",
            "passed": False
        })

    # Validar fechas
    if "date" in leer_datos.columns:
        try:
            max_date = pd.to_datetime(leer_datos["date"]).max()
            today = pd.to_datetime(datetime.today().date())
            fecha_valida = max_date <= today + pd.Timedelta(days=30)
        except Exception:
            fecha_valida = False
    else:
        fecha_valida = False
    rules.append({"rule": "fecha máxima razonable", "passed": bool(fecha_valida)})

    # Unicidad (location, date)
    if "location" in leer_datos.columns and "date" in leer_datos.columns:
        duplicated = leer_datos.duplicated(subset=["location", "date"]).sum()
    else:
        duplicated = 0
    rules.append({
        "rule": "unicidad (location, date)",
        "passed": duplicated == 0,
        "metadata": {"filas_duplicadas": str(duplicated)}
    })

    # population > 0
    if "population" in leer_datos.columns:
        pop_valid = (leer_datos["population"] > 0).all()
    else:
        pop_valid = False
    rules.append({
        "rule": "population > 0",
        "passed": bool(pop_valid)
    })

    # new_cases ≥ 0
    if "new_cases" in leer_datos.columns:
        negativos = (leer_datos["new_cases"] < 0).sum()
    else:
        negativos = 0
    rules.append({
        "rule": "new_cases ≥ 0",
        "passed": negativos == 0,
        "metadata": {"negativos": str(negativos)}
    })

    passed = bool(all(r["passed"] for r in rules))
    
    # ✅ Tabla de resumen
    resumen = []
    for r in rules:
        filas_afectadas = r["metadata"].get("filas_duplicadas", r["metadata"].get("negativos", "0")) if "metadata" in r else "0"
        notas = "Continuar con advertencia" if not r["passed"] else "OK"
        resumen.append({
            "nombre_regla": r["rule"],
            "estado": "Sí" if r["passed"] else "No",
            "filas_afectadas": filas_afectadas,
            "notas": notas
        })

    metadata = {r["rule"]: "Sí" if r["passed"] else "No" for r in rules}
    metadata["resumen"] = str(resumen)

    return dg.AssetCheckResult(passed=passed, metadata=metadata)


# ---------------------------------------------------------
# Asset: Datos procesados
# ---------------------------------------------------------
@dg.asset(
    description="Filtra y limpia datos para Ecuador y Perú."
)
def datos_procesados(leer_datos: pd.DataFrame) -> pd.DataFrame:
    df = leer_datos[leer_datos["location"].isin(PERU_ECUADOR)].copy()
    df = df.dropna(subset=["new_cases", "people_vaccinated"])
    
    # ✅ Eliminar duplicados (mantener último)
    if df.duplicated(subset=["location", "date"]).any():
        df = df.drop_duplicates(subset=["location", "date"], keep="last")
    
    return df[["location", "date", "new_cases", "people_vaccinated", "population"]]


# ---------------------------------------------------------
# Asset: Métrica - Incidencia 7 días
# ---------------------------------------------------------
@dg.asset(
    description="Calcula incidencia acumulada por 100k habitantes (promedio móvil 7 días)."
)
def metrica_incidencia_7d(datos_procesados: pd.DataFrame) -> pd.DataFrame:
    df = datos_procesados.copy()
    df["incidencia_diaria"] = (df["new_cases"] / df["population"]) * 100000
    df = df.sort_values(["location", "date"])
    df["incidencia_7d"] = (
        df.groupby("location")["incidencia_diaria"]
        .rolling(7)
        .mean()
        .reset_index(0, drop=True)
    )
    return df[["date", "location", "incidencia_7d"]].dropna()


# ---------------------------------------------------------
# Asset: Métrica - Factor de crecimiento
# ---------------------------------------------------------
@dg.asset(
    description="Calcula el factor de crecimiento semanal (casos semana actual vs anterior)."
)
def metrica_factor_crec_7d(datos_procesados: pd.DataFrame) -> pd.DataFrame:
    df = datos_procesados.copy()
    df = df.sort_values(["location", "date"])
    res = []
    for loc, g in df.groupby("location"):
        # ✅ No usar set_index("date")
        actual = g["new_cases"].rolling(7).sum()
        previa = g["new_cases"].shift(7).rolling(7).sum()
        factor = actual / previa

        # ✅ Manejar inf y NaN
        factor = factor.replace([float('inf'), float('-inf')], None)
        factor = factor.fillna(0)

        tmp = pd.DataFrame({
            "date": g["date"],
            "location": loc,
            "casos_semana": actual.values,
            "factor_crec_7d": factor.values
        })
        res.append(tmp)
    
    result = pd.concat(res).dropna(subset=["factor_crec_7d"])
    return result[(result["factor_crec_7d"] >= 0) & (result["factor_crec_7d"] <= 10)]


# ---------------------------------------------------------
# Asset Check: Validar salida
# ---------------------------------------------------------
@dg.asset_check(asset=metrica_incidencia_7d)
def chequeos_salida_incidencia(metrica_incidencia_7d: pd.DataFrame) -> dg.AssetCheckResult:
    if metrica_incidencia_7d.empty:
        return dg.AssetCheckResult(passed=False, metadata={"error": "DataFrame vacío"})

    valid_range = (metrica_incidencia_7d["incidencia_7d"] >= 0) & (metrica_incidencia_7d["incidencia_7d"] <= 2000)
    passed = bool(valid_range.all())

    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            "rango_valido": "Sí" if passed else "No",
            "total_filas": str(len(metrica_incidencia_7d)),
            "fuera_de_rango": str(int((~valid_range).sum()))
        }
    )


# ---------------------------------------------------------
# Asset: Exportar a Excel
# ---------------------------------------------------------
@dg.asset(
    description="Exporta un reporte comparativo entre Ecuador y Perú."
)
def reporte_excel_covid(
    metrica_incidencia_7d: pd.DataFrame,
    metrica_factor_crec_7d: pd.DataFrame
) -> str:
    output_path = "/workspaces/Proyecto-dagster-covid/reporte_covid.xlsx"

    # Asegurar que 'date' sea datetime
    df_incidencia = metrica_incidencia_7d.copy()
    df_factor = metrica_factor_crec_7d.copy()
    df_incidencia["date"] = pd.to_datetime(df_incidencia["date"])
    df_factor["date"] = pd.to_datetime(df_factor["date"])

    # Pivotear incidencia
    df_incidencia_pivot = df_incidencia.pivot_table(
        index="date",
        columns="location",
        values="incidencia_7d",
        aggfunc="first"
    ).reset_index()
    df_incidencia_pivot.columns.name = ""
    df_incidencia_pivot = df_incidencia_pivot.rename_axis(None, axis=1)

    # Pivotear factor y casos
    df_factor_pivot = df_factor.pivot_table(
        index="date",
        columns="location",
        values="factor_crec_7d",
        aggfunc="first"
    ).reset_index()
    df_factor_pivot.columns.name = ""
    df_factor_pivot = df_factor_pivot.rename_axis(None, axis=1)

    df_casos_pivot = df_factor.pivot_table(
        index="date",
        columns="location",
        values="casos_semana",
        aggfunc="first"
    ).reset_index()
    df_casos_pivot.columns.name = ""
    df_casos_pivot = df_casos_pivot.rename_axis(None, axis=1)
    df_casos_pivot = df_casos_pivot.add_suffix("_casos")
    df_casos_pivot = df_casos_pivot.rename(columns={"date_casos": "date"})

    # Combinar
    df_factor_final = pd.merge(df_factor_pivot, df_casos_pivot, on="date", how="left")

    # Reordenar: Ecuador primero
    def reorder_columns(df):
        cols = ["date"]
        ecu = [col for col in df.columns if "Ecuador" in str(col)]
        per = [col for col in df.columns if "Peru" in str(col)]
        other = [col for col in df.columns if col not in cols + ecu + per]
        return df[cols + ecu + per + other]

    df_incidencia_pivot = reorder_columns(df_incidencia_pivot)
    df_factor_final = reorder_columns(df_factor_final)

    # Exportar a Excel con formato
    try:
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            df_incidencia_pivot.to_excel(writer, sheet_name="Incidencia 7D", index=False)
            df_factor_final.to_excel(writer, sheet_name="Factor Crecimiento", index=False)

            # Formatear fechas
            for sheet_name in writer.sheets:
                ws = writer.sheets[sheet_name]
                for row in ws.iter_rows(min_row=2, max_col=1):
                    for cell in row:
                        if isinstance(cell.value, pd.Timestamp):
                            cell.number_format = "yyyy-mm-dd"
                            cell.alignment = cell.alignment.copy(horizontal='center')

    except Exception as e:
        raise RuntimeError(f"Error al escribir el archivo Excel: {e}")

    return output_path