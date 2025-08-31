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
    description="Descarga el dataset completo desde OWID."
)
def leer_datos() -> pd.DataFrame:
    URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"
    response = requests.get(URL)
    response.raise_for_status()
    
    df = pd.read_csv(
        StringIO(response.text),
        usecols=["location", "date", "new_cases", "people_vaccinated", "population"]
    )
    
    df["date"] = pd.to_datetime(df["date"])
    df["location"] = df["location"].astype(str).str.strip().str.title()
    
    return df

# ---------------------------------------------------------
# Asset Check: Validaciones en datos de entrada
# ---------------------------------------------------------
@dg.asset_check(asset=leer_datos)
def chequeos_entrada(leer_datos: pd.DataFrame) -> dg.AssetCheckResult:
    rules = []

    # Columnas presentes
    for col in ["location", "date", "new_cases", "population"]:
        rules.append({
            "rule": f"{col} presente",
            "passed": col in leer_datos.columns
        })

    # Fechas razonables
    try:
        max_date = leer_datos["date"].max()
        today = pd.to_datetime(datetime.today().date())
        fecha_valida = max_date <= today + pd.Timedelta(days=30)
    except:
        fecha_valida = False
    rules.append({"rule": "max(date) <= hoy + 30", "passed": bool(fecha_valida)})

    # Unicidad (location, date)
    duplicated = leer_datos.duplicated(subset=["location", "date"]).sum()
    rules.append({"rule": "unicidad (location, date)", "passed": duplicated == 0})

    # population > 0
    pop_valid = (leer_datos["population"] > 0).all()
    rules.append({"rule": "population > 0", "passed": bool(pop_valid)})

    passed = bool(all(r["passed"] for r in rules))
    metadata = {r["rule"]: "Sí" if r["passed"] else "No" for r in rules}
    return dg.AssetCheckResult(passed=passed, metadata=metadata)

# ---------------------------------------------------------
# Asset: Datos procesados
# ---------------------------------------------------------
@dg.asset
def datos_procesados(leer_datos: pd.DataFrame) -> pd.DataFrame:
    df = leer_datos[leer_datos["location"].isin(PERU_ECUADOR)].copy()
    df = df.dropna(subset=["new_cases", "people_vaccinated"])
    df = df.drop_duplicates()
    return df[["location", "date", "new_cases", "people_vaccinated", "population"]]

# ---------------------------------------------------------
# Asset: Métrica - Incidencia 7 días
# ---------------------------------------------------------
@dg.asset
def metrica_incidencia_7d(datos_procesados: pd.DataFrame) -> pd.DataFrame:
    df = datos_procesados.copy()
    df["incidencia_diaria"] = (df["new_cases"] / df["population"]) * 100000
    df = df.sort_values(["location", "date"])
    df["incidencia_7d"] = df.groupby("location")["incidencia_diaria"].rolling(7).mean().reset_index(0, drop=True)
    return df[["date", "location", "incidencia_7d"]].dropna()

# ---------------------------------------------------------
# Asset: Métrica - Factor de crecimiento
# ---------------------------------------------------------
@dg.asset
def metrica_factor_crec_7d(datos_procesados: pd.DataFrame) -> pd.DataFrame:
    df = datos_procesados.copy()
    df = df.sort_values(["location", "date"])
    res = []
    for loc, g in df.groupby("location"):
        g = g.set_index("date")
        actual = g["new_cases"].rolling(7).sum()
        previa = g["new_cases"].shift(7).rolling(7).sum()
        factor = actual / previa
        tmp = pd.DataFrame({
            "date": g.index,
            "location": loc,
            "casos_semana": actual,
            "factor_crec_7d": factor
        })
        res.append(tmp)
    return pd.concat(res).dropna(subset=["factor_crec_7d"])

# ---------------------------------------------------------
# Asset Check: Validar salida
# ---------------------------------------------------------
@dg.asset_check(asset=metrica_incidencia_7d)
def chequeos_salida_incidencia(metrica_incidencia_7d: pd.DataFrame) -> dg.AssetCheckResult:
    if metrica_incidencia_7d.empty:
        return dg.AssetCheckResult(passed=False, metadata={"error": "vacío"})
    
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
@dg.asset
def reporte_excel_covid(
    metrica_incidencia_7d: pd.DataFrame,
    metrica_factor_crec_7d: pd.DataFrame
) -> str:
    output_path = "/workspaces/Proyecto-dagster-covid/reporte_covid.xlsx"
    
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        metrica_incidencia_7d.to_excel(writer, sheet_name="Incidencia 7D", index=False)
        metrica_factor_crec_7d.to_excel(writer, sheet_name="Factor Crecimiento", index=False)
        
    return output_path