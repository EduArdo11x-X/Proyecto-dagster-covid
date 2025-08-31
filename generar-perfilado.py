import pandas as pd
import requests
from io import StringIO

# ✅ URL correcta
URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"

try:
    # Descargar datos
    response = requests.get(URL)
    response.raise_for_status()
    print("✅ Datos descargados correctamente")

    # Leer CSV
    df = pd.read_csv(StringIO(response.text))
    print(f"✅ CSV cargado: {df.shape[0]:,} filas, {df.shape[1]} columnas")

    # Verificar columnas disponibles
    print("📌 Columnas disponibles:")
    print(df.columns.tolist())

    # Verificar si 'location' existe
    if "location" not in df.columns:
        raise KeyError("", list(df.columns))

    # Filtrar por Ecuador y Perú
    df_filtro = df[df["location"].isin(["Ecuador", "Peru"])].copy()
    print(f"✅ Filtrado: {len(df_filtro)} filas para Ecuador y Perú")

    # Validar columnas clave
    required_cols = ["new_cases", "people_vaccinated", "date"]
    for col in required_cols:
        if col not in df_filtro.columns:
            raise ValueError(f"❌ Columna faltante: {col}")

    # Calcular perfilado
    perfil = pd.DataFrame([{
        "columnas": str(list(df_filtro.columns)),
        "tipos": str(df_filtro.dtypes.to_dict()),
        "new_cases_min": float(df_filtro["new_cases"].min()),
        "new_cases_max": float(df_filtro["new_cases"].max()),
        "missing_new_cases_pct": float(df_filtro["new_cases"].isna().mean() * 100),
        "missing_people_vaccinated_pct": float(df_filtro["people_vaccinated"].isna().mean() * 100),
        "fecha_min": str(df_filtro["date"].min()),
        "fecha_max": str(df_filtro["date"].max())
    }])

    # Guardar
    perfil.to_csv("tabla_perfilado.csv", index=False)
    print("✅ Archivo 'tabla_perfilado.csv' guardado exitosamente")

except requests.exceptions.RequestException as e:
    print(f"❌ Error de red: {e}")
except Exception as e:
    print(f"❌ Error procesando datos: {e}")
    raise