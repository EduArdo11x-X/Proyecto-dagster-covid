
from dagster import Definitions
from . import assets

defs = Definitions(
    assets=[
        assets.leer_datos,
        assets.datos_procesados,
        assets.metrica_incidencia_7d,
        assets.metrica_factor_crec_7d,
        assets.reporte_excel_covid,
    ],
    asset_checks=[
        assets.chequeos_entrada,
        assets.chequeos_salida_incidencia,
    ],
)