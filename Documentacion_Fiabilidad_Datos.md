# Documento de Fiabilidad y Consistencia de Datos - Kobo Flash

## 1. Introducción
Este documento explica las mejoras implementadas en el sistema de procesamiento de datos para garantizar que la información visualizada en Looker y reportes sea **sólida, constante y libre de variaciones** por desajustes horarios o errores de geolocalización.

---

## 2. El Problema Original: Desfasajes por Recorte de Hora
Anteriormente, los datos podían sufrir un desfasaje de un día debido a:
1. **Truncamiento de Datos**: Al subir la fecha sin la hora (sólo Año-Mes-Día), el sistema perdía precisión.
2. **Ajustes Redundantes**: Se aplicaban cálculos de zona horaria sobre datos que ya estaban en hora local, lo que provocaba que un registro de hoy "saltara" a ayer al restarle horas de forma innecesaria.

---

## 3. Solución de Fechas y Timestamps

### A. Preservación del Timestamp Completo (`start`)
- **Qué hace**: El sistema guarda la fecha **y la hora exacta** (ej. `2026-02-19 14:00:00`) en lugar de solo la fecha.
- **Beneficio**: Evita que los motores de base de datos asuman que el registro ocurrió a la medianoche (00:00:00).

### B. `fecha_reporte` (fuente de verdad diaria)
- **Qué hace**: Extrae el día directamente del campo `start`. Si el registro es de madrugada (antes de las 3:00 hs, turno TN), se asigna al día anterior ya que operativamente pertenece a esa jornada.
- **Uso**: Columna definitiva para el Eje X de todos los gráficos diarios.

### C. `inicio_semana_lunes` (fuente de verdad semanal)
- **Qué hace**: Calcula el lunes correspondiente a cada registro automáticamente.
- **Beneficio**: Los totales semanales son estables e independientes de la configuración regional del equipo que abre el reporte.

---

## 4. Clasificación Geográfica

### Fuente de datos
El sistema usa **`Mapas flash.geojson`** como fuente única de zonas operativas Flash. Reemplaza los archivos `Palermo_Norte.kmz` y `comunas.shp` que se usaban anteriormente.

### Zonas Flash reconocidas

| Código | Nombre | Prioridad |
|---|---|---|
| `C2` | Recoleta | 1 (más alta) |
| `C14` | Palermo | 2 |
| `C13` | Belgrano-Núñez | 3 |
| `C12` | Comuna 12 | 4 |
| `C1A` | Retiro / Recoleta Norte | 5 |
| `C6` | Caballito | 6 |
| `Otro` | Fuera de zona Flash | — |

La capa **"Zona de Frontera"** del GeoJSON se ignora; no clasifica ningún punto.

### Lógica base (GPS)
El sistema clasifica cada punto contra los polígonos del GeoJSON en **orden estricto de prioridad**. Si un punto cae dentro de más de un polígono (solapamiento), gana el de mayor prioridad (`C2 > C14 > C13 > C12 > C1A > C6`). Puntos que no caen en ninguna zona quedan como `"Otro"`.

### Override por Flash Declarado (desde 2026-03-17)
A partir del 17 de marzo de 2026, el formulario Kobo incorpora la pregunta **"Nombre del relevamiento"** (`tipo_flash`), donde el operador indica en qué flash está trabajando.

**Mapeo `tipo_flash` → zona:**

| `tipo_flash` (código Kobo) | Label en formulario | Zona asignada |
|---|---|---|
| `1` | Comuna 2 (Recoleta) | `C2` |
| `2` | Comuna 14 (Palermo) | `C14` |
| `3` | Comuna 13 (Belgrano-Núñez) | `C13` |
| `4` | Otro | Sin override (solo GPS) |
| `5` | Comuna 12 | `C12` |
| `6` | Comuna 1 | `C1A` |
| `7` | Comuna 6 (Caballito) | `C6` |

**Lógica de override (100 metros):**

Si el GPS clasifica el punto en una zona distinta a la declarada, el sistema mide la distancia en metros reales (CRS EPSG:22185, Gauss-Kruger Faja 5) desde el punto hasta el borde de la zona declarada:
- **Distancia < 100m** → se confía en la declaración del operador y se reasigna.
- **Distancia ≥ 100m** → se mantiene el resultado del GPS (probable error de carga).

**Histórico**: los registros anteriores al 17/03/2026 tienen `tipo_flash = NULL` y se clasifican únicamente por GPS. Esto es correcto y no afecta la comparabilidad histórica.

---

## 5. Estabilidad y Unicidad

1. **Sin duplicados**: El campo `_uuid` de Kobo actúa como guardia. El sistema verifica siempre que un UUID no exista en Neon antes de insertarlo.
2. **Carga incremental**: En cada ejecución se detecta el `MAX(_submission_time)` existente en Neon y solo se bajan registros posteriores a ese momento desde Kobo.
3. **Histórico alineado**: Todos los registros históricos fueron reclasificados con la nueva lógica de zonas Flash para garantizar comparabilidad total.

---

## 6. Columnas en la Base de Datos (Neon)

| Columna | Descripción |
|---|---|
| `start` | Timestamp completo del evento (fecha + hora exacta) |
| `hora_start` | Hora extraída de `start` en formato HH:MM:SS |
| `fecha_reporte` | Fecha del operativo (con corrección de madrugada TN) |
| `inicio_semana_lunes` | Lunes de la semana a la que pertenece el registro |
| `Turno` | TM / TO / TT / TN según hora del `start` |
| `Localizacion` | Código de zona Flash: `C2`, `C14`, `C13`, `C12`, `C1A`, `C6` o `"Otro"` |
| `tipo_flash` | Flash declarado por operador (1=C2, 2=C14, 3=C13, 4=Otro, 5=C12, 6=C1A, 7=C6). NULL en histórico. |
| `tipo_flash_otro` | Texto libre cuando `tipo_flash = 4`. NULL en histórico. |
| `_uuid` | Identificador único de Kobo (garantiza unicidad) |
| `_submission_time` | Momento en que se envió el formulario a Kobo |

---

## 7. Alerta: Valores Aberrantes en Personas

Se detectaron registros con valores imposibles en la columna `Cantidad de personas en situación de calle observadas` (ej: 24.235.782, 450.435.398). Estos son **errores de carga** del operador y no representan datos reales. Todos provienen de `username not found`.

**Recomendación para Looker**: aplicar un filtro `<= 100` sobre esa columna para excluir estos outliers de cualquier suma o promedio. Los registros en sí son válidos (el punto georreferenciado es correcto), solo el campo de personas está errado.

---

## 8. Guía de Uso para Analistas

| Necesidad | Campo a usar |
|---|---|
| Gráfico diario | `fecha_reporte` |
| Gráfico semanal | `inicio_semana_lunes` |
| Conteo de puntos (sin duplicados) | `COUNT(_uuid)` |
| Filtrar por zona Flash | `Localizacion` (`C2`, `C14`, `C13`, `C12`, `C1A`, `C6`, `"Otro"`) |
| Identificar tipo de operativo | `tipo_flash` (disponible desde 17/03/2026) |
| Sumar personas (sin outliers) | `SUM` con filtro `<= 100` |

---

## 9. Automatización

El proceso corre automáticamente vía **GitHub Actions** de lunes a viernes cada hora (minuto 15). También puede dispararse manualmente desde la pestaña Actions en GitHub con el botón "Run workflow".

---

**Conclusión**: El sistema es una tubería directa y limpia desde Kobo hasta Looker. La clasificación geográfica usa el GeoJSON `Mapas flash.geojson` como fuente única de verdad, combinada con la intención declarada del operador (override 100m) para minimizar errores de borde. Cada punto tiene exactamente una zona asignada. Cualquier cambio en el tablero responde a cargas reales de los equipos en calle, no a errores del sistema.
