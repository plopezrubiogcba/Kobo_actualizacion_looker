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

### Lógica base (GPS)
El sistema clasifica cada punto registrado contra dos capas geográficas en orden de prioridad:

1. **Palermo Norte** (polígono KMZ priorizado): si el punto cae dentro → `Localizacion = 14.5`
2. **Comunas de CABA** (shapefile): si no es Palermo Norte → `Localizacion = número de comuna (1-15)`

El valor `14.5` indica el polígono especial de Palermo Norte, que es una zona priorizada **dentro** de la comuna 14 donde se concentra el operativo Flash Palermo.

### Override por Flash Declarado (desde 2026-03-17)
A partir del 17 de marzo de 2026, el formulario Kobo incorpora la pregunta **"Nombre del relevamiento"** (`tipo_flash`), donde el operador indica en qué flash está trabajando antes de georreferenciar el punto.

**¿Por qué es necesario?** El GPS tiene un margen de error de 5-30 metros. En zonas de borde entre comunas, un operador parado en la vereda correcta puede quedar registrado en la comuna equivocada por pocos metros.

**Lógica de override (100 metros):**

| Flash declarado | `tipo_flash` (código Kobo) | Localizacion asignada |
|---|---|---|
| Flash Recoleta | `1` | `2` (Comuna 2) |
| Flash Palermo Norte | `2` | `14.5` (polígono priorizado) |
| Flash Belgrano | `3` | `13` (Comuna 13) |
| Otro | `4` | Solo GPS, sin override |

**Regla**: si el GPS clasifica el punto en una zona distinta a la declarada, el sistema mide la distancia en metros reales (CRS métrico EPSG:22185, Gauss-Kruger Faja 5) desde el punto hasta el borde de la zona declarada:
- **Distancia < 100m** → se confía en la declaración del operador y se reasigna.
- **Distancia ≥ 100m** → se mantiene el resultado del GPS (probable error de carga).

**Histórico**: los registros anteriores al 17/03/2026 tienen `tipo_flash = NULL` y se clasifican únicamente por GPS. Esto es correcto y no afecta la comparabilidad histórica.

---

## 5. Estabilidad y Unicidad

1. **Sin duplicados**: El campo `_uuid` de Kobo actúa como guardia. El sistema verifica siempre que un UUID no exista en Neon antes de insertarlo.
2. **Carga incremental**: En cada ejecución se detecta el `MAX(_submission_time)` existente en Neon y solo se bajan registros posteriores a ese momento desde Kobo.
3. **Histórico alineado**: Se procesaron los registros históricos alineándolos a la lógica actual, por lo que el pasado es 100% comparable con el presente.

---

## 6. Columnas en la Base de Datos (Neon)

| Columna | Descripción |
|---|---|
| `start` | Timestamp completo del evento (fecha + hora exacta) |
| `hora_start` | Hora extraída de `start` en formato HH:MM:SS |
| `fecha_reporte` | Fecha del operativo (con corrección de madrugada TN) |
| `inicio_semana_lunes` | Lunes de la semana a la que pertenece el registro |
| `Turno` | TM / TO / TT / TN según hora del `start` |
| `Localizacion` | Número de comuna o 14.5 para Palermo Norte |
| `tipo_flash` | Flash declarado por el operador (1=Recoleta, 2=Palermo, 3=Belgrano, 4=Otro). NULL en histórico. |
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
| Filtrar por zona | `Localizacion` (14.5 = Palermo Norte, resto = número de comuna) |
| Identificar tipo de operativo | `tipo_flash` (disponible desde 17/03/2026) |
| Sumar personas (sin outliers) | `SUM` con filtro `<= 100` |

---

## 9. Automatización

El proceso corre automáticamente vía **GitHub Actions** de lunes a viernes cada hora (minuto 15). También puede dispararse manualmente desde la pestaña Actions en GitHub con el botón "Run workflow".

---

**Conclusión**: El sistema es una tubería directa y limpia desde Kobo hasta Looker. La clasificación geográfica combina GPS con la intención declarada del operador para minimizar errores de borde. Cualquier cambio en el tablero responde a cargas reales de los equipos en calle, no a errores del sistema.
