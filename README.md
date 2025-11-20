Automatización ETL: KoboToolbox → Google Sheets
Este proyecto ejecuta un flujo de trabajo ETL (Extract, Transform, Load) automatizado que sincroniza datos de encuestas de KoboToolbox con una hoja de Google Sheets para alimentar un dashboard de Looker.

El script realiza enriquecimiento geoespacial de los datos, clasificando cada punto según su ubicación en las Comunas de CABA o el Anillo Digital.

----------------------------
# Funcionalidades

Extracción: Descarga la base completa desde la API v2 de KoboToolbox.

Transformación Geoespacial:

Convierte coordenadas lat/lon.

Cruce espacial (Point in Polygon): Determina si el punto cae en el "Anillo Digital" (KML) o en una Comuna específica (Shapefile).

Asignación de Turnos según hora de registro.

Carga Incremental: Verifica los _uuid existentes en Google Sheets y sube únicamente los registros nuevos (Append) para optimizar recursos y evitar duplicados.

Híbrido: Funciona tanto localmente como en la nube (GitHub Actions).
-----------------------------
# Estructura del Repositorio
Los archivos geoespaciales deben estar en la raíz para que el script los detecte automáticamente.
├── main.py                 # Script principal (Lógica ETL)
├── requirements.txt        # Dependencias de Python
├── Recoleta Nueva...kml    # Capa del Anillo Digital
├── comunas.shp             # Geometría de Comunas (CABA)
├── comunas.shx             # Índice del Shapefile (Vital)
├── comunas.dbf             # Base de datos del Shapefile (Nombres de comunas)
└── .github/workflows/      # Configuración de ejecución automática (Cron)


----------------------------------------------
# Configuración
1. Dependencias
Para correr localmente:
pip install -r requirements.txt



2. Variables de Entorno (GitHub Secrets)
Para que la automatización funcione en GitHub Actions, se deben configurar los siguientes Repository Secrets:
Secreto,Descripción
KOBO_TOKEN,Token de autenticación de la cuenta KoboToolbox.
GOOGLE_CREDENTIALS_JSON,Contenido completo del JSON de la Service Account de Google Cloud.

-------------------------------------
# Automatización
El flujo de trabajo está configurado en GitHub Actions para ejecutarse automáticamente (ej. cada hora) mediante un disparador CRON.

Levanta un entorno Ubuntu.

Instala librerías espaciales (libspatialindex).

Ejecuta main.py.

Si detecta registros nuevos en Kobo que no están en el Sheet, los procesa y los anexa.
