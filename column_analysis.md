# Comparación: Columnas en Scripts vs Tabla Neon PostgreSQL

## ❌ Problemas Encontrados

### 1. Nombres de columnas con diferencias:
- **Script**: `latitude`, `longitude`
- **Neon**: `_Georreferenciación del punto_latitude`, `_Georreferenciación del punto_longitude`

### 2. Columnas FALTANTES en scripts (presentes en Neon):
- `Ingrese número de documento del usuario que completa el formulario`
- `Ingrese solo los digitos de su documento`

### 3. Nombres incorrectos:
- **Script**: `Se observan niños/as en el punto`
- **Neon**: `Se observan niños/as en el lugar`
- **Script**: `__version__`
- **Neon**: `_version_`

### 4. Columnas en SCRIPTS que NO están en tabla Neon:
- `Turno`
- `hora_start`
- `Poligono`
- `Localizacion`
- `Características observables del punto`
- `estructura`
- `colchon`
- `Características observables del punto/Basura, ropa, bolsos, etc`
- `Características observables del punto/No se observan cosas`

## ✅ Columnas que coinciden correctamente:
- start, end, today, username, deviceid
- Georreferenciación del punto
- _Georreferenciación del punto_altitude
- _Georreferenciación del punto_precision
- Cantidad de personas en situación de calle observadas
- La/s persona/s esta/n
- En el lugar hay… (y todas sus variantes)
- _id, _uuid, _submission_time, _validation_status, _notes, _status, _submitted_by, _tags, _index

## 📋 Recomendaciones:

1. **Renombrar columnas** de coordenadas para que coincidan con Neon
2. **Agregar columnas faltantes** del formulario Kobo
3. **Corregir nombre** de la columna de niños/as
4. **Corregir** `__version__` a `_version_`
5. **Decidir**: ¿Las columnas extras (Turno, Poligono, Localizacion) deben agregarse a la tabla Neon?
