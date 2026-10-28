# DynamoDB Management Dashboard & Bot Administration Suite

Un panel web local, moderno e interactivo para la **administración integral, comparación, edición, auditoría, limpieza y carga de datos en DynamoDB** para agentes conversacionales y bots en entornos multiamplificación (**DESA**, **TEST** y **PROD**).

Diseñado con una estética oscura inspirada en VS Code y GitHub, el panel combina un backend en Python multihilo con el poder del editor **Monaco** (el mismo núcleo de VS Code) para ofrecer una experiencia fluida, rápida y segura.

---

## Tabla de Contenidos

1. [Características Principales](#-características-principales)
2. [Vistas y Funcionalidades Detalladas](#-vistas-y-funcionalidades-detalladas)
   - [1. Vista Comparar (Diff Editor)](#1-vista-comparar-diff-editor)
   - [2. Vista JSON Individual](#2-vista-json-individual)
   - [3. Vista Gestión de Prompts](#3-vista-gestión-de-prompts)
   - [4. Vista Modelos (Matriz de Modelos)](#4-vista-modelos-matriz-de-modelos)
   - [5. Vista Auditoría de API Keys](#5-vista-auditoría-de-api-keys)
   - [6. Vista Limpieza de Registros DynamoDB](#6-vista-limpieza-de-registros-dynamodb)
   - [7. Vista Gestión de Tablas DynamoDB (Custom Table)](#7-vista-gestión-de-tablas-dynamodb-custom-table)
3. [Políticas de Seguridad y Resguardo](#-políticas-de-seguridad-y-resguardo)
4. [Estructura del Proyecto](#-estructura-del-proyecto)
5. [Requisitos e Instalación](#-requisitos-e-instalación)
6. [Puesta en Marcha](#-puesta-en-marcha)
7. [Suite de Pruebas Automatizadas](#-suite-de-pruebas-automatizadas)

---

## Características Principales

- **Multi-Ambiente Nativo:** Conmutación instantánea entre **DESA**, **TEST** y **PROD** mediante credenciales y perfiles de AWS SSO configurados.
- **Doble Confirmación Crítica en PROD:** Todas las operaciones de escritura o borrado en Producción exigen tipeo explícito de confirmación (`PROD`, `ELIMINAR EN PROD`, `CARGAR EN PROD` o `VACIAR EN PROD`).
- **Backups Automáticos Obligatorios:** Cada guardado, eliminación o replicación genera instantáneamente un archivo de respaldo con timestamp en el directorio `backups/`.
- **Integridad de Datos Garantizada:** Reglas estrictas de seguridad impiden la alteración o eliminación accidental de la tabla de configuraciones troncal (`{env}_sonia_config`).
- **Editor Monaco Integrado:** Edición de JSON y prompts con validación de sintaxis, minimapa, diff side-by-side, ajuste de línea y búsqueda avanzada.
- **Ejecución Local Segura:** El servidor HTTP embebido escucha exclusivamente en la interfaz local de loopback (`127.0.0.1`), protegiendo los datos de accesos externos.

---

## Vistas y Funcionalidades Detalladas

### 1. Vista Comparar (Diff Editor)
Permite comparar la configuración completa de un bot entre dos ambientes distintos (ej. DESA vs TEST, TEST vs PROD o DESA vs PROD).

- **Editor Lado a Lado (Side-by-Side):** Visualización de diferencias con resaltado de sintaxis JSON.
- **Selectores de Ambiente Independientes:** Permite elegir qué ambiente ver a la izquierda y cuál a la derecha.
- **Botón Swap (`⇄`):** Intercambia los lados de la comparación en un solo clic.
- **Navegación de Diferencias:** Atajos de teclado (`F7` para siguiente cambio, `Shift+F7` para anterior) y botones directos.
- **Filtro de Líneas:** Casilla para *Ocultar líneas iguales* y concentrarse solo en las discrepancias.
- **Buscador en Tiempo Real:** Filtra clientes y bots a medida que escribís.
- **Guardado Parcial con Backup:** Permite editar directamente el JSON de cualquiera de los lados, visualizar un modal con el diff exacto de los campos modificados antes de confirmar y guardar con backup automático.
- **Protección PROD:** En caso de guardar sobre Producción, exige ingresar el nombre exacto del bot.

---

### 2. Vista JSON Individual
Editor enfocado para inspeccionar y modificar la configuración completa de un bot en formato JSON para un único ambiente.

- **Selector Rápido de Ambiente y Bot.**
- **Edición Monaco con Validación:** Detecta errores de sintaxis JSON antes de intentar el guardado.
- **Control de Cambios Sin Guardar:** Alerta preventiva si el usuario intenta cambiar de bot o de vista teniendo modificaciones pendientes.
- **Guardado con Doble Confirmación:** Genera backup automático en `backups/` antes de aplicar los cambios en DynamoDB.

---

### 3. Vista Gestión de Prompts
Área de trabajo especializada para la inspección, redacción y comparación diferencial de los templates de prompts de los bots.

- **Navegación por Árbol de Prompts:** Muestra todos los prompts configurados en `AGENT_CONFIG → PROMPTS` para el bot seleccionado.
- **Editor Monaco de Prompts (`prompt-markdown`):**
  - Resaltado sintáctico de variables dinámicas (`{variable}`), etiquetas XML (`<context>`, etc.), palabras clave y formato markdown.
  - Píldoras interactivas de variables con búsqueda y posicionamiento automático en el cursor.
  - Editor en pantalla completa (`F11` o botón Expandir).
- **Diff Side-by-Side de Ambientes en Rojo y Verde (Monaco Diff Editor auténtico):**
  - Implementación con `monaco.editor.createDiffEditor()` y algoritmo de diferencias avanzado (`diffAlgorithm: "advanced"`).
  - **Ajuste Automático de Línea (Word Wrap) sin pérdida:**
    - Ambos paneles configuran `wordWrap: "on"`, `wrappingStrategy: "advanced"`, `scrollBeyondLastColumn: 0` y `horizontalScrolling: false`.
    - Las líneas largas se adaptan al ancho disponible de cada panel sin exigir desplazamiento horizontal ni cortar texto, preservando los números de línea reales y sin alterar los saltos de línea al guardar.
    - Botón **Ajuste de línea** para alternar interactivamente entre ajuste visual y desplazamiento horizontal.
  - **Resaltado Visual de Alto Contraste (Estilo VS Code):**
    - **Lado Izquierdo (Original):** Líneas eliminadas o discrepantes en **Rojo translúcido**, con badge `ORIGINAL (ROJO)` y el ambiente de referencia.
    - **Lado Derecho (Modificado):** Líneas añadidas o discrepantes en **Verde translúcido**, con badge `MODIFICADO (VERDE)` y el ambiente actual.
    - Detección precisa de cambios a nivel de caracteres dentro de la misma línea (`char-insert` / `char-delete`).
    - Divisor central interactivo (*sash*) redimensionable al 50% con soporte para arrastrar.
  - Botón **Volver a editar** para alternar de inmediato entre la vista de diferencias y el editor mono-panel.
  - Botón directo para copiar la versión del template desde el ambiente de referencia.

---

### 4. Vista Modelos (Matriz de Modelos)
Matriz inteligente para auditar y gestionar los modelos de lenguaje (LLM) y variables de configuración asignadas a cada bot.

- **Matriz de Configuración por Bot:**
  - `OPENAI_MODEL` (ej. `gpt-4o`, `gpt-4o-mini`, etc.)
  - `TEMPERATURE`
  - `REASONING_EFFORT`
  - `RESPONSE_FORMAT_TYPE`
  - `MAX_COMPLETION_TOKENS`
  - `INPUT_VARIABLE`
- **Replicador de Estructura de Prompts (`Replicar estructura de…`):**
  - Permite seleccionar un ambiente origen (ej. DESA) y replicar su estructura de prompts en el ambiente destino (ej. TEST o PROD).
  - Modal interactivo con análisis diferencial previo: muestra qué bots sufrirán cambios y cuáles ya coinciden.
  - Generación de backup automático de cada bot afectado antes de la replicación.
- **Edición Masiva por Lotes (Batch Apply):**
  - Selección de múltiples bots para actualizar parámetros en bloque de forma consistente.
  - Función de **Deshacer Lote (`Deshacer lote`)** para revertir instantáneamente la última operación masiva realizada.
- **Historial de Auditoría (`Historial de cambios`):** Consulta de las modificaciones realizadas en la sesión.
- **Exportación a Excel (`Descargar Excel`):** Descarga una planilla `.xlsx` con el mapeo completo de modelos, prompts y parámetros por bot.
- **Editor Fullscreen de Prompts:** Maximiza el template del prompt para redactar cómodamente sin distracciones.

---

### 5. Vista Auditoría de API Keys
Panel de auditoría y seguridad para revisar el aprovisionamiento, integridad y colisiones de API keys entre ambientes y clientes bots.

- **Filtros Rápidos Intuitivos (con contadores en tiempo real):**
  - **Colisión Prod:** Filtra los bots donde la API key de Producción está asignada a **otro cliente** (colisión crítica entre clientes).
  - **Desa ≠ Test:** Filtra los bots donde la clave de Desarrollo difiere de la de Testing.
  - **Sin Prod:** Filtra los bots que aún no tienen clave configurada en Producción.
  - **Todos:** Listado general completo con buscador por texto (cliente, bot o clave parcial).

- **Reglas del Círculo de Estado (Status Dot) y Diagnóstico Visual:**
  - **La API key de PROD debe ser ÚNICA:** No debe compartirse con ambientes inferiores (**DESA** ni **TEST**) ni con otros clientes.
  - **Código de colores del círculo de estado:**
    - **VERDE:** Si la clave de Producción está presente, es **única** (no se reutiliza en Desa ni en Test) y no colisiona con otros clientes (compartir clave entre bots del mismo cliente en Prod es legítimo y se mantiene en verde).
    - **AMARILLO (Advertencia de Unicidad):** Si la API key de **DESA y/o TEST es igual a la de PROD** (`desa == prod` o `test == prod`), el círculo pasa a ser **AMARILLO**. El tooltip detalla: `Advertencia: [DESA/TEST] tiene la misma API key que PROD. La API key de PROD debe ser única.` Las celdas afectadas se marcan en amarillo (`= PROD ...XXXX` y `⚠ ...XXXX` en Producción).
    - **ROJO (Colisión Crítica):** **Únicamente si OTRO CLIENTE tiene la misma clave en Producción**. El tooltip desplegable advierte de inmediato: `COLISIÓN EN PROD: Clave compartida con otro cliente: [clienteX/botY]`.
  - **Diferencias entre Desa y Test (Celdas en Amarillo):**
    - Si la clave de `test` difiere de `desa`, las celdas de las API keys en las columnas **Desa** y **Test** se colorean en **amarillo** (`≠ ...XXXX`).
    - Si no coinciden con Prod ni colisionan con otros clientes en Prod, el círculo de estado se mantiene verde y el tooltip avisa la diferencia.
  - **Celda de Producción:**
    - En caso de colisión con otro cliente, la clave se resalta en **rojo** (`≠ ...XXXX`).
    - Si se reutiliza en Desa o Test, se resalta en **amarillo** de advertencia (`⚠ ...XXXX`).
    - Si es propia o compartida legítimamente con otros bots del mismo cliente y única respecto a no-prod, se muestra en **verde** (`✓ ...XXXX`).
- **Enmascaramiento Seguro:** Por defecto las claves se muestran truncadas (`...XXXX`). Un botón permite revelar los valores completos para auditoría.
- **Exportación a Excel:** Reporte listo para auditoría en formato `.xlsx`.

---

### 6. Vista Limpieza de Registros DynamoDB
Herramienta de depuración y administración para dar de baja o eliminar bots de prueba y sus prompts de la tabla de configuración (`{env}_sonia_config`).

- **Eliminación del Cliente Bot en DynamoDB:**
  - Ejecuta un `delete_item` sobre `{env}_sonia_config` con clave compuesta `client` + `bot`.
  - Da de baja las configuraciones, parámetros y existencia del agente en el ambiente correspondiente (**DESA**, **TEST** o **PROD**).
- **Selección Individual o Masiva:**
  - Checkboxes individuales por bot y selector global.
  - Buscador en tiempo real por cliente o nombre de bot.
- **Backups Previos Automáticos Obligatorios:** Cada registro eliminado se resguarda íntegramente en formato JSON en `backups/cleanup/<env>_<cliente>_<bot>_<timestamp>.json` antes de ejecutar la eliminación física.
- **Bitácora de Auditoría Permanente:** Cada operación destructiva se registra en `backups/cleanup_audit.jsonl` detallando usuario, timestamp, ambiente y bot eliminado.
- **Exportación a CSV:** Descarga la selección de bots en CSV con codificación `UTF-8 con BOM` para compatibilidad nativa con Excel.
- **Protección Estricta en PROD:** Exige ingresar la frase exacta `ELIMINAR EN PROD` para desbloquear la ejecución.

---

### 7. Vista Gestión de Tablas DynamoDB (Custom Table)
Módulo integral para auditar tablas DynamoDB y cargar masivamente datos estructurados desde archivos externos (`.xlsx`, `.xls`, `.csv`).

#### Pestaña 1: Cargar Tabla (Carga masiva a ancho completo)
> [!IMPORTANT]
> **No crea tablas nuevas:** Esta sección está diseñada exclusivamente para **cargar datos en tablas ya existentes** en DynamoDB. Si se selecciona o ingresa una tabla que no existe en el listado de DynamoDB del ambiente, el sistema arroja una **excepción** preventiva (`ValueError`) y cancela la operación.

- **Diseño a Ancho Completo (Full-Width Workspace):**
  - Se eliminó la columna lateral y el encabezado redundante de pasos para aprovechar el 100% del ancho de pantalla, brindando máxima amplitud visual para la edición de columnas y la previsualización de datos.
- **Tarjetas Principales de Configuración (Proporciones Fijas 50% / 50%):**
  - **Grid rígida de 2 columnas:** Mediante `grid-template-columns: minmax(0, 1fr) minmax(0, 1fr)` y contención de overflow (`min-width: 0`), la distribución de los recuadros se mantiene estrictamente equilibrada por defecto, impidiendo que datos largos o planillas complejas deformen o aplasten la columna de configuración izquierda.
  - **Configuración de la tabla:** Input a ancho completo con autocompletado (`<datalist>`) sincronizado con las tablas existentes en DynamoDB. **Detección automática del ambiente:** Ya no requiere selección manual redundante; el ambiente (**DESA**, **TEST** o **PROD**) se infiere instantáneamente del prefijo del nombre de la tabla (`desa_...`, `test_...`, `prod_...`) con badge visual reactivo.
  - **Archivo de origen con opción de eliminación rápida:** Carga arrastrando o seleccionando archivos Excel (`.xlsx`, `.xls`) o `.csv`. Al cargar un archivo, se habilita una **cruz destacada (`✕`)** en el chip y un botón superior en la cabecera del recuadro (`✕ Quitar archivo`) que permite removerlo al instante en caso de haberlo subido por error y seleccionar uno nuevo sin recargar la página.
  - **Editor dinámico de columnas:**
    - Columnas de ejemplo iniciales: `custom_key`, `alias_display`, `cliente`, `dni`, `tipo_contacto`, `deuda`.
    - Botón `+ Agregar columna` para añadir campos adicionales.
    - Botón para eliminar campos no requeridos.
    - Clave de partición obligatoria: `custom_key` cuenta con badge visual y vector de llave estilizado `Clave de partición (PK)`, se encuentra bloqueada y no puede ser eliminada.
    - Validación en vivo contra nombres vacíos o columnas duplicadas, manteniendo siempre sus dimensiones intactas.
  - **Vista previa y correspondencia con desplazamiento horizontal:**
    - Lee encabezados del archivo y los compara contra las columnas configuradas.
    - Notificación de correspondencia: indica si todas las columnas están presentes o identifica explícitamente cuáles faltan en el archivo.
    - Diagnóstico de Partition Key: advierte si existen filas con `custom_key` vacía (omitidas) o claves duplicadas (sobrescritura).
    - **Tabla de previsualización con scroll lateral e interactivo:** Dotada de desplazamiento horizontal (`overflow-x: auto`) y vertical (`max-height: 230px`), garantizando que registros con múltiples columnas o celdas extensas (como estructuras JSON) se puedan examinar cómodamente hacia el costado sin ensanchar jamás la tarjeta ni romper la simetría del panel.
    - Truncado inteligente con ellipsis y tooltip nativo (`title`) en cada celda para inspeccionar el valor completo al pasar el mouse.
- **Barra de Resumen Inferior y Ejecución:**
  - Ubicada al pie de la vista con ancho completo: reúne operación seleccionada, badge de ambiente, tabla destino, archivo cargado (tamaño y filas), cantidad de columnas y partition key (`custom_key`).
  - Botón principal de acción `Cargar datos en tabla ➔`.
- **Resumen Completo Previo a la Carga (Modal de Validación):**
  - Al pulsar el botón de carga, se abre un diálogo modal que despliega el **resumen exhaustivo de la operación antes de validar o escribir cualquier dato**:
    - Operación seleccionada (`Cargar tabla`).
    - Tabla existente de DynamoDB destino y verificación previa de existencia.
    - Ambiente destino (**DESA**, **TEST**, **PROD**).
    - Archivo de origen con metadatos exactos (peso y cantidad de filas).
    - Listado completo de columnas mapeadas a importar.
    - Total de filas a procesar y recordatorio de omisión para filas sin `custom_key`.
    - En caso de **PRODUCCIÓN**, exige el tipeo de confirmación explícito `CARGAR EN PROD` para desbloquear la ejecución.
  - Carga masiva eficiente mediante `batch_writer` de Boto3 con barra de progreso en tiempo real.

#### Pestaña 2: Listar Tablas
- Consulta y muestra todas las tablas DynamoDB del ambiente seleccionado.
- Detalla la **Partition Key (HASH)**, **Sort Key (RANGE)** y la cantidad aproximada de registros.
- Buscador rápido por nombre de tabla.
- Botón directo `Vaciar…` que preconfigura la tabla en la pestaña siguiente.

#### Pestaña 3: Vaciar Tabla
- Permite purgar todos los registros de una tabla específica sin eliminar su estructura ni definición en DynamoDB.
- Confirmación obligatoria y validación estricta en Producción escribiendo `VACIAR EN PROD`.

---

## Políticas de Seguridad y Resguardo

| Política | Implementación |
|---|---|
| **Ambiente Local** | Servidor acotado a `http://127.0.0.1:8765`, sin exposición remota. |
| **Identificación por Color** | Código cromático unificado: **DESA** en Rojo (`#ef4444`), **TEST** en Amarillo (`#f59e0b`) y **PROD** en Verde (`#22c55e`). |
| **Backups Previos Obligatorios** | Resguardo JSON automático en carpeta `backups/` antes de cualquier guardado, eliminación o carga. |
| **Protección PROD** | Desbloqueo mediante texto exacto (`PROD`, `CARGAR EN PROD`, `ELIMINAR EN PROD`, `VACIAR EN PROD`). |
| **Validación de Tablas Existentes** | La sección Cargar Tabla no crea tablas; arroja excepción preventiva si la tabla no existe en DynamoDB. |
| **Auditoría Continua** | Archivo `backups/cleanup_audit.jsonl` registra cada operación destructiva con fecha y métricas. |
| **Límite de Carga Controlado** | Carga máxima de hasta 50 MB por archivo para evitar saturación de memoria. |

---

## Estructura del Proyecto

```text
├── Compare_ui.py            # Servidor HTTP multihilo, interfaz web HTML/CSS/JS y controladores API
├── custom_table_manager.py  # Lógica de creación de tablas, validación de columnas, preview y carga masiva
├── cleanup_manager.py       # Lógica de detección de registros y vaciado seguro de datos de bots
├── models_manager.py        # Mapeo de modelos LLM, prompts, replicación entre ambientes y exportación
├── promote_config.py        # Configuración de entornos AWS (perfiles SSO, regiones y tablas de config)
├── backups/                 # Carpeta donde se almacenan automáticamente los backups JSON y bitácoras
│   ├── cleanup/             # Backups previos a eliminaciones de bots
│   └── cleanup_audit.jsonl  # Log de auditoría de limpiezas
└── tests/                   # Suite de pruebas automatizadas
    ├── test_custom_table.py # Tests para preview, columnas dinámicas y batch writer
    ├── test_cleanup.py      # Tests para limpieza segura y backups
    └── test_models.py       # Tests para matriz de modelos y replicación
```

---

## Requisitos e Instalación

### Requisitos previos:
- **Python 3.10+** (recomendado Python 3.11 o superior).
- **AWS CLI v2** instalado y configurado con acceso SSO a las cuentas de AWS correspondientes.
- Permisos de lectura/escritura en DynamoDB en las cuentas destino.

### Dependencias de Python:
Instalá las librerías necesarias ejecutando:

```bash
pip install boto3 pandas openpyxl
```

---

### 8. Vista de Auditoría y Métricas de Prompts
Panel analítico para auditar la longitud, peso y consumo de tokens de cada prompt clasificado por modelo y cliente/bot.

- **Captura por Modelo y Orden de Menor a Mayor:**
  - Lista completa de prompts ordenada por defecto de **menor a mayor** según su cantidad de caracteres.
  - Selector de modelo integrado (`gpt-5.6-luna`, `gpt-4.1`, `gpt-4.1-mini`, etc.) para auditar el impacto de cada motor de IA.
- **Visualización de Longitud con Barra de Alerta Dinámica:**
  - `<= 10.000` caracteres: **Azul** (longitud saludable).
  - `> 10.000` y `<= 14.000` caracteres: **Amarilla** (advertencia de crecimiento).
  - `> 14.000` caracteres: **Roja** (crítica / sobrepeso de prompt).
  - Con porcentaje de escala relativo y conteo de tokens estimados (`~tok`).
- **Navegación e Integración Directa:**
  - Botón **Ver prompt**: redirige instantáneamente a la sección **Gestión de Prompts**, seleccionando y abriendo dicho prompt para su edición o comparación diff.
  - Enlace **Abrir bot**: permite abrir el JSON completo del bot en un solo clic.
- **Acceso rápido:**
  - Botón en la barra lateral izquierda (`rail`) y atajo de teclado **Ctrl+8**.

---

## Puesta en Marcha

1. **Iniciar sesión en AWS SSO:**
   ```bash
   aws sso login --profile <tu-perfil-desa>
   ```

2. **Ejecutar la aplicación:**
   ```bash
   python Compare_ui.py
   ```
   *(Opcional: podés especificar un puerto personalizado con `python Compare_ui.py 9000`).*

3. **Abrir en el navegador:**
   La aplicación abrirá automáticamente o podrás acceder navegando a:
   ```text
   http://127.0.0.1:8765
   ```

4. **Navegación:**
   Utilizá la barra lateral (`rail`) o los atajos de teclado para alternar entre:
   - `Ctrl+1`: Comparar ambientes
   - `Ctrl+2`: JSON individual
   - `Ctrl+3`: Gestión de Prompts
   - `Ctrl+4`: Matriz de Modelos
   - `Ctrl+5`: Auditoría de API Keys
   - `Ctrl+6`: Limpieza DynamoDB
   - `Ctrl+7`: Gestión de Tablas DynamoDB
   - `Ctrl+8`: Métricas de Prompts

---

## Suite de Pruebas Automatizadas

El proyecto cuenta con una cobertura integral de tests unitarios que validan la lógica de negocio, manejo de errores, parseo de archivos y protecciones de seguridad:

```bash
python -m unittest discover tests
```

Para ejecutar pruebas individuales de un módulo en particular:
```bash
# Pruebas de Custom Tables y carga masiva
python -m unittest tests/test_custom_table.py

# Pruebas de limpieza y backups
python -m unittest tests/test_cleanup.py
```
