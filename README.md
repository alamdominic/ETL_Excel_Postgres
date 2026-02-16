# ETL Server para Sincronización de Excel a PostgreSQL

Este proyecto implementa un servidor ETL (Extract, Transform, Load) diseñado para extraer datos de hojas de cálculo de Microsoft Excel, procesarlos y cargarlos de manera eficiente en una base de datos PostgreSQL.

## Descripción General

El sistema automatiza la sincronización de datos entre un archivo Excel local y una base de datos PostgreSQL. Está diseñado para ser robusto, tolerante a fallos y fácil de configurar. El proceso principal se centra en la inserción incremental de nuevos registros, utilizando un identificador único para evitar duplicados.

## Características Principales

- **Carga de Datos desde Excel**: Extrae datos directamente de hojas de cálculo (`.xlsx`).
- **Sincronización Incremental**: Solo inserta registros nuevos basados en un ID de transferencia, optimizando el rendimiento.
- **Manejo Robusto de Metadatos**: Alinea y convierte automáticamente los tipos de datos del DataFrame de Pandas para que coincidan con el esquema de la tabla de destino en PostgreSQL.
- **Notificaciones por Correo Electrónico**: Envía informes de estado (éxito o fallo) por correo electrónico al finalizar cada proceso.
- **Registro Detallado (Logging)**: Genera logs detallados en archivos y en la consola para facilitar el seguimiento y la depuración.
- **Configuración Centralizada**: Gestiona las credenciales y la configuración de la base de datos de forma segura.

## Estructura del Proyecto

```
ETL_SERVER/
│
├── config/             # Módulos de configuración (BD, email).
├── db/                 # Lógica de interacción con la base de datos (inserción, queries).
├── excel_docs/         # Archivos Excel de entrada (ignorados por Git).
├── logs/               # Archivos de log generados (ignorados por Git).
├── upsert/             # Lógica para operaciones de actualización/inserción (actualmente en desuso).
├── utils/              # Módulos de utilidad (envío de email, extracción de Excel, etc.).
│
├── main_orchestrator.py # Punto de entrada principal del ETL.
├── README.md            # Este archivo.
└── .gitignore           # Archivos y carpetas a ignorar por Git.
```

## Instalación y Configuración

1.  **Clonar el Repositorio**:

    ```bash
    git clone <URL-del-repositorio>
    cd ETL_SERVER
    ```

2.  **Instalar Dependencias**:
    Asegúrate de tener `pip` instalado y ejecuta:

    ```bash
    pip install pandas psycopg2-binary
    ```

3.  **Configurar Variables de Entorno**:
    Crea un archivo `.env` en la raíz del proyecto o configura las siguientes variables de entorno en tu sistema:

    **Para la conexión a la base de datos:**
    - `DB_HOST_LOCAL_PG`: Host de la base de datos.
    - `DB_USER_LOCAL_PG`: Usuario de la base de datos.
    - `DB_PASSWORD_LOCAL_PG`: Contraseña del usuario.
    - `DB_NAME_LOCAL_PG`: Nombre de la base de datos.
    - `DB_PORT_LOCAL_PG`: Puerto (por defecto `5432`).

    **Para el envío de correos electrónicos:**
    - `EMAIL_SENDER`: Dirección de correo del remitente.
    - `EMAIL_PASSWORD`: Contraseña o token de aplicación del correo electrónico.
    - `RECIPIENT_EMAIL`: Dirección de correo que recibirá los reportes.

    **Para la configuración de archivos:**
    - `EXCEL_FILE_PATH`: Ruta completa al archivo Excel de entrada (opcional).

## Uso

1.  **Configurar el Orquestador**:
    Abre `main_orchestrator.py` y ajusta las siguientes variables:
    - `EXCEL_PATH`: Ruta al archivo Excel que contiene los datos.
    - `sheets_name`: Lista de nombres de las hojas que se procesarán.

2.  **Ejecutar el Proceso ETL**:
    Desde la raíz del proyecto, ejecuta el siguiente comando en tu terminal:
    ```bash
    python main_orchestrator.py
    ```

## Flujo de Trabajo del ETL

1.  **Inicio**: El `main_orchestrator.py` inicia el proceso y configura el logging.
2.  **Iteración por Hoja**: Recorre la lista de hojas de Excel definidas.
3.  **Obtención de Esquema**: Se conecta a la base de datos para obtener el esquema (columnas y tipos de dato) de la tabla de destino.
4.  **Lectura de Excel**: Extrae los datos de la hoja de cálculo a un DataFrame de Pandas.
5.  **Alineación de Datos**: Compara el DataFrame con el esquema de la BD y realiza conversiones de tipo para asegurar la compatibilidad.
6.  **Filtrado de Registros**: Identifica el último `no_de_transferencia` en la tabla y filtra el DataFrame para procesar únicamente los registros más recientes.
7.  **Inserción de Datos**: Inserta los nuevos registros en la tabla de PostgreSQL correspondiente.
8.  **Notificación**: Envía un correo electrónico con el resultado del proceso (éxito o fallo) y adjunta el archivo de log.

## Manejo de Errores

- Si una hoja de Excel no es reconocida, el proceso la omite y lo registra.
- Si ocurre un error durante la conexión a la base de datos, la lectura del archivo o la inserción de datos, el error se captura, se registra en el log y se notifica por correo electrónico.
- El sistema está diseñado para continuar con la siguiente hoja si una falla, minimizando la interrupción del proceso completo.
