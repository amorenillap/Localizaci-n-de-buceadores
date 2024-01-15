"""
Codigo para crear una base de datos y una tabla en PostgreSQL con PostGIS
y llenarla con datos geográficos de un buceador.
"""
import time
import psycopg2
from psycopg2 import sql

# Parámetros de conexión para la base de datos por defecto
conn_params_default = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': '12Qw12qw12qw.',
    'host': 'localhost'
}

# Conectar a la base de datos por defecto con autocommit
# Autocommit permite que cada operación de la base de datos se confirme automáticamente
conn_default = psycopg2.connect(**conn_params_default)
conn_default.autocommit = True
cur_default = conn_default.cursor()

# Verificar si la base de datos 'buceador_posicion' existe
# Si existe, fetchone() devolverá un valor distinto de None
cur_default.execute(
    "SELECT 1 FROM pg_database WHERE datname = 'buceador_posicion'")
exists = cur_default.fetchone()

# Si no existe, crear la nueva base de datos
if not exists:
    cur_default.execute(sql.SQL("CREATE DATABASE {}").format(
        sql.Identifier('buceador_posicion'))
    )

# Cerrar la conexión por defecto
cur_default.close()
conn_default.close()

# Parámetros de conexión base de datos buceador_posicion
conn_params_buceador = {
    'dbname': 'buceador_posicion',
    'user': 'postgres',
    'password': '12Qw12qw12qw.',
    'host': 'localhost'
}

# Conectar a la nueva base de datos y crear la tabla si no existe
conn_buceador = psycopg2.connect(**conn_params_buceador)
cur_buceador = conn_buceador.cursor()
# Asegurarse de que la extensión PostGIS está habilitada
cur_buceador.execute("CREATE EXTENSION IF NOT EXISTS postgis;")

# Crear la tabla 'posiciones_buceador' si no existe
cur_buceador.execute("""
    CREATE TABLE IF NOT EXISTS posiciones_buceador (
        id SERIAL PRIMARY KEY,
        latitud DOUBLE PRECISION NOT NULL,
        longitud DOUBLE PRECISION NOT NULL,
        geom GEOMETRY(Point, 4326),
        timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    )
""")

# Datos de latitud y longitud para insertar en la tabla
latitud = [
    37.61781346,
    37.61774224,
    37.61766381,
    37.61761777,
    37.61752489,
    37.61747076,
    37.61741485,
    37.61737081,
    37.61732283,
    37.61726296,
    37.61723855,
    37.61723079,
    37.61721479,
    37.61721725,
    37.61719332,
    37.61717675,
    37.61715985,
    37.61714014,
    37.61713177,
    37.61711601,
    37.61710013,
    37.61706555,
    37.6170202,
    37.61696683,
    37.61693523,
    37.61689198,
    37.6168532,
    37.61681673,
    37.61679361,
    37.61676145,
    37.61672381,
    37.61668254,
    37.61664935,
    37.61661109,
    37.61658766,
    37.6165246,
    37.61648556,
    37.61644484,
    37.61639895,
    37.61636482,
    37.61632372,
    37.61629186,
    37.6162392,
    37.61621996
]

longitud = [
    -0.713952459,
    -0.713951991,
    -0.713946112,
    -0.713931588,
    -0.713906899,
    -0.713903413,
    -0.713906252,
    -0.713931184,
    -0.713944516,
    -0.713969993,
    -0.714008500,
    -0.714034144,
    -0.714079500,
    -0.714131689,
    -0.714165533,
    -0.714190620,
    -0.714220745,
    -0.714253230,
    -0.714295876,
    -0.714330192,
    -0.714361492,
    -0.714365994,
    -0.714377810,
    -0.714386522,
    -0.714393094,
    -0.714391776,
    -0.714395440,
    -0.714411027,
    -0.714419657,
    -0.714437127,
    -0.714444636,
    -0.714433454,
    -0.714420609,
    -0.714398873,
    -0.714378061,
    -0.714361676,
    -0.714375729,
    -0.714391659,
    -0.714402622,
    -0.714404557,
    -0.714410708,
    -0.714416784,
    -0.714416087,
    -0.714449135
]

# Insertar los datos de latitud y longitud en la tabla
for i, (lat, lon) in enumerate(zip(latitud, longitud)):
    cur_buceador.execute(
        """
        INSERT INTO posiciones_buceador (latitud, longitud, geom)
        VALUES (%s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
        """,
        (lat, lon, lon, lat)
    )
    # Esperar 0.2 segundos entre cada inserción para simular un flujo de datos en tiempo real
    time.sleep(0.2)

# Confirmar la transacción
# Esto es necesario para asegurarse de que los cambios se guarden en la base de datos
conn_buceador.commit()

# Cerrar cursor y conexión
cur_buceador.close()
conn_buceador.close()
