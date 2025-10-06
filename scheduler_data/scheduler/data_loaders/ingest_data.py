import pandas as pd
import requests
from datetime import datetime
import uuid
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.snowflake import Snowflake
from os import path
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from mage_ai.data_preparation.shared.secrets import get_secret_value
import gc
import time


if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader


# Configuración de reintentos
MAX_RETRIES = 3
RETRY_DELAY = 5  # segundos
BACKOFF_MULTIPLIER = 2  # delay exponencial


@data_loader
def load_data(*args, **kwargs):
    """
    Pipeline de backfill con streaming y reintentos
    Capa Bronze: mantiene columnas originales + metadatos de ingesta
    
    Parámetros:
    - service: 'yellow', 'green', 'taxi_zones', etc.
    - year: Año a procesar
    - months: Lista de meses [1,2,3] o None para todos
    - chunk_size: Filas por chunk (default: 1000000)
    - force_reload: Sobrescribir datos existentes (default: False)
    - max_retries: Número máximo de reintentos (default: 3)
    """

    print(f"DEBUG kwargs completos: {kwargs}")
    print(f"DEBUG year en kwargs: {'year' in kwargs}")
    print(f"DEBUG valor de year: {kwargs.get('year', 'NO ENCONTRADO')}")
    
    service = kwargs.get('service', 'yellow')
    year = int(kwargs.get('year', 2015))
    months = kwargs.get('months', None)
    database = get_secret_value('SNOWFLAKE_DATABASE')
    schema = get_secret_value('SNOWFLAKE_SCHEMA')
    chunk_size = int(kwargs.get('chunk_size', 1000000))
    force_reload = kwargs.get('force_reload', False)
    max_retries = int(kwargs.get('max_retries', MAX_RETRIES))
    
    # Normalizar months a lista de enteros
    if months is None:
        months = list(range(1, 13))
    elif isinstance(months, int):
        months = [months]
    elif isinstance(months, str):
        months = [int(m.strip()) for m in months.strip('[]').split(',')]
    elif isinstance(months, list):
        months = [int(m) for m in months]
    
    batch_run_id = str(uuid.uuid4())
    
    execution_date = kwargs.get('execution_date')
    if execution_date:
        if isinstance(execution_date, datetime):
            batch_timestamp = execution_date.strftime('%Y-%m-%d %H:%M:%S')
        else:
            batch_timestamp = str(execution_date)
    else:
        batch_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    print(f"=" * 80)
    print(f"BACKFILL STREAMING CON REINTENTOS - {service.upper()} {year}")
    print(f"Meses: {months}, Chunk size: {chunk_size:,}, Max reintentos: {max_retries}")
    print(f"Batch ID: {batch_run_id[:8]}")
    print(f"=" * 80)
    
    results = {
        'batch_run_id': batch_run_id,
        'service': service,
        'year': year,
        'batch_timestamp': batch_timestamp,
        'months_attempted': len(months),
        'months_successful': 0,
        'months_skipped': 0,
        'months_failed': 0,
        'months_gap': 0,
        'total_rows_loaded': 0,
        'monthly_results': []
    }
    
    for month in months:
        print(f"\n[{month:02d}] Procesando {service} {year}-{month:02d}")
        
        month_result = process_month_streaming(
            service=service,
            year=year,
            month=month,
            database=database,
            schema=schema,
            chunk_size=chunk_size,
            force_reload=force_reload,
            batch_run_id=batch_run_id,
            batch_timestamp=batch_timestamp,
            max_retries=max_retries
        )
        
        results['monthly_results'].append(month_result)
        
        if month_result['success']:
            results['months_successful'] += 1
            results['total_rows_loaded'] += month_result.get('rows_loaded', 0)
        elif month_result.get('skipped'):
            results['months_skipped'] += 1
        elif month_result.get('gap'):
            results['months_gap'] += 1
        else:
            results['months_failed'] += 1
        
        gc.collect()
    
    print(f"\n{'=' * 80}")
    print(f"RESUMEN: Exitosos={results['months_successful']}, Saltados={results['months_skipped']}, "
          f"Brechas={results['months_gap']}, Fallidos={results['months_failed']}")
    print(f"Total filas: {results['total_rows_loaded']:,}")
    print(f"{'=' * 80}")
    
    return results


def retry_with_backoff(func, *args, max_retries=MAX_RETRIES, retry_delay=RETRY_DELAY, **kwargs):
    """Ejecuta una función con reintentos exponenciales"""
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            
            wait_time = retry_delay * (BACKOFF_MULTIPLIER ** attempt)
            print(f"    Intento {attempt + 1}/{max_retries} falló: {e}")
            print(f"    Reintentando en {wait_time}s...")
            time.sleep(wait_time)
    
    raise Exception(f"Falló después de {max_retries} intentos")


def download_file_with_retry(url, service, max_retries=MAX_RETRIES):
    """Descarga archivo con reintentos"""
    def _download():
        response = requests.get(url, timeout=300)
        response.raise_for_status()
        
        if service == 'taxi_zones':
            return pd.read_csv(url)
        else:
            return pd.read_parquet(url)
    
    return retry_with_backoff(_download, max_retries=max_retries)


def process_month_streaming(service, year, month, database, schema, chunk_size, 
                           force_reload, batch_run_id, batch_timestamp, max_retries):
    """Procesa un mes con streaming y reintentos"""
    run_id = str(uuid.uuid4())
    
    try:
        # Verificar datos existentes
        if not force_reload and service != 'taxi_zones':
            existing_data = check_existing_data(database, schema, service, year, month)
            if existing_data:
                print(f"    Saltando: {existing_data['count']:,} registros ya existen")
                return {
                    'success': False,
                    'skipped': True,
                    'year': year,
                    'month': month,
                    'existing_count': existing_data['count']
                }
        
        # Descargar archivo con reintentos
        if service == 'taxi_zones':
            url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
            filename = "taxi_zone_lookup.csv"
        else:
            base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
            filename = f"{service}_tripdata_{year:04d}-{month:02d}.parquet"
            url = f"{base_url}/{filename}"
        
        try:
            df = download_file_with_retry(url, service, max_retries=max_retries)
            print(f"    Descargado: {len(df):,} filas, {len(df.columns)} columnas")
        except Exception as e:
            print(f"    Brecha: archivo no existe después de {max_retries} intentos")
            register_gap(database, schema, service, year, month)
            return {'success': False, 'gap': True, 'year': year, 'month': month}
        
        total_rows = len(df)
        total_chunks = (total_rows + chunk_size - 1) // chunk_size
        
        # Preparar conexión y tabla con reintentos
        conn = retry_with_backoff(
            get_snowflake_connection, 
            database.upper(), 
            schema.upper(),
            max_retries=max_retries
        )
        
        table_name = get_table_name(service)
        
        # Crear tabla con columnas dinámicas
        if service in ['yellow', 'green']:
            ensure_table_exists_dynamic(conn, table_name, database, schema, df.columns, service)
        elif service == 'taxi_zones':
            ensure_table_exists_static(conn, table_name, database, schema, 'taxi_zones')
        
        # DELETE datos existentes del período
        if service != 'taxi_zones':
            cursor = conn.cursor()
            try:
                existing = check_existing_data(database, schema, service, year, month)
                if existing and existing['count'] > 0:
                    print(f"    Eliminando {existing['count']:,} registros existentes...")
                    retry_with_backoff(
                        cursor.execute,
                        f"""DELETE FROM {database.upper()}.{schema.upper()}.{table_name.upper()}
                            WHERE _data_year = {year} AND _data_month = {month}""",
                        max_retries=max_retries
                    )
            finally:
                cursor.close()
        else:
            cursor = conn.cursor()
            try:
                retry_with_backoff(
                    cursor.execute,
                    f"TRUNCATE TABLE {database.upper()}.{schema.upper()}.{table_name.upper()}",
                    max_retries=max_retries
                )
            finally:
                cursor.close()
        
        print(f"    Procesando y exportando en {total_chunks} chunks...")
        total_rows_inserted = 0
        
        # Procesar y exportar cada chunk con reintentos
        for chunk_num in range(total_chunks):
            start_idx = chunk_num * chunk_size
            end_idx = min((chunk_num + 1) * chunk_size, total_rows)
            
            chunk_df = df.iloc[start_idx:end_idx].copy()
            
            # Agregar metadatos
            chunk_df['_run_id'] = run_id
            chunk_df['_batch_run_id'] = batch_run_id
            chunk_df['_ingest_ts'] = batch_timestamp
            chunk_df['_source_file'] = filename
            chunk_df['_service_type'] = service
            
            if service != 'taxi_zones':
                chunk_df['_data_year'] = year
                chunk_df['_data_month'] = month
            
            # EXPORTAR con reintentos
            success = retry_with_backoff(
                export_chunk_streaming,
                chunk_df, service, database, schema, table_name, conn,
                max_retries=max_retries
            )
            
            if not success:
                conn.close()
                del df
                gc.collect()
                raise Exception(f"Error exportando chunk {chunk_num + 1}")
            
            total_rows_inserted += len(chunk_df)
            del chunk_df
            
            if (chunk_num + 1) % 5 == 0:
                gc.collect()
            
            if (chunk_num + 1) % 10 == 0 or (chunk_num + 1) == total_chunks:
                print(f"      Chunk {chunk_num + 1}/{total_chunks}: {total_rows_inserted:,} filas exportadas")
        
        del df
        gc.collect()
        conn.close()
        
        print(f"    OK: {total_rows_inserted:,} filas")
        
        if service in ['yellow', 'green']:
            save_audit_coverage(database, schema, service, year, month)
        
        return {
            'success': True,
            'year': year,
            'month': month,
            'run_id': run_id,
            'batch_run_id': batch_run_id,
            'rows_loaded': total_rows_inserted
        }
            
    except Exception as e:
        print(f"    Error final: {e}")
        return {'success': False, 'year': year, 'month': month, 'error': str(e)}


def export_chunk_streaming(chunk_df, service, database, schema, table_name, conn):
    """Exporta un chunk individual - lanza excepción para reintentos"""
    cursor = None
    try:
        # Convertir timestamps
        for col in chunk_df.columns:
            if pd.api.types.is_datetime64_any_dtype(chunk_df[col]):
                chunk_df[col] = chunk_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            elif chunk_df[col].dtype == 'object' and len(chunk_df) > 0:
                first_val = chunk_df[col].iloc[0]
                if isinstance(first_val, datetime):
                    chunk_df[col] = chunk_df[col].apply(
                        lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if isinstance(x, datetime) else str(x)
                    )
        
        if '_ingest_ts' in chunk_df.columns:
            chunk_df['_ingest_ts'] = chunk_df['_ingest_ts'].astype(str)
        
        cursor = conn.cursor()
        temp_table = f"TMP_{uuid.uuid4().hex[:8]}".upper()
        
        cursor.execute(f"DROP TABLE IF EXISTS {database.upper()}.{schema.upper()}.{temp_table}")
        cursor.execute(f"""
            CREATE TEMPORARY TABLE {temp_table} 
            LIKE {database.upper()}.{schema.upper()}.{table_name.upper()}
        """)
        
        success, nchunks, nrows, _ = write_pandas(
            conn=conn, df=chunk_df, table_name=temp_table,
            database=database.upper(), schema=schema.upper(),
            quote_identifiers=False
        )
        
        if not success:
            raise Exception("write_pandas falló")
        
        cursor.execute(f"""
            INSERT INTO {database.upper()}.{schema.upper()}.{table_name.upper()}
            SELECT * FROM {database.upper()}.{schema.upper()}.{temp_table}
        """)
        
        cursor.execute(f"DROP TABLE IF EXISTS {database.upper()}.{schema.upper()}.{temp_table}")
        cursor.close()
        
        return True
        
    except Exception as e:
        if cursor:
            cursor.close()
        raise Exception(f"Error en chunk export: {e}")


def ensure_table_exists_dynamic(conn, table_name, database, schema, original_columns, service):
    """Crea tabla con todas las columnas originales del Parquet + metadatos"""
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema.upper()}' AND TABLE_NAME = '{table_name.upper()}'
        """)
        
        if cursor.fetchone()[0] == 0:
            columns_def = []
            for col in original_columns:
                columns_def.append(f"{col} VARCHAR")
            
            metadata_cols = [
                "_run_id VARCHAR",
                "_batch_run_id VARCHAR",
                "_ingest_ts TIMESTAMP",
                "_source_file VARCHAR",
                "_service_type VARCHAR",
                "_data_year NUMBER",
                "_data_month NUMBER"
            ]
            
            all_columns = columns_def + metadata_cols
            columns_and_types = ",\n                ".join(all_columns)
            
            cursor.execute(f"""
                CREATE TABLE {database.upper()}.{schema.upper()}.{table_name.upper()} (
                    {columns_and_types}
                )
            """)
            print(f"    Tabla {table_name} creada con {len(original_columns)} columnas + 7 metadatos")
    finally:
        cursor.close()


def ensure_table_exists_static(conn, table_name, database, schema, table_type):
    """Crea tabla estática para taxi_zones"""
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema.upper()}' AND TABLE_NAME = '{table_name.upper()}'
        """)
        
        if cursor.fetchone()[0] == 0:
            if table_type == 'taxi_zones':
                columns_and_types = """
                    LocationID NUMBER,
                    Borough VARCHAR,
                    Zone VARCHAR,
                    service_zone VARCHAR,
                    _run_id VARCHAR,
                    _batch_run_id VARCHAR,
                    _ingest_ts TIMESTAMP,
                    _source_file VARCHAR,
                    _service_type VARCHAR
                """
            
            cursor.execute(f"""
                CREATE TABLE {database.upper()}.{schema.upper()}.{table_name.upper()} (
                    {columns_and_types}
                )
            """)
    finally:
        cursor.close()


def get_table_name(service):
    return 'TAXI_ZONES' if service == 'taxi_zones' else f"{service}_tripdata".upper()


def get_snowflake_connection(database, schema):
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_loader = ConfigFileLoader(config_path, 'default')
    config = config_loader.config
    
    return snowflake.connector.connect(
        account=config.get('SNOWFLAKE_ACCOUNT'),
        user=config.get('SNOWFLAKE_USER'),
        password=config.get('SNOWFLAKE_PASSWORD'),
        warehouse=config.get('SNOWFLAKE_WAREHOUSE'),
        database=database,
        schema=schema,
        insecure_mode=True
    )


def check_existing_data(database, schema, service, year, month):
    try:
        conn = get_snowflake_connection(database, schema)
        table_name = get_table_name(service)
        cursor = conn.cursor()
        
        cursor.execute(f"""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema.upper()}' AND TABLE_NAME = '{table_name.upper()}'
        """)
        
        if cursor.fetchone()[0] == 0:
            cursor.close()
            conn.close()
            return None
        
        cursor.execute(f"""
            SELECT COUNT(*) as cnt
            FROM {database}.{schema}.{table_name}
            WHERE _data_year = {year} AND _data_month = {month}
        """)
        
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        
        return {'count': int(count)} if count > 0 else None
    except:
        return None


def ensure_audit_table_exists(conn, database, schema):
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
            SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema.upper()}' AND TABLE_NAME = 'AUDIT_COVERAGE'
        """)
        
        if cursor.fetchone()[0] == 0:
            cursor.execute(f"""
                CREATE TABLE {database.upper()}.{schema.upper()}.AUDIT_COVERAGE (
                    service_type VARCHAR,
                    _data_year NUMBER,
                    _data_month NUMBER,
                    row_count NUMBER,
                    gap BOOLEAN,
                    registered_at TIMESTAMP
                )
            """)
    finally:
        cursor.close()


def register_gap(database, schema, service, year, month):
    try:
        gap_df = pd.DataFrame([{
            'service_type': service,
            '_data_year': year if service != 'taxi_zones' else None,
            '_data_month': month if service != 'taxi_zones' else None,
            'row_count': 0,
            'gap': True,
            'registered_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }])
        
        conn = get_snowflake_connection(database, schema)
        try:
            ensure_audit_table_exists(conn, database, schema)
            write_pandas(conn=conn, df=gap_df, table_name='AUDIT_COVERAGE',
                        database=database.upper(), schema=schema.upper(),
                        auto_create_table=False, quote_identifiers=False)
        finally:
            conn.close()
    except:
        pass


def save_audit_coverage(database, schema, service, year, month):
    try:
        conn = get_snowflake_connection(database, schema)
        ensure_audit_table_exists(conn, database, schema)
        
        query = f"""
        SELECT {year} as _data_year, {month} as _data_month,
               COUNT(*) as row_count, '{service}' as service_type,
               FALSE as gap, CURRENT_TIMESTAMP() as registered_at
        FROM {database}.{schema}.{get_table_name(service)}
        WHERE _data_year = {year} AND _data_month = {month}
        """
        
        result = pd.read_sql(query, conn)
        
        if len(result) > 0:
            write_pandas(conn=conn, df=result, table_name='AUDIT_COVERAGE',
                        database=database.upper(), schema=schema.upper(),
                        auto_create_table=False, quote_identifiers=False)
        
        conn.close()
    except:
        pass