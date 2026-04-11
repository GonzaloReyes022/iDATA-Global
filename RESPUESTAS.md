# iDATA-Global — Olist E-commerce Pipeline
**Challenge:** Diseñar una estrategia de observabilidad y calidad de datos en producción para el pipeline de Olist.

> **Contexto:** El pipeline de Olist original es un script que corre una sola vez, como una carga batch inicial. Para que las preguntas tengan sentido en un contexto real, lo pensamos como un pipeline productivo mensual. Los datos llegan a S3 a principios de cada mes y se procesan a través de las capas bronze → silver → gold.

---

## 1. ¿Qué puede salir mal?

### 1.1 Schema inesperado en la fuente (DataDrift)— Bronze

Si la fuente de origen cambia la estructura de los archivos (agrega una columna, renombra un campo, cambia un tipo de dato), el pipeline puede ingestar datos malformados o fallar directamente. Esto es especialmente probable en integraciones con sistemas externos que no controlamos.

**Cómo se detecta:** en la capa bronze, al leer el CSV con el schema forzado. Usando PySpark con `mode=PERMISSIVE` y `columnNameOfCorruptRecord`, los registros que no matcheen el schema van a una columna `_corrupt_record`  sin frenar todo el pipeline(ejemplo el pregunta 2).

---
### 1.2 Datos duplicados — Silver

Si el sistema de origen sufre envia eventos duplicados, en un e-commerce esto generaría métricas de ventas falsamente infladas.

**Cómo se detecta:** en la capa Silver se aplica una función de ventana (Window) particionada por clave primaria (ej. order_id). Se ordenan los registros por su timestamp de ingesta descendente para conservar únicamente el más reciente (row_num == 1). Además, el pipeline calcula la diferencia de filas y emite un log con la cantidad exacta de duplicados eliminados para su monitoreo

---

### 1.3 Datos invalidos o inconsistentes — Silver

Registros que pasan la validación de schema pero violan reglas de negocio: un `order_status` fuera del catálogo, un timestamp de entrega anterior al de compra, o un `customer_id` nulo. Estos no rompen el pipeline pero generan métricas incorrectas en gold si se propagan.

**Cómo se detecta:** en silver, con las reglas de negocio que marcan cada registro como `_is_valid = True/False`. Si la tasa de invalidos supera el umbral configurado (e.g., más del 5%), se loguea y puede dispararse una alerta. Los datos invalidos se quedan en silver y se filtran antes de escribir en gold.

---

### 1.4 Degradación de rendimiento — Orquestación

Un pico de volumen (temporada de fin de año, campañas de descuentos) puede hacer que el procesamiento supere el tiempo esperado o que los workers de Spark queden sin memoria. Con recursos fijos en Docker (4GB RAM, 2 cores) esto es particularmente vulnerable.

**Cómo se detecta:** a nivel de orquestación. Airflow permite configurar `execution_timeout` por tarea; si se supera, la tarea falla y dispara la alerta correspondiente. Monitorear el tiempo de ejecución histórico por tarea también ayuda a detectar degradación progresiva antes de que llegue a un timeout.

---
### 1.5 Alertas — Orquestación

El pipeline solo tiene registro de lo acontecido en logs, pero no hay alertas configuradas para notificar al equipo si algo falla. Se podría integrar una notificación a Slack como extensión directa del `logger.warning` que ya existe (ver sección 3 para el flujo completo):

```python
# spark_jobs/silver/silver.py
# Se agregaría dentro de SilverCleaner.run() si validation_rate < 95

import requests

def notify_slack(dataset: str, invalid_pct: float, threshold: float, batch_id: str) -> None:
    webhook_url = "https://hooks.slack.com/services/..."  # variable de entorno en producción
    message = {
        "text": (
            f"[ALERTA DE CALIDAD] Dataset: {dataset} | Batch: {batch_id}\n"
            f"Invalidos: {invalid_pct}% (umbral: {threshold}%)\n"
            f"Revisar registros con _is_valid=False en la capa silver."
        )
    }
    requests.post(webhook_url, json=message)
```

---

### 1.6 Ausencia de datos en la fuente — Orquestación

Suponiendo que el pipeline consume datos cada mes y corre el 1ro de cada mes, pero los datos del mes anterior todavía no llegaron produciria delay en el export, error en el sistema de origen, problema de red. Sin un mecanismo de espera, el pipeline correría con datos viejos o fallaria con un error poco claro. Esto se puede simplificar guardando las fuentes en S3 con una estructura particionada por fecha (year=2024/month=01/) y nos permitiria usar sensores para validar la existencia de los archivos.

**Cómo se detecta:** antes del procesamiento, podriamos implementar con un `S3KeySensor` que espera a que existan los archivos del período correspondiente en el bucket de S3. Si no aparecen dentro del timeout configurado, la tarea falla con un mensaje claro y se puede alertar al equipo.

---

### S3KeySensor adaptado al pipeline mensual de Olist
Ejemplo de como seria la estructura del DAG con S3KeySensor para esperar los archivos de cada mes antes de procesar:
**Estructura esperada en S3** (particionado por año y mes, ver sección de particionado más abajo):

```
s3://olist-ecommerce-raw/
  orders/year=2024/month=01/orders.csv
  customers/year=2024/month=01/customers.csv
  order_items/year=2024/month=01/order_items.csv
  products/year=2024/month=01/products.csv
  order_payments/year=2024/month=01/order_payments.csv
  geolocation/year=2024/month=01/geolocation.csv
  sellers/year=2024/month=01/sellers.csv
  order_reviews/year=2024/month=01/order_reviews.csv
```

```python
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule
import pendulum
#Olist esta dividido en 8 datasets
DATASETS = [
    'orders', 'customers', 'order_items', 'products',
    'order_payments', 'geolocation', 'sellers', 'order_reviews',
]

SPARK_CONTAINER = "olist-ecommerce-spark-1"

def spark_submit_cmd(script: str) -> str:
    return (
        f"docker exec {SPARK_CONTAINER} "
        f"/opt/spark/bin/spark-submit --master local[*] --driver-memory 2g "
        f"{script}"
    )

with DAG(
    dag_id='olist_monthly_pipeline',
    start_date=pendulum.datetime(2024, 1, 1, tz='America/Sao_Paulo'),
    schedule='0 6 1 * *',  # 1ro de cada mes a las 6am — da margen para que lleguen los archivos
    catchup=False,          # no reejecutar todos los meses históricos al activar el DAG
    max_active_runs=1,
    tags=['olist', 'ecommerce', 'monthly'],
) as dag:

    sensor_tasks = []
    bronze_tasks = []

    for dataset in DATASETS:
        wait_for_data = S3KeySensor(
            task_id=f'wait_for_{dataset}',
            bucket_name='olist-ecommerce-raw',
            bucket_key=(
                f'{dataset}/'
                f'year={{{{ logical_date.year }}}}/'
                f'month={{{{ logical_date.strftime("%m") }}}}/'
                f'{dataset}.csv'
            ),
            mode='reschedule',       # libera el worker slot mientras espera, no bloquea otros DAGs
            timeout=60 * 60 * 6,     # espera máximo 6 horas antes de fallar
            poke_interval=60 * 10,   # chequea S3 cada 10 minutos
            soft_fail=True,          # si el archivo no llega, el pipeline continúa sin ese dataset
        )

        # BashOperator con docker exec — igual que el DAG existente del proyecto
        ingest_bronze = BashOperator(
            task_id=f'bronze_{dataset}',
            bash_command=spark_submit_cmd(f"/opt/spark_jobs/bronze/ingest.py --dataset {dataset}"),
        )

        wait_for_data >> ingest_bronze
        sensor_tasks.append(wait_for_data)
        bronze_tasks.append(ingest_bronze)

    run_silver = BashOperator(
        task_id='run_silver',
        bash_command=spark_submit_cmd("/opt/spark_jobs/silver/silver.py"),
        trigger_rule=TriggerRule.ALL_DONE,    # corre aunque algún dataset no haya llegado
    )

    run_gold_fact = BashOperator(
        task_id='gold_fact_orders',
        bash_command=spark_submit_cmd("/opt/spark_jobs/gold/fact_table.py"),
        trigger_rule=TriggerRule.ALL_SUCCESS,  # gold solo corre si silver fue exitoso completo
    )

    bronze_tasks >> run_silver >> run_gold_fact
```

**Diferencia clave en `TriggerRule`:** silver usa `ALL_DONE` porque queremos procesar lo que llegó aunque algún dataset no haya aparecido. Gold usa `ALL_SUCCESS` porque si silver falló (no solo algunos datasets faltantes, sino un error real), no queremos generar una capa gold con datos faltantes.

---

## 2. Monitoreo de datos

Se implementan validaciones en cada capa. Bronze valida el schema de entrada; silver aplica reglas de negocio y calcula métricas de calidad; gold valida el schema final y los tipos.

**Bronze — validación de schema:**

```python
# spark_jobs/config/schemas.py
BRONZE_ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_purchase_timestamp", StringType(), True),
    StructField("order_approved_at", StringType(), True),
    StructField("order_delivered_carrier_date", StringType(), True),
    StructField("order_delivered_customer_date", StringType(), True),
    StructField("order_estimated_delivery_date", StringType(), True),
])

# spark_jobs/bronze/ingest.py
class BronzeOrdersIngester():
    def read_source(self, source_path: str, file_format: str = "csv") -> DataFrame:
        return (
            self.spark.read
                .option("header", "true")
                .option("mode", "PERMISSIVE")                     # no falla ante registros malformados
                .option("columnNameOfCorruptRecord", "_corrupt_record")
                .schema(self.bronze_schema)                     # schema forzado para detectar cambios en la fuente
                .csv(source_path)
        )
```

**Silver — reglas de negocio y métricas de calidad:**
Ejemplo de validación de reglas de negocio en silver para Orders, con métricas de calidad y logging de alertas si la tasa de válidos es baja:
```python
# spark_jobs/silver/silver.py
class SilverCleaner:
    def validate_business_rules(self, df: DataFrame) -> DataFrame:
        """Marca _is_valid=False en registros que no cumplen las reglas de negocio.
        No elimina registros — solo los marca para filtrar en gold."""
        if not self.validation_rules:
            return df.withColumn("_is_valid", lit(True))

        combined_rule = self.validation_rules[0]
        for rule in self.validation_rules[1:]:
            combined_rule = combined_rule & rule
        return df.withColumn("_is_valid", combined_rule)

    def get_quality_metrics(self, df: DataFrame) -> Dict[str, Any]:
        total = df.count()
        valid = df.filter(col("_is_valid")).count()
        return {
            "total_records": total,
            "valid_records": valid,
            "invalid_records": total - valid,
            "validation_rate": round(valid / total * 100, 2) if total > 0 else 0,
        }

    def run(self) -> Dict[str, Any]:
        ...
        quality_metrics = self.get_quality_metrics(df)
        logger.info(quality_metrics)

        if quality_metrics["validation_rate"] < 95:
            logger.warning(
                f"Tasa de validación baja: {quality_metrics['validation_rate']}% "
                f"— revisar registros invalidos en silver para el batch de {execution_date}"
            )
        ...


class SilverOrdersCleaner(SilverCleaner):
    def __init__(self, spark: SparkSession, ...):
        super().__init__(
            spark=spark,
            station="orders",
            pk_column="order_id",
            validation_rules=[
                col("order_id").isNotNull(),
                col("customer_id").isNotNull(),
                col("order_purchase_timestamp").isNotNull(),
                col("order_status").isNotNull(),
                col("order_status").isin(VALID_ORDER_STATUSES),
                col("order_estimated_delivery_date").isNotNull(),
            ],
        )
```

Algo que descubri investigando recientemente fue metricas para control de volumetria de datos, tal que este porcentaje se pueda comparar mes a mes para detectar degradación progresiva. Si venia de procesar 50K registros y de repente procesa 5K, aunque la tasa de validación sea alta, hay un cambio importante que amerita revisión.

```python
# spark_jobs/silver/silver.py — extensión de get_quality_metrics()
# Pseudocódigo: comparación de volumetría mensual contra histórico
# Se agregaría como paso adicional en SilverCleaner.run()

def check_volume_anomaly(self, current_total: int, dataset: str) -> None:
    # Lee el total de registros del mes anterior desde los logs o una tabla de métricas
    previous_total = self.read_last_month_record_count(dataset)  # implementación futura
    if previous_total and previous_total > 0:
        change_pct = abs(current_total - previous_total) / previous_total * 100
        if change_pct > 30:  # umbral: más del 30% de variación respecto al mes anterior
            logger.warning(
                f"Variación volumétrica alta en {dataset}: "
                f"{previous_total:,} registros el mes anterior vs {current_total:,} este mes "
                f"({change_pct:.1f}% de diferencia)"
            )
```

---

## 3. Alertas y respuesta

Si alguna métrica falla, el flujo desde la detección hasta la resolución sería el siguiente:

**1. Detección:** el job de silver calcula las métricas de calidad después de aplicar las validaciones. Si la tasa de válidos cae por debajo del umbral (95%), se loguea el evento con el detalle del batch — fecha, dataset, cantidad de invalidos, razón del fallo. Esto ya está implementado.

**2. Alerta:** desde el mismo job se podría enviar una notificación a Slack con la información esencial del problema (qué dataset, qué batch, cuántos invalidos, link al log). Sería una extensión directa del `logger.warning` que ya existe — el pseudocódigo está en el punto 1.5.

**3. Contención:** el job de gold filtraría solo los registros con `_is_valid = True` antes de escribir. Los datos invalidos se quedan en silver y no llegan a la capa de consumo. La columna `_is_valid` ya está implementada en silver; el filtro en gold sería el complemento necesario.

**4. Investigación:** se revisaría los registros con `_is_valid = False` en la capa silver del batch afectado, identificaría el patrón (problema en el dato origen? regla de negocio muy estricta? bug en la transformación?) y decidiría si hay que reejecutar.

**5. Resolución:** corregido el problema, ya sea en el dato origen o en la lógica, se podría reejecutar el pipeline idempotentemente.

---

## 4. Trade-offs

### Latencia vs. Costos de Cómputo

Con un pipeline mensual de e-commerce la latencia no es crítica — los reportes no necesitan ser en tiempo real. Esto permite procesar en batch en horarios de bajo costo y no preocuparse por optimizaciones de latencia. Lo que sí rige el diseño son los SLA (Service Level Agreements — Acuerdos de Nivel de Servicio): si el negocio necesita los reportes el 1ro de cada mes, el pipeline simplemente debe garantizar terminar dentro de esa ventana de tiempo.

### Costo vs. Complejidad Operativa

Usar S3 + Spark es más económico en términos de licenciamiento que un data warehouse administrado (Snowflake, Redshift, BigQuery), pero requiere más trabajo operativo: gestionar el cluster, manejar versiones y monitorear memoria. Para un equipo pequeño, esto puede ser un cuello de botella operativo más que un ahorro real. Sin embargo, S3 en particular es muy barato para almacenar (~$0.023/GB/mes) y tiene la ventaja arquitectónica de desacoplar la llegada de datos del procesamiento.

### Orquestador desde el día uno?
Depende de dónde vienen los datos. Si los datos ya están en S3, para una primera version no necesitaria un orquestador complejo — un cron job mensual (reloj interno) con scripts de Python puede ser suficiente para validar la idea. Lo que agrega un orquestador como Airflow es: visibilidad del estado de cada tarea, retries automáticos, alertas integradas.

En este caso, dado que el proyecto ya usa Airflow, el S3KeySensor es una extensión natural — casi sin costo adicional de complejidad — y resuelve el problema de "¿qué pasa si los datos no llegaron?" de forma elegante.

### Overhead de Cómputo Distribuido (Spark vs Single-node)

El dataset de Olist tiene un volumen manejable. Un trade-off importante es haber elegido Spark (procesamiento distribuido). Para este volumen, una herramienta single-node como Pandas, Polars o DuckDB procesaría los datos más rápido y con menor costo de infraestructura. Sin embargo, se sacrificó esa eficiencia inicial a cambio de escalabilidad: al usar Spark, si el e-commerce pasa de 100 mil a 10 millones de órdenes, el código no cambia, solo se escala el cluster y luego nos ahorramos el costo de refactorizarlo en caso de usar pandas inicialmente.

### Observabilidad simple pero efectiva

Métricas por umbral (ej. exigir 95% de registros válidos), logs estructurados y alertas automáticas son simples de implementar y ya dan bastante cobertura. Lo que se sacrifica con este enfoque determinístico es no detectar una degradación gradual: si mes a mes la tasa de invalidos sube silenciosamente del 2% al 3% y luego al 4%, el umbral fijo nunca saltará, pero hay un problema latente. Detectar esto requiere comparación historica — como la volumetría mes a mes propuesta en la sección 2 — lo cual agrega complejidad que identifico como mejora futura pero que no implementaría en una primera versión.

### Qué no implementaría en una versión inicial

- **Tabla de cuarentena separada para invalidos:** la columna `_is_valid` en la misma tabla silver es suficiente para filtrar hacia gold y para investigar problemas. Una tabla separada agrega complejidad de mantenimiento (otro dataset a gestionar, más joins para investigar) sin un beneficio proporcional en esta escala.
- **Detección de anomalías estadística:** detectar si la distribución de un campo cambia repentinamente mes a mes (ej. desviaciones en el precio promedio de órdenes o cantidad de pedidos por región) requiere un baseline estadístico histórico. Podría ser implementado a futuro para mayor madurez, pero es excesivo para una primera versión.
-**Infraestructura On-Premise (Servidores Propios):** Para un e-commerce con volumen de datos pequeño, alojar la base de datos en un servidor local evitaría los costos de la nube. Sin embargo, descartaría este enfoque, el supuesto "ahorro" se pierde rapidamente en el costo operativo, mantener el hardware, aplicar parches de seguridad y gestionar caídas. Apostar por un esquema Cloud-Native (S3) desde el día uno asegura alta disponibilidad y escalabilidad sin ese peso operativo
-**Un orquestador complejo desde cero:** Si este proyecto se construyera desde cero absoluto para correr solo una vez al mes, desplegar un clúster de Airflow sería over-engineering. Para un MVP, usaría soluciones más simples y serverless (como AWS EventBridge gatillando una función Lambda). Nota: En la propuesta se incluye Airflow y el S3KeySensor únicamente porque el repositorio base original ya contaba con esta tecnología implementada, aprovechando así un costo de desarrollo ya pagado.
