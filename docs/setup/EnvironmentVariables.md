# Variables de Ambientes 

## Variables base del microservicio

## Variables obligatorias.

* PROJECT_NAME

  Contiene la lista de trackers separadas por punto y coma (;) que se consideran para generar data.

  Por ejemplo:
  ```yaml
  - PROJECT_NAME=samsung-dealers-sales;samsung-dealers-goals;samsung-dealers-sales-abs
  ```

* STORAGE_PATH

  Es la ruta absoluta de la carpeta raiz  donde se depositan los csv generados 
por hans-gruber


* SQLITE3_PATH

  Es la ruta absoluta donde contiene el archivo sqlite3 de la base de datos de hans-gruber


## Variables opcionales.

* STORE_OBJECT_NAME

 Declaracion de cual es el nombre del objecto stores para la integracion. Default 'bstores'.
 Esto es necesario para poder determinar que store necesita tener asociado en su metadata timezones y schedules.

* MONGODB_HOST 
  
Hostname de la base de datos de mongo para hans-gruber
 
* MONGODB_PORT

 Definicion del puerto que usara para la conexion a mongo (por default 27017)

* LOGGING_LEVEL

  Que nivel de mensajes de logging seran desplegados por el microservicio (default INFO).
  
  Niveles:
  * CRITICAL
  * ERROR
  * WARNING
  * INFO
  * DEBUG

  Ejemplo
  ```yaml
  - LOGGING_LEVEL=DEBUG
  ```
### Frecuencia de extracion de datos.

Existen diferentes variables para definir la periocidad de extracion de data que a la vez 
algunas trabajan en conjunto a continuacion explicaremos como usarlas.


* FREQUENCY_DATA_EXTRACTION_HOURLY

  Define la extracion con periocidad por hora.

  Ejemplo:
  ```yaml
  - FREQUENCY_DATA_EXTRACTION_HOURLY=2
  ```
  Con ese valor la extracion es cada 2 horas.

* FREQUENCY_DATA_EXTRACTION_MINUTES

  Define la extraccion con periocidad por minutos

* FETCHING_DATA_DAILY_START_TIME

  Define la extraccion a un determinado hora del dia. Ademas es posible definir varias horas al dia
usando sepracion por punto y coma(;)
  Ejemplo:
  ```yaml
   - FETCHING_DATA_DAILY_START_TIME=6:00;12:00;18:00
  ```
  La hora:minuto es considerado en horario UTC.

* FETCHING_DATA_DAILY_RANGE_DAYS

  Define el rango de dias que ira a buscar la extracion desde el dia actual hacia atras. Esta variable solo 
es valida si FETCHING_DATA_DAILY_START_TIME es definida.

* FETCHING_DATA_WEEKLY_DAYS
  Define la extraccion a una determinada hora durante un dia de la semana. Ademas es posible definir varios dias de la semana seprados por (;)
  Esta variable se define en grupo de 4 valores minimo
  * tracker
  * dia de la semana
  * hora:minuto
  * rango de dias

  Ejemplo:
  ```yaml
  - FETCHING_DATA_WEEKLY_DAYS=jardindehadas-softland-sales;saturday;5:00;40
  ```
  
  Se ejecutara el tracker jardindehadas-softland-sales el sabado a las 5:00 UTC cuarenta dias hacia atras 
hasta la fecha de hoy.


## Variables por fuentes

### Softland
#### Trackers
* softland-softland-sales
  ##### Variables
  * SQLSERVER_HOST
  * SQLSERVER_USER
  * SQLSERVER_PASSWORD
  * SQLSERVER_DATABASE
  * TIMEZONE_NAME
  
* softland-softland-sales
 
  ##### Variables
  * SQLSERVER_HOST
  * SQLSERVER_USER
  * SQLSERVER_PASSWORD
  * SQLSERVER_DATABASE
  * TIMEZONE_NAME

### Google Ads
#### Trackers
* google-ads
  ##### Variables
  * ADWORDS_CLIENT_ID
  * ADWORDS_CLIENT_SECRET
  * ADWORDS_REFRESH_TOKEN
  * ADWORDS_DEVELOPER_TOKEN
  * ADWORDS_CUSTOMER_CLIENT_ID
  * TIMEZONE_NAME


### MercadoLibre
#### Trackers
* mercado-libre-ventas
  ##### Variables
  * ML_CLIENT_ID
  * ML_CLIENT_SECRET
  * ML_CLIENT_USER_ID
  * TIMEZONE_NAME

### OpenBravo
#### Trackers
* openbravo-sales
  ##### Variables
  * OPENBRAVO_USER
  * OPENBRAVO_PASS
  * TIMEZONE_NAME

### Templates
#### Trackers
* goals-generator-by-templates
  ##### Variables 
  * SOURCE_INT_PATH
* clone-metrics-by-templates
  ##### Variables
  * SOURCE_INT_PATH

### Bsale
TBD until bsale code standarization

core/fetcher/bsale.py:        tokens = os.environ['TOKENS'].split(';')
core/fetcher/bsale.py:        storage_path = os.environ['STORAGE_PATH']
core/fetcher/bsale.py:        document_types_to_add = os.environ['DOCUMENT_TYPES_TO_ADD'].split(';')
core/fetcher/bsale.py:        document_types_to_subtract = os.environ['DOCUMENT_TYPES_TO_SUBTRACT'].split(';')
core/fetcher/bsale.py:        timezone_name = os.environ['TIMEZONE_NAME']
core/fetcher/bsale.py:        tokens = os.environ['TOKENS'].split(';')
core/fetcher/bsale.py:        storage_path = os.environ['STORAGE_PATH']
core/fetcher/bsale.py:        document_types_to_add = os.environ['DOCUMENT_TYPES_TO_ADD'].split(';')
core/fetcher/bsale.py:        document_types_to_subtract = os.environ['DOCUMENT_TYPES_TO_SUBTRACT'].split(';')
core/fetcher/bsale.py:        timezone_name = os.environ['TIMEZONE_NAME']
core/fetcher/bsale.py:        tokens = os.environ['TOKENS'].split(';')
core/fetcher/bsale.py:        storage_path = os.environ['STORAGE_PATH']
core/fetcher/bsale.py:        timezone_name = os.environ['TIMEZONE_NAME']
core/fetcher/bsale.py:            tokens = os.environ['TOKENS'].split(';')
core/fetcher/bsale.py:            storage_path = os.environ['STORAGE_PATH']
core/fetcher/bsale.py:            document_types_to_add = os.environ['DOCUMENT_TYPES_TO_ADD'].split(';')
core/fetcher/bsale.py:            document_types_to_subtract = os.environ['DOCUMENT_TYPES_TO_SUBTRACT'].split(';')
core/fetcher/bsale.py:            payment_types_to_subtract = os.environ['PAYMENT_TYPES_TO_SUBTRACT'].split(';')
core/fetcher/bsale.py:            timezone_name = os.environ['TIMEZONE_NAME']
core/fetcher/bsale.py:        tokens = os.environ['TOKENS'].split(';')
core/fetcher/bsale.py:        storage_path = os.environ['STORAGE_PATH']
core/fetcher/bsale.py:        timezone_name = os.environ['TIMEZONE_NAME']
core/fetcher/bsale.py:        price_list_id = os.environ['PRICE_LIST_ID']












