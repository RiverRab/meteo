import meteo
import psycopg2

# Connect to an existing database
conn = psycopg2.connect("dbname=meteoproject user=postgres")