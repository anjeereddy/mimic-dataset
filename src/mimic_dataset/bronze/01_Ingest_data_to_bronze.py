

def main():
    spark.sql("""
            CREATE DATABASE IF NOT EXISTS {schema_name}.bronze
            """.format(schema_name = schema_name))
    spark.sql("""
            CREATE DATABASE IF NOT EXISTS {schema_name}.silver
            """.format(schema_name = schema_name))
    spark.sql("""
            CREATE DATABASE IF NOT EXISTS {schema_name}.gold
            """.format(schema_name = schema_name))