# Databricks notebook source
# MAGIC %md
# MAGIC Function that mounts ADLS gen 2 container to Databricks via SAS Token and container name.

# COMMAND ----------

def mount_adls_with_SAS_token(container_name: str, SAS_token: str) -> str:
    if not any(mount.mountPoint == f'/mnt/{container_name}' for mount in dbutils.fs.mounts()):
        try:
            configs: dict = {'fs.azure.account.auth.type': 'CustomAccessToken',
                    'fs.azure.account.custom.token.provider.class': spark.conf.get('spark.databricks.passthrough.adls.gen2.tokenProviderClassName'),
                    f'fs.azure.sas.{container_name}.sarealestateproject.dfs.core.windows.net': SAS_token}

            dbutils.fs.mount(
            source = f"abfss://{container_name}@sarealestateproject.dfs.core.windows.net/",
            mount_point = f'/mnt/{container_name}',
            extra_configs = configs)

            return "Mount succeeded!"
        except Exception as e:
            return f"Mount exception: {repr(e)}" 

# COMMAND ----------

# MAGIC %md
# MAGIC Mount Azure ADLS gen 2 bronze container to Databricks.

# COMMAND ----------

bronze_container_name: str = "bronze"
bronze_SAS_token: str = "sp=racwdlmeop&st=2024-05-08T13:50:26Z&se=2024-05-30T21:50:26Z&spr=https&sv=2022-11-02&sr=c&sig=K%2FFxxC5OgCbLOZUUmjMgKEARTsbCFPvymArVuWlpxUQ%3D"

mount_result: str = mount_adls_with_SAS_token(bronze_container_name, bronze_SAS_token)
print(mount_result)

# COMMAND ----------

# MAGIC %md
# MAGIC Mount Azure ADLS gen 2 silver container to Databricks.

# COMMAND ----------

silver_container_name: str = "silver"
silver_SAS_token: str = "sp=racwdle&st=2024-05-08T12:39:38Z&se=2024-05-31T20:39:38Z&spr=https&sv=2022-11-02&sr=c&sig=KqF%2BLGZW05QCCp4f%2FCa2m65GZH5oj5UxF6U%2Bk4Df9nk%3D"

mount_result: str = mount_adls_with_SAS_token(silver_container_name, silver_SAS_token)
print(mount_result)

# COMMAND ----------

# MAGIC %md
# MAGIC Mount Azure ADLS gen 2 gold container to Databricks.

# COMMAND ----------

gold_container_name: str = "gold"
gold_SAS_token: str = "sp=racwdle&st=2024-05-08T12:40:32Z&se=2024-05-31T20:40:32Z&spr=https&sv=2022-11-02&sr=c&sig=8a4gsCqE%2B2h4pXvYye7SBYFYb3unJlFRllkubyyF5YA%3D"

mount_result: str = mount_adls_with_SAS_token(gold_container_name, gold_SAS_token)
print(mount_result)

# COMMAND ----------

# MAGIC %md
# MAGIC Refresh Databricks mounted storages.

# COMMAND ----------

dbutils.fs.refreshMounts()

# COMMAND ----------

# MAGIC %md
# MAGIC Unmounting Azure ADLS gen 2 containers from Databricks.
# MAGIC
# MAGIC If you want to unmount one of the mounted storages uncoment coresponding line.

# COMMAND ----------

# dbutils.fs.unmount('/mnt/bronze')
# dbutils.fs.unmount('/mnt/silver')
# dbutils.fs.unmount('/mnt/gold')

# COMMAND ----------

# MAGIC %md
# MAGIC Check what is contained in bronze Azure ADLS gen 2 container. 

# COMMAND ----------

dbutils.fs.ls('/mnt/bronze')

# COMMAND ----------

# MAGIC %md
# MAGIC Check what is contained in silver Azure ADLS gen 2 container. 

# COMMAND ----------

dbutils.fs.ls('/mnt/silver')

# COMMAND ----------

# MAGIC %md
# MAGIC Check what is contained in gold Azure ADLS gen 2 container. 

# COMMAND ----------

dbutils.fs.ls('/mnt/gold')
