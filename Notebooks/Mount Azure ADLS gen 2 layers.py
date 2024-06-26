# Databricks notebook source
if not any(mount.mountPoint == '/mnt/bronze' for mount in dbutils.fs.mounts()):
  try:
    configs = {'fs.azure.account.auth.type': 'CustomAccessToken',
               'fs.azure.account.custom.token.provider.class': spark.conf.get('spark.databricks.passthrough.adls.gen2.tokenProviderClassName'),
              'fs.azure.sas.bronze.sarealestateproject.dfs.core.windows.net': 'sp=racwdlmeop&st=2024-05-08T13:50:26Z&se=2024-05-30T21:50:26Z&spr=https&sv=2022-11-02&sr=c&sig=K%2FFxxC5OgCbLOZUUmjMgKEARTsbCFPvymArVuWlpxUQ%3D'}

    dbutils.fs.mount(
      source = "abfss://bronze@sarealestateproject.dfs.core.windows.net/",
      mount_point = '/mnt/bronze',
      extra_configs = configs
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

if not any(mount.mountPoint == '/mnt/silver' for mount in dbutils.fs.mounts()):
  try:
    configs = {'fs.azure.account.auth.type': 'CustomAccessToken',
               'fs.azure.account.custom.token.provider.class': spark.conf.get('spark.databricks.passthrough.adls.gen2.tokenProviderClassName'),
              'fs.azure.sas.silver.sarealestateproject.dfs.core.windows.net': "sp=racwdle&st=2024-05-08T12:39:38Z&se=2024-05-31T20:39:38Z&spr=https&sv=2022-11-02&sr=c&sig=KqF%2BLGZW05QCCp4f%2FCa2m65GZH5oj5UxF6U%2Bk4Df9nk%3D"}
    dbutils.fs.mount(
      source = "abfss://silver@sarealestateproject.dfs.core.windows.net/",
      mount_point = '/mnt/silver',
      extra_configs = configs
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

if not any(mount.mountPoint == '/mnt/gold' for mount in dbutils.fs.mounts()):
  try:
    configs = {'fs.azure.account.auth.type': 'CustomAccessToken',
              'fs.azure.account.custom.token.provider.class': spark.conf.get('spark.databricks.passthrough.adls.gen2.tokenProviderClassName'),
              'fs.azure.sas.gold.sarealestateproject.dfs.core.windows.net': "sp=racwdle&st=2024-05-08T12:40:32Z&se=2024-05-31T20:40:32Z&spr=https&sv=2022-11-02&sr=c&sig=8a4gsCqE%2B2h4pXvYye7SBYFYb3unJlFRllkubyyF5YA%3D"}

    dbutils.fs.mount(
      source = "abfss://gold@sarealestateproject.dfs.core.windows.net/",
      mount_point = '/mnt/gold',
      extra_configs = configs
    )
    print("mount succeeded!")
  except Exception as e:
    print("mount exception", e)

# COMMAND ----------

dbutils.fs.refreshMounts()

# COMMAND ----------

# MAGIC %md
# MAGIC If you want to unmount one of the mounted storages uncoment coresponding line.

# COMMAND ----------

# dbutils.fs.unmount('/mnt/bronze')
# dbutils.fs.unmount('/mnt/silver')
# dbutils.fs.unmount('/mnt/gold')

# COMMAND ----------

dbutils.fs.ls('/mnt/bronze')

# COMMAND ----------

dbutils.fs.ls('/mnt/silver')

# COMMAND ----------

dbutils.fs.ls('/mnt/gold')
