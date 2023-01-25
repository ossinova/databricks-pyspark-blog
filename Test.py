# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Does this work

# COMMAND ----------


def is_unique(s):
    s = list(s)
    s.sort()

    for i in range(len(s) - 1):
        if s[i] == s[i + 1]:
            return 0
    else:
        return 1


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM a
