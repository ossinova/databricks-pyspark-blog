# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC # Databricks + Black = Magic
# MAGIC Databricks now have the support of Black - a python code formatter (**In Public Preview**)
# MAGIC 
# MAGIC Black is preinstalled on clusters using Runtime >= 11.2, for older version run  `%pip install black==22.3.0 tokenize-rt==4.2.1` to install necessary libraries
# MAGIC 
# MAGIC You can access Black within your notebook by presssing `ctr + shift + f` or by going to `Edit -> Format cell(s)`

# COMMAND ----------

# Example unformatted

def is_unique(
               s
               ):
    s = list(s
                )
    s.sort()
 
 
    for i in range(len(s) - 1):
        if s[i] == s[i + 1]:
            return 0
    else:
        return 1


# COMMAND ----------

# Example formatted using Black

def is_unique(s):
    s = list(s)
    s.sort()

    for i in range(len(s) - 1):
        if s[i] == s[i + 1]:
            return 0
    else:
        return 1

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC Using Databricks built-in Black support allows the developer to adopt to the PEP8 standard. Keeping your code well-formatted, clean and readable. 
# MAGIC 
# MAGIC To quote Black's README:
# MAGIC 
# MAGIC > Black is the uncompromising Python code formatter. By using it, you agree to cede control over minutiae of hand-formatting. In return, Black gives you speed, determinism, and freedom from pycodestyle nagging about formatting. You will save time and mental energy for more important matters. 
# MAGIC 
# MAGIC Black: [Gtihub](https://github.com/psf/black)  
# MAGIC Databricks: [Docs](https://docs.databricks.com/notebooks/notebooks-code.html#format-python-cells)
