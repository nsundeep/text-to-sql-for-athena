import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame
from pyspark.sql import functions as F


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
logger = glueContext.get_logger()
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node field_answers
field_answers_node1725300476405 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "myprojects_custom_field_answers",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "field_answers_node1725300476405"
)

# Script generated for node team_fields
team_fields_node1725300363579 = glueContext.create_dynamic_frame.from_options(
    connection_type = "mysql",
    connection_options = {
        "useConnectionProperties": "true",
        "dbtable": "myprojects_custom_team_fields",
        "connectionName": "Aurora connection",
    },
    transformation_ctx = "team_fields_node1725300363579"
)

# Script generated for node SQL Query
SqlQuery2273 = '''
SELECT 
  fa.id AS IDX, 
  tf.field_name, 
  jt.label AS field_answer 
FROM 
  myprojects_custom_team_fields tf 
JOIN 
  myprojects_custom_field_answers fa 
ON 
  tf.id = fa.field_id 
LATERAL VIEW 
  explode(from_json(tf.field_data, 'struct<data:array<struct<label:string, value:int>>>').data) jt AS jt 
WHERE 
  jt.value = fa.field_ans 
  AND tf.field_type = 'select' AND tf.field_name in ("actual_sqm","approved_sop","architectural_concept","architect_name","brand","channel","construction_start","distribution_stream","expected_opening_date","final_cost","final_sqft","final_sqm","furniture_reused","general_contractor","is_lux_project","kom_date","local_architect","LVSA_Drafter","LVSA_Paris_Architect","LVSA_Paris_dir","LVSA_Presented_to_Paris","LVSA_priority","millworker_name","opening_date","project_id","project_relevant_rai","project_status","project_type","required_layout_date","retail_format","sales_organization","target_completion_day","target_handover_day","venchi_4urspace_mui","venchi_ec_mui","venchi_ice_amount","venchi_notes_mui","wbs_element","wonderLux_variant","xpm_company","xpm_responsible","year","zip_code")
  
  union
  
  select fa1.id as IDX, tf1.`field_name`,  fa1.field_ans 
from myprojects_custom_team_fields tf1, myprojects_custom_field_answers fa1
where tf1.id = fa1.`field_id` 
and tf1.field_type != 'select' AND tf1.field_name in ("actual_sqm","approved_sop","architectural_concept","architect_name","brand","channel","construction_start","distribution_stream","expected_opening_date","final_cost","final_sqft","final_sqm","furniture_reused","general_contractor","is_lux_project","kom_date","local_architect","LVSA_Drafter","LVSA_Paris_Architect","LVSA_Paris_dir","LVSA_Presented_to_Paris","LVSA_priority","millworker_name","opening_date","project_id","project_relevant_rai","project_status","project_type","required_layout_date","retail_format","sales_organization","target_completion_day","target_handover_day","venchi_4urspace_mui","venchi_ec_mui","venchi_ice_amount","venchi_notes_mui","wbs_element","wonderLux_variant","xpm_company","xpm_responsible","year","zip_code")

order by IDX
'''
SQLQuery_node1725301297052 = sparkSqlQuery(glueContext, query = SqlQuery2273, mapping = {"myprojects_custom_team_fields":team_fields_node1725300363579, "myprojects_custom_field_answers":field_answers_node1725300476405}, transformation_ctx = "SQLQuery_node1725301297052")

#logger.info("Show the query results")

#logger.info(SQLQuery_node1725301297052.toDF().show())


# Script generated for node Pivot Rows Into Columns
PivotRowsIntoColumns_node1725303799628 = SQLQuery_node1725301297052.toDF().groupBy("IDX").pivot("field_name").agg(F.first("field_answer"))

logger.info(PivotRowsIntoColumns_node1725303799628.show())

jdbc_conf = glueContext.extract_jdbc_conf(connection_name="Aurora connection")

PivotRowsIntoColumns_node1725303799628.write.option("truncate", "true").jdbc(
    url=jdbc_conf["fullUrl"],
    table="myprojects_custom_data",
    mode="overwrite",
    properties={"user": jdbc_conf["user"], "password": jdbc_conf["password"]}
)

#logger.info("Database table updated")
job.commit()