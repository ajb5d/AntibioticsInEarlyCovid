from pyspark.sql import functions as F
import pandas as pd

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b47d8fe1-8c03-4e38-9b24-24a327b6d8fd"),
    Base_Cohort=Input(rid="ri.foundry.main.dataset.f80c5b44-af52-42a2-b276-48c355fe97b4")
)
# For each row in the base_cohort, add a row to the table for admission day (day 0) through hospital day 21 (admission day + 5)
# This table will be used to link drug admins and labs to a specific hospital day
def Cohort_Hospitalization_Dates(Base_Cohort):
    df = Base_Cohort.select("person_id", "first_COVID_hospitalization_start_date")
    df = df.withColumn("period_end", F.date_add(F.col('first_COVID_hospitalization_start_date'), 21))
    df = df.withColumn("date", F.sequence(F.col('first_COVID_hospitalization_start_date'), F.col('period_end')))
    df = df.withColumn("date", F.explode(F.col("date")))
    df = df.withColumn("hospital_day", F.datediff(F.col('date'), F.col('first_COVID_hospitalization_start_date')))
    df = df.drop('period_end')
    
    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1343691b-62fc-451d-9442-0f056bdbef61")
)
from pyspark.sql.types import *
def Concept_Routes_of_Interest():
    schema = StructType([StructField("concept_id", IntegerType(), True), StructField("concept_name", StringType(), True), StructField("is_parenteral", BooleanType(), True), StructField("is_enteral", BooleanType(), True), StructField("is_missing", BooleanType(), True)])
    return spark.createDataFrame([[40490898,"Lower respiratory tract route",None,None,None],[4171884,"Intravenous peripheral route",True,None,None],[4170113,"Intravenous central route",True,None,None],[44801748,"Haemodiafiltration route",True,None,None],[4233974,"Urethral route",None,None,None],[45956875,"Route of administration not applicable",None,None,None],[40486069,"Respiratory tract route",None,None,None],[4222259,"Intraventricular route - cardiac",True,None,None],[4132711,"Nasogastric route",None,True,None],[4163770,"Subconjunctival route",None,None,None],[4006860,"Intra-articular route",None,None,None],[4243022,"Intraperitoneal route",True,None,None],[40487501,"Digestive tract route",None,True,None],[4240824,"Intra-arterial route",True,None,None],[40492287,"Intravascular route",True,None,None],[4302612,"Intramuscular route",True,None,None],[0,"No matching concept",None,None,True],[4229543,"Intratracheal route",None,None,None],[4156707,"Intrapleural route",None,None,None],[4142048,"Subcutaneous route",None,None,None],[40492301,"Intragastric route",None,True,None],[4213522,"Intraosseous route",True,None,None],[4269621,"Intrauterine route",None,None,None],[40489990,"Intracolonic route",None,None,None],[4303409,"Intracameral route",None,None,None],[4292410,"Intraluminal route",True,None,None],[40489989,"Intrajejunal route",None,True,None],[4171047,"Intravenous route",True,None,None],[4305834,"Nasojejunal route",None,True,None],[4133177,"Jejunostomy route",None,True,None],[4222254,"Body cavity route",None,None,None],[4157760,"Intraocular route",None,None,None],[4156706,"Intradermal route",None,None,None],[4217202,"Intrathecal route",None,None,None],[4262099,"Transdermal route",None,None,None],[4132254,"Gastrostomy route",None,True,None],[3440180,"Intrathecal route",None,None,None],[4186834,"Gastroenteral route",None,True,None],[4172316,"Nasoduodenal route",None,True,None],[4232601,"Transmucosal route",None,None,None],[4303277,"Oropharyngeal route",None,True,None],[4167393,"Intrathoracic route",None,None,None],[4168656,"Intratympanic route",None,None,None],[4157758,"Intralesional route",None,None,None],[4302785,"Intravitreal route",None,None,None],[4302354,"Intraduodenal route",None,True,None],[40493258,"Intrahepatic route",None,True,None],[4303795,"Orogastric route",None,True,None],[4169440,"Intrasinal route",None,True,None],[46272911,"Intraneural route",None,None,None],[4184451,"Ophthalmic route",None,None,None],[4312507,"Injection route",True,None,None],[4262914,"Nasal route",None,None,None],[4157759,"Intralymphatic route",None,None,None],[4186838,"Intravesical route",None,None,None],[4011083,"By inhalation",None,None,None],[44814650,"No information",None,None,True],[3661892,"PEG tube route",None,True,None],[4223965,"Intrabiliary route",None,None,None],[35807218,"Topical",None,None,None],[35807334,"Nasal",None,None,None],[4263689,"Topical route",None,None,None],[4167540,"Enteral route",None,True,None],[35807246,"Intramuscular",True,None,None],[4181897,"Buccal route",None,None,None],[4163765,"Dental route",None,None,None],[35627167,"Percutaneous",True,None,None],[4112421,"Intravenous",True,None,None],[35807199,"Intravenous",True,None,None],[35807338,"Intrathecal",None,None,None],[4023156,"Otic route",None,None,None],[4132161,"Oral route",None,True,None],[45956874,"Inhalation",None,None,None],[4290759,"Rectal route",None,None,None],[35807427,"Inhalation",None,None,None],[44814649,"Other",None,None,True],[4057765,"Vaginal route",None,None,None],[4120036,"Inhaling",None,None,None],[4231622,"Topical",None,None,None],[35807209,"Rectal",None,None,None],[4128794,"Oral",None,True,None],[35807196,"Oral",None,True,None]], schema=schema)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f4c2e394-87a3-486a-a8c1-a5ccbc651cc6"),
    Drug_Counts=Input(rid="ri.foundry.main.dataset.5021f644-faf0-45ad-b16d-74842df01c21")
)
## This block simplifies product names into their principal active ingredient for building figure 1b

from pyspark.sql.types import StringType
def Drug_Exposures_With_Labels(Drug_Counts):
    label_udf = F.udf(name_to_label, StringType())
    return Drug_Counts.withColumn('drug_name', label_udf('drug_concept_name'))

def name_to_label(name):
    # These for each of these strings, if present then map to the harmonized drug name
    labels = ['vancomycin', 'cefepime', 'ceftriaxone', 'azithromycin', 'cefazolin', 'amoxicillin', 
                'doxycycline', 'cephalexin', 'piperacillin', 'metronidazole', 'sulfamethoxazole',
                'ampicillin', 'meropenem', 'ampicillin', 'levofloxacin', 'ciprofloxacin', 'daptomycin', 
                'linezolid', 'clindamycin', 'metronidazole', 'penicillin', 'daptomycin', 'cefdinir', 'aztreonam', 
                'gentamicin', 'moxifloxacin', 'ceftazidime', 'ertapenem', 'tobramycin', 'minocycline',
                'nitrofurantoin', 'cefpodoxime', 'cefuroxime', 'kanamycin', 'ofloxacin', 'nafcillin',
                'cefoxitin', 'fosfomycin', 'cefotaxime', 'imipenem', 'tetracycline', 'clarithromycin', 
                'amikacin', 'colistin', 'tedizolid', 'cefixime', 'dalbavancin', 'ceftolozane', 'plazomicin',
                'spectinomycin', 'oxacillin', 'erythromycin', 'cefaclor', 'cefadroxil', 'ceftaroline', 
                'omadacycline', 'tigecycline', 'cefotetan']
    haystack = name.lower()
    for label in labels:
        if label in haystack:
            return label

    drop = ['tazobactam', 'sulbactam', 'sulfamethoxazole', 'bacitracin', 'clavulanate', 'methenamine']
    # For these strings, drop if present. This is mainly needed for the beta-lactam/beta-lactamase inhibitor 
    # products -- some partners send the components seperately (i.e. one row for piperacillin and one for tazobactam)
    # If it's sent as one row (i.e. Piperacillin/Tazobactam) then the block above will map it correctly but here we need 
    # to drop the potential duplicates
    for needle in drop:
        if needle in haystack:
            return "DROP"
    
    return None
    

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6be2cb81-7213-460d-9230-9d8f1e71f8bd")
)
from pyspark.sql.types import *
def Lab_Measures_Of_Interest():
    schema = StructType([StructField("concept_id", IntegerType(), True), StructField("measure_name", StringType(), True)])
    return spark.createDataFrame([[44817130,"pct"],[3046279,"pct"],[3010813,"wbc"],[40652446,"wbc"],[3003282,"wbc"],[3000905,"wbc"]], schema=schema)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2872658a-3c84-4e21-9bb3-ab0e749f98b8"),
    Base_Cohort=Input(rid="ri.foundry.main.dataset.f80c5b44-af52-42a2-b276-48c355fe97b4"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    observation=Input(rid="ri.foundry.main.dataset.b998b475-b229-471c-800e-9421491409f3"),
    procedure_occurrence=Input(rid="ri.foundry.main.dataset.f6f0b5e0-a105-403a-a98f-0ee1c78137dc")
)
## This is taken from the logic liasion template but modified to only include IMV and ECMO with the day during the hospitalization

def Severity_Of_Illness_By_Day(concept_set_members, procedure_occurrence, Base_Cohort, observation):
    IMV_concept_ids = list(concept_set_members.where((concept_set_members.concept_set_name=="[ICU/MODS]IMV") & (concept_set_members.is_most_recent_version=='true')).select('concept_id').toPandas()['concept_id'])
    ECMO_concept_ids = list(concept_set_members.where((concept_set_members.concept_set_name=="Kostka - ECMO") & (concept_set_members.is_most_recent_version=='true')).select('concept_id').toPandas()['concept_id'])

    base_df = Base_Cohort.select('person_id', 'first_COVID_hospitalization_start_date', 'first_COVID_hospitalization_end_date')

    df = procedure_occurrence.where(procedure_occurrence.procedure_date.isNotNull())
    df = df.withColumnRenamed('procedure_date', 'visit_date')
    df = df.withColumnRenamed('procedure_concept_id', 'concept_id')

    df = df.select('person_id', 'visit_date', 'concept_id')
    df = df.join(base_df, 'person_id', 'inner')
    df = df.where(df.visit_date.between(df.first_COVID_hospitalization_start_date, df.first_COVID_hospitalization_end_date))

    df = df.withColumn('hospital_day', F.datediff(df.visit_date, df.first_COVID_hospitalization_start_date))
    df = df.withColumn('imv', F.when(df.concept_id.isin(IMV_concept_ids), 1).otherwise(0))
    df = df.withColumn('ecmo', F.when(df.concept_id.isin(ECMO_concept_ids), 1).otherwise(0))

    proc_df = df

    df = observation.where(observation.observation_date.isNotNull())
    df = df.withColumnRenamed('observation_date', 'visit_date')
    df = df.withColumnRenamed('observation_concept_id', 'concept_id')

    df = df.select('person_id', 'visit_date', 'concept_id')
    df = df.join(base_df, 'person_id', 'inner')
    df = df.where(df.visit_date.between(df.first_COVID_hospitalization_start_date, df.first_COVID_hospitalization_end_date))

    df = df.withColumn('hospital_day', F.datediff(df.visit_date, df.first_COVID_hospitalization_start_date))
    df = df.withColumn('imv', F.when(df.concept_id.isin(IMV_concept_ids), 1).otherwise(0))
    df = df.withColumn('ecmo', F.when(df.concept_id.isin(ECMO_concept_ids), 1).otherwise(0))

    df = df.union(proc_df)

    df = df.groupBy('person_id', 'hospital_day').agg(
        F.max('imv').alias('imv'),
        F.max('ecmo').alias('ecmo'))

    return df

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.187cb29c-c246-4186-8b2f-e79041d627be")
)
from pyspark.sql.types import *
def state_to_region():
    schema = StructType([StructField("state_name", StringType(), True), StructField("state", StringType(), True), StructField("region", StringType(), True), StructField("division", StringType(), True)])
    return spark.createDataFrame([["Alaska","AK","West","Pacific"],["Alabama","AL","South","East South Central"],["Arkansas","AR","South","West South Central"],["Arizona","AZ","West","Mountain"],["California","CA","West","Pacific"],["Colorado","CO","West","Mountain"],["Connecticut","CT","Northeast","New England"],["District of Columbia","DC","South","South Atlantic"],["Delaware","DE","South","South Atlantic"],["Florida","FL","South","South Atlantic"],["Georgia","GA","South","South Atlantic"],["Hawaii","HI","West","Pacific"],["Iowa","IA","Midwest","West North Central"],["Idaho","ID","West","Mountain"],["Illinois","IL","Midwest","East North Central"],["Indiana","IN","Midwest","East North Central"],["Kansas","KS","Midwest","West North Central"],["Kentucky","KY","South","East South Central"],["Louisiana","LA","South","West South Central"],["Massachusetts","MA","Northeast","New England"],["Maryland","MD","South","South Atlantic"],["Maine","ME","Northeast","New England"],["Michigan","MI","Midwest","East North Central"],["Minnesota","MN","Midwest","West North Central"],["Missouri","MO","Midwest","West North Central"],["Mississippi","MS","South","East South Central"],["Montana","MT","West","Mountain"],["North Carolina","NC","South","South Atlantic"],["North Dakota","ND","Midwest","West North Central"],["Nebraska","NE","Midwest","West North Central"],["New Hampshire","NH","Northeast","New England"],["New Jersey","NJ","Northeast","Middle Atlantic"],["New Mexico","NM","West","Mountain"],["Nevada","NV","West","Mountain"],["New York","NY","Northeast","Middle Atlantic"],["Ohio","OH","Midwest","East North Central"],["Oklahoma","OK","South","West South Central"],["Oregon","OR","West","Pacific"],["Pennsylvania","PA","Northeast","Middle Atlantic"],["Rhode Island","RI","Northeast","New England"],["South Carolina","SC","South","South Atlantic"],["South Dakota","SD","Midwest","West North Central"],["Tennessee","TN","South","East South Central"],["Texas","TX","South","West South Central"],["Utah","UT","West","Mountain"],["Virginia","VA","South","South Atlantic"],["Vermont","VT","Northeast","New England"],["Washington","WA","West","Pacific"],["Wisconsin","WI","Midwest","East North Central"],["West Virginia","WV","South","South Atlantic"],["Wyoming","WY","West","Mountain"]], schema=schema)

