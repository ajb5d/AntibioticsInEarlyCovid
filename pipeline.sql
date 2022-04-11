

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f80c5b44-af52-42a2-b276-48c355fe97b4"),
    covid_facts=Input(rid="ri.foundry.main.dataset.75d7da57-7b0e-462c-b41d-c9ef4f756198")
)
/* Select all the hospitalized cases from the logic liasion template */

SELECT *
FROM covid_facts
WHERE
    COVID_associated_hospitalization_indicator = 1

@transform_pandas(
    Output(rid="ri.vector.main.execute.abd42423-d858-4223-8327-0161066dfd15"),
    Base_Cohort=Input(rid="ri.foundry.main.dataset.f80c5b44-af52-42a2-b276-48c355fe97b4")
)
/* This is a sanity check that everyone in the dataset has a positive test result */
SELECT 
    person_id
    , COVID_first_diagnosis_date
    , COVID_first_PCR_or_AG_lab_positive
    , COVID_first_poslab_or_diagnosis_date
    , first_COVID_hospitalization_start_date
    , first_COVID_hospitalization_end_date
FROM Base_Cohort
WHERE
    NOT COVID_first_PCR_or_AG_lab_positive BETWEEN DATE_SUB(first_COVID_hospitalization_start_date,16) AND DATE_ADD(first_COVID_hospitalization_start_date, 2)

@transform_pandas(
    Output(rid="ri.vector.main.execute.2c168a90-1036-4c99-972a-8931bb27a6b6"),
    Base_Cohort=Input(rid="ri.foundry.main.dataset.f80c5b44-af52-42a2-b276-48c355fe97b4")
)
/* This is a check that each person_id in the dataset is a unique person (i.e. no re-admissions) */
SELECT
    COUNT(*)
    , COUNT(DISTINCT person_id)
FROM Base_Cohort

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b8883699-ff6d-404c-9b1d-bb05eafe80e1"),
    Final_Analysis_Cohort=Input(rid="ri.foundry.main.dataset.b6368e45-75aa-407a-aaaf-e4a9724ae9ac")
)
-- Summarize cohort contributions by data_partner_id. This is used to drop 
-- centers with very few hospitalized patients. 
SELECT
    data_partner_id
    , CAST(COUNT(*) AS INT) AS record_count
FROM Final_Analysis_Cohort
GROUP BY data_partner_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5021f644-faf0-45ad-b16d-74842df01c21"),
    Early_Antibiotic_Exposures=Input(rid="ri.foundry.main.dataset.6e76efbd-dd8d-48bb-bc85-970aba19a374")
)
-- This aggregates counts by drug_names for the first 0 - 4 hospital days of the cohort 
-- The results are used to create Figure 1b

SELECT
    drug_concept_id
    , MAX(drug_concept_name) as drug_concept_name
    , COUNT(*) AS c
FROM Early_Antibiotic_Exposures
WHERE hospital_day < 5
GROUP BY drug_concept_id
ORDER BY COUNT(*) DESC

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3acd36f3-ec12-4236-b8c0-8162ed3f2911"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
/* Pull the concept_ids for our concept set of antibiotics (ATC Codes J01 and children) */ 

SELECT
    concept_id
FROM concept_set_members
WHERE 
    codeset_id = 54810628 
    AND is_most_recent_version = TRUE

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2e4c4495-2185-4477-8b49-79242f595aba"),
    Concept_Routes_of_Interest=Input(rid="ri.foundry.main.dataset.1343691b-62fc-451d-9442-0f056bdbef61"),
    Early_Antibiotic_Exposures=Input(rid="ri.foundry.main.dataset.6e76efbd-dd8d-48bb-bc85-970aba19a374")
)
/* Some partners don't submit routes of admin and we need to impute the route based on the partners that do 
   submit this info. This query builds a list of drug_concept_ids and and calculates the frequency of enteral 
   administration (when the route is recorded). Concept_Routes_of_Interest is a hand curated list of routes and their
   equivalent category (i.e. IV, IM, and IVP all get rolled into parenteral. */ 
SELECT
    drug_concept_id
    , COUNT(*) AS occurance_count
    , AVG(CASE WHEN is_enteral THEN 1 ELSE 0 END) AS enteral_rate
    , AVG(CASE WHEN is_parenteral THEN 1 ELSE 0 END) AS parenteral_rate
FROM Early_Antibiotic_Exposures
LEFT JOIN Concept_Routes_of_Interest ON
    Early_Antibiotic_Exposures.route_concept_id = Concept_Routes_of_Interest.concept_id
WHERE
    is_enteral OR is_parenteral
GROUP BY drug_concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f9ae6e03-c975-4f18-8296-7a96c11aa177"),
    Drugs_Labelled_By_Person_And_Day=Input(rid="ri.foundry.main.dataset.6691ab77-c551-40d2-bed8-2215733bdd4b")
)
-- Aggregate the drugs by hospital day for figure 1b
SELECT
    hospital_day
    , drug_name
    , CAST(COUNT(*) AS INT) AS c
FROM Drugs_Labelled_By_Person_And_Day
GROUP BY 
    hospital_day
    , drug_name

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6691ab77-c551-40d2-bed8-2215733bdd4b"),
    Drug_Exposures_With_Labels=Input(rid="ri.foundry.main.dataset.f4c2e394-87a3-486a-a8c1-a5ccbc651cc6"),
    Early_Antibiotic_Exposures=Input(rid="ri.foundry.main.dataset.6e76efbd-dd8d-48bb-bc85-970aba19a374")
)
-- Keep distinct (patient, day, drug) tuples but drop the rows flagged for removal. 
SELECT DISTINCT
    person_id
    , hospital_day
    , drug_name
FROM Early_Antibiotic_Exposures
INNER JOIN Drug_Exposures_With_Labels USING (drug_concept_id)
WHERE
    drug_name <> 'DROP'

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.76a63714-cb94-4a5e-a499-52a1a2abb4e8"),
    Early_Antibiotic_Exposures_Mapped_to_Routes=Input(rid="ri.foundry.main.dataset.0d60b0bf-2156-4b34-a4b3-6082f382e1c2")
)
/* This identifies drugs that fell through the route mapping. Most of 
these are either topical or inhaled agents (or agents that can be inhaled 
but don't have a route recorded). This was used to refine the logic 
for Early_Antibiotic_Exposures_Mapped_to_Routes */ 

SELECT 
    drug_concept_id
    , MAX(drug_concept_name) AS drug_concept_name
    , COUNT(*) AS occurence_count
FROM Exposures_By_Day
WHERE
    (route_concept_id IS NULL OR route_missing)
    AND (NOT is_enteral AND NOT is_parenteral)
GROUP BY drug_concept_id
ORDER BY COUNT(*) DESC

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.6e76efbd-dd8d-48bb-bc85-970aba19a374"),
    Cohort_Hospitalization_Dates=Input(rid="ri.foundry.main.dataset.b47d8fe1-8c03-4e38-9b24-24a327b6d8fd"),
    Drugs_All_Antibiotics=Input(rid="ri.foundry.main.dataset.3acd36f3-ec12-4236-b8c0-8162ed3f2911"),
    drug_exposure=Input(rid="ri.foundry.main.dataset.ec252b05-8f82-4f7f-a227-b3bb9bc578ef")
)
-- Take filtered drug exposures (antibiotics) and link them to the hospital days 

SELECT
    drug_exposure.person_id
    , drug_exposure.drug_exposure_start_date
    , drug_exposure.drug_exposure_end_date
    , drug_exposure.drug_concept_id
    , drug_exposure.drug_concept_name
    , drug_exposure.route_concept_id
    , Cohort_Hospitalization_Dates.hospital_day
FROM drug_exposure
/* Filter All Drug Exposures to Just Antibiotics */
INNER JOIN Drugs_All_Antibiotics ON
    drug_exposure.drug_concept_id = Drugs_All_Antibiotics.concept_id
/* Filter all antibiotics to just exposures to our cohort during our period of interest */
INNER JOIN Cohort_Hospitalization_Dates ON
    Cohort_Hospitalization_Dates.person_id = drug_exposure.person_id
    AND (
        /* If end_date is null then keep this row if it equals on of our hospital days of interest */
        (drug_exposure.drug_exposure_end_date IS NULL AND drug_exposure.drug_exposure_start_date = Cohort_Hospitalization_Dates.date)
        /* Otherwise keep if date between start and end) */
        OR (Cohort_Hospitalization_Dates.date >= drug_exposure.drug_exposure_start_date AND Cohort_Hospitalization_Dates.date <= drug_exposure.drug_exposure_end_date)
    )

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.0d60b0bf-2156-4b34-a4b3-6082f382e1c2"),
    Concept_Routes_of_Interest=Input(rid="ri.foundry.main.dataset.1343691b-62fc-451d-9442-0f056bdbef61"),
    Drugs_Common_Route=Input(rid="ri.foundry.main.dataset.2e4c4495-2185-4477-8b49-79242f595aba"),
    Early_Antibiotic_Exposures=Input(rid="ri.foundry.main.dataset.6e76efbd-dd8d-48bb-bc85-970aba19a374")
)
/* Here we map the filtered exposures to a route either enteral or parenteral */ 
SELECT
    Early_Antibiotic_Exposures.person_id
    , Early_Antibiotic_Exposures.hospital_day
    , Early_Antibiotic_Exposures.drug_concept_id
    , drug_concept_name
    , CASE
        -- The route is directly mapped
        WHEN is_enteral THEN true 
        WHEN is_parenteral THEN false
        -- Route is not mapped, but >90% of the time this drug_concept_id has an enteral route
        WHEN Drugs_Common_Route.enteral_rate > 0.9 THEN true
        -- Drug name looks like a enteral product
        WHEN LOWER(drug_concept_name) LIKE '%oral%' OR LOWER(drug_concept_name) like '%tablet%' OR LOWER(drug_concept_name) LIKE '%capsule%' THEN true
        -- This is a manual list of drugs that fell through the critera above but are typically enteral
        WHEN Early_Antibiotic_Exposures.drug_concept_id IN (1734104,1738521,1707164,1742253,997881,35861919,45774861,1742287,35151057) THEN true
    ELSE false END AS is_enteral
    , CASE
        -- The route is directly mapped
        WHEN is_parenteral THEN true
        WHEN is_enteral THEN false
        -- Route is not mapped, but >90% of the time this drug is given parenterally
        WHEN Drugs_Common_Route.parenteral_rate > 0.9 THEN true
        -- Drug name looks like a parenteral product
        WHEN LOWER(drug_concept_name) LIKE '%injectable%' OR LOWER(drug_concept_name) like '%injection%' OR LOWER(drug_concept_name) like '%syringe%' THEN true
        -- same as above but parenteral
        WHEN Early_Antibiotic_Exposures.drug_concept_id IN (1736887,1836241,46274210,36789702,45774861,46221507) THEN true
    ELSE false END AS is_parenteral
FROM Early_Antibiotic_Exposures
LEFT JOIN Concept_Routes_of_Interest ON
    Early_Antibiotic_Exposures.route_concept_id = Concept_Routes_of_Interest.concept_id
LEFT JOIN Drugs_Common_Route ON
    Early_Antibiotic_Exposures.drug_concept_id = Drugs_Common_Route.drug_concept_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1e64acb5-e806-4248-b022-a55b3cfd2f46"),
    Lab_Measures_During_Hospitalization=Input(rid="ri.foundry.main.dataset.82742d6f-b700-4f8f-b94a-38c85c57ebee")
)
-- Create a high or low level for the PCT (if measured) by using the maximum value
SELECT
    person_id
    , CASE WHEN MAX(harmonized_value_as_number) > 0.5 THEN 'high' ELSE 'low' END AS pct_group 
FROM Lab_Measures_During_Hospitalization
WHERE hospital_day < 2 AND measure_name = 'pct'
GROUP BY
    person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.2d5fcd77-d8b8-40e7-be82-c98a6ac86124"),
    Severity_Of_Illness_By_Day=Input(rid="ri.foundry.main.dataset.2872658a-3c84-4e21-9bb3-ab0e749f98b8")
)
-- Aggregate IMV or ECMO on day 0 or 1 for early severity of illness
SELECT
    person_id
    , MAX(imv) AS early_imv
    , MAX(ecmo) AS early_ecmo
FROM Severity_Of_Illness_By_Day
WHERE
    hospital_day < 2
GROUP BY person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.781ab12e-d7db-4563-bd9e-8894a5b5590a"),
    Early_Antibiotic_Exposures_Mapped_to_Routes=Input(rid="ri.foundry.main.dataset.0d60b0bf-2156-4b34-a4b3-6082f382e1c2")
)
/* This aggregated the mapped exposures into a count of exposures by day with 
flags for parenteral and enteral exposures */ 
SELECT
    person_id
    , hospital_day
    , MAX(CASE WHEN is_enteral THEN 1 ELSE 0 END) AS enteral_exposure
    , MAX(CASE WHEN is_parenteral THEN 1 ELSE 0 END) AS parenteral_exposure
FROM Early_Antibiotic_Exposures_Mapped_to_Routes
GROUP BY person_id, hospital_day

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c15035f7-be8f-4165-a3c5-9308a69367a2"),
    Exposures_By_Day=Input(rid="ri.foundry.main.dataset.781ab12e-d7db-4563-bd9e-8894a5b5590a")
)
-- Here we build a variety of outcomes for the primary and sensitivity analysis

SELECT
    person_id
    , SUM(CASE WHEN hospital_day <= 4 THEN parenteral_exposure END) AS parenteral_days_to_day_4
    , SUM(CASE WHEN hospital_day <= 5 THEN parenteral_exposure END) AS parenteral_days_to_day_5
    , SUM(CASE WHEN hospital_day <= 6 THEN parenteral_exposure END) AS parenteral_days_to_day_6
    , SUM(CASE WHEN hospital_day <= 4 THEN GREATEST(parenteral_exposure, enteral_exposure, 0) END) AS any_days_to_day_4
    , SUM(CASE WHEN hospital_day <= 5 THEN GREATEST(parenteral_exposure, enteral_exposure, 0) END) AS any_days_to_day_5
    , SUM(CASE WHEN hospital_day <= 6 THEN GREATEST(parenteral_exposure, enteral_exposure, 0) END) AS any_days_to_day_6
FROM Exposures_By_Day
GROUP BY person_id

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.582aaae3-361a-459b-94dd-ba5fe47ee5b9"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
-- Aggregate by partner and month for Figure 1a
SELECT
    data_partner_id
    , reporting_period
    , CAST(COUNT(*) AS INT) AS case_count
    , AVG(target) AS exposure_average
FROM Final_Analysis_Cohort_With_Exclusions
GROUP BY 
    data_partner_id
    , reporting_period

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1c8882c6-f528-4c21-b342-680458570149"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
--Aggregate by region and month for an exploratory figure
SELECT
    division
    , reporting_period
    , CAST(COUNT(*) AS INT) AS case_count
    , AVG(target) AS exposure_average
FROM Final_Analysis_Cohort_With_Exclusions
GROUP BY 
    division
    , reporting_period

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b6368e45-75aa-407a-aaaf-e4a9724ae9ac"),
    Base_Cohort=Input(rid="ri.foundry.main.dataset.f80c5b44-af52-42a2-b276-48c355fe97b4"),
    Early_Procalcitonin_During_Hospitalization=Input(rid="ri.foundry.main.dataset.1e64acb5-e806-4248-b022-a55b3cfd2f46"),
    Early_Severity_Of_Illness=Input(rid="ri.foundry.main.dataset.2d5fcd77-d8b8-40e7-be82-c98a6ac86124"),
    Exposures_Summary=Input(rid="ri.foundry.main.dataset.c15035f7-be8f-4165-a3c5-9308a69367a2"),
    First_WBC_For_Hospitalization=Input(rid="ri.foundry.main.dataset.f1c75948-9178-4d4e-9b5c-9487df60c2fa"),
    zipprefix_to_state=Input(rid="ri.foundry.main.dataset.16cb2b8d-b1b6-480b-83b7-31a5bba5f3af")
)
-- Merge the interim dataframes into a main analysis data frame. 

SELECT
    data_partner_id
    , age_at_covid
    , gender_concept_name
    , race_ethnicity
    , COALESCE(zipprefix_to_state.division, "Other/Missing") AS division
    , COALESCE(zipprefix_to_state.region, "Other/Missing") AS region
    , sdoh2
    , First_WBC_For_Hospitalization.wbc
    , COALESCE(BMI_max_observed_or_calculated_before_covid, BMI_max_observed_or_calculated_post_covid) AS bmi
    , CASE WHEN OBESITY_before_covid_indicator = 1 OR OBESITY_post_covid_indicator = 1 THEN 1 ELSE 0 END AS obesity
    , CASE WHEN TOBACCOSMOKER_before_covid_indicator = 1 OR TOBACCOSMOKER_post_covid_indicator = 1 THEN 1 ELSE 0 END AS smoker
    , CASE 
        WHEN age_at_covid >= 80 THEN 4
        WHEN age_at_covid >= 70 THEN 3
        WHEN age_at_covid >= 60 THEN 2
        WHEN age_at_covid >= 50 THEN 1
        ELSE 0 END AS CCI_age
    , CASE WHEN MYOCARDIALINFARCTION_before_covid_indicator = 1 OR MYOCARDIALINFARCTION_post_covid_indicator =1 THEN 1 ELSE 0 END AS CCI_mi
    , CASE WHEN CONGESTIVEHEARTFAILURE_before_covid_indicator = 1 OR CONGESTIVEHEARTFAILURE_post_covid_indicator = 1 THEN 1 ELSE 0 END AS CCI_chf
    , CASE WHEN PERIPHERALVASCULARDISEASE_before_covid_indicator = 1 OR PERIPHERALVASCULARDISEASE_post_covid_indicator = 1 THEN 1 ELSE 0 END AS CCI_pvd
    , CASE WHEN CEREBROVASCULARDISEASE_before_covid_indicator = 1 OR CEREBROVASCULARDISEASE_post_covid_indicator = 1 THEN 1 ELSE 0 END AS CCI_stroke
    , CASE WHEN DEMENTIA_before_covid_indicator = 1 OR DEMENTIA_post_covid_indicator = 1 THEN 1 ELSE 0 END AS CCI_dementia
    , CASE WHEN CHRONICLUNGDISEASE_before_covid_indicator = 1 OR CHRONICLUNGDISEASE_post_covid_indicator = 1 THEN 1 ELSE 0 END AS CCI_copd
    , CASE WHEN RHEUMATOLOGICDISEASE_before_covid_indicator = 1 OR RHEUMATOLOGICDISEASE_post_covid_indicator = 1 THEN 1 ELSE 0 END AS CCI_ctd
    , CASE WHEN PEPTICULCER_before_covid_indicator = 1 OR PEPTICULCER_post_covid_indicator = 1 THEN 1 ELSE 0 END AS CCI_pud
    , CASE
        WHEN MODERATESEVERELIVERDISEASE_before_covid_indicator = 1 OR MODERATESEVERELIVERDISEASE_post_covid_indicator = 3 THEN 2
        WHEN MILDLIVERDISEASE_before_covid_indicator = 1 OR MILDLIVERDISEASE_post_covid_indicator = 1 THEN 1 
        ELSE 0 END AS CCI_liver
    , CASE 
        WHEN DIABETESCOMPLICATED_before_covid_indicator = 1 OR DIABETESCOMPLICATED_post_covid_indicator = 1 THEN 2 
        WHEN DIABETESUNCOMPLICATED_before_covid_indicator = 1 OR DIABETESUNCOMPLICATED_post_covid_indicator = 1 THEN 1
        ELSE 0 END AS CCI_diabetes
    , CASE WHEN HEMIPLEGIAORPARAPLEGIA_before_covid_indicator = 1 OR HEMIPLEGIAORPARAPLEGIA_post_covid_indicator = 1 THEN 2 ELSE 0 END AS CCI_hemiplegia
    , CASE WHEN KIDNEYDISEASE_before_covid_indicator = 1 OR KIDNEYDISEASE_post_covid_indicator = 1 THEN 2 ELSE 0 END AS CCI_ckd
    , CASE
        WHEN METASTATICSOLIDTUMORCANCERS_before_covid_indicator = 1 OR METASTATICSOLIDTUMORCANCERS_post_covid_indicator = 1 THEN 6
        WHEN MALIGNANTCANCER_before_covid_indicator = 1 OR MALIGNANTCANCER_post_covid_indicator = 1 THEN 2 
        ELSE 0 END AS CCI_cancer
    , CASE WHEN HIVINFECTION_before_covid_indicator = 1 OR HIVINFECTION_post_covid_indicator = 1 THEN 2 ELSE 0 END AS CCI_aids
    , Base_Cohort.first_COVID_hospitalization_start_date
    , date_trunc('month', Base_Cohort.first_COVID_hospitalization_start_date) AS reporting_period
    , DATEDIFF(Base_Cohort.first_COVID_hospitalization_end_date, Base_Cohort.first_COVID_hospitalization_start_date) AS total_length_of_stay
    , COALESCE(pct_group, 'not measured') AS pct_group
    , COALESCE(Early_Severity_Of_Illness.early_imv, 0) AS early_imv
    , COALESCE(Early_Severity_Of_Illness.early_ecmo, 0) AS early_ecmo
    , COVID_patient_death_during_covid_hospitalization_indicator AS hospital_mortality
    , COALESCE(parenteral_days_to_day_4, 0) AS parenteral_days_to_day_4
    , COALESCE(parenteral_days_to_day_5, 0) AS parenteral_days_to_day_5
    , COALESCE(parenteral_days_to_day_6, 0) AS parenteral_days_to_day_6
    , COALESCE(any_days_to_day_4, 0) AS any_days_to_day_4
    , COALESCE(any_days_to_day_5, 0) AS any_days_to_day_5
    , COALESCE(any_days_to_day_6, 0) AS any_days_to_day_6
    , CASE WHEN Exposures_Summary.parenteral_days_to_day_5 > 3 THEN 1 ELSE 0 END AS outcome_iv_day5
    , CASE WHEN Exposures_Summary.any_days_to_day_5 > 3 THEN 1 ELSE 0 END AS outcome_any_day5
    , CASE WHEN Exposures_Summary.parenteral_days_to_day_4 > 4 THEN 1 ELSE 0 END AS outcome_iv_day4
    , CASE WHEN Exposures_Summary.any_days_to_day_4 > 4 THEN 1 ELSE 0 END AS outcome_any_day4
FROM Base_Cohort
LEFT JOIN Exposures_Summary USING (person_id)
LEFT JOIN Early_Procalcitonin_During_Hospitalization USING (person_id)
LEFT JOIN Early_Severity_Of_Illness USING (person_id)
LEFT JOIN First_WBC_For_Hospitalization USING (person_id)
LEFT JOIN zipprefix_to_state ON
    SUBSTR(postal_code, 0, 3) = zipprefix_to_state.prefix

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9"),
    Data_Partner_Statistics=Input(rid="ri.foundry.main.dataset.b8883699-ff6d-404c-9b1d-bb05eafe80e1"),
    Final_Analysis_Cohort=Input(rid="ri.foundry.main.dataset.b6368e45-75aa-407a-aaaf-e4a9724ae9ac")
)
--  Filter the main analysis data frame for excluded patients (either by time or center)
SELECT
    data_partner_id
    , CAST(age_at_covid AS INTEGER) AS age_at_covid
    , race_ethnicity
    , region
    , division
    , sdoh2
    , bmi
    , wbc
    , obesity
    , smoker
    , CCI_age + CCI_mi + CCI_chf + CCI_pvd + CCI_stroke + CCI_dementia + CCI_copd + CCI_ctd + CCI_pud + CCI_liver + CCI_diabetes + CCI_hemiplegia + CCI_ckd + CCI_cancer + CCI_aids AS CCI
    , CASE 
        WHEN gender_concept_name IN ('MALE', 'FEMALE') THEN gender_concept_name
        ELSE 'UNKNOWN_OTHER' END AS gender_concept_name
    , reporting_period
    , first_COVID_hospitalization_start_date
    , total_length_of_stay
    , outcome_iv_day5 AS target
    , CAST(parenteral_days_to_day_5 AS INTEGER) AS exposure_days
    , outcome_iv_day4
    , outcome_any_day4
    , outcome_iv_day5
    , outcome_any_day5
    , pct_group
    , early_imv
    , early_ecmo
    , hospital_mortality
FROM Final_Analysis_Cohort
WHERE
    reporting_period >= '2020-03-01'
    AND reporting_period < '2022-01-01'
    AND data_partner_id IN (SELECT data_partner_id FROM Data_Partner_Statistics WHERE record_count > 500)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f1c75948-9178-4d4e-9b5c-9487df60c2fa"),
    Lab_Measures_During_Hospitalization=Input(rid="ri.foundry.main.dataset.82742d6f-b700-4f8f-b94a-38c85c57ebee")
)
-- Select the first WBC value for the hospitalization
SELECT
    person_id
    , harmonized_value_as_number AS wbc
FROM Lab_Measures_During_Hospitalization
WHERE measure_name = 'wbc' AND seq_no = 1

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.82742d6f-b700-4f8f-b94a-38c85c57ebee"),
    Base_Cohort=Input(rid="ri.foundry.main.dataset.f80c5b44-af52-42a2-b276-48c355fe97b4"),
    Lab_Measures_Of_Interest=Input(rid="ri.foundry.main.dataset.6be2cb81-7213-460d-9230-9d8f1e71f8bd"),
    measurement=Input(rid="ri.foundry.main.dataset.d6054221-ee0c-4858-97de-22292458fa19")
)
-- Identify lab measures of interest (WBC, PCT) for the entire cohort  and link them to hospital days

SELECT
    measurement.person_id
    , Lab_Measures_Of_Interest.measure_name
    , measurement.harmonized_value_as_number
    , measurement.measurement_date
    , measurement.measurement_datetime
    , DATEDIFF(measurement.measurement_date, Base_Cohort.first_COVID_hospitalization_start_date) AS hospital_day
    , ROW_NUMBER() OVER (PARTITION BY measurement.person_id, Lab_Measures_Of_Interest.measure_name ORDER BY measurement.measurement_datetime ASC) AS seq_no
FROM measurement
INNER JOIN Lab_Measures_Of_Interest ON
    measurement.measurement_concept_id = Lab_Measures_Of_Interest.concept_id
INNER JOIN Base_Cohort ON
    measurement.person_id = Base_Cohort.person_id
    AND measurement.measurement_date BETWEEN Base_Cohort.first_COVID_hospitalization_start_date AND Base_Cohort.first_COVID_hospitalization_end_date
WHERE
    measurement.harmonized_value_as_number IS NOT NULL

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.5b252774-2a8b-4339-8201-ea7aeb430a18"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
-- Aggregate outcomes for the whole cohort by month for statistical testing
SELECT
    reporting_period
    , COUNT(*) AS n
    , SUM(target) AS events
    , COUNT(*) - SUM(target) AS non_events
    , AVG(target) AS rate
FROM Final_Analysis_Cohort_With_Exclusions
GROUP BY reporting_period
ORDER BY reporting_period

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.bcc3fbe8-812b-4b64-adff-c40607846d06"),
    Data_Partner_Statistics=Input(rid="ri.foundry.main.dataset.b8883699-ff6d-404c-9b1d-bb05eafe80e1"),
    Final_Analysis_Cohort=Input(rid="ri.foundry.main.dataset.b6368e45-75aa-407a-aaaf-e4a9724ae9ac")
)
-- Create an anlaysis data frame for the sensitivity modeling
SELECT
    data_partner_id
    , CAST(age_at_covid AS INTEGER) AS age_at_covid
    , race_ethnicity
    , sdoh2
    , bmi
    , wbc
    , obesity
    , smoker
    , CCI_age + CCI_mi + CCI_chf + CCI_pvd + CCI_stroke + CCI_dementia + CCI_copd + CCI_ctd + CCI_pud + CCI_liver + CCI_diabetes + CCI_hemiplegia + CCI_ckd + CCI_cancer + CCI_aids AS CCI
    , CASE 
        WHEN gender_concept_name IN ('MALE', 'FEMALE') THEN gender_concept_name
        ELSE 'UNKNOWN_OTHER' END AS gender_concept_name
    , reporting_period
    , total_length_of_stay
    , pct_group
    , early_imv
    , early_ecmo
    , CAST(any_days_to_day_4 AS INTEGER) AS any_days_to_day_4
    , CAST(any_days_to_day_5 AS INTEGER) AS any_days_to_day_5
    , CAST(any_days_to_day_6 AS INTEGER) AS any_days_to_day_6
    , CAST(parenteral_days_to_day_4 AS INTEGER) AS parenteral_days_to_day_4
    , CAST(parenteral_days_to_day_5 AS INTEGER) AS parenteral_days_to_day_5
    , CAST(parenteral_days_to_day_6 AS INTEGER) AS parenteral_days_to_day_6
FROM Final_Analysis_Cohort
WHERE
    reporting_period >= '2020-03-01'
    AND reporting_period < '2022-01-01'
    AND data_partner_id IN (SELECT data_partner_id FROM Data_Partner_Statistics WHERE record_count > 500)

@transform_pandas(
    Output(rid="ri.vector.main.execute.51de5e03-2f96-42ba-acf2-0c0383a2c73c"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
-- count the number of exposed patients for the textual results
SELECT COUNT(*)
FROM Final_Analysis_Cohort_With_Exclusions
WHERE target = 1

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.16cb2b8d-b1b6-480b-83b7-31a5bba5f3af"),
    state_to_region=Input(rid="ri.foundry.main.dataset.187cb29c-c246-4186-8b2f-e79041d627be"),
    zip_codes=Input(rid="ri.foundry.main.dataset.43798435-ca6f-48ef-89ef-a6535207d69a")
)
/* Create a map of zip prefixes (i.e 123xx) to States and then map that state to a region code (for the regionality analysis) */

WITH agg_data AS (
    SELECT
        SUBSTR(postal_code, 0, 3) AS prefix
        ,admin_code1 AS state
        , COUNT(*) AS zip_count
    FROM zip_codes
    GROUP BY SUBSTR(postal_code, 0, 3), admin_code1
), rank_data AS (
    SELECT
        prefix
        , state
        , zip_count
        , ROW_NUMBER() OVER (PARTITION BY prefix ORDER BY zip_count) AS ranking
    FROM
        agg_data
)
SELECT
    prefix
    , state
    , division
    , region
FROM rank_data
LEFT JOIN state_to_region USING (state)
WHERE ranking = 1

