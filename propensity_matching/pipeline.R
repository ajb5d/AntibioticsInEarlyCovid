

@transform_pandas(
    Output(rid="ri.vector.main.execute.743c131c-e428-460e-8c21-c4efdafedf1d"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
library(MatchIt)
library(MatchThem)
library(mice)
library(lubridate)
library(tidyverse)
library(cobalt)

Sensitivity_Any_Route_Mean_Imputation <- function(Final_Analysis_Cohort_With_Exclusions) {
    options(width = 320)

    df <- Final_Analysis_Cohort_With_Exclusions %>%
        mutate(target = outcome_any_day5) %>%
        select(data_partner_id:reporting_period, target, early_imv:late_antibiotic_exposure, early_trauma_flag, early_major_procedure, -sdoh2, early_vasopressor_use, late_cdad) %>%
        mutate(
            data_partner_id = as_factor(data_partner_id), 
            gender_concept_name = as_factor(gender_concept_name) %>% fct_explicit_na(),
            reporting_period = as_factor(year(reporting_period) * 12 + month(reporting_period) - (2020*12+3)),
            race_ethnicity = as_factor(race_ethnicity)
        )

    datasets <- mice(df, method = 'mean')

    models <- matchthem(target ~ reporting_period + early_ecmo + early_imv + early_trauma_flag + early_vasopressor_use + early_major_procedure + race_ethnicity + s(wbc) + s(age_at_covid) + obesity + smoker + s(CCI), datasets, distance='gam', apparoach='within')

    print(bal.tab(models))

    love.plot(models, stats = c("mean.diffs", "variance.ratios"), thresholds = c(m = 0.1, v = 2), abs = TRUE, binary = "std", var.order = "unadjusted")

    print("hospital_mortality")
    results <- with(models, glm(hospital_mortality ~ target, family=binomial()))
    output <- pool(results)
    print(summary(output, conf.int=TRUE))

    print("long_imv")
    results <- with(models, glm(long_imv ~ target, family=binomial()))
    output <- pool(results)
    print(summary(output, conf.int=TRUE))

    print("late_abx")
    results <- with(models, glm(late_antibiotic_exposure ~ target, family=binomial()))
    output <- pool(results)
    print(summary(output, conf.int=TRUE))

    print("late_cdad")
    results <- with(models, glm(late_cdad ~ target, family=binomial()))
    output <- pool(results)
    print(summary(output, conf.int=TRUE))
    
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.a7d74819-5bfa-43dd-b277-098a62838b96"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
library(MatchIt)
library(MatchThem)
library(mice)
library(lubridate)
library(tidyverse)
library(cobalt)

psm_with_imputation <- function(Final_Analysis_Cohort_With_Exclusions) {
    options(width = 320)

    df <- Final_Analysis_Cohort_With_Exclusions %>%
        select(data_partner_id:reporting_period, target, early_imv:late_antibiotic_exposure, early_trauma_flag, early_major_procedure, -sdoh2, early_vasopressor_use, late_cdad) %>%
        mutate(
            data_partner_id = as_factor(data_partner_id), 
            gender_concept_name = as_factor(gender_concept_name) %>% fct_explicit_na(),
            reporting_period = as_factor(year(reporting_period) * 12 + month(reporting_period) - (2020*12+3)),
            race_ethnicity = as_factor(race_ethnicity)
        )

    datasets <- mice(df)

    models <- matchthem(target ~ reporting_period + early_ecmo + early_imv + early_vasopressor_use + early_trauma_flag + early_major_procedure + race_ethnicity + s(wbc) + s(age_at_covid) + obesity + smoker + s(CCI), datasets, distance='gam', apparoach='within')

    print(bal.tab(models))

    love.plot(models, stats = c("mean.diffs", "variance.ratios"), thresholds = c(m = 0.1,
    v = 2), abs = TRUE, binary = "std", var.order = "unadjusted")

    print("hospital_mortality")
    results <- with(models, glm(hospital_mortality ~ target, family=binomial()))
    output <- pool(results)
    print(summary(output, conf.int=TRUE))

    print("long_imv")
    results <- with(models, glm(long_imv ~ target, family=binomial()))
    output <- pool(results)
    print(summary(output, conf.int=TRUE))

    print("late_abx")
    results <- with(models, glm(late_antibiotic_exposure ~ target, family=binomial()))
    output <- pool(results)
    print(summary(output, conf.int=TRUE))

    print("late_cdad")
    results <- with(models, glm(late_cdad ~ target, family=binomial()))
    output <- pool(results)
    print(summary(output, conf.int=TRUE))
    
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.479e3783-64db-4647-af78-478d238e94b6"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
library(MatchIt)
library(MatchThem)
library(mice)
library(lubridate)
library(tidyverse)
library(cobalt)

psm_with_mean_imputation <- function(Final_Analysis_Cohort_With_Exclusions) {
    options(width = 320)

    df <- Final_Analysis_Cohort_With_Exclusions %>%
        select(data_partner_id:reporting_period, target, early_imv:late_antibiotic_exposure, early_trauma_flag, early_major_procedure, -sdoh2, early_vasopressor_use, late_cdad) %>%
        mutate(
            data_partner_id = as_factor(data_partner_id), 
            gender_concept_name = as_factor(gender_concept_name) %>% fct_explicit_na(),
            reporting_period = as_factor(year(reporting_period) * 12 + month(reporting_period) - (2020*12+3)),
            race_ethnicity = as_factor(race_ethnicity)
        )

    datasets <- mice(df, method = 'mean')

    models <- matchthem(target ~ reporting_period + early_ecmo + early_imv + early_trauma_flag + early_vasopressor_use + early_major_procedure + race_ethnicity + s(wbc) + s(age_at_covid) + obesity + smoker + s(CCI), datasets, distance='gam', apparoach='within')

    print(bal.tab(models))

    love.plot(models, stats = c("mean.diffs", "variance.ratios"), thresholds = c(m = 0.1, v = 2), abs = TRUE, binary = "std", var.order = "unadjusted")

    print("hospital_mortality")
    results <- with(models, glm(hospital_mortality ~ target, family=binomial()))
    output <- pool(results)
    print(summary(output, conf.int=TRUE))

    print("long_imv")
    results <- with(models, glm(long_imv ~ target, family=binomial()))
    output <- pool(results)
    print(summary(output, conf.int=TRUE))

    print("late_abx")
    results <- with(models, glm(late_antibiotic_exposure ~ target, family=binomial()))
    output <- pool(results)
    print(summary(output, conf.int=TRUE))

    print("late_cdad")
    results <- with(models, glm(late_cdad ~ target, family=binomial()))
    output <- pool(results)
    print(summary(output, conf.int=TRUE))
    
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.23c0b28e-3adc-4010-8a6c-40e1f5c21d20"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
library(MatchIt)
library(MatchThem)
library(mice)
library(lubridate)
library(tidyverse)
library(cobalt)

sensitivity_any_route_imputation <- function(Final_Analysis_Cohort_With_Exclusions) {
    options(width = 320)

    df <- Final_Analysis_Cohort_With_Exclusions %>%
        select(data_partner_id:reporting_period, target, early_imv:late_antibiotic_exposure, early_trauma_flag, early_major_procedure, -sdoh2, early_vasopressor_use, late_cdad) %>%
        mutate(
            data_partner_id = as_factor(data_partner_id), 
            gender_concept_name = as_factor(gender_concept_name) %>% fct_explicit_na(),
            reporting_period = as_factor(year(reporting_period) * 12 + month(reporting_period) - (2020*12+3)),
            race_ethnicity = as_factor(race_ethnicity)
        )

    datasets <- mice(df)

    models <- matchthem(target ~ reporting_period + early_ecmo + early_imv + early_vasopressor_use + early_trauma_flag + early_major_procedure + race_ethnicity + s(wbc) + s(age_at_covid) + obesity + smoker + s(CCI), datasets, distance='gam', apparoach='within')

    print(bal.tab(models))

    love.plot(models, stats = c("mean.diffs", "variance.ratios"), thresholds = c(m = 0.1,
    v = 2), abs = TRUE, binary = "std", var.order = "unadjusted")

    print("hospital_mortality")
    results <- with(models, glm(hospital_mortality ~ target, family=binomial()))
    output <- pool(results)
    print(summary(output, conf.int=TRUE))

    print("long_imv")
    results <- with(models, glm(long_imv ~ target, family=binomial()))
    output <- pool(results)
    print(summary(output, conf.int=TRUE))

    print("late_abx")
    results <- with(models, glm(late_antibiotic_exposure ~ target, family=binomial()))
    output <- pool(results)
    print(summary(output, conf.int=TRUE))

    print("late_cdad")
    results <- with(models, glm(late_cdad ~ target, family=binomial()))
    output <- pool(results)
    print(summary(output, conf.int=TRUE))
    
    return(NULL)
}

