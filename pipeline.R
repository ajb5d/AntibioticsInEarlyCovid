library(tidyverse)
library(patchwork)

@transform_pandas(
    Output(rid="ri.vector.main.execute.97b2c02d-6cb2-4f5f-9f4a-114c68a79177"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
explore_figure_severity <- function(Final_Analysis_Cohort_With_Exclusions) {
    ## This was an exploratory figure about EEAU by severity of illness and PCT measure. It was not included in the final paper

    df <- Final_Analysis_Cohort_With_Exclusions %>% mutate(pct_group = as_factor(pct_group), early_imv = as_factor(early_imv == 1), early_ecmo = as_factor(early_ecmo == 1))

    display_df <- df %>% group_by(pct_group, reporting_period) %>% summarise(count = n(), avg = mean(target))
    p1 <- ggplot(display_df, aes(reporting_period, avg, color=pct_group)) + geom_line()

    display_df <- df %>% group_by(early_imv, reporting_period) %>% summarise(count = n(), avg = mean(target))
    p2 <- ggplot(display_df, aes(reporting_period, avg, color=early_imv)) + geom_line()

    display_df <- df %>% group_by(early_ecmo, reporting_period) %>% summarise(count = n(), avg = mean(target))
    p3 <- ggplot(display_df, aes(reporting_period, avg, color=early_ecmo)) + geom_line()

    print(p1 / (p2 + p3))

    return(NULL)
}   

@transform_pandas(
    Output(rid="ri.vector.main.execute.11865f3f-8fe0-4765-b65c-8b3281d1f1f0"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
explore_rates_by_month_overall <- function(Final_Analysis_Cohort_With_Exclusions) {
    ## This summarizes rates over time for the whole cohort
    df <- Final_Analysis_Cohort_With_Exclusions %>% group_by(reporting_period) %>% summarise(avg = mean(target))
    plt <- ggplot(df) + geom_line(aes(reporting_period, avg))
    print(plt)
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.2268fa48-551e-43d8-972c-a9a7b345d959"),
    Drugs_Labelled_By_Hospital_Day=Input(rid="ri.foundry.main.dataset.f9ae6e03-c975-4f18-8296-7a96c11aa177"),
    Figure_Data_By_Center=Input(rid="ri.foundry.main.dataset.582aaae3-361a-459b-94dd-ba5fe47ee5b9")
)
letter_figure_1 <- function(Figure_Data_By_Center, Drugs_Labelled_By_Hospital_Day) {

    ## this builds the figure 1 for the publication
    df <- Figure_Data_By_Center %>%
        mutate(reporting_period = as.character(reporting_period) %>% str_remove("-..$") %>% as.factor())
        
    p1 <- ggplot(df, aes(x = reporting_period, y = exposure_average)) + 
        geom_boxplot() +
        geom_jitter(aes(size = case_count), alpha = 0.5, width = 0.3) +
        scale_y_continuous(labels = scales::label_percent(), limits = c(0,1)) + 
        scale_x_discrete(guide = guide_axis(n.dodge = 2)) + 
        scale_size_binned_area(labels = scales::label_comma()) +
        theme_bw() + 
        labs(x = "Month", y = "Admissions with EEUA", size = "Included Encounters")

    df2 <- Drugs_Labelled_By_Hospital_Day %>%
        mutate(drug_name = str_to_title(drug_name) %>%
                            as_factor() %>% 
                            fct_explicit_na() %>% 
                            fct_lump_n(10, w = c) %>%
                            fct_reorder(c), 
                            hospital_day = as_factor(hospital_day)) %>%
        group_by(hospital_day, drug_name) %>%
            summarise(c = sum(c))

    p2 <- ggplot(df2, aes(hospital_day, c, fill = drug_name)) + 
        geom_bar(stat = 'identity') + 
        scale_y_continuous(labels = scales::label_comma()) + 
        scale_fill_brewer(palette="Set3") + 
        theme_bw() +
        labs(x = "Hospital Day", y = "Count of Daily Antibiotic Orders", fill = "Drug")

    plt <-  p1 / p2 + plot_annotation(tag_levels = 'A')

    #image: svg
    svg(filename=graphicsFile, width=11, height=8, bg="white")
    print(plt)

    return(NULL)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.52ffcf1d-d564-409b-b53d-1eaaecc131f2"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
library(gtsummary)
library(labelled)

publication_table1 <- function(Final_Analysis_Cohort_With_Exclusions) {

    ## This builds the table1 for the publication

    part_a <- Final_Analysis_Cohort_With_Exclusions %>%
        select(target, age_at_covid, gender_concept_name) %>%
        mutate(
            target = as_factor(target) %>% fct_recode("No EEAU" = "0", "EEAU" = "1"),
            gender_concept_name = na_if(gender_concept_name, "UNKNOWN_OTHER") %>% as_factor() %>% fct_recode("Female" = "FEMALE", "Male" = "MALE") %>% fct_infreq()) %>%
        set_variable_labels(age_at_covid = "Age", gender_concept_name = 'Gender') %>%
        tbl_summary(by = target, missing = "no") %>%
        add_n() %>%
        add_p()

    part_b <- Final_Analysis_Cohort_With_Exclusions %>%
        select(target, total_length_of_stay, bmi, obesity, sdoh2, race_ethnicity, CCI, exposure_days, smoker, pct_group, wbc, early_ecmo, early_imv, hospital_mortality) %>%
        mutate(
            target = as_factor(target) %>% fct_recode("No EEAU" = "0", "EEAU" = "1"),
            race_ethnicity = as_factor(race_ethnicity) %>% fct_infreq(),
            pct_group = as_factor(pct_group) %>% fct_recode("<0.5" = "low", ">0.5" = "high", "Not Measured" = "not measured") %>% fct_infreq()
        ) %>%
        set_variable_labels(
            total_length_of_stay = "Inpatient Length of Stay",
            race_ethnicity = "Race/Ethnicity",
            bmi = 'Body Mass Index',
            obesity = 'Obesity',
            CCI = 'Charlson Comorbidity Index',
            smoker = 'Current Tobacco Use',
            sdoh2 = 'BU/ShareCare SDoH Index v2', 
            exposure_days = "Days of Early Antibiotic Exposure",
            early_ecmo = 'ECMO On Hospital Day < 2',
            early_imv = 'IMV On Hospital Day < 2',
            hospital_mortality = 'In Hospital Mortality', 
            wbc = "Initial White Blood Cell Count (10^9/L)",
            pct_group = "Procalcitonin") %>%
        tbl_summary(by = target) %>%
        add_n() %>%
        add_p()

    table1 <- tbl_stack(list(part_a, part_b))

    print(table1)

    return(as_tibble(table1, col_labels = FALSE))
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.645ea4ff-1c4f-4c72-93f6-4207c9b9b238"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
library(lubridate)
library(mgcv)
library(broom)

regression_model2 <- function(Final_Analysis_Cohort_With_Exclusions) {
    # This model compares PCT_high vs PCT_Low for EEAU 
    options(scipen = 100)
    
    df <- Final_Analysis_Cohort_With_Exclusions %>%
        select(target, age_at_covid, bmi,  obesity, sdoh2, race_ethnicity, CCI, exposure_days, smoker, pct_group, early_ecmo, early_imv, data_partner_id, gender_concept_name, reporting_period) %>%
        filter(pct_group != "not measured") %>%
        mutate(
            data_partner_id = as_factor(data_partner_id), 
            gender_concept_name = as_factor(gender_concept_name) %>% fct_explicit_na(),
            reporting_period = as_factor(year(reporting_period) * 12 + month(reporting_period) - (2020*12+3)),
            pct_group = as_factor(pct_group),
            race_ethnicity = as_factor(race_ethnicity)
        )

    m <- gam(target ~ s(age_at_covid) + gender_concept_name + s(reporting_period, bs = 're') + pct_group + early_imv + early_ecmo + s(data_partner_id, bs = 're') + s(CCI), data = df, family = "binomial", method = "REML")

    print(summary(m))

    print(exp(coef(m)))
    print(exp(confint.default(m)))

    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.98f5ea7d-f776-4b26-b670-a55bcc15a066"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
library(lubridate)
library(mgcv)
regression_model_1 <- function(Final_Analysis_Cohort_With_Exclusions) {
    # This model examines IMV and ECMO's assocation with EEAU
    options(scipen = 100)
    
    df <- Final_Analysis_Cohort_With_Exclusions %>%
        select(target, age_at_covid, bmi,  obesity, sdoh2, race_ethnicity, CCI, exposure_days, smoker, pct_group, early_ecmo, early_imv, data_partner_id, gender_concept_name, reporting_period) %>%
        mutate(
            data_partner_id = as_factor(data_partner_id), 
            gender_concept_name = as_factor(gender_concept_name) %>% fct_explicit_na(),
            reporting_period = as_factor(year(reporting_period) * 12 + month(reporting_period) - (2020*12+3)),
            pct_group = as_factor(pct_group),
            race_ethnicity = as_factor(race_ethnicity)
        )

    m <- gam(target ~ s(age_at_covid) + gender_concept_name + s(reporting_period, bs = 're') + early_imv + early_ecmo + s(data_partner_id, bs = 're') + s(CCI), data = df, family = "binomial", method = "REML")

    print(summary(m))

    print(exp(coef(m)))
    print(exp(confint.default(m)))

    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.28570f6d-6c46-4e6f-8a2d-6bba116f0a65"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
library(lubridate)
library(mgcv)
regression_model_region_effect <- function(Final_Analysis_Cohort_With_Exclusions) {
    ## This is the regression model to evaluate the assocation of early IMV, early ECMO, and regional variations with EEAU
    
    options(scipen = 100)
    
    df <- Final_Analysis_Cohort_With_Exclusions %>%
        select(target, age_at_covid, bmi,  obesity, sdoh2, race_ethnicity, CCI, exposure_days, smoker, pct_group, early_ecmo, early_imv, data_partner_id, gender_concept_name, reporting_period, region, division) %>%
        mutate(
            data_partner_id = as_factor(data_partner_id), 
            region = as_factor(region) %>% fct_infreq(),
            division = as_factor(division) %>% fct_infreq(), 
            gender_concept_name = as_factor(gender_concept_name) %>% fct_explicit_na(),
            reporting_period = as_factor(year(reporting_period) * 12 + month(reporting_period) - (2020*12+3)),
            pct_group = as_factor(pct_group),
            race_ethnicity = as_factor(race_ethnicity)
        )

    m <- gam(target ~ s(age_at_covid) + gender_concept_name + s(reporting_period, bs = 're') + early_imv + early_ecmo + s(region, bs = 're') + s(data_partner_id, bs = 're') + s(CCI), data = df, family = "binomial", method = "REML")

    print(summary(m))

    print(exp(coef(m)))
    print(exp(confint.default(m)))

    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.af8bbfee-35c8-4038-bd76-afb4d7c4c467"),
    Results_Statistics_For_Month=Input(rid="ri.foundry.main.dataset.5b252774-2a8b-4339-8201-ea7aeb430a18")
)
results_statistics_month <- function(Results_Statistics_For_Month) {
    ## Here we compare the rates of the biggest and smallest month to show that they are different

    Results_Statistics_For_Month <- Results_Statistics_For_Month %>%
        mutate_at(vars(n:non_events), as.integer) %>%
        arrange(desc(rate))

    biggest_month <- head(Results_Statistics_For_Month, 1)
    smallest_month <- tail(Results_Statistics_For_Month, 1)

    tbl <- rbind(biggest_month, smallest_month)
    
    print(tbl)
    print(tbl %>% select(events, non_events) %>% as.matrix %>% chisq.test())

    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.1f0e2e29-d3d0-4c9c-834e-5cdef6ed8c5c"),
    Sensitivity_Analysis=Input(rid="ri.foundry.main.dataset.bcc3fbe8-812b-4b64-adff-c40607846d06")
)
library(lubridate)
library(glue)
library(mgcv)

sensitivity_regression <- function(Sensitivity_Analysis) {
    # Preform the sensitivity analysis by using any of these columns as the outcome and varying the the threshold. 
    OUTCOMES <- c("any_days_to_day_4", "any_days_to_day_5", "any_days_to_day_6", "parenteral_days_to_day_4", "parenteral_days_to_day_5", "parenteral_days_to_day_6")

    for (outcome in OUTCOMES) {
        analysis_df <- Sensitivity_Analysis %>%
            mutate(
                data_partner_id = as_factor(data_partner_id), 
                gender_concept_name = as_factor(gender_concept_name) %>% fct_explicit_na(),
                reporting_period = as_factor(year(reporting_period) * 12 + month(reporting_period) - (2020*12+3)),
                pct_group = as_factor(pct_group),
                race_ethnicity = as_factor(race_ethnicity)
            ) %>% 
            select(data_partner_id:early_ecmo, !!outcome) %>%
            rename(exposure = !!outcome)

        MAX <- as.numeric(str_replace(outcome, ".*_", ""))

        for (threshold in seq(3,MAX)) {
            temp_df <- analysis_df %>%
                mutate(event = exposure >= threshold)

            print(glue("=== {outcome} > {threshold} "))

            m <- gam(event ~ s(age_at_covid) + gender_concept_name + s(reporting_period, bs = 're') + early_imv + early_ecmo + s(data_partner_id, bs = 're') + s(CCI), data = temp_df, family = "binomial", method = "REML")
            print(summary(m))

            temp_df <- temp_df %>%
                filter(pct_group != "not measured") %>%
                mutate(pct_group = as_factor(pct_group))

            m <- gam(event ~ s(age_at_covid) + gender_concept_name + s(reporting_period, bs = 're') + pct_group + early_imv + early_ecmo + s(data_partner_id, bs = 're') + s(CCI), data = temp_df, family = "binomial", method = "REML")
            print(summary(m))
            
        }
    }

    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.d118c5df-804d-471b-9d92-18ebe45f0c5f"),
    Figure_Data_By_Region=Input(rid="ri.foundry.main.dataset.1c8882c6-f528-4c21-b342-680458570149")
)
unnamed <- function(Figure_Data_By_Region) {
    # exploratory figure about regional changes over time, not used in final manuscript
    
    df <- Figure_Data_By_Region %>%
        mutate(
            reporting_period = as.character(reporting_period) %>% str_remove("-..$") %>% as.factor(),
            division = as_factor(division) %>% fct_infreq()
        )
        
    p1 <- ggplot(df, aes(x = reporting_period, y = exposure_average, group = division, color = division)) + 
        geom_line()

    print(p1)

    return(NULL)
}

