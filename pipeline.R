library(tidyverse)

@transform_pandas(
    Output(rid="ri.vector.main.execute.4b903e98-04be-4dba-bd2a-f7c1ebf1ee10"),
    exposure_month_summary=Input(rid="ri.foundry.main.dataset.5cebc675-a49d-4cfd-b186-f15061b0a323")
)
library(lubridate)
covariates_regression_on_time <- function(exposure_month_summary) {

    dat <- exposure_month_summary %>%
        mutate(t = year(reporting_period) * 12 + month(reporting_period) - (2020*12+3),
            early = t <= 5,
            t_early = if_else(t <= 5, t, 0),
            t_late =  if_else(t > 5, t, 0))%>%
        mutate_at(vars(early_imv_count:early_vasopressor_use_count), ~ .x / encounter_count)

    model <- glm(early_imv_count ~ early + t_early + t_late, data = dat)
    print(summary(model))

    model <- glm(early_ecmo_count ~  early + t_early + t_late, data = dat)
    print(summary(model))

    model <- glm(early_trauma_dx_count ~  early + t_early + t_late, data = dat)
    print(summary(model))

    model <- glm(early_major_procedure_count ~  early + t_early + t_late, data = dat)
    print(summary(model))

    model <- glm(early_vasopressor_use_count ~  early + t_early + t_late, data = dat)
    print(summary(model))

    return(dat)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.97b2c02d-6cb2-4f5f-9f4a-114c68a79177"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
library(patchwork)

explore_figure_severity <- function(Final_Analysis_Cohort_With_Exclusions) {

    df <- Final_Analysis_Cohort_With_Exclusions %>%
                mutate(pct_group = as_factor(pct_group),
                        early_imv = as_factor(early_imv == 1),
                        early_ecmo = as_factor(early_ecmo == 1),
                        early_vasopressor_use = as_factor(early_vasopressor_use == 1),
                        early_surgery = as_factor(early_major_procedure == 1),
                        early_trauma = as_factor(early_trauma_flag == 1))

    display_df <- df %>%
        group_by(early_imv, reporting_period) %>%
        summarise(count = n(), avg = mean(target))

    p2 <- ggplot(display_df, aes(reporting_period, avg, color=early_imv)) +
            geom_line() + 
            scale_x_datetime(date_labels = "%Y-%m") + 
            scale_y_continuous(labels = scales::label_percent()) +
            scale_color_manual(values = c("TRUE" = "blue", "FALSE" = "red"), labels = c("TRUE" = "Present", "FALSE" = "Absent")) + 
            theme_bw() + 
            theme(legend.position="bottom") + 
            labs(x = "Month", y = "% of Admissions with EEAU", color="IMV")

    display_df <- df %>% group_by(early_ecmo, reporting_period) %>% summarise(count = n(), avg = mean(target))
    p3 <- ggplot(display_df, aes(reporting_period, avg, color=early_ecmo)) +
            geom_line() +
            scale_x_datetime(date_labels = "%Y-%m") + 
            scale_y_continuous(labels = scales::label_percent()) + 
            scale_color_manual(values = c("TRUE" = "blue", "FALSE" = "red"), labels = c("TRUE" = "Present", "FALSE" = "Absent")) + 
            theme_bw() + 
            theme(legend.position="bottom") + 
            labs(x = "Month", y = "% of Admissions with EEAU", color="ECMO")

    display_df <- df %>% group_by(early_surgery, reporting_period) %>% summarise(count = n(), avg = mean(target))
    p4 <- ggplot(display_df, aes(reporting_period, avg, color=early_surgery)) +
            geom_line() + 
            scale_x_datetime(date_labels = "%Y-%m") + 
            scale_y_continuous(labels = scales::label_percent()) + 
            scale_color_manual(values = c("TRUE" = "blue", "FALSE" = "red"), labels = c("TRUE" = "Present", "FALSE" = "Absent")) + 
            theme_bw() + 
            theme(legend.position="bottom") + 
            labs(x = "Month", y = "% of Admissions with EEAU", color="Surgery")

    display_df <- df %>% group_by(early_trauma, reporting_period) %>% summarise(count = n(), avg = mean(target))
    p5 <- ggplot(display_df, aes(reporting_period, avg, color=early_trauma)) +
            geom_line() + 
            scale_x_datetime(date_labels = "%Y-%m") + 
            scale_y_continuous(labels = scales::label_percent()) + 
            scale_color_manual(values = c("TRUE" = "blue", "FALSE" = "red"), labels = c("TRUE" = "Present", "FALSE" = "Absent")) + 
            theme_bw() + 
            theme(legend.position="bottom") + 
            labs(x = "Month", y = "% of Admissions with EEAU", color="Trauma")

    display_df <- df %>% group_by(early_vasopressor_use, reporting_period) %>% summarise(count = n(), avg = mean(target))
    p6 <- ggplot(display_df, aes(reporting_period, avg, color=early_vasopressor_use)) +
            geom_line() + 
            scale_x_datetime(date_labels = "%Y-%m") + 
            scale_y_continuous(labels = scales::label_percent()) + 
            scale_color_manual(values = c("TRUE" = "blue", "FALSE" = "red"), labels = c("TRUE" = "Present", "FALSE" = "Absent")) + 
            theme_bw() + 
            theme(legend.position="bottom") + 
            labs(x = "Month", y = "% of Admissions with EEAU", color="Early Vasopressors")

    display_df <- df %>% group_by(pct_group, reporting_period) %>% summarise(count = n(), avg = mean(target))
    p7 <- ggplot(display_df, aes(reporting_period, avg, color=pct_group)) +
            geom_line() + 
            scale_x_datetime(date_labels = "%Y-%m") + 
            scale_y_continuous(labels = scales::label_percent()) + 
            theme_bw() + 
            theme(legend.position="bottom") + 
            labs(x = "Month", y = "% of Admissions with EEAU", color="PCT")
        
    #image: svg
    fig <- (p2 + p3) / (p4 + p5) / (p6 + p7)

    print(fig)

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
    Output(rid="ri.foundry.main.dataset.53af476d-78a7-44bb-aa7b-3f18c9cad44a"),
    exposure_month_summary=Input(rid="ri.foundry.main.dataset.5cebc675-a49d-4cfd-b186-f15061b0a323")
)
exposure_regression_on_time <- function(exposure_month_summary) {
    dat <- exposure_month_summary %>%
        mutate(t = year(reporting_period) * 12 + month(reporting_period) - (2020*12+3),
               rate = target_count / encounter_count)

    
    model <- glm(rate ~ t, data = dat)
    print(summary(model))

    model <- glm(target_count ~ t + offset(log(encounter_count)), data = dat, family="poisson")
    print(summary(model))

    dat <- exposure_month_summary %>%
        mutate(t = year(reporting_period) * 12 + month(reporting_period) - (2020*12+3),
               rate = target_count / encounter_count) %>%
        filter(t > 5)

    
    model <- glm(rate ~ t, data = dat)
    print(summary(model))

    model <- glm(target_count ~ t + offset(log(encounter_count)), data = dat, family="poisson")
    print(summary(model))

    return(dat)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.7eb84399-e702-4733-a5d4-35ffdc2644b8"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
fig_condition_prevalence <- function(Final_Analysis_Cohort_With_Exclusions) {
    df <- Final_Analysis_Cohort_With_Exclusions %>%
        #mutate(reporting_period = as.character(reporting_period) %>% str_remove("-..$") %>% as.factor()) %>%
        group_by(reporting_period) %>%
        summarise(n = n(), early_ecmo = mean(early_ecmo), early_imv = mean(early_imv), early_trauma = mean(early_trauma_flag), early_surgery = mean(early_major_procedure), late_antibiotic_exposure = mean(late_antibiotic_exposure), long_imv = mean(long_imv))

    print(df)
        
    #scale_x_discrete(guide = guide_axis(n.dodge = 2)) + 

    # p1 <- ggplot(df) +
    #     geom_line(aes(reporting_period, early_ecmo)) +
    #     scale_x_datetime(date_labels = "%Y-%m") + 
    #     scale_y_continuous(labels = scales::label_percent()) + 
    #     theme_bw() + 
    #     labs(x = "Month", y = "% of Admissions with Early ECMO Use")
    
    # p2 <- ggplot(df) +
    #     geom_line(aes(reporting_period, early_imv)) +
    #     scale_x_datetime(date_labels = "%Y-%m") + 
    #     scale_y_continuous(labels = scales::label_percent()) + 
    #     theme_bw() + 
    #     labs(x = "Month", y = "% of Admissions with Early IMV Use")

    # p3 <- ggplot(df) +
    #     geom_line(aes(reporting_period, early_trauma)) + 
    #     scale_x_datetime(date_labels = "%Y-%m") + 
    #     scale_y_continuous(labels = scales::label_percent()) + 
    #     theme_bw() + 
    #     labs(x = "Month", y = "% of Admissions with Trauma")

    # p4 <- ggplot(df) +
    #     geom_line(aes(reporting_period, late_antibiotic_exposure)) +
    #     scale_x_datetime(date_labels = "%Y-%m") + 
    #     scale_y_continuous(labels = scales::label_percent()) + 
    #     theme_bw() + 
    #     labs(x = "Month", y = "% of Admissions with Late Extended Spectrum Anitbiotic Exposure")

    # p5 <- ggplot(df) +
    #     geom_line(aes(reporting_period, long_imv)) + 
    #     scale_x_datetime(date_labels = "%Y-%m") + 
    #     scale_y_continuous(labels = scales::label_percent()) + 
    #     theme_bw() + 
    #     labs(x = "Month", y = "% of Admissions with Prolonged IMV")

    # p6 <- ggplot(df) +
    #     geom_line(aes(reporting_period, early_surgery)) +
    #     scale_x_datetime(date_labels = "%Y-%m") + 
    #     scale_y_continuous(labels = scales::label_percent()) + 
    #     theme_bw() + 
    #     labs(x = "Month", y = "% of Admissions with Early Surgery")

    # print((p1 + p2 + p3) / (p4 + p5 + p6))

    p1 <- ggplot(df) +
        geom_line(aes(reporting_period, early_ecmo), color = "blue") +
        scale_x_datetime(date_labels = "%Y-%m") + 
        scale_y_continuous(labels = scales::label_percent()) + 
        theme_bw() + 
        labs(x = "Month", y = "% of Admissions with Early ECMO Use")

    print(p1)
    
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.a8ee73db-92c2-4dd3-9ee9-8dc8bdd41900"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
library(patchwork)
fig_outcomes_by_time <- function(Final_Analysis_Cohort_With_Exclusions) {
    df <- Final_Analysis_Cohort_With_Exclusions %>%
        group_by(reporting_period) %>%
        summarise(n = n(),
            target = mean(target), 
            late_antibiotic_exposure = mean(late_antibiotic_exposure),
            long_imv = mean(long_imv),
            hospital_mortality = mean(hospital_mortality),
            late_cdad = mean(late_cdad)) %>%
        mutate(
            late_antibiotic_exposure_c = scale(late_antibiotic_exposure/target, center=FALSE, scale=FALSE),
            long_imv_c = scale(long_imv/target, center=FALSE, scale=FALSE),
            hospital_mortality_c = scale(hospital_mortality/target, center=FALSE, scale=FALSE),
            late_cdad_c = scale(late_cdad/target, center=FALSE, scale=FALSE),
        )

    p1 <- ggplot(df) +
        geom_line(aes(reporting_period, late_antibiotic_exposure ), color = "blue") +
        scale_x_datetime(date_labels = "%Y-%m") + 
        scale_y_continuous(labels = scales::label_percent()) + 
        theme_bw() + 
        labs(x = "Month", y = "% with Late Antibiotic Exposure")

    p2 <- ggplot(df) +
        geom_line(aes(reporting_period, long_imv), color = "blue") +
        scale_x_datetime(date_labels = "%Y-%m") + 
        scale_y_continuous(labels = scales::label_percent()) + 
        theme_bw() + 
        labs(x = "Month", y = "% with Prolonged IMV")

    p3 <- ggplot(df) +
        geom_line(aes(reporting_period, hospital_mortality), color = "blue") +
        scale_x_datetime(date_labels = "%Y-%m") + 
        scale_y_continuous(labels = scales::label_percent()) + 
        theme_bw() + 
        labs(x = "Month", y = "% with Hospital Mortality")

    p4 <- ggplot(df) +
        geom_line(aes(reporting_period, late_cdad), color = "blue") +
        scale_x_datetime(date_labels = "%Y-%m") + 
        scale_y_continuous(labels = scales::label_percent()) + 
        theme_bw() + 
        labs(x = "Month", y = "% with Late CDAD")

    p5 <- ggplot(df) +
        geom_line(aes(reporting_period, target), color = "blue") +
        scale_x_datetime(date_labels = "%Y-%m") + 
        scale_y_continuous(labels = scales::label_percent()) + 
        theme_bw() + 
        labs(x = "Month", y = "%  with EEAU")

    left <- p1 + p2 + p3 + p4 + p5

    rp1 <- ggplot(df) +
        geom_line(aes(reporting_period, late_antibiotic_exposure_c), color = "blue") +
        theme_bw() + 
        labs(x = "Month", y = "Ratio of Late Antibiotic to EEAU")

    rp2 <- ggplot(df) +
        geom_line(aes(reporting_period, long_imv_c), color = "blue") +
        theme_bw() + 
        labs(x = "Month", y = "Ratio of Prolonged IMV to EEAU")

    rp3 <- ggplot(df) +
        geom_line(aes(reporting_period, hospital_mortality_c), color = "blue") +
        theme_bw() + 
        labs(x = "Month", y = "Ratio of Hospital Mortality to EEAU")

    rp4 <- ggplot(df) +
        geom_line(aes(reporting_period, late_cdad_c), color = "blue") +
        theme_bw() + 
        labs(x = "Month", y = "Ratio of Late CDAD to EEAU")

    right <- rp1 + rp2 + rp3 + rp4 
    print(left / right)
    
    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.849c7d9f-bdf9-40c1-8ac5-8177c69eb857"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
figure_trends <- function(Final_Analysis_Cohort_With_Exclusions) {
    df <- Final_Analysis_Cohort_With_Exclusions %>%
        group_by(reporting_period) %>%
        summarise(early_ecmo = mean(early_ecmo), early_imv = mean(early_imv), early_trauma = mean(early_trauma_flag), early_surgery = mean(early_major_procedure), early_vasopressor_use = mean(early_vasopressor_use))  %>%
        pivot_longer(!reporting_period, names_to = "metric", values_to = "rate") %>%
        mutate(metric = case_when(
            metric == "early_ecmo" ~ "ECMO",
            metric == "early_imv" ~ "IMV",
            metric == "early_trauma" ~ "Trauma",
            metric == "early_surgery" ~ "Surgery",
            metric == "early_vasopressor_use" ~ "Vasopressor Use", 
            TRUE ~ metric
        ))

    
    p1 <- ggplot(df, aes(x = reporting_period, y = rate, color = metric)) +
        geom_line()  + 
        scale_x_datetime(date_labels = "%Y-%m") + 
        scale_y_continuous(labels = scales::label_percent()) + 
        theme_bw() + 
        labs(x = "Month", y = "% of Admissions", color="Early Condition")

    #image: svg
    print(p1)

    return(NULL)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.49835047-d81c-4200-8d14-44faec86091a"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
library(lubridate)
library(mgcv)
library(broom)

iptw_test_preds <- function(Final_Analysis_Cohort_With_Exclusions) {

    df <- Final_Analysis_Cohort_With_Exclusions %>%
        select(data_partner_id:reporting_period, target, early_imv:late_antibiotic_exposure, early_trauma_flag, early_major_procedure, late_cdad, early_vasopressor_use) %>%
        mutate(
            data_partner_id = as_factor(data_partner_id), 
            gender_concept_name = as_factor(gender_concept_name) %>% fct_explicit_na(),
            reporting_period = as_factor(year(reporting_period) * 12 + month(reporting_period) - (2020*12+3)),
            race_ethnicity = as_factor(race_ethnicity)
        )

    m <- gam(target ~ s(age_at_covid) + gender_concept_name + s(reporting_period, bs = 're') + early_imv + early_ecmo + early_vasopressor_use + early_trauma_flag + early_major_procedure + s(data_partner_id, bs = 're') + s(CCI), data = df, family = "binomial", method = "REML")

    df$pred <- predict(m, newdata=df)

    return(df)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.2268fa48-551e-43d8-972c-a9a7b345d959"),
    Drugs_Labelled_By_Hospital_Day=Input(rid="ri.foundry.main.dataset.f9ae6e03-c975-4f18-8296-7a96c11aa177"),
    Figure_Data_By_Center=Input(rid="ri.foundry.main.dataset.582aaae3-361a-459b-94dd-ba5fe47ee5b9")
)
library(patchwork)

letter_figure_1 <- function(Figure_Data_By_Center, Drugs_Labelled_By_Hospital_Day) {

    ## this builds the figure 1 for the publication
    df <- Figure_Data_By_Center %>%
        mutate(reporting_period = as.character(reporting_period) %>% str_remove("-..$") %>% as.factor())
        
    p1 <- ggplot(df, aes(x = reporting_period, y = exposure_average)) + 
        geom_jitter(aes(size = case_count), alpha = 0.5, width = 0.3) +
        geom_boxplot(color = "red", fill = NA, outlier.shape = NA) +
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
    Output(rid="ri.vector.main.execute.219f1278-5ad7-4d83-810b-0e6b7168e23e"),
    psm=Input(rid="ri.foundry.main.dataset.b25f82cf-ac77-43f6-80ed-ffdcd56b0b15")
)
model_att <- function(psm) {
    m <- glm(late_antibiotic_exposure ~ target, data = psm, family = binomial())
    print(summary(m))
    print(exp(coef(m)))
    print(exp(confint(m)))

    m <- glm(hospital_mortality ~ target, data = psm, family = binomial())
    print(summary(m))
    print(exp(coef(m)))
    print(exp(confint(m)))

    m <- glm(long_imv ~ target, data = psm, family = binomial())
    print(summary(m))
    print(exp(coef(m)))
    print(exp(confint(m)))

    m <- glm(late_cdad ~ target, data = psm, family = binomial())
    print(summary(m))
    print(exp(coef(m)))
    print(exp(confint(m)))

    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.efb4b5a1-fbcd-419b-94d5-01551495eb9d"),
    Figure_Data_By_Center=Input(rid="ri.foundry.main.dataset.582aaae3-361a-459b-94dd-ba5fe47ee5b9"),
    Results_Statistics_For_Month=Input(rid="ri.foundry.main.dataset.5b252774-2a8b-4339-8201-ea7aeb430a18")
)
library(patchwork)

paper_figure_1 <- function(Figure_Data_By_Center, Results_Statistics_For_Month) {        
    p1 <- ggplot(Figure_Data_By_Center, aes(x = reporting_period, y = exposure_average)) + 
        geom_jitter(aes(size = case_count), alpha = 0.5, width = 0.3) +
        geom_line(aes(x= reporting_period, y = rate, color="Average EEUA Rate"),data=Results_Statistics_For_Month, size=1.5) + 
        scale_y_continuous(labels = scales::label_percent(), limits = c(0,1)) + 
        scale_x_datetime(labels = scales::label_date(format="%Y-%m")) + 
        scale_size_binned_area(labels = scales::label_comma()) +
        theme_bw() + 
        labs(x = "Month", y = "Admissions with EEUA", size = "Included Encounters", color = "")

    #image: svg
    svg(filename=graphicsFile, width=11, height=8, bg="white")
    print(p1)

    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.16191937-4402-4324-aa16-5044fa97b6e1"),
    Drugs_Labelled_By_Hospital_Day=Input(rid="ri.foundry.main.dataset.f9ae6e03-c975-4f18-8296-7a96c11aa177")
)
library(patchwork)

paper_figure_2 <- function( Drugs_Labelled_By_Hospital_Day) {
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

    #image: svg
    svg(filename=graphicsFile, width=11, height=8, bg="white")
    print(p2)

    return(NULL)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.b25f82cf-ac77-43f6-80ed-ffdcd56b0b15"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
library(MatchIt)
library(lubridate)

psm <- function(Final_Analysis_Cohort_With_Exclusions) {
    options(width = 320)
    df <- Final_Analysis_Cohort_With_Exclusions %>%
        select(data_partner_id:reporting_period, target, early_imv:late_antibiotic_exposure, early_trauma_flag, early_major_procedure, early_vasopressor_use, late_cdad, primary_outcome) %>%
        mutate(
            data_partner_id = as_factor(data_partner_id), 
            gender_concept_name = as_factor(gender_concept_name) %>% fct_explicit_na(),
            reporting_period = as_factor(year(reporting_period) * 12 + month(reporting_period) - (2020*12+3)),
            race_ethnicity = as_factor(race_ethnicity)
        )

    df <- df %>%
        select(target, reporting_period, early_imv, early_ecmo, early_vasopressor_use, early_trauma_flag, early_major_procedure, race_ethnicity, age_at_covid, hospital_mortality, long_imv, late_cdad, late_antibiotic_exposure, CCI, obesity, smoker, data_partner_id, primary_outcome) %>%
        drop_na()

    m.out <- matchit(target ~ reporting_period + early_ecmo + early_imv + early_trauma_flag + early_vasopressor_use + early_major_procedure + race_ethnicity + s(age_at_covid) + obesity + smoker + s(CCI), data=df, distance='gam')

    print(summary(m.out))

    return(match.data(m.out, data=df))
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
        select(target, total_length_of_stay, bmi, obesity, sdoh2, race_ethnicity, CCI, exposure_days, smoker, pct_group, wbc, early_ecmo, early_imv, early_vasopressor_use, early_trauma_flag, early_major_procedure, hospital_mortality, late_antibiotic_exposure, long_imv, late_cdad ) %>%
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
            early_vasopressor_use = 'Vasopressor On Hospital Day < 2',
            early_trauma_flag = 'Traumatic Diagnosis',
            early_major_procedure = 'Major Surgical Procedure', 
            hospital_mortality = 'In Hospital Mortality',
            late_antibiotic_exposure = 'Late Antibiotic Exposure',
            long_imv = "Prolonged IMV",
            late_cdad = "Late CDAD",
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
        select(target, age_at_covid, bmi,  obesity, sdoh2, race_ethnicity, CCI, exposure_days, smoker, pct_group, early_ecmo, early_imv, early_vasopressor_use, early_major_procedure, early_trauma_flag, data_partner_id, gender_concept_name, reporting_period) %>%
        filter(pct_group != "not measured") %>%
        mutate(
            data_partner_id = as_factor(data_partner_id), 
            gender_concept_name = as_factor(gender_concept_name) %>% fct_explicit_na(),
            reporting_period = as_factor(year(reporting_period) * 12 + month(reporting_period) - (2020*12+3)),
            pct_group = as_factor(pct_group) %>% fct_relevel("low"),
            race_ethnicity = as_factor(race_ethnicity)
        )

    m <- gam(target ~ s(age_at_covid) + gender_concept_name + s(reporting_period, bs = 're') + pct_group + early_imv + early_ecmo + early_vasopressor_use + early_trauma_flag + early_major_procedure + s(data_partner_id, bs = 're') + s(CCI), data = df, family = "binomial", method = "REML")

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
        select(target, age_at_covid, bmi,  obesity, sdoh2, race_ethnicity, CCI, exposure_days, smoker, pct_group, early_ecmo, early_imv, early_vasopressor_use, early_major_procedure, gender_concept_name, reporting_period, early_trauma_flag, data_partner_id) %>%
        mutate(
            data_partner_id = as_factor(data_partner_id), 
            gender_concept_name = as_factor(gender_concept_name) %>% fct_explicit_na(),
            reporting_period = as_factor(year(reporting_period) * 12 + month(reporting_period) - (2020*12+3)),
            pct_group = as_factor(pct_group),
            race_ethnicity = as_factor(race_ethnicity)
        )

    m <- gam(target ~ s(age_at_covid) + gender_concept_name + s(reporting_period, bs = 're') + early_imv + early_ecmo + early_vasopressor_use + early_trauma_flag + early_major_procedure + s(data_partner_id, bs = 're') + s(CCI), data = df, family = "binomial", method = "REML")

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
        select(target, age_at_covid, bmi,  obesity, sdoh2, race_ethnicity, CCI, exposure_days, smoker, pct_group, early_ecmo, early_imv, early_vasopressor_use, data_partner_id, early_major_procedure, early_trauma_flag, gender_concept_name, reporting_period, region, division) %>%
        mutate(
            data_partner_id = as_factor(data_partner_id), 
            region = as_factor(region) %>% fct_infreq(),
            division = as_factor(division) %>% fct_infreq(), 
            gender_concept_name = as_factor(gender_concept_name) %>% fct_explicit_na(),
            reporting_period = as_factor(year(reporting_period) * 12 + month(reporting_period) - (2020*12+3)),
            pct_group = as_factor(pct_group),
            race_ethnicity = as_factor(race_ethnicity)
        )

    m <- gam(target ~ s(age_at_covid) + gender_concept_name + s(reporting_period, bs = 're') + early_imv + early_ecmo + early_vasopressor_use + early_trauma_flag + early_major_procedure + s(region, bs = 're') + s(data_partner_id, bs = 're') + s(CCI), data = df, family = "binomial", method = "REML")

    print(summary(m))

    print(exp(coef(m)))
    print(exp(confint.default(m)))

    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.26c11267-a3d2-40ad-8343-545e2524a10b"),
    Figure_Data_By_Center=Input(rid="ri.foundry.main.dataset.582aaae3-361a-459b-94dd-ba5fe47ee5b9")
)
library(lubridate)
library(mgcv)

regression_on_time <- function(Figure_Data_By_Center) {
    data <- Figure_Data_By_Center %>%
        mutate(month  = year(reporting_period) * 12 + month(reporting_period) - (2020*12+3))

    model <- glm(cbind(exposure_count, case_count - exposure_count) ~ month, data = data, family = binomial())
    print(summary(model))
    return(data)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.35202812-8624-47d6-b7bc-5e8f09d20cda"),
    course_duration_for_final_cohort=Input(rid="ri.foundry.main.dataset.68e6e987-d359-4ca2-a60d-737f9441e8bc")
)
result_course_duration_for_final_cohort <- function(course_duration_for_final_cohort) {

    print(quantile(course_duration_for_final_cohort$duration))

    df <- course_duration_for_final_cohort %>%
        group_by(outcome_iv_day4) %>%
        summarise(
            p25 = quantile(duration, probs=0.25),
            p50 = quantile(duration, probs=0.5),
            p75 = quantile(duration, probs=0.75))
    return(df)
    
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.a606e980-bf65-4f22-a6ee-9edcae462fd9"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
result_length_of_stay_among_non_survivors <- function(Final_Analysis_Cohort_With_Exclusions) {
    df <- Final_Analysis_Cohort_With_Exclusions %>%
        filter(hospital_mortality == 1) %>%
        summarise(n = n(), med_los = median(total_length_of_stay), avg_los = mean(total_length_of_stay), p25 = quantile(total_length_of_stay, probs=0.25), p75 = quantile(total_length_of_stay, probs=0.75))

    return(df)
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

    return(Results_Statistics_For_Month)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.46dbfabf-5ac8-4b98-afdc-37cfac295c8e"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
library(lubridate)
reviewer_exposures_by_covariate_by_time <- function(Final_Analysis_Cohort_With_Exclusions) {

    df <- Final_Analysis_Cohort_With_Exclusions %>%
        select(reporting_period, target, early_imv, early_ecmo, early_trauma_flag, early_major_procedure, early_vasopressor_use, pct_group) %>%
        mutate(pct_high = if_else(pct_group == "high", 1, 0),
                pct_low = if_else(pct_group == "low", 1, 0)) %>%
        mutate_at(vars(early_imv:pct_low), ~ if_else(.x == 1, target, as.integer(NA))) %>%
        group_by(reporting_period) %>%
        summarise_at(vars(early_imv:pct_low), ~ mean(.x, na.rm=TRUE)) %>%
        mutate(t = year(reporting_period) * 12 + month(reporting_period) - (2020*12+3),
                early = t <= 5,
                t_early = if_else(t <= 5, t, 0),
                t_late =  if_else(t > 5, t, 0))

    model <- glm(early_imv ~ early + t_early + t_late, data = df)
    print(summary(model))

    model <- glm(early_ecmo ~ early + t_early + t_late, data = df)
    print(summary(model))

    model <- glm(early_trauma_flag ~ early + t_early + t_late, data = df)
    print(summary(model))

    model <- glm(early_vasopressor_use ~ early + t_early + t_late, data = df)
    print(summary(model))

    model <- glm(early_major_procedure ~ early + t_early + t_late, data = df)
    print(summary(model))

    model <- glm(pct_high ~ early + t_early + t_late, data = df)
    print(summary(model))

    model <- glm(pct_low ~ early + t_early + t_late, data = df)
    print(summary(model))

    
    df <- Final_Analysis_Cohort_With_Exclusions %>%
        select(reporting_period, target, early_imv, early_ecmo, early_trauma_flag, early_major_procedure, early_vasopressor_use, pct_group) %>%
        mutate(pct_high = if_else(pct_group == "high", 1, 0),
                pct_low = if_else(pct_group == "low", 1, 0)) %>%
        mutate_at(vars(early_imv:pct_low), ~ if_else(.x == 1, target, as.integer(NA))) %>%
        group_by(reporting_period) %>%
        summarise_at(vars(early_imv:pct_low), ~ mean(.x, na.rm=TRUE)) %>%
        mutate(t = year(reporting_period) * 12 + month(reporting_period) - (2020*12+3),
                early = t <= 5,
                t_early = if_else(t <= 5, t, 0),
                t_late =  if_else(t > 5, t, 0))
    
    return(df)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.07b39539-5d86-4f7b-b7f7-66d41b5cb4db"),
    Final_Analysis_Cohort_With_Exclusions=Input(rid="ri.foundry.main.dataset.9cad2abe-8986-4105-996b-fd7d15040eb9")
)
reviwer_table_statistics <- function(Final_Analysis_Cohort_With_Exclusions) {
    df <- Final_Analysis_Cohort_With_Exclusions %>%
        group_by(target) %>%
        count(pct_group) %>%
        pivot_wider(names_from=target, values_from=n)
    
    return(df)
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

    #     for (threshold in seq(3,MAX)) {
    #         temp_df <- analysis_df %>%
    #             mutate(event = exposure >= threshold)

    #         print(glue("=== {outcome} > {threshold} "))

    #         m <- gam(event ~ s(age_at_covid) + gender_concept_name + s(reporting_period, bs = 're') + early_imv + early_ecmo + early_vasopressor_use + s(data_partner_id, bs = 're') + s(CCI), data = temp_df, family = "binomial", method = "REML")
    #         print(summary(m))

    #         temp_df <- temp_df %>%
    #             filter(pct_group != "not measured") %>%
    #             mutate(pct_group = as_factor(pct_group))

    #         m <- gam(event ~ s(age_at_covid) + gender_concept_name + s(reporting_period, bs = 're') + pct_group + early_imv + early_ecmo + early_vasopressor_use + s(data_partner_id, bs = 're') + s(CCI), data = temp_df, family = "binomial", method = "REML")
    #         print(summary(m))
            
    #     }
    }

    return(NULL)
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.498b01d7-281b-4375-b82a-c5cbfb3a5e4b"),
    Figure_Data_By_Center=Input(rid="ri.foundry.main.dataset.582aaae3-361a-459b-94dd-ba5fe47ee5b9")
)
library(patchwork)

test_figure <- function(Figure_Data_By_Center) {

    df <- Figure_Data_By_Center %>% mutate(center = as_factor(data_partner_id))
        
    p1 <- ggplot(df, aes(x = reporting_period, y = exposure_average, color = center)) + geom_line()

    #image: svg
    svg(filename=graphicsFile, width=11, height=8, bg="white")
    print(p1)

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

@transform_pandas(
    Output(rid="ri.vector.main.execute.f9a98aed-b076-41bb-83c1-e4520ac3f948"),
    iptw_test_preds=Input(rid="ri.foundry.main.dataset.49835047-d81c-4200-8d14-44faec86091a")
)
unnamed_3 <- function(iptw_test_preds) {
    df <- iptw_test_preds %>%
        mutate(pred = plogis(pred), group = cut_interval(pred, 10)) %>%
        group_by(group) %>%
        summarise(n = n(), r = mean(target))
}

@transform_pandas(
    Output(rid="ri.vector.main.execute.823dd5b2-cf53-41c1-afea-7e6c618cc836"),
    iptw_test_preds=Input(rid="ri.foundry.main.dataset.49835047-d81c-4200-8d14-44faec86091a")
)
unnamed_4 <- function(iptw_test_preds) {
        df <- iptw_test_preds %>%
        mutate(pred = plogis(pred),
            weight = target / pred + (1 - target) / ( 1 - pred))

    df <- df %>% mutate(weight = ifelse(weight > 10, 10, weight))

    m <- glm(long_imv ~ target, data=df, family=binomial(),  weights=weight)

    print(m)
    print(summary(m))

    m <- glm(late_antibiotic_exposure ~ target, data=df, family=binomial(),  weights=weight)

    print(m)
    print(summary(m))

    m <- glm(hospital_mortality ~ target, data=df, family=binomial(),  weights=weight)

    print(m)
    print(summary(m))

    return(NULL)
}

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.e0401515-2b29-4f86-84a4-8c7b9ea637a9"),
    iptw_test_preds=Input(rid="ri.foundry.main.dataset.49835047-d81c-4200-8d14-44faec86091a")
)
weights <- function(iptw_test_preds) {
    df <- iptw_test_preds %>%
        mutate(pred = plogis(pred),
            weight = target / pred + (1 - target) / ( 1 - pred))

    df <- df %>% mutate(weight = ifelse(weight > 10, 10, weight))

    p1 <- ggplot() +
        geom_histogram(aes(x = weight), bins=30, data = df %>% filter(target == 1)) + 
        geom_histogram(aes(x = weight, y = -..count..), bins=30, data = df %>% filter(target == 0))

    print(p1)

    return(df) 
}

