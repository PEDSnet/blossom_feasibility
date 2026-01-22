#' Get cohort of patients who have the condition of interest (sepsis) within
#' a certain number of days before/after the original visit date (adt_date or visit_start_date)
#' and calculate min, max and number of condition dates (min_picu_date, max_picu_date, n_picu_dates)
get_conds <- function(codeset,
                      cohort,
                      date_type,
                      days_pre_max,
                      days_post_max,
                      cond_tbl = cdm_tbl('condition_occurrence')) {
  
  conds <- cond_tbl %>%
    inner_join(codeset %>% distinct(concept_id), by=c('condition_concept_id'='concept_id')) %>%
    inner_join(cohort, by=c('person_id')) %>%
    filter(date_diff('day', as.Date(rlang::sym(date_type)), as.Date(condition_start_date)) <= days_post_max) %>%
    filter(date_diff('day', as.Date(condition_start_date), as.Date(rlang::sym(date_type))) <= days_pre_max)
  
  conds_src <- cond_tbl %>%
    anti_join(conds, by=c('condition_occurrence_id')) %>%
    inner_join(codeset %>% distinct(concept_id), by=c('condition_source_concept_id'='concept_id')) %>%
    inner_join(cohort, by=c('person_id')) %>%
    filter(date_diff('day', as.Date(rlang::sym(date_type)), as.Date(condition_start_date)) <= days_post_max) %>%
    filter(date_diff('day', as.Date(condition_start_date), as.Date(rlang::sym(date_type))) <= days_pre_max)
  
  conds_all <- conds %>%
    distinct(person_id, rlang::sym(date_type)) %>%
    full_join(conds_src %>% distinct(person_id, rlang::sym(date_type)), by=c('person_id',date_type)) %>%
    group_by(person_id) %>%
    summarise(min_date=min(as.Date(rlang::sym(date_type))),
              max_date=max(as.Date(rlang::sym(date_type))),
              n_dates=n()) %>%
    ungroup()
  
  return(conds_all)
  
}


#' Get cohort of patients who have indication of suspected sepsis within
#' a certain number of days before/after the original visit date (adt_date or visit_start_date)
#' and calculate min, max and number of condition dates (min_picu_date, max_picu_date, n_picu_dates)
get_suspected_sepsis <- function(cohort,
                                 date_type,
                                 days_pre_max,
                                 days_post_max,
                                 px_blood_culture_codes = results_tbl('px_blood_culture'),
                                 lab_blood_culture_codes = results_tbl('lab_blood_culture'),
                                 iv_fluids_codes = results_tbl('iv_fluids'),
                                 med_broad_spec_abx_codes = results_tbl('med_broad_spec_abx'),
                                 proc_tbl = cdm_tbl('procedure_occurrence'),
                                 meas_tbl = cdm_tbl('measurement_labs'),
                                 drug_tbl = cdm_tbl('drug_exposure')
) {
  
  px_blood_culture_tbl <- proc_tbl %>%
    inner_join(px_blood_culture_codes %>% distinct(concept_id), by=c('procedure_concept_id'='concept_id')) %>%
    inner_join(cohort, by=c('person_id')) %>%
    filter(date_diff('day', as.Date(rlang::sym(date_type)), as.Date(procedure_date)) <= days_post_max) %>%
    filter(date_diff('day', as.Date(procedure_date), as.Date(rlang::sym(date_type))) <= days_pre_max) %>%
    distinct(person_id, rlang::sym(date_type))
  
  lab_blood_culture_tbl <- meas_tbl %>%
    inner_join(lab_blood_culture_codes %>% distinct(concept_id), by=c('measurement_concept_id'='concept_id')) %>%
    inner_join(cohort, by=c('person_id')) %>%
    filter(date_diff('day', as.Date(rlang::sym(date_type)), as.Date(measurement_date)) <= days_post_max) %>%
    filter(date_diff('day', as.Date(measurement_date), as.Date(rlang::sym(date_type))) <= days_pre_max) %>%
    distinct(person_id, rlang::sym(date_type))
  
  iv_fluids_tbl <- drug_tbl %>%
    inner_join(iv_fluids_codes %>% distinct(concept_id), by=c('drug_concept_id'='concept_id')) %>%
    inner_join(cohort, by=c('person_id')) %>%
    filter(date_diff('day', as.Date(rlang::sym(date_type)), as.Date(drug_exposure_start_date)) <= days_post_max) %>%
    filter(date_diff('day', as.Date(drug_exposure_start_date), as.Date(rlang::sym(date_type))) <= days_pre_max) %>%
    distinct(person_id, rlang::sym(date_type))
  
  med_broad_spec_abx_tbl <- drug_tbl %>%
    inner_join(med_broad_spec_abx_codes %>% distinct(concept_id), by=c('drug_concept_id'='concept_id')) %>%
    inner_join(cohort, by=c('person_id')) %>%
    filter(date_diff('day', as.Date(rlang::sym(date_type)), as.Date(drug_exposure_start_date)) <= days_post_max) %>%
    filter(date_diff('day', as.Date(drug_exposure_start_date), as.Date(rlang::sym(date_type))) <= days_pre_max) %>%
    distinct(person_id, rlang::sym(date_type))
  
  suspected_sepsis_all <- px_blood_culture_tbl %>%
    full_join(lab_blood_culture_tbl, by=c('person_id', date_type)) %>%
    inner_join(iv_fluids_tbl, by=c('person_id', date_type)) %>%
    inner_join(med_broad_spec_abx_tbl, by=c('person_id', date_type)) %>%
    group_by(person_id) %>%
    summarise(min_date=min(as.Date(rlang::sym(date_type))),
              max_date=max(as.Date(rlang::sym(date_type))),
              n_dates=n()) %>%
    ungroup()
  
  return(suspected_sepsis_all)
  
}


#' Add demographics for patients
#' Check race/ethnicity categories
get_demog <- function(cohort,
                      date_type,
                      person_tbl = cdm_tbl('person')) {
  
  demog <- cohort %>%
    left_join(person_tbl %>%
                select(person_id, site, birth_date, gender_concept_id,
                       race_concept_id, ethnicity_concept_id), by=c('person_id')) %>%
    mutate(age = date_diff('day', as.Date(birth_date), as.Date(rlang::sym(date_type)))/365.25) %>%
    mutate(
      sex_cat=case_when(#gender_concept_id==8507L ~ 'Male',
        gender_concept_id==8532L ~ 'Female',
        #gender_concept_id==8507L ~ 'Male',
        #TRUE ~ 'Other/unknown/ambiguous'
        TRUE ~ 'Male or other/unknown/ambiguous'
        )) %>%
    mutate(
      raceth_cat=case_when(ethnicity_concept_id == 38003563L ~ 'Hispanic',
                           ethnicity_concept_id == 38003564L & race_concept_id == 8527L ~ 'NH_White',
                           ethnicity_concept_id == 38003564L & race_concept_id == 8516L ~ 'NH_Black/AA',
                           ethnicity_concept_id == 38003564L & race_concept_id == 8515L ~ 'NH_Asian',
                           ethnicity_concept_id == 38003564L & race_concept_id == 44814659L ~ 'NH_Other_or_Multiple_Race', #NH multiple race
                           ethnicity_concept_id == 38003564L & race_concept_id %in% c(8657L, 8557L, 38003615L) ~ 'NH_Other_or_Multiple_Race', #NH other
                           TRUE ~ 'Other/Unknown')) %>%
    mutate(
      age_group = case_when(age < 1 ~ '<1',
                            age < 5 ~ '01 to 04',
                            age < 10 ~ '05 to 09',
                            age < 14 ~ '10 to 13',
                            age < 18 ~ '14 to 17',
                            TRUE ~ 'Old or Unknown')) 
}

#' Summarize demographics
get_demog_summary <- function(cohort=results_tbl('confirmed_sepsis_adt'),
                              vars=c('site','raceth_cat','sex_cat','age_group')) {
  
  demog_list <- list()
  
  total_pats <- cohort %>% distinct(person_id) %>% count() %>% pull()
  
  demog_list[[1]] <- cohort %>% distinct(person_id) %>% summarise(n=n()) %>%
    mutate(category='Total') %>%
    mutate(variable='Total') %>%
    mutate(percent_of_total=round(100*n/total_pats,2)) %>%
    collect()
  
  for(a in 1:length(vars)) {
    current_var <- vars[[a]]
    
    demog_list[[a+1]] <- cohort %>% group_by(rlang::sym(current_var)) %>% summarise(n=n()) %>%
      rename(category=rlang::sym(current_var)) %>%
      mutate(variable=current_var) %>%
      mutate(percent_of_total=round(100*n/total_pats,2)) %>%
      collect()
    
  }
  
  demog_list_all <- reduce(.x=demog_list,
                           .f=dplyr::union) %>%
    select(variable, category, n, percent_of_total) %>%
    rename(n_patients=n)
  
}


#' Make attrition table
get_attrition_tbl <- function(all_pat_ct=all_pat_ct,
                          picu_ct=picu_ct,
                          picu_adt_2y_pat_ct=picu_adt_2y_pat_ct,
                          picu_visit_2y_pat_ct=picu_visit_2y_pat_ct,
                          picu_adt_under18_pat_ct=picu_adt_under18_pat_ct,
                          picu_visit_under18_pat_ct=picu_visit_under18_pat_ct,
                          confirmed_sepsis_adt_ct=confirmed_sepsis_adt_ct,
                          confirmed_sepsis_visit_ct=confirmed_sepsis_visit_ct,
                          suspected_sepsis_adt_ct=suspected_sepsis_adt_ct,
                          suspected_sepsis_visit_ct=suspected_sepsis_visit_ct,
                          confirmed_sepsis_adt_cchmc_colorado_ct=confirmed_sepsis_adt_cchmc_colorado_ct,
                          confirmed_sepsis_visit_cchmc_colorado_ct=confirmed_sepsis_visit_cchmc_colorado_ct,
                          suspected_sepsis_adt_cchmc_colorado_ct=suspected_sepsis_adt_cchmc_colorado_ct,
                          suspected_sepsis_visit_cchmc_colorado_ct=suspected_sepsis_visit_cchmc_colorado_ct,
                          ER_linked_to_inpatient_ct=ER_linked_to_inpatient_ct,
                          suspected_sepsis_ed_ip_ct=suspected_sepsis_ed_ip_ct,
                          suspected_sepsis_ed_ip_cchmc_colorado_ct=suspected_sepsis_ed_ip_cchmc_colorado_ct
                          
) {
  
  
  counts <- c(all_pat_ct, picu_ct,
              picu_adt_2y_pat_ct, picu_visit_2y_pat_ct,
              picu_adt_under18_pat_ct, picu_visit_under18_pat_ct,
              confirmed_sepsis_adt_ct, confirmed_sepsis_visit_ct,
              suspected_sepsis_adt_ct, suspected_sepsis_visit_ct,
              confirmed_sepsis_adt_cchmc_colorado_ct, confirmed_sepsis_visit_cchmc_colorado_ct,
              suspected_sepsis_adt_cchmc_colorado_ct, suspected_sepsis_visit_cchmc_colorado_ct,
              ER_linked_to_inpatient_ct, suspected_sepsis_ed_ip_ct,
              suspected_sepsis_ed_ip_cchmc_colorado_ct
              )
  
  descriptions <- c('1. All Patients in the PEDSnet CDM: v59', '2A. From 1: All patients with a PICU record',
                    '3A. From 2A: Patients with a PICU record from 01/01/2009 - 7/31/2025 (by adt_date and filtering for admissions or transfers in)',
                    '3B. From 2A: Patients with a PICU record from 01/01/2009 - 7/31/2025 (by visit_start_date)',
                    '4A. From 3A: Patients aged >=0 and <18 as of their adt_date',
                    '4B. From 3B: Patients aged >=0 and <18 as of their visit_start_date',
                    '5A. From 4A: Patients with a sepsis condition code within 7 days before or after their adt_date',
                    '5B: From 4B: Patients with a sepsis condition code within 7 days before or after their visit_start_date',
                    '5C: From 4A: Patients with suspected sepsis (blood culture lab/measurement AND broad spectrum antibiotics AND IV fluids within 7 days before or after their adt_date)',
                    '5D: From 4B: Patients with suspected sepsis (blood culture lab/measurement AND broad spectrum antibiotics AND IV fluids within 7 days before or after their visit_start_date)',
                    '6A. From 5A: Patients subset to sites: CCHMC and Colorado',
                    '6B: From 5B: Patients subset to sites: CCHMC and Colorado',
                    '6C: From 5C: Patients subset to sites: CCHMC and Colorado',
                    '6D: From 5D: Patients subset to sites: CCHMC and Colorado',
                    '2X: From 1: Patients with an ED visit linked to an inpatient visit',
                    '3X: From 2X: Patients aged >=0 and <18 as of their sepsis visit_start_date, with suspected sepsis (blood culture lab/measurement AND broad spectrum antibiotics AND IV fluids in the same visit)',
                    '4X: From 3X: Patients subset to sites: CCHMC and Colorado'
  )
  
  attrition_tbl <- cbind(descriptions, counts) %>% as_tibble()
  
  return(attrition_tbl)
}


