################### SOURCE WHATEVER FILES ######################
library(argos)
library(srcr)
library(stringr)

library(RPostgres)
library(RPresto)
library(assertthat)
library(dplyr)
library(tidyr)
library(purrr)
library(DBI)
library(dbplyr)
library(lubridate)
library(tibble) #' includes the view function 
library(squba)
library(readr)

#source(file.path(getwd(), 'setup', 'setup.R'))
source(file.path(getwd(), 'setup', 'argos_wrapper.R'))
source(file.path(getwd(), 'code', 'funs.R'))
source('/Users/hirabayask/.srcr/set_httr_config.R')

##############################################################

# library(DBI)
# db=config('db_src')
# dbExecute(db, 'create schema blossom_feasibility')

iceberg_session_v59 <- initialize_session(session_name = 'idd_iceberg',
                                          db_conn = srcr('trino_v59_iceberg'),
                                          is_json = FALSE,
                                          cdm_schema = 'pedsnet_dcc_v59', ## replace with location of CDM data
                                          results_schema = 'blossom_feasibility',
                                          retain_intermediates = TRUE,
                                          db_trace = TRUE, ## set to TRUE for SQL code to print to the console (like verbose)
                                          vocabulary_schema = 'v59_vocabulary',
                                          results_tag = '')

#set_argos_default(postgres_session_v58)
set_argos_default(iceberg_session_v59)

################################################################################

#' Take a look at latest visit dates per site
cdm_tbl('visit_occurrence') %>%
  group_by(site) %>%
  summarise(max_visit_date=max(as.Date(visit_start_date))) %>%
  view()

################################################################################
#' Find patients in PEDSnet

all_pat_ct <- cdm_tbl('person') %>% distinct(person_id) %>% count() %>% pull()

################################################################################
#' Find patients with a PICU visit

picu <- cdm_tbl('adt_occurrence') %>%
  filter(service_concept_id==2000000078L) %>%
  left_join(cdm_tbl('visit_occurrence') %>%
              select(visit_occurrence_id, visit_start_date, visit_end_date), by=c('visit_occurrence_id')) %>%
  left_join(cdm_tbl('person') %>% select(person_id, birth_date), by=c('person_id'))

picu_ct <- picu %>% distinct(person_id) %>% count() %>% pull()

################################################################################
#' Find patients with a PICU visit within 2 years prior to the latest visit date in v59
#' (looking at the earliest of the latest visit dates per site by v59, approximately)
#' UPDATE: changed to look at a PICU record at any time within the CDM rather than just 2 years prior

picu_start_date = '2009-01-01'
picu_end_date = '2025-07-31'

picu_adt_2y <- picu %>%
  filter(as.Date(adt_date) >= as.Date(picu_start_date)) %>%
  filter(as.Date(adt_date) <= as.Date(picu_end_date)) %>%
  filter(adt_type_concept_id %in% c(2000000083L, 2000000085L))

picu_adt_2y_pat_ct <- picu_adt_2y %>% distinct(person_id) %>% count() %>% pull()

picu_visit_2y <- picu %>%
  filter(as.Date(visit_start_date) >= as.Date(picu_start_date)) %>%
  filter(as.Date(visit_start_date) <= as.Date(picu_end_date))

picu_visit_2y_pat_ct <- picu_visit_2y %>% distinct(person_id) %>% count() %>% pull()
  
################################################################################
#' Find patients aged >=0 and <18 at time of PICU admission

picu_adt_under18 <- picu_adt_2y %>%
  distinct(person_id, birth_date, adt_date) %>%
  mutate(age = date_diff('day', as.Date(birth_date), as.Date(adt_date))/365.25) %>%
  filter(age >= 0L) %>%
  filter(age < 18L) %>%
  left_join(cdm_tbl('person') %>% select(person_id, gender_concept_id), by=c('person_id')) %>%
  filter(gender_concept_id %in% c(8507L, 8532L))

picu_adt_under18_pat_ct <- picu_adt_under18 %>% distinct(person_id) %>% count() %>% pull()

################################################################################
#' Identify "confirmed sepsis": patients with a sepsis condition code within 7 days of adt_date

sepsis_codes <- load_codeset('dx_sepsis')

confirmed_sepsis_adt <- get_conds(codeset = sepsis_codes,
                                  cohort = picu_adt_under18,
                                  date_type = 'adt_date',
                                  days_pre_max = 7L,
                                  days_post_max = 7L) %>%
  rename(min_picu_date=min_date,
         max_picu_date=max_date,
         n_picu_dates=n_dates)

################################################################################
#' Identify demographics/race/ethnicity breakdown

#' Look at breaking down age groups by approximate quantile
# age_quantile_adt <- results_tbl('confirmed_sepsis_adt') %>% select(age) %>%
#   mutate(age=as.numeric(age)) %>% collect() %>% pull() %>%
#   quantile(probs=c(0.2, 0.4, 0.6, 0.8))

confirmed_sepsis_adt_demog <- get_demog(cohort=confirmed_sepsis_adt,
                                        date_type='max_picu_date')
output_tbl(confirmed_sepsis_adt_demog, 'confirmed_sepsis_adt')

confirmed_sepsis_adt_ct <- results_tbl('confirmed_sepsis_adt') %>% distinct(person_id) %>% count() %>% pull()

confirmed_sepsis_adt_cchmc_colorado_ct <- results_tbl('confirmed_sepsis_adt') %>% filter(site %in% c('cchmc', 'colorado')) %>% distinct(person_id) %>% count() %>% pull()

################################################################################
#' Summarize demographics

demog_summary_adt <- get_demog_summary(cohort=results_tbl('confirmed_sepsis_adt') %>%
                                         filter(sex_cat %in% c('Male','Female')),
                                   vars=c('site',#'raceth_cat',
                                          'race_cat2','eth_cat2','sex_cat','age_group')) %>%
  mutate(variable=case_when(variable=='site' ~ 'Site',
                            #variable=='raceth_cat' ~ 'Race/Ethnicity',
                            variable=='race_cat2' ~ 'Race',
                            variable=='eth_cat2' ~ 'Ethnicity',
                            variable=='sex_cat' ~ 'Sex',
                            variable=='age_group' ~ 'Age Group',
                            variable=='Total' ~ '1_Total',
                            TRUE ~ variable)) %>%
  mutate(n_pct = paste0(n_patients, ' (', percent_of_total, ')'))

output_tbl(demog_summary_adt, 'demog_summary_sepsis_adt')
#db_remove_table(db, dbplyr::in_schema(config('results_schema'), 'demog_summary_sepis_adt'))

#' Look at just Cincinnati and Colorado
demog_summary_adt_cchmc_colorado <- get_demog_summary(cohort=results_tbl('confirmed_sepsis_adt') %>%
                                                        filter(sex_cat %in% c('Male','Female')) %>%
                                                        filter(site %in% c('cchmc','colorado')),
                                       vars=c('site',#'raceth_cat',
                                              'race_cat2','eth_cat2','sex_cat','age_group')) %>%
  mutate(variable=case_when(variable=='site' ~ 'Site',
                            #variable=='raceth_cat' ~ 'Race/Ethnicity',
                            variable=='race_cat2' ~ 'Race',
                            variable=='eth_cat2' ~ 'Ethnicity',
                            variable=='sex_cat' ~ 'Sex',
                            variable=='age_group' ~ 'Age Group',
                            variable=='Total' ~ '1_Total',
                            TRUE ~ variable)) %>%
  mutate(n_pct = paste0(n_patients, ' (', percent_of_total, ')'))

output_tbl(demog_summary_adt_cchmc_colorado, 'demog_summary_sepsis_adt_cchmc_colorado')

################################################################################
#' Use Mitch's codesets for suspected sepsis, but look within 7 days of picu date

px_blood_culture_codes <- results_tbl('px_blood_culture')
lab_blood_culture_codes <- results_tbl('lab_blood_culture')
iv_fluids_codes <- results_tbl('iv_fluids')
med_broad_spec_abx_codes <- results_tbl('med_broad_spec_abx')

suspected_sepsis_adt <- get_suspected_sepsis(cohort = picu_adt_under18,
                                            date_type = 'adt_date',
                                            days_pre_max = 7L,
                                            days_post_max = 7L,
                                            px_blood_culture_codes = results_tbl('px_blood_culture'),
                                            lab_blood_culture_codes = results_tbl('lab_blood_culture'),
                                            iv_fluids_codes = results_tbl('iv_fluids'),
                                            med_broad_spec_abx_codes = results_tbl('med_broad_spec_abx')) %>%
  rename(min_picu_date=min_date,
         max_picu_date=max_date,
         n_picu_dates=n_dates)

################################################################################
#' Identify demographics/race/ethnicity breakdown

suspected_sepsis_adt_demog <- get_demog(cohort=suspected_sepsis_adt,
                                        date_type='max_picu_date')
output_tbl(suspected_sepsis_adt_demog, 'suspected_sepsis_adt')

suspected_sepsis_adt_ct <- results_tbl('suspected_sepsis_adt') %>%
  filter(sex_cat %in% c('Male','Female')) %>%
  distinct(person_id) %>%
  count() %>% pull()

suspected_sepsis_adt_cchmc_colorado_ct <- results_tbl('suspected_sepsis_adt') %>%
  filter(sex_cat %in% c('Male','Female')) %>%
  filter(site %in% c('cchmc', 'colorado')) %>%
  distinct(person_id) %>% count() %>% pull()

################################################################################
#' Summarize demographics

demog_summary_suspected_adt <- get_demog_summary(cohort=results_tbl('suspected_sepsis_adt') %>%
                                                   filter(sex_cat %in% c('Male','Female')),
                                       vars=c('site',#'raceth_cat',
                                              'race_cat2','eth_cat2','sex_cat','age_group')) %>%
  mutate(variable=case_when(variable=='site' ~ 'Site',
                            #variable=='raceth_cat' ~ 'Race/Ethnicity',
                            variable=='race_cat2' ~ 'Race',
                            variable=='eth_cat2' ~ 'Ethnicity',
                            variable=='sex_cat' ~ 'Sex',
                            variable=='age_group' ~ 'Age Group',
                            variable=='Total' ~ '1_Total',
                            TRUE ~ variable)) %>%
  mutate(n_pct = paste0(n_patients, ' (', percent_of_total, ')'))

output_tbl(demog_summary_suspected_adt, 'demog_summary_suspected_sepsis_adt')
#db_remove_table(db, dbplyr::in_schema(config('results_schema'), 'demog_summary_sepis_adt'))

#' Look at just Cincinnati and Colorado
demog_summary_suspected_adt_cchmc_colorado <- get_demog_summary(cohort=results_tbl('suspected_sepsis_adt') %>%
                                                                  filter(sex_cat %in% c('Male','Female')) %>%
                                                                  filter(site %in% c('cchmc','colorado')),
                                                      vars=c('site',#'raceth_cat',
                                                             'race_cat2','eth_cat2', 'sex_cat','age_group')) %>%
  mutate(variable=case_when(variable=='site' ~ 'Site',
                            #variable=='raceth_cat' ~ 'Race/Ethnicity',
                            variable=='race_cat2' ~ 'Race',
                            variable=='eth_cat2' ~ 'Ethnicity',
                            variable=='sex_cat' ~ 'Sex',
                            variable=='age_group' ~ 'Age Group',
                            variable=='Total' ~ '1_Total',
                            TRUE ~ variable)) %>%
  mutate(n_pct = paste0(n_patients, ' (', percent_of_total, ')'))

output_tbl(demog_summary_suspected_adt_cchmc_colorado, 'demog_summary_suspected_sepsis_adt_cchmc_colorado')

################################################################################
#' Specify "confirmed sepsis" as suspected sepsis + sepsis condition code

demog_summary_adt_2level <- get_demog_summary(cohort=results_tbl('confirmed_sepsis_adt') %>%
                                         inner_join(results_tbl('suspected_sepsis_adt') %>% distinct(person_id),
                                                    by=c('person_id')) %>%
                                           filter(sex_cat %in% c('Male','Female')),
                                       vars=c('site',#'raceth_cat',
                                              'race_cat2','eth_cat2','sex_cat','age_group')) %>%
  mutate(variable=case_when(variable=='site' ~ 'Site',
                            #variable=='raceth_cat' ~ 'Race/Ethnicity',
                            variable=='race_cat2' ~ 'Race',
                            variable=='eth_cat2' ~ 'Ethnicity',
                            variable=='sex_cat' ~ 'Sex',
                            variable=='age_group' ~ 'Age Group',
                            variable=='Total' ~ '1_Total',
                            TRUE ~ variable)) %>%
  mutate(n_pct = paste0(n_patients, ' (', percent_of_total, ')'))

output_tbl(demog_summary_adt_2level, 'demog_summary_sepsis_adt_2level')

#' Look at just Cincinnati and Colorado
demog_summary_adt_cchmc_colorado_2level <- get_demog_summary(cohort=results_tbl('confirmed_sepsis_adt') %>%
                                                        inner_join(results_tbl('suspected_sepsis_adt') %>% distinct(person_id),
                                                                   by=c('person_id')) %>%
                                                          filter(sex_cat %in% c('Male','Female')) %>%
                                                        filter(site %in% c('cchmc','colorado')),
                                                      vars=c('site',#'raceth_cat',
                                                             'race_cat2','eth_cat2','sex_cat','age_group')) %>%
  mutate(variable=case_when(variable=='site' ~ 'Site',
                            #variable=='raceth_cat' ~ 'Race/Ethnicity',
                            variable=='race_cat2' ~ 'Race',
                            variable=='eth_cat2' ~ 'Ethnicity',
                            variable=='sex_cat' ~ 'Sex',
                            variable=='age_group' ~ 'Age Group',
                            variable=='Total' ~ '1_Total',
                            TRUE ~ variable)) %>%
  mutate(n_pct = paste0(n_patients, ' (', percent_of_total, ')'))

output_tbl(demog_summary_adt_cchmc_colorado_2level, 'demog_summary_sepsis_adt_cchmc_colorado_2level')


confirmed_sepsis_adt_2level <- results_tbl('confirmed_sepsis_adt') %>%
  inner_join(results_tbl('suspected_sepsis_adt') %>% distinct(person_id), by=c('person_id')) %>%
  filter(sex_cat %in% c('Male','Female')) %>% distinct(person_id)

################################################################################
#' Get counts of race and ethnicity by male/female: suspected sepsis

suspected_sepsis_raceth_by_sex <- get_raceth_by_sex(cohort = results_tbl('suspected_sepsis_adt'))

output_tbl(suspected_sepsis_raceth_by_sex, 'suspected_sepsis_adt_raceth_sex')

suspected_sepsis_raceth_by_sex_cchmc_colorado <- get_raceth_by_sex(cohort = results_tbl('suspected_sepsis_adt') %>%
                                                                     filter(site %in% c('cchmc','colorado')))

output_tbl(suspected_sepsis_raceth_by_sex_cchmc_colorado, 'suspected_sepsis_adt_raceth_sex_cchmc_colorado')

################################################################################
#' Get counts of race and ethnicity by male/female: confirmed sepsis

confirmed_sepsis_raceth_by_sex <- get_raceth_by_sex(cohort = results_tbl('confirmed_sepsis_adt') %>%
                                                      inner_join(results_tbl('suspected_sepsis_adt') %>% distinct(person_id),
                                                                 by=c('person_id')))

output_tbl(confirmed_sepsis_raceth_by_sex, 'confirmed_sepsis_adt_raceth_sex')

confirmed_sepsis_raceth_by_sex_cchmc_colorado <- get_raceth_by_sex(cohort = results_tbl('confirmed_sepsis_adt') %>%
                                                                     inner_join(results_tbl('suspected_sepsis_adt') %>% distinct(person_id),
                                                                                by=c('person_id')) %>%
                                                                     filter(site %in% c('cchmc','colorado')))

output_tbl(confirmed_sepsis_raceth_by_sex_cchmc_colorado, 'confirmed_sepsis_adt_raceth_sex_cchmc_colorado')

#' Gather together race/ethnicity and male/female tables: suspected and confirmed sepsis
sus_raceth_sex <- results_tbl('suspected_sepsis_adt_raceth_sex') %>% mutate(sepsis_type='suspected') %>% mutate(site='All')
sus_raceth_sex_cchmc_colorado <- results_tbl('suspected_sepsis_adt_raceth_sex_cchmc_colorado') %>% mutate(sepsis_type='suspected') %>% mutate(site='CCHMC and Colorado')
con_raceth_sex <- results_tbl('confirmed_sepsis_adt_raceth_sex')  %>% mutate(sepsis_type='confirmed') %>% mutate(site='All')
con_raceth_sex_cchmc_colorado <- results_tbl('confirmed_sepsis_adt_raceth_sex_cchmc_colorado') %>% mutate(sepsis_type='confirmed') %>% mutate(site='CCHMC and Colorado')

all_raceth_sex <- sus_raceth_sex %>%
  dplyr::union(sus_raceth_sex_cchmc_colorado) %>%
  dplyr::union(con_raceth_sex) %>%
  dplyr::union(con_raceth_sex_cchmc_colorado) %>%
  arrange(sepsis_type, site, race_cat) %>% as_data_frame()

#write_csv(all_raceth_sex, 'specs/all_raceth_sex.csv')

################################################################################
#' Make attrition table

confirmed_sepsis_adt_ct <- confirmed_sepsis_adt_2level %>% count() %>% pull()
confirmed_sepsis_adt_cchmc_colorado_ct <- confirmed_sepsis_adt_2level %>% add_site() %>%
  filter(site %in% c('cchmc', 'colorado')) %>% count() %>% pull()

attrition_tbl2 <- get_attrition_tbl2(all_pat_ct=all_pat_ct,
                                     picu_ct=picu_ct,
                                     picu_adt_2y_pat_ct=picu_adt_2y_pat_ct,
                                     picu_adt_under18_pat_ct=picu_adt_under18_pat_ct,
                                     suspected_sepsis_adt_ct=suspected_sepsis_adt_ct,
                                     confirmed_sepsis_adt_ct=confirmed_sepsis_adt_ct,
                                     suspected_sepsis_adt_cchmc_colorado_ct=suspected_sepsis_adt_cchmc_colorado_ct,
                                     confirmed_sepsis_adt_cchmc_colorado_ct=confirmed_sepsis_adt_cchmc_colorado_ct
)

output_tbl(attrition_tbl2, 'attrition2')


