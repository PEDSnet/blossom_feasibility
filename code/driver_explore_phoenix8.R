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
################################################################################
################################################################################

#' Take a look at concepts composing the Phoenix-8 Sepsis score components

######################################################
#' Components in pSOFA not shown here; work has been conducted for the TRIP study

######################################################
#' Mechanical ventilation: Previous codeset exists
mv_codes <- load_codeset('px_2021_11_MechanicalVentilation_V1')

mv1 <- cdm_tbl('procedure_occurrence') %>% inner_join(mv_codes %>% select(concept_id), by=c('procedure_concept_id'='concept_id'))
mv2 <- cdm_tbl('condition_occurrence') %>% inner_join(mv_codes %>% select(concept_id), by=c('condition_concept_id'='concept_id'))
mv1 %>% group_by(site) %>% summarise(n=n())

######################################################
#' D-dimer: Previous codeset exists
dd_codes <- load_codeset('ddimer_labs')

dd1 <- cdm_tbl('measurement_labs') %>% inner_join(dd_codes %>% select(concept_id), by=c('measurement_concept_id'='concept_id'))
dd1 %>% group_by(site) %>% summarise(n=n())

######################################################
#' ANC (absolute neutrophil count): Previous codeset exists
anc_codes <- load_codeset('anc_labs')

anc1 <- cdm_tbl('measurement_labs') %>% inner_join(anc_codes %>% select(concept_id), by=c('measurement_concept_id'='concept_id'))
anc1 %>% group_by(site) %>% summarise(n=n())

######################################################
#' ALT (alanine aminotransferase blood test): Previous codeset exists
alt_codes <- load_codeset('labs_alt')

alt1 <- cdm_tbl('measurement_labs') %>% inner_join(alt_codes %>% select(concept_id), by=c('measurement_concept_id'='concept_id'))
alt1 %>% group_by(site) %>% summarise(n=n())


######################################################
#' Generating preliminary review of concepts
######################################################
#' International normalized ratio
inr <- vocabulary_tbl('concept') %>% filter(regexp_like(tolower(concept_name),'international')) %>%
  filter(regexp_like(tolower(concept_name),'normalized')) %>%
  filter(regexp_like(tolower(concept_name),'ratio'))

inr1 <- cdm_tbl('measurement_labs') %>% inner_join(inr %>% distinct(concept_id), by=c('measurement_concept_id'='concept_id'))
#inr2 <- cdm_tbl('measurement_vitals') %>% inner_join(inr %>% distinct(concept_id), by=c('measurement_concept_id'='concept_id'))
#inr3 <- cdm_tbl('procedure_occurrence') %>% inner_join(inr %>% distinct(concept_id), by=c('procedure_concept_id'='concept_id'))
#inr4 <- cdm_tbl('observation') %>% inner_join(inr %>% distinct(concept_id), by=c('observation_concept_id'='concept_id'))
#inr5 <- cdm_tbl('condition_occurrence') %>% inner_join(inr %>% distinct(concept_id), by=c('condition_concept_id'='concept_id'))
inr1 %>% group_by(site) %>% summarise(n=n())

######################################################
#' Pupillary exam
pup <- vocabulary_tbl('concept') %>% filter(regexp_like(tolower(concept_name),'pupillary|pupil|mydriasis')) %>%
  filter(regexp_like(tolower(concept_name),'reactive|fixed'))
  #filter(regexp_like(tolower(concept_name),'exam'))

pup1 <- cdm_tbl('condition_occurrence') %>% inner_join(pup %>% distinct(concept_id), by=c('condition_concept_id'='concept_id'))
#pup2 <- cdm_tbl('observation') %>% inner_join(pup %>% distinct(concept_id), by=c('observation_concept_id'='concept_id'))
pup1 %>% group_by(site) %>% summarise(n=n())

######################################################
#' Serum lactate

sl <- vocabulary_tbl('concept') %>% filter(regexp_like(tolower(concept_name),'serum')) %>%
  filter(regexp_like(tolower(concept_name),'lactate')) %>%
  filter(!regexp_like(tolower(concept_name),'dehydrogenase')) 

sl1 <- cdm_tbl('measurement_labs') %>% inner_join(sl %>% distinct(concept_id), by=c('measurement_concept_id'='concept_id'))
sl1 %>% group_by(site) %>% summarise(n=n())
sl_codes2 <- sl1 %>% distinct(measurement_concept_id) %>% left_join(sl, by=c('measurement_concept_id'='concept_id'))

######################################################
#' Serum fibrinogen

sf <- vocabulary_tbl('concept') %>% filter(regexp_like(tolower(concept_name),'serum')) %>%
  filter(regexp_like(tolower(concept_name),'fibrinogen'))

sf1 <- cdm_tbl('measurement_labs') %>% inner_join(sl %>% distinct(concept_id), by=c('measurement_concept_id'='concept_id'))
sf1 %>% group_by(site) %>% summarise(n=n())
sf_codes2 <- sf1 %>% distinct(measurement_concept_id) %>% left_join(sf, by=c('measurement_concept_id'='concept_id'))

######################################################
#' Blood glucose

bg <- vocabulary_tbl('concept') %>% filter(regexp_like(tolower(concept_name),'blood')) %>%
  filter(regexp_like(tolower(concept_name),'glucose'))

bg1 <- cdm_tbl('measurement_labs') %>% inner_join(bg %>% distinct(concept_id), by=c('measurement_concept_id'='concept_id'))
bg1 %>% group_by(site) %>% summarise(n=n())
bg_codes2 <- bg1 %>% distinct(measurement_concept_id) %>% left_join(bg, by=c('measurement_concept_id'='concept_id'))

######################################################
#' ALC (absolute lymphocyte count)

#' I think these codes are perhaps too expansive
alc <- vocabulary_tbl('concept') %>%
  #filter(regexp_like(tolower(concept_name),'absolute')) %>%
  filter(regexp_like(tolower(concept_name),'lymphocyte')) #%>%
  #filter(regexp_like(tolower(concept_name),'count'))

alc1 <- cdm_tbl('measurement_labs') %>% inner_join(alc %>% distinct(concept_id), by=c('measurement_concept_id'='concept_id'))
alc1 %>% group_by(site) %>% summarise(n=n())
alc_codes2 <- alc1 %>% distinct(measurement_concept_id) %>% left_join(alc, by=c('measurement_concept_id'='concept_id'))

######################################################
#' Add my CSV with Phoenix-8 Sepsis Score component notes

phoenix8_notes <- read_csv('specs/phoenix8_components.csv')
output_tbl(phoenix8_notes, 'phoenix8_notes')














