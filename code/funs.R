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
        gender_concept_id==8507L ~ 'Male',
        TRUE ~ 'Other/unknown/ambiguous'
        #TRUE ~ 'Male or other/unknown/ambiguous'
        )) %>%
    mutate(
      raceth_cat=case_when(ethnicity_concept_id == 38003563L ~ 'Hispanic',
                           ethnicity_concept_id == 38003564L & race_concept_id %in% c(8527L, 38003615L) ~ 'NH_White_or_Middle_Eastern_North_African',
                           ethnicity_concept_id == 38003564L & race_concept_id == 8516L ~ 'NH_Black/AA',
                           ethnicity_concept_id == 38003564L & race_concept_id == 8515L ~ 'NH_Asian',
                           ethnicity_concept_id == 38003564L & race_concept_id %in% c(44814659L, 8657L, 8557L) ~ 'NH_Multiple_or_AI_AN_NH_OPI', #NH American Indian/Alaska Native or Native Hawaiian/Other Pacific Islander
                           TRUE ~ 'Other/Unknown')) %>%
    mutate(eth_cat=case_when(ethnicity_concept_id == 38003563L ~ 'Hispanic',
                             ethnicity_concept_id == 38003564L ~ 'Not_Hispanic',
                             ethnicity_concept_id %in% c(44814660L, 44814649L, 44814650L,
                                                         44814653L) ~ 'Refuse_Other_Unknown',
                             TRUE ~ 'Other/Unknown'
    )) %>%
    mutate(race_cat = case_when(race_concept_id %in% c(8527L, 38003615L) ~ 'White_or_Middle_Eastern_North_African',
                                race_concept_id == 8516L ~ 'Black',
                                race_concept_id == 8515L ~ 'Asian',
                                race_concept_id == 8657L ~ 'American_Indian_Alaska_Native',
                                race_concept_id == 8557L ~ 'Native_Hawaiian_Other_PI',
                                #race_concept_id == 38003615L ~ 'Middle_Eastern_North_African',
                                race_concept_id == 44814659L ~ 'Multiple_Race',
                                race_concept_id %in% c(44814650L, 44814653L,
                                                       44814649L, 44814660) ~ 'Unknown_Other_Refuse'
    )) %>%
    mutate(eth_cat2=case_when(ethnicity_concept_id == 38003563L ~ 'Hispanic',
                             #ethnicity_concept_id == 38003564L ~ 'Not_Hispanic',
                             #ethnicity_concept_id %in% c(44814660L, 44814649L, 44814650L,
                            #                             44814653L) ~ 'Refuse_Other_Unknown',
                             TRUE ~ 'Not_Hispanic/Other/Unknown'
    )) %>%
    mutate(race_cat2 = case_when(race_concept_id %in% c(8527L, 38003615L) ~ 'White_or_Middle_Eastern_North_African',
                                race_concept_id == 8516L ~ 'Black_or_African_American',
                                race_concept_id == 8515L ~ 'Asian',
                                race_concept_id %in% c(8657L, 8557L, 44814659L) ~ 'Multiple_or_AI_AN_or_NH_OPI',
                                race_concept_id %in% c(44814650L, 44814653L,
                                                       44814649L, 44814660) ~ 'Unknown_Other_Refuse'
    )) %>%
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
                              vars=c('site','race_cat2','eth_cat2',#'raceth_cat',
                                     'sex_cat','age_group')) {
  
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


#' #' Make attrition table
#' get_attrition_tbl <- function(all_pat_ct=all_pat_ct,
#'                           picu_ct=picu_ct,
#'                           picu_adt_2y_pat_ct=picu_adt_2y_pat_ct,
#'                           picu_visit_2y_pat_ct=picu_visit_2y_pat_ct,
#'                           picu_adt_under18_pat_ct=picu_adt_under18_pat_ct,
#'                           picu_visit_under18_pat_ct=picu_visit_under18_pat_ct,
#'                           confirmed_sepsis_adt_ct=confirmed_sepsis_adt_ct,
#'                           confirmed_sepsis_visit_ct=confirmed_sepsis_visit_ct,
#'                           suspected_sepsis_adt_ct=suspected_sepsis_adt_ct,
#'                           suspected_sepsis_visit_ct=suspected_sepsis_visit_ct,
#'                           confirmed_sepsis_adt_cchmc_colorado_ct=confirmed_sepsis_adt_cchmc_colorado_ct,
#'                           confirmed_sepsis_visit_cchmc_colorado_ct=confirmed_sepsis_visit_cchmc_colorado_ct,
#'                           suspected_sepsis_adt_cchmc_colorado_ct=suspected_sepsis_adt_cchmc_colorado_ct,
#'                           suspected_sepsis_visit_cchmc_colorado_ct=suspected_sepsis_visit_cchmc_colorado_ct,
#'                           ER_linked_to_inpatient_ct=ER_linked_to_inpatient_ct,
#'                           suspected_sepsis_ed_ip_ct=suspected_sepsis_ed_ip_ct,
#'                           suspected_sepsis_ed_ip_cchmc_colorado_ct=suspected_sepsis_ed_ip_cchmc_colorado_ct
#'                           
#' ) {
#'   
#'   
#'   counts <- c(all_pat_ct, picu_ct,
#'               picu_adt_2y_pat_ct, picu_visit_2y_pat_ct,
#'               picu_adt_under18_pat_ct, picu_visit_under18_pat_ct,
#'               confirmed_sepsis_adt_ct, confirmed_sepsis_visit_ct,
#'               suspected_sepsis_adt_ct, suspected_sepsis_visit_ct,
#'               confirmed_sepsis_adt_cchmc_colorado_ct, confirmed_sepsis_visit_cchmc_colorado_ct,
#'               suspected_sepsis_adt_cchmc_colorado_ct, suspected_sepsis_visit_cchmc_colorado_ct,
#'               ER_linked_to_inpatient_ct, suspected_sepsis_ed_ip_ct,
#'               suspected_sepsis_ed_ip_cchmc_colorado_ct
#'               )
#'   
#'   descriptions <- c('1. All Patients in the PEDSnet CDM: v59', '2A. From 1: All patients with a PICU record',
#'                     '3A. From 2A: Patients with a PICU record from 01/01/2009 - 7/31/2025 (by adt_date and filtering for admissions or transfers in)',
#'                     '3B. From 2A: Patients with a PICU record from 01/01/2009 - 7/31/2025 (by visit_start_date)',
#'                     '4A. From 3A: Patients aged >=0 and <18 as of their adt_date',
#'                     '4B. From 3B: Patients aged >=0 and <18 as of their visit_start_date',
#'                     '5A. From 4A: Patients with a sepsis condition code within 7 days before or after their adt_date',
#'                     '5B: From 4B: Patients with a sepsis condition code within 7 days before or after their visit_start_date',
#'                     '5C: From 4A: Patients with suspected sepsis (blood culture lab/measurement AND broad spectrum antibiotics AND IV fluids within 7 days before or after their adt_date)',
#'                     '5D: From 4B: Patients with suspected sepsis (blood culture lab/measurement AND broad spectrum antibiotics AND IV fluids within 7 days before or after their visit_start_date)',
#'                     '6A. From 5A: Patients subset to sites: CCHMC and Colorado',
#'                     '6B: From 5B: Patients subset to sites: CCHMC and Colorado',
#'                     '6C: From 5C: Patients subset to sites: CCHMC and Colorado',
#'                     '6D: From 5D: Patients subset to sites: CCHMC and Colorado',
#'                     '2X: From 1: Patients with an ED visit linked to an inpatient visit',
#'                     '3X: From 2X: Patients aged >=0 and <18 as of their sepsis visit_start_date, with suspected sepsis (blood culture lab/measurement AND broad spectrum antibiotics AND IV fluids in the same visit)',
#'                     '4X: From 3X: Patients subset to sites: CCHMC and Colorado'
#'   )
#'   
#'   attrition_tbl <- cbind(descriptions, counts) %>% as_tibble()
#'   
#'   return(attrition_tbl)
#' }


#' Make attrition table
get_attrition_tbl2 <- function(all_pat_ct=all_pat_ct,
                              picu_ct=picu_ct,
                              picu_adt_2y_pat_ct=picu_adt_2y_pat_ct,
                              picu_adt_under18_pat_ct=picu_adt_under18_pat_ct,
                              suspected_sepsis_adt_ct=suspected_sepsis_adt_ct,
                              confirmed_sepsis_adt_ct=confirmed_sepsis_adt_ct,
                              suspected_sepsis_adt_cchmc_colorado_ct=suspected_sepsis_adt_cchmc_colorado_ct,
                              confirmed_sepsis_adt_cchmc_colorado_ct=confirmed_sepsis_adt_cchmc_colorado_ct
) {
  
  
  counts <- c(all_pat_ct,
              picu_ct,
              picu_adt_2y_pat_ct,
              picu_adt_under18_pat_ct,
              suspected_sepsis_adt_ct,
              confirmed_sepsis_adt_ct,
              suspected_sepsis_adt_cchmc_colorado_ct,
              confirmed_sepsis_adt_cchmc_colorado_ct
              
  )
  
  descriptions <- c('1. All Patients in the PEDSnet CDM: v59',
                    '2. From 1: All patients with a PICU record',
                    '3. From 2: Patients with a PICU record from 01/01/2009 - 7/31/2025',
                    '4. From 3: Patients aged >=0 and <18 with male or female sex',
                    '5: From 4: Patients with suspected sepsis (blood culture lab/measurement AND broad spectrum antibiotics AND IV fluids within 7 days)',
                    '6. From 5: Patients with confirmed sepsis (suspected sepsis + a sepsis condition code within 7 days)',
                    '5B: From 5: Patients with suspected sepsis -- subset to CCHMC and Colorado',
                    '6B. From 6: Patients with confirmed sepsis -- subset to CCHMC and Colorado'
                    
  )
  
  attrition_tbl <- cbind(descriptions, counts) %>% as_tibble()
  
  return(attrition_tbl)
}


#' 
#' #' Find ICU entry events
#' #'
#' #' Given a date range and information about what constitutes an ICU admission of
#' #' interest, retrieve the ADT records corresponding to those events.
#' #'
#' #' @param start The earliest Date to considerx
#' #' @param end The latest Date to consider
#' #' @param adt The tibble containing ADT records of interest
#' #' @param icus A vector of concept IDs for ICU services of interest
#' #' @param events A vector of concept IDs for the ADT type concepts of interest
#' #'
#' #' @return A tibble of included events
#' #' @md
#' get_icu_admits <- function(start = as.Date('2019-07-01'),
#'                            end = as.Date('2020-07-01'),
#'                            adt = cdm_tbl('adt_occurrence'),
#'                            icus = c(2000000078L),
#'                            events = c(2000000083L, 2000000085L)) {
#'   adt %>%
#'     filter(adt_type_concept_id %in% local(events) &
#'              between(adt_date, {{start}}, {{end}}) &
#'              service_concept_id %in% local(icus))
#' }
#' 
#' #' Build a set of episodes from ADT events
#' #'
#' #' Given a set of ADT event records, construct a set of episodes,
#' #' or time intervals bounded by `Transfer in` and `Transfer out` events.
#' #' If multiple `Transfer in` events occur in a row, the earliest is used.
#' #' If multiple `Transfer out` events occur in a row, the latest is used.
#' #' Both ends of an episode must have the same `visit_occurrence_id`. Each
#' #' episode is assigned as the episode ID the `adt_occurence_id` of the
#' #' starting event.
#' #'
#' #' Normally, an episode is constructed only when paired in/out events are
#' #' found.  However, you may optionally supply a set of `visit_occurrence`
#' #' records, in which case the `visit_start_datetime` is treated as a potential
#' #' `Transfer in` event (i.e. if the first ADT event is a `Transfer out`, an
#' #' episode from admission to the transfer event is constructed) and vice versa.
#' #' Where an episode starts at admission, the episode ID is -1.
#' #'
#' #' @param adt_occurrence The tibble of ADT events
#' #' @param visit_occurrence The tibble of visits.  Defaults to NA.
#' #' @param start_types A vector of values for `adt_type_concept_id` that can
#' #'   start an episode.  Defaults to c(2000000083 [Admission], 2000000085
#' #'   [Transfer in]).
#' #' @param end_types A vector of values for `adt_type_concept_id` that can end an
#' #'   episode.  Defaults to c(2000000084 [Discharge], 2000000086 [Transfer out]).
#' #' @param max_gap A duration object specifying the distance between the end of
#' #'   one ADT-defined episode and the beginning of the next below which the two
#' #'   should be merged into a single episode.
#' #'
#' #' @return A local tibble of episodes
#' #' @md
#' build_episodes <- function(adt_occurrence,
#'                            visit_occurrence = NA,
#'                            start_types = c(2000000083L, 2000000085L),
#'                            end_types = c(2000000084L, 2000000086L),
#'                            ignore_types=c(2000000087L),
#'                            max_gap = dhours(12)) {
#'   # Throw an error if there are more than 2 start/stop events at a given
#'   # datetime as we have not handled that case
#'   max <- adt_occurrence %>%
#'     mutate(
#'       type = case_when(
#'         adt_type_concept_id %in% start_types ~ "start",
#'         adt_type_concept_id %in% end_types ~ "end",
#'         .default = "other"
#'       )
#'     ) %>%
#'     count(person_id,
#'           adt_datetime,
#'           start_or_end = type %in% c("start", "end")) %>%
#'     summarize(max = max(n)) %>%
#'     pull()
#'   
#'   assertthat::assert_that(max <= 2, msg = paste("There was more than one",
#'                                                 "start/stop event at a given datetime; this function does not currently",
#'                                                 "handle that case"))
#'   
#'   if (any(!is.na(visit_occurrence)))
#'     visit_occurrence <- collect(visit_occurrence)
#'   collect(adt_occurrence) %>%
#'     filter(!adt_type_concept_id%in%ignore_types)%>%
#'     group_by(person_id, visit_occurrence_id, site) %>%
#'     group_modify(
#'       function (adts, v_id) {
#'         # Create a mock episode to simplify unions later
#'         ep <- tibble(episode_id = 0L,
#'                      episode_start_date = Sys.Date(),
#'                      episode_start_datetime = Sys.time(),
#'                      episode_end_date = Sys.Date(),
#'                      episode_end_datetime = Sys.time())
#'         adts <- adts %>% select(adt_occurrence_id, adt_datetime, adt_date,
#'                                 adt_type_concept_id) %>%
#'           # Remove pairs of start/stops that happened at the same datetime
#'           # It is impossible to definitively order them and they do not change
#'           # the state (ongoing or not) of the episode
#'           mutate(type = case_when(adt_type_concept_id %in% start_types ~ "start",
#'                                   adt_type_concept_id %in% end_types ~ "end",
#'                                   .default = "other")) %>%
#'           group_by(adt_datetime,
#'                    # We do not want to exclude census events
#'                    start_or_end = type %in% c("start", "end")) %>%
#'           mutate(both = ("start" %in% type & "end" %in% type)) %>%
#'           filter(!both) %>%
#'           ungroup() %>%
#'           select(-c(type, start_or_end, both)) %>%
#'           arrange(adt_datetime)
#'         
#'         # See whether we can find a visit start and end as bookends to orphaned
#'         # ADT events
#'         if (any(!is.na(visit_occurrence))) {
#'           v <- filter(visit_occurrence,
#'                       visit_occurrence_id == local(v_id$visit_occurrence_id))
#'           if (nrow(v) &&
#'               ! any(adts[[1, 'adt_type_concept_id']] == start_types))
#'             adts <- add_row(adts,adt_datetime = v$visit_start_datetime,
#'                             adt_date = v$visit_start_date,
#'                             adt_type_concept_id = start_types[1],
#'                             adt_occurrence_id = -1)
#'           if (nrow(v) &&
#'               ! any(adts[[nrow(adts), 'adt_type_concept_id']] == end_types))
#'             adts <- add_row(adts, adt_datetime = v$visit_end_datetime,
#'                             adt_date = v$visit_end_date,
#'                             adt_type_concept_id = end_types[1],
#'                             adt_occurrence_id = -2)
#'           adts <- arrange(adts, adt_datetime)
#'         }
#'         
#'         
#'         # As much as while() loops are discouraged in R, this is much clearer
#'         # than trying to play with vectorized operations
#'         i <- 1
#'         in_episode <- FALSE
#'         while( i <= nrow(adts)) {
#'           if (! in_episode &&
#'               any(adts[[i,'adt_type_concept_id']] == start_types)) {
#'             in_episode <- TRUE
#'             ep_start_dt <- adts[[i, 'adt_datetime']]
#'             ep_start_d  <- adts[[i, 'adt_date']]
#'             ep_id <- adts[[i, 'adt_occurrence_id']]
#'             while(i < nrow(adts) &&
#'                   any(adts[[i, 'adt_type_concept_id']] == start_types)) i <- i + 1
#'           }
#'           else {
#'             if (in_episode & any(adts[[i, 'adt_type_concept_id']] == end_types)) {
#'               # If you prefer to close the episode at the first transfer out event,
#'               # move this runoff to after the episode construction
#'               while(i < nrow(adts) &&
#'                     any(adts[[i + 1, 'adt_type_concept_id']] == end_types)) i <- i + 1
#'               # Don't construct an episode solely from the visit bookends
#'               if (nrow(adts) == 2 &&
#'                   ep_id == -1 && adts[[i, 'adt_occurrence_id']] == -2) {
#'                 i <- i + 1
#'                 next
#'               }
#'               ep <- add_row(ep, episode_id = ep_id,
#'                             episode_start_date = ep_start_d,
#'                             episode_start_datetime = ep_start_dt,
#'                             episode_end_date = adts[[i, 'adt_date']],
#'                             episode_end_datetime = adts[[i, 'adt_datetime']])
#'               in_episode <- FALSE
#'             }
#'             i <- i + 1
#'           }
#'         }
#'         
#'         # Now, merge adjacent episodes
#'         # Remember to get rid of the mock episode
#'         ep <- filter(ep, episode_id != 0) %>%
#'           arrange(episode_start_datetime, episode_end_datetime)
#'         merged <- slice(ep,1)
#'         i <- 2
#'         j <- 1
#'         while (i <= nrow(ep)) {
#'           if (ep[[i, 'episode_start_datetime']] -
#'               merged[[j, 'episode_end_datetime']] < max_gap) {
#'             merged[[j, 'episode_end_datetime']] <- ep[[i, 'episode_end_datetime']]
#'             merged[[j, 'episode_end_date']] <- ep[[i, 'episode_end_date']]
#'           }
#'           else {
#'             merged <- add_row(merged, nth(ep,i))
#'             j <- j + 1
#'           }
#'           i <- i + 1
#'         }
#'         ungroup(merged) %>% distinct()
#'       })%>%
#'     ungroup()
#' }


get_raceth_by_sex <- function(cohort = results_tbl('confirmed_sepsis_adt') %>%
                                filter(site %in% c('cchmc','colorado'))) {
  
  raceth_by_sex <- cohort %>%
    filter(gender_concept_id %in% c(8532L, 8507L)) %>% #'remove patients with ambiguous/other/unknown sex
    mutate(sex_cat=case_when(gender_concept_id==8532 ~ 'Female',
                             gender_concept_id==8507 ~ 'Male')) %>%
    mutate(eth_cat=case_when(ethnicity_concept_id == 38003563L ~ 'Hispanic',
                             #ethnicity_concept_id == 38003564L ~ 'Not_Hispanic',
                             #ethnicity_concept_id %in% c(44814660L, 44814649L, 44814650L,
                             #                             44814653L) ~ 'Refuse_Other_Unknown',
                             # TRUE ~ 'Other/Unknown'
                             TRUE ~ 'Not_Hispanic_or_Refuse_Other_Unknown'
    )) %>%
    group_by(sex_cat, eth_cat, race_cat) %>%
    summarise(n=n()) %>%
    ungroup() %>%
    collect() %>%
    pivot_wider(names_from=c('eth_cat','sex_cat'), values_from='n', values_fill=0)
  
}




