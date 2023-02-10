This repository contains the code to support "Trends in early empiric antibiotic use in patients hospitalized with COVID-19: A retrospective cohort study in the National Covid Cohort Collaborative (N3C)" by Widere et. al. currently submitted for publication.

The analysis had two main workbooks:

- The main workbook at the root of this repository
- A supplementary workbook for propensity matching in the propensity_matching subdirectory. The imputation and matching process uses packages not part of the standard N3C environment and this split prevents the main workbook from requiring a custom environment (that can take a long time to start). 

The analysis was executed on the [National Covid Cohort Collaborative (N3C)](https://covid.cd2h.org/) [Enclave](https://covid.cd2h.org/enclave). 

[![DOI](https://zenodo.org/badge/480441352.svg)](https://zenodo.org/badge/latestdoi/480441352)

