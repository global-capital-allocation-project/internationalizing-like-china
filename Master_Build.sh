#!/bin/bash

############################################################################################################################
# SETUP
############################################################################################################################

# run profile
source master_shell_profile.sh

# Defined by the user
start_year=2014
end_year=2020

# Email notification setting
mailtype="ALL"

# job settings
nodes=1

# first argument is job name
job=${1}

# second (if exists) argument is job name within morningstar
job_ms=${2}

# set user variables
U=${whoami}
echo "System: "${system}
echo "User: "${U}

# sanity-checking that gcap_data variables exist
if [[ -z "$gcap_data" ]]; then
    echo "Empty gcap_data variable: Exiting..."
    exit 1
fi

# sanity-checking that gcap_data variables exist
if [[ -z "$system_part" ]]; then
    echo "Empty partitions variable: Exiting..."
    exit 1
fi

############################################################################################################################
# JOB PARAMETERS
############################################################################################################################

# Default Parameters
partition_default="$system_part"
time_default="0-4:00:00"
ntasks_default=1
mem_default="150G"

# Part 1: Composition of Foreign Ownership of China-Issued RMB Bonds

partition_aggregate_data_series="$system_part"
time_aggregate_data_series="0-5:00:00"
ntasks_aggregate_data_series=1
mem_aggregate_data_series="50G"

partition_reserves_estimate="$system_part"
time_reserves_estimate="0-5:00:00"
ntasks_reserves_estimate=1
mem_reserves_estimate="50G"

partition_private_estimate="$system_part"
time_private_estimate="0-5:00:00"
ntasks_private_estimate=1
mem_private_estimate="100G"

partition_foreign_holdings_figures="$system_part"
time_foreign_holdings_figures="0-5:00:00"
ntasks_foreign_holdings_figures=1
mem_foreign_holdings_figures="50G"

# Part 2: Entry into Domestic Markets

partition_entry_read_tables="$system_part"
time_entry_read_tables="0-5:00:00"
ntasks_entry_read_tables=1
mem_entry_read_tables="50G"

partition_entry_clean_and_combine="$system_part"
time_entry_clean_and_combine="0-5:00:00"
ntasks_entry_clean_and_combine=1
mem_entry_clean_and_combine="50G"

partition_entry_main_analysis="$system_part"
time_entry_main_analysis="0-5:00:00"
ntasks_entry_main_analysis=1
mem_entry_main_analysis="100G"

partition_entry_figures="$system_part"
time_entry_figures="0-5:00:00"
ntasks_entry_figures=1
mem_entry_figures="50G"

# Part 3: Holdings Similarity

partition_correlations="maggiori"
time_correlations="0-5:00:00"
ntasks_correlations=1
mem_correlations="100G"
array_correlations="2014-2020"
array_correlations_alt="1-7"

partition_figures="maggiori"
time_figures="0-5:00:00"
ntasks_figures=1
mem_figures="50G"

############################################################################################################################
# EXECUTING JOBS: 0_data_builds
############################################################################################################################

# CGS Build

case ${job} in
"cgs_build" | "all")

    # unzip raw files
    CGS_unzip_ID=`sbatch \
       --partition=${partition_default} --time=${time_default} \
        --nodes=${nodes} --ntasks=${ntasks_default} --job-name=CGS_unzip \
        --output="${gcap_data}/erroroutput/CGS_unzip-%A_%a.out" --error="${gcap_data}/erroroutput/CGS_unzip-%A_%a.err" \
        --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_default} \
       "${rmb_replication}/0_data_builds/cgs/CGS_Unzip.sh" ${gcap_data} | awk '{print $NF}'` 
    echo "Submitted CGS_unzip Job: "${CGS_unzip_ID}
    sleep 1
    depend="--depend=afterok:${CGS_unzip_ID}"

    # runs cgs.do: cleaning cgs identifiers used in morningstar build
    CGS_ID=`sbatch \
        --partition=${partition_default} --time=${time_default} \
        --nodes=${nodes} --ntasks=${ntasks_default} --job-name=CGS \
		--output="${gcap_data}/erroroutput/CGS-%A_%a.out" --error="${gcap_data}/erroroutput/CGS-%A_%a.err" \
        --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_default} ${depend} \
        "${rmb_replication}/0_data_builds/cgs/CGS_Build_Controller.sh" CGS ${gcap_data} | awk '{print $NF}'` 
    echo "Submitted CGS Job: "${CGS_ID}
    rm -f "${rmb_replication}"/*.log
    
    
# Morningstar Build

;;&
"morningstar_preBBG" | "all")

# set up dependency if necessary 
if [ "$job" = "morningstar_preBBG" ] ; then
    depend=""
else
    depend="--depend=afterok:${CGS_ID}"
fi
    
    # Morningstar Pre Bloomberg Processing
    Morningstar_Build_PreBloomberg_ID=`sbatch \
       --partition=${partition_default} --time=${time_default} \
        --nodes=${nodes} --ntasks=${ntasks_default} --job-name=Morningstar_Build_PreBloomberg \
        --output="${gcap_data}/erroroutput/Morningstar_Build_PreBloomberg-%A_%a.out" --error="${gcap_data}/erroroutput/Morningstar_Build_PreBloomberg-%A_%a.err" \
        --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_default} \
       "${rmb_replication}/0_data_builds/morningstar/Morningstar_Build_PreBloomberg.sh" ${U} ${job_ms} | awk '{print $NF}'` 
    echo "Submitted Morningstar_Build_PreBloomberg Job: "${Morningstar_Build_PreBloomberg_ID}
    sleep 1
    depend="--depend=afterok:${Morningstar_Build_PreBloomberg_ID}"
    
;;&
"morningstar_postBBG" | "all")

# set up dependency if necessary 
if [ "$job" = "morningstar_postBBG" ] ; then
    depend=""
else
    depend="--depend=afterok:${Morningstar_Build_PreBloomberg_ID}"
fi

    # Morningstar Post Bloomberg Processing
    Morningstar_Build_PostBloomberg_ID=`sbatch \
       --partition=${partition_default} --time=${time_default} \
        --nodes=${nodes} --ntasks=${ntasks_default} --job-name=Morningstar_Build_PostBloomberg \
        --output="${gcap_data}/erroroutput/Morningstar_Build_PostBloomberg-%A_%a.out" --error="${gcap_data}/erroroutput/Morningstar_Build_PostBloomberg-%A_%a.err" \
        --mail-user=${mailuser} --mail-type=${mailtype} --requeue --mem=${mem_default} \
       "${rmb_replication}/0_data_builds/morningstar/Morningstar_Build_PostBloomberg.sh" ${U} ${job_ms} | awk '{print $NF}'` 
    echo "Submitted Morningstar_Build_PostBloomberg Job: "${Morningstar_Build_PostBloomberg_ID}
    sleep 1
    depend="--depend=afterok:${Morningstar_Build_PostBloomberg_ID}"
    

############################################################################################################################
# EXECUTING JOBS: 1_foreign_holdings
############################################################################################################################

;;&
"foreign_holdings" | "all")

# set up dependency if necessary 
if [ "$job" = "foreign_holdings" ] ; then
    depend=""
else
    depend="--depend=afterok:${JOB_ms_files_ID}"
fi

    # translating SHCH files 
    code_file=translate_shch
    JOB_translate_shch_ID=`sbatch \
        --partition=${partition_default} --time=${time_default} \
        --mem=${mem_default} --nodes=${nodes} --ntasks=${ntasks_default} \
        --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
        --output="${gcap_data}/erroroutput/${code_file}_%A_%a.out" --error="${gcap_data}/erroroutput/${code_file}_%A_%a.err" \
        --job-name=${code_file} \
        $depend \
        "${rmb_replication}/1_foreign_holdings/foreign_holdings_controller.sh" ${code_file} ${gcap_data} | awk '{print $NF}'`
    echo "Submitted ${code_file} Job: "${JOB_translate_shch_ID}
    
    # combining aggregate data sources for foreign holdings
    code_file=aggregate_data_series 
    JOB_aggregate_data_series_ID=`sbatch \
        --partition=${partition_aggregate_data_series} --time=${time_aggregate_data_series} \
        --mem=${mem_aggregate_data_series} --nodes=${nodes} --ntasks=${ntasks_aggregate_data_series} \
        --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
        --output="${gcap_data}/erroroutput/${code_file}_%A_%a.out" --error="${gcap_data}/erroroutput/${code_file}_%A_%a.err" \
        --job-name=${code_file} \
        --depend=afterok:${JOB_translate_shch_ID} \
        "${rmb_replication}/1_foreign_holdings/foreign_holdings_controller.sh" ${code_file} ${gcap_data} | awk '{print $NF}'`
    echo "Submitted ${code_file} Job: "${JOB_aggregate_data_series_ID}
    
    # estimates for reserve holdings
    code_file=reserves_estimate
    JOB_reserves_estimate_ID=`sbatch \
        --partition=${partition_reserves_estimate} --time=${time_reserves_estimate} \
        --mem=${mem_reserves_estimate} --nodes=${nodes} --ntasks=${ntasks_reserves_estimate} \
        --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
        --output="${gcap_data}/erroroutput/${code_file}_%A_%a.out" --error="${gcap_data}/erroroutput/${code_file}_%A_%a.err" \
        --job-name=${code_file} \
        --depend=afterok:${JOB_aggregate_data_series_ID} \
        "${rmb_replication}/1_foreign_holdings/foreign_holdings_controller.sh" ${code_file} ${gcap_data} | awk '{print $NF}'`
    echo "Submitted ${code_file} Job: "${JOB_reserves_estimate_ID}
    
    # estimates for private holdings
    code_file=private_estimate
    JOB_private_estimate_ID=`sbatch \
        --partition=${partition_private_estimate} --time=${time_private_estimate} \
        --mem=${mem_private_estimate} --nodes=${nodes} --ntasks=${ntasks_private_estimate} \
        --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
        --output="${gcap_data}/erroroutput/${code_file}_%A_%a.out" --error="${gcap_data}/erroroutput/${code_file}_%A_%a.err" \
        --job-name=${code_file} \
        --depend=afterok:${JOB_reserves_estimate_ID} \
        "${rmb_replication}/1_foreign_holdings/foreign_holdings_controller.sh" ${code_file} ${gcap_data} | awk '{print $NF}'`
    echo "Submitted ${code_file} Job: "${JOB_private_estimate_ID}
    
    # estimating offshore holdings 
    code_file=offshore_estimate
    JOB_offshore_estimate_ID=`sbatch \
        --partition=${partition_default} --time=${time_default} \
        --mem=${mem_default} --nodes=${nodes} --ntasks=${ntasks_default} \
        --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
        --output="${gcap_data}/erroroutput/${code_file}_%A_%a.out" --error="${gcap_data}/erroroutput/${code_file}_%A_%a.err" \
        --job-name=${code_file} \
        $depend \
        "${rmb_replication}/1_foreign_holdings/foreign_holdings_controller.sh" ${code_file} ${gcap_data} | awk '{print $NF}'`
    echo "Submitted ${code_file} Job: "${JOB_offshore_estimate_ID}
    
    # combining for related figures: Paper Figure 1, Appendix Figures A.I to A.V
    code_file=foreign_holdings_figures
    JOB_foreign_holdings_figures_ID=`sbatch \
        --partition=${partition_foreign_holdings_figures} --time=${time_foreign_holdings_figures} \
        --mem=${mem_foreign_holdings_figures} --nodes=${nodes} --ntasks=${ntasks_foreign_holdings_figures} \
        --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
        --output="${gcap_data}/erroroutput/${code_file}_%A_%a.out" --error="${gcap_data}/erroroutput/${code_file}_%A_%a.err" \
        --job-name=${code_file} \
        --depend=afterok:${JOB_private_estimate_ID}:${JOB_offshore_estimate_ID} \
        "${rmb_replication}/1_foreign_holdings/foreign_holdings_controller.sh" ${code_file} ${gcap_data} | awk '{print $NF}'`
    echo "Submitted ${code_file} Job: "${JOB_foreign_holdings_figures_ID}

############################################################################################################################
# EXECUTING JOBS: 2_investor_entry
############################################################################################################################

;;&
"investor_entry" | "all")

#no dependencies required
    
    # cleaning and combining tables
    code_file=entry_clean_and_combine
    JOB_entry_clean_and_combine_ID=`sbatch \
        --partition=${partition_entry_clean_and_combine} --time=${time_entry_clean_and_combine} \
        --mem=${mem_entry_clean_and_combine} --nodes=${nodes} --ntasks=${ntasks_entry_clean_and_combine} --requeue \
        --output="${gcap_data}/erroroutput/${code_file}_%A_%a.out" --error="${gcap_data}/erroroutput/${code_file}_%A_%a.err" \
        --job-name=${code_file} \
        "${rmb_replication}/2_investor_entry/entry_controller.sh" ${code_file} ${gcap_data} | awk '{print $NF}'`
    echo "Submitted ${code_file} Job: "${JOB_entry_clean_and_combine_ID}
    sleep 1
    
    # investor entry analysis: part 1
    code_file=entry_main_analysis_part_1
    JOB_entry_main_analysis_part_1_ID=`sbatch \
        --partition=${partition_entry_main_analysis} --time=${time_entry_main_analysis} \
        --mem=${mem_entry_main_analysis} --nodes=${nodes} --ntasks=${ntasks_entry_main_analysis} --requeue \
        --output="${gcap_data}/erroroutput/${code_file}_%A_%a.out" --error="${gcap_data}/erroroutput/${code_file}_%A_%a.err" \
        --job-name=${code_file} \
        --depend=afterok:${JOB_entry_clean_and_combine_ID} \
        "${rmb_replication}/2_investor_entry/entry_controller.sh" ${code_file} ${gcap_data} | awk '{print $NF}'`
    echo "Submitted ${code_file} Job: "${JOB_entry_main_analysis_part_1_ID}
    sleep 1
    
    # investor entry analysis: part 2
    code_file=entry_main_analysis_part_2
    JOB_entry_main_analysis_part_2_ID=`sbatch \
        --partition=${partition_entry_main_analysis} --time=${time_entry_main_analysis} \
        --mem=${mem_entry_main_analysis} --nodes=${nodes} --ntasks=${ntasks_entry_main_analysis} --requeue \
        --output="${gcap_data}/erroroutput/${code_file}_%A_%a.out" --error="${gcap_data}/erroroutput/${code_file}_%A_%a.err" \
        --job-name=${code_file} \
        --depend=afterok:${JOB_entry_main_analysis_part_1_ID} \
        "${rmb_replication}/2_investor_entry/entry_controller.sh" ${code_file} ${gcap_data} | awk '{print $NF}'`
    echo "Submitted ${code_file} Job: "${JOB_entry_main_analysis_part_2_ID}
    sleep 1
    
    # combining for related figures: Paper Figure 2, Appendix Figures VI
    code_file=entry_figures
    JOB_entry_figures_ID=`sbatch \
        --partition=${partition_entry_figures} --time=${time_entry_figures} \
        --mem=${mem_entry_figures} --nodes=${nodes} --ntasks=${ntasks_entry_figures} --requeue \
        --output="${gcap_data}/erroroutput/${code_file}_%A_%a.out" --error="${gcap_data}/erroroutput/${code_file}_%A_%a.err" \
        --job-name=${code_file} \
        --depend=afterok:${JOB_entry_main_analysis_part_2_ID} \
        "${rmb_replication}/2_investor_entry/entry_controller.sh" ${code_file} ${gcap_data} | awk '{print $NF}'`
    echo "Submitted ${code_file} Job: "${JOB_entry_figures_ID}
    sleep 1
    

############################################################################################################################
# EXECUTING JOBS: 3_holdings_similarity and miscellaneous analyses
############################################################################################################################

;;&
"holdings_similarity" | "all")

# set up dependency if necessary 
if [ "$job" = "holdings_similarity" ] ; then
    depend=""
else
    depend="--depend=afterok:${JOB_ms_files_ID}"
fi

    # computing portfolio holding correlations (main text) - run 2020 first to keep a constant country list
    code_file=correlations
    JOB_correlations_2020_ID=`sbatch \
        --partition=${partition_correlations} --time=${time_correlations} \
        --mem=${mem_correlations} --nodes=${nodes} --ntasks=${ntasks_correlations} \
        --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
        --output="${gcap_data}/erroroutput/${code_file}_%A_%a.out" --error="${gcap_data}/erroroutput/${code_file}_%A_%a.err" \
        --job-name=${code_file} \
        --array=2020 \
        $depend \
        "${rmb_replication}/3_holdings_similarity/holdings_controller.sh" ${code_file} ${gcap_data} | awk '{print $NF}'`
    echo "Submitted ${code_file} Job: "${JOB_correlations_2020_ID}

    # computing portfolio holding correlations (main text) - run for other years, based on the 2020 country list
    code_file=correlations
    JOB_correlations_ID=`sbatch \
        --partition=${partition_correlations} --time=${time_correlations} \
        --mem=${mem_correlations} --nodes=${nodes} --ntasks=${ntasks_correlations} \
        --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
        --output="${gcap_data}/erroroutput/${code_file}_%A_%a.out" --error="${gcap_data}/erroroutput/${code_file}_%A_%a.err" \
        --job-name=${code_file} \
        --array=2014-2019 \
        --depend=afterok:${JOB_correlations_2020_ID} \
        "${rmb_replication}/3_holdings_similarity/holdings_controller.sh" ${code_file} ${gcap_data} | awk '{print $NF}'`
    echo "Submitted ${code_file} Job: "${JOB_correlations_ID}
    
    # computing portfolio holding correlations for alternative specifications (appendix)
    code_file=correlations_alt
    JOB_correlations_alt_ID=`sbatch \
        --partition=${partition_correlations} --time=${time_correlations} \
        --mem=${mem_correlations} --nodes=${nodes} --ntasks=${ntasks_correlations} \
        --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
        --output="${gcap_data}/erroroutput/${code_file}_%A_%a.out" --error="${gcap_data}/erroroutput/${code_file}_%A_%a.err" \
        --job-name=${code_file} \
        --array=${array_correlations_alt} \
        --depend=afterok:${JOB_correlations_ID} \
        "${rmb_replication}/3_holdings_similarity/holdings_controller.sh" ${code_file} ${gcap_data} | awk '{print $NF}'`
    echo "Submitted ${code_file} Job: "${JOB_correlations_alt_ID}
  
    # computing correlations for other assets, by nationality (appendix)
    code_file=correlations_other_assets
    JOB_correlations_other_assets_ID=`sbatch \
        --partition=${partition_correlations} --time=${time_correlations} \
        --mem=${mem_correlations} --nodes=${nodes} --ntasks=${ntasks_correlations} \
        --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
        --output="${gcap_data}/erroroutput/${code_file}_%A_%a.out" --error="${gcap_data}/erroroutput/${code_file}_%A_%a.err" \
        --job-name=${code_file} \
        --depend=afterok:${JOB_correlations_ID} \
        "${rmb_replication}/3_holdings_similarity/holdings_controller.sh" ${code_file} ${gcap_data} | awk '{print $NF}'`
    echo "Submitted ${code_file} Job: "${JOB_correlations_other_assets_ID}

    # fund holdings summary statistics (appendix)
    code_file=table_summary 
    JOB_table_summary_ID=`sbatch \
        --partition=${partition_default} --time=${time_default} \
        --mem=${mem_default} --nodes=${nodes} --ntasks=${ntasks_default} \
        --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
        --output="${gcap_data}/erroroutput/${code_file}_%A_%a.out" --error="${gcap_data}/erroroutput/${code_file}_%A_%a.err" \
        --job-name=${code_file} \
        --depend=afterok:${JOB_correlations_ID} \
        "${rmb_replication}/3_holdings_similarity/holdings_controller.sh" ${code_file} ${gcap_data} | awk '{print $NF}'`
    echo "Submitted ${code_file} Job: "${JOB_table_summary_ID}
    
    # gravity regressions (appendix)
    code_file=gravity 
    JOB_gravity_ID=`sbatch \
        --partition=${partition_default} --time=${time_default} \
        --mem=${mem_default} --nodes=${nodes} --ntasks=${ntasks_default} \
        --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
        --output="${gcap_data}/erroroutput/${code_file}_%A_%a.out" --error="${gcap_data}/erroroutput/${code_file}_%A_%a.err" \
        --job-name=${code_file} \
        --depend=afterok:${JOB_correlations_ID} \
        "${rmb_replication}/3_holdings_similarity/holdings_controller.sh" ${code_file} ${gcap_data} | awk '{print $NF}'`
    echo "Submitted ${code_file} Job: "${JOB_gravity_ID}
    
    # computing bond betas (appendix)
    code_file=bond_betas 
    JOB_bond_betas_ID=`sbatch \
        --partition=${partition_default} --time=${time_default} \
        --mem=${mem_default} --nodes=${nodes} --ntasks=${ntasks_default} \
        --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
        --output="${gcap_data}/erroroutput/${code_file}_%A_%a.out" --error="${gcap_data}/erroroutput/${code_file}_%A_%a.err" \
        --job-name=${code_file} \
        $depend \
        "${rmb_replication}/3_holdings_similarity/holdings_controller.sh" ${code_file} ${gcap_data} | awk '{print $NF}'`
    echo "Submitted ${code_file} Job: "${JOB_bond_betas_ID}
    
    # figures and tables (all figures for main text and appendix in this section)
    code_file=figures
    JOB_figures_ID=`sbatch \
        --partition=${partition_default} --time=${time_default} \
        --mem=${mem_default} --nodes=${nodes} --ntasks=${ntasks_default} \
        --mail-user=${mailuser} --mail-type=${mailtype} --requeue \
        --output="${gcap_data}/erroroutput/${code_file}_%A_%a.out" --error="${gcap_data}/erroroutput/${code_file}_%A_%a.err" \
        --job-name=${code_file} \
        --depend=afterok:${JOB_gravity_ID}:${JOB_table_summary_ID}:${JOB_correlations_alt_ID}:${JOB_correlations_other_assets_ID}:${JOB_bond_betas_ID} \
        "${rmb_replication}/3_holdings_similarity/holdings_controller.sh" ${code_file} ${gcap_data} | awk '{print $NF}'`
    echo "Submitted ${code_file} Job: "${JOB_figures_ID}

############################################################################################################################
# EXECUTING JOBS: 4_theory
############################################################################################################################

;;&
"theory" | "all")

#no dependencies required

    # cleaning and combining tables
    code_file=master_file
    JOB_master_file_ID=`sbatch \
        --partition=${partition_entry_clean_and_combine} --time=${time_entry_clean_and_combine} \
        --mem=${mem_entry_clean_and_combine} --nodes=${nodes} --ntasks=${ntasks_entry_clean_and_combine} --requeue \
        --output="${gcap_data}/erroroutput/${code_file}_%A_%a.out" --error="${gcap_data}/erroroutput/${code_file}_%A_%a.err" \
        --job-name=${code_file} \
        "${rmb_replication}/4_theory/theory_controller.sh" ${code_file} ${gcap_data} | awk '{print $NF}'`
    echo "Submitted ${code_file} Job: "${JOB_master_file_ID}
    sleep 1

;;
esac

echo "Job IDs for all jobs submitted"