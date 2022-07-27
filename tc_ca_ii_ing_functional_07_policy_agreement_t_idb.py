import FW.Initialize.initialize_global_variables as iniVar
from UDFs.project_config import project_setup_paths as psp
import FW.Compare_Report.compare_report as cp
from FW.FW_logger import loggerPass, loggerInfo, get_from_reporting_dict, loggerDisplay
import FW.FW_Lib_Connect as dataConnection
import time, os, sys, threading
from FW.FW_tags import tags
from FW.FW_individual_script_runner import run_individual_script
import pandas as pd

#####################################################################################################################
@tags('functional', 'policy_agreement_t','ca_ii','ods','idb')
def test_main(test_name=''):
    psp.Globalinitialize()
    testData = iniVar.getTestData(r"CA_II\ca_ii_ods_idb_policy_agreement.py")
    ls_refCols = ['policy_number_text']

######################################################################################################################################################################
    src_sql = testData.except_sql_pair_1_AminusB
    df_src = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", src_sql)

    trg_sql = testData.except_sql_pair_1_BminusA
    df_trg = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", trg_sql)

    ls_allCols_to_validate = df_src.columns.values
    cp.compare([df_src, ls_refCols, ls_allCols_to_validate], [df_trg, ls_refCols, ls_allCols_to_validate],
               report_tab_name='Data_Validation_1')
######################################################################################################################################################################
    src_sql = testData.except_sql_pair_2_AminusB
    df_src = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", src_sql)

    trg_sql = testData.except_sql_pair_2_BminusA
    df_trg = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", trg_sql)

    ls_allCols_to_validate = df_src.columns.values
    cp.compare([df_src, ls_refCols, ls_allCols_to_validate], [df_trg, ls_refCols, ls_allCols_to_validate],
               report_tab_name='Data_Validation_2')
#######################################################################################################################################################################
    src_sql = testData.except_sql_pair_3_AminusB
    df_src = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", src_sql)

    trg_sql = testData.except_sql_pair_3_BminusA
    df_trg = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", trg_sql)

    ls_allCols_to_validate = df_src.columns.values
    cp.compare([df_src, ls_refCols, ls_allCols_to_validate], [df_trg, ls_refCols, ls_allCols_to_validate],
               report_tab_name='Data_Validation_3')
#######################################################################################################################################################################
    src_sql = testData.except_sql_pair_4_AminusB
    df_src = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", src_sql)

    trg_sql = testData.except_sql_pair_4_BminusA
    df_trg = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", trg_sql)

    ls_allCols_to_validate = df_src.columns.values
    cp.compare([df_src, ls_refCols, ls_allCols_to_validate], [df_trg, ls_refCols, ls_allCols_to_validate],
               report_tab_name='Data_Validation_4')
#######################################################################################################################################################################
    src_sql = testData.except_sql_pair_5_AminusB
    df_src = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", src_sql)

    trg_sql = testData.except_sql_pair_5_BminusA
    df_trg = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", trg_sql)

    ls_allCols_to_validate = df_src.columns.values
    cp.compare([df_src, ls_refCols, ls_allCols_to_validate], [df_trg, ls_refCols, ls_allCols_to_validate],
               report_tab_name='Data_Validation_5')
######################################################################################################################################################################
    src_sql = testData.except_sql_pair_6_AminusB
    df_src = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", src_sql)

    trg_sql = testData.except_sql_pair_6_BminusA
    df_trg = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", trg_sql)

    ls_allCols_to_validate = df_src.columns.values
    cp.compare([df_src, ls_refCols, ls_allCols_to_validate], [df_trg, ls_refCols, ls_allCols_to_validate],
               report_tab_name='Data_Validation_6')
######################################################################################################################################################################
    src_sql = testData.except_sql_pair_7_AminusB
    df_src = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", src_sql)

    trg_sql = testData.except_sql_pair_7_BminusA
    df_trg = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", trg_sql)

    ls_allCols_to_validate = df_src.columns.values
    cp.compare([df_src, ls_refCols, ls_allCols_to_validate], [df_trg, ls_refCols, ls_allCols_to_validate],
               report_tab_name='Data_Validation_7')
######################################################################################################################################################################
    src_sql = testData.except_sql_pair_8_AminusB
    df_src = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", src_sql)

    trg_sql = testData.except_sql_pair_8_BminusA
    df_trg = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", trg_sql)

    ls_allCols_to_validate = df_src.columns.values
    cp.compare([df_src, ls_refCols, ls_allCols_to_validate], [df_trg, ls_refCols, ls_allCols_to_validate],
               report_tab_name='Data_Validation_8')
######################################################################################################################################################################
    src_sql = testData.except_sql_pair_9_AminusB
    df_src = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", src_sql)

    trg_sql = testData.except_sql_pair_9_BminusA
    df_trg = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", trg_sql)

    ls_allCols_to_validate = df_src.columns.values
    cp.compare([df_src, ls_refCols, ls_allCols_to_validate], [df_trg, ls_refCols, ls_allCols_to_validate],
               report_tab_name='Data_Validation_9')
######################################################################################################################################################################
    src_sql = testData.except_sql_pair_10_AminusB
    df_src = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", src_sql)

    trg_sql = testData.except_sql_pair_10_BminusA
    df_trg = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", trg_sql)

    ls_allCols_to_validate = df_src.columns.values
    cp.compare([df_src, ls_refCols, ls_allCols_to_validate], [df_trg, ls_refCols, ls_allCols_to_validate],
               report_tab_name='Data_Validation_10')
######################################################################################################################################################################
    src_sql = testData.except_sql_pair_11_AminusB
    df_src = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", src_sql)

    trg_sql = testData.except_sql_pair_11_BminusA
    df_trg = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", trg_sql)

    ls_allCols_to_validate = df_src.columns.values
    cp.compare([df_src, ls_refCols, ls_allCols_to_validate], [df_trg, ls_refCols, ls_allCols_to_validate],
               report_tab_name='Data_Validation_11')
######################################################################################################################################################################
    src_sql = testData.except_sql_pair_12_AminusB
    df_src = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", src_sql)

    trg_sql = testData.except_sql_pair_12_BminusA
    df_trg = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", trg_sql)

    ls_allCols_to_validate = df_src.columns.values
    cp.compare([df_src, ls_refCols, ls_allCols_to_validate], [df_trg, ls_refCols, ls_allCols_to_validate],
               report_tab_name='Data_Validation_12')
######################################################################################################################################################################
    src_sql = testData.except_sql_pair_13_AminusB
    df_src = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", src_sql)

    trg_sql = testData.except_sql_pair_13_BminusA
    df_trg = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", trg_sql)

    ls_allCols_to_validate = df_src.columns.values
    cp.compare([df_src, ls_refCols, ls_allCols_to_validate], [df_trg, ls_refCols, ls_allCols_to_validate],
               report_tab_name='Data_Validation_13')
######################################################################################################################################################################
    src_sql = testData.except_sql_pair_14_AminusB
    df_src = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", src_sql)

    trg_sql = testData.except_sql_pair_14_BminusA
    df_trg = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", trg_sql)

    ls_allCols_to_validate = df_src.columns.values
    cp.compare([df_src, ls_refCols, ls_allCols_to_validate], [df_trg, ls_refCols, ls_allCols_to_validate],
               report_tab_name='Data_Validation_14')
######################################################################################################################################################################
    src_sql = testData.except_sql_pair_15_AminusB
    df_src = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", src_sql)

    trg_sql = testData.except_sql_pair_15_BminusA
    df_trg = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", trg_sql)

    ls_allCols_to_validate = df_src.columns.values
    cp.compare([df_src, ls_refCols, ls_allCols_to_validate], [df_trg, ls_refCols, ls_allCols_to_validate],
               report_tab_name='Data_Validation_15')
######################################################################################################################################################################
    src_sql = testData.except_sql_pair_16_AminusB
    df_src = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", src_sql)

    trg_sql = testData.except_sql_pair_16_BminusA
    df_trg = dataConnection.read_PostgreSQL_to_df_Source(r"Acturial_Connections\aods_can_indv_ins_sit", trg_sql)

    ls_allCols_to_validate = df_src.columns.values
    cp.compare([df_src, ls_refCols, ls_allCols_to_validate], [df_trg, ls_refCols, ls_allCols_to_validate],
               report_tab_name='Data_Validation_16')
######################################################################################################################################################################
# generate reports
def test_reporting(rptCnt=1, testName=None, Error=None, totaltestsCount=1):
    cp.prepareReport(rptCnt, testName, Error, totaltestsCount)


if __name__ == "__main__":
    dry_run = False
    current_test_name = os.path.basename(sys.argv[0])
    run_individual_script(test_main, test_reporting, current_test_name, dry_run=dry_run, verbose_debug=False)

