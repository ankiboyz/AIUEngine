# list_of_sql_stmnts.py

from CCM import models
import config

''' This files stores the list of SQL statements used for the respective Dbs; These queries for DEFAULT are compatible with
Oracle as Database. 
The SQL_ID is appended with underscore and DATABASE_VENDOR to resolve the query string needed at runtime.
the method general_methods.get_the_sql_str_for_db is responsible for resolving the query string.
'''

# SELECT A.CONTROL_ID, A.MIN_START_DATE, B.ID, C.STATUS
# 	FROM
# 		(select CONTROL_ID, MIN(START_DATE) AS MIN_START_DATE from glt_ccm_xtnd_monitor_header WHERE STATUS IN ('SUBMITTED') GROUP BY CONTROL_ID) A,
# 		glt_ccm_xtnd_monitor_header B,
# 		glt_ccm_xtnd_cntrl_ngn_assoc C
# 		WHERE
# 			A.CONTROL_ID = B.CONTROL_ID
# 			AND A.MIN_START_DATE=B.START_DATE
# 			AND A.CONTROL_ID = C.CONTROL_ID
# 			AND C.ENGINE_ID = 'EINSTEIN'
# 			AND C.STATUS = 'DOWN'
# 	ORDER BY A.MIN_START_DATE ASC

# SQL_ID_1 : Query to get the control_Ids for whom the consumers need be launched from this Engine_ID , getting the Ids
# with min start_dates whose consumers are down.

SQL_ID_1_DEFAULT = f'SELECT A.CONTROL_ID, A.MIN_START_DATE, B.ID, C.STATUS FROM (select CONTROL_ID, MIN(START_DATE) AS \
                   MIN_START_DATE from {models.CCMMonitorHDR.__dict__["__table__"]} WHERE STATUS IN (\'SUBMITTED\') GROUP BY CONTROL_ID) A, \
                            {models.CCMMonitorHDR.__dict__["__table__"]} B, \
                            {models.CCMControlEngineAssoc.__dict__["__table__"]} C \
                            WHERE \
                                A.CONTROL_ID = B.CONTROL_ID \
                                AND A.MIN_START_DATE=B.START_DATE \
                                AND A.CONTROL_ID = C.CONTROL_ID \
                                AND C.ENGINE_ID = \'{config.ENGINE_ID}\' \
                                AND C.STATUS = \'DOWN\' \
                    ORDER BY A.MIN_START_DATE ASC'
