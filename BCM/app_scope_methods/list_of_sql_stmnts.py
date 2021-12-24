# list_of_sql_stmnts.py

from BCM import models
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

# /* Enhanced Query - because of more than one records for same control at the same start date were being submitted
#  so taking out the min of ID out of those to be worked at. */
# SELECT A.CONTROL_ID, A.MIN_START_DATE, MIN(B.ID) AS ID, C.STATUS
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
# 	GROUP BY  A.CONTROL_ID, A.MIN_START_DATE,  C.STATUS	  /* Group By and min(B.ID) - Added since more than one records for same control at the same start date were being submitted */
# 	ORDER BY A.MIN_START_DATE ASC

# outdated one - SQL_ID_1_DEFAULT = f'SELECT A.CONTROL_ID, A.MIN_START_DATE, B.ID, C.STATUS FROM (select CONTROL_ID, MIN(START_DATE) AS \
#                    MIN_START_DATE from {models.BCMMonitorHDR.__dict__["__table__"]} WHERE STATUS IN (\'SUBMITTED\') GROUP BY CONTROL_ID) A, \
#                             {models.BCMMonitorHDR.__dict__["__table__"]} B, \
#                             {models.BCMControlEngineAssoc.__dict__["__table__"]} C \
#                             WHERE \
#                                 A.CONTROL_ID = B.CONTROL_ID \
#                                 AND A.MIN_START_DATE=B.START_DATE \
#                                 AND A.CONTROL_ID = C.CONTROL_ID \
#                                 AND C.ENGINE_ID = \'{config.ENGINE_ID}\' \
#                                 AND C.STATUS = \'DOWN\' \
#                     ORDER BY A.MIN_START_DATE ASC'

SQL_ID_1_DEFAULT = f'SELECT A.CONTROL_ID, A.MIN_START_DATE, MIN(B.ID) AS ID, C.STATUS FROM (select CONTROL_ID, MIN(START_DATE) AS \
                   MIN_START_DATE from {models.BCMMonitorHDR.__dict__["__table__"]} WHERE STATUS IN (\'SUBMITTED\') GROUP BY CONTROL_ID) A, \
                            {models.BCMMonitorHDR.__dict__["__table__"]} B, \
                            {models.BCMControlEngineAssoc.__dict__["__table__"]} C \
                            WHERE \
                                A.CONTROL_ID = B.CONTROL_ID \
                                AND A.MIN_START_DATE=B.START_DATE \
                                AND A.CONTROL_ID = C.CONTROL_ID \
                                AND C.ENGINE_ID = \'{config.ENGINE_ID}\' \
                                AND C.STATUS = \'DOWN\' \
                    GROUP BY  A.CONTROL_ID, A.MIN_START_DATE,  C.STATUS \
                    ORDER BY A.MIN_START_DATE ASC'
# This is the SQL to find out the maximum run sequence for the detail table.
# The value of {HEADER_ID} being replaced will be the onus of the query executor.
# For Oracle the bind parameters can be provided as below, it needs be supported by read_sql method of pandas for
# the respective database. Here, it is :HEADER_ID
# SQL_ID_2_DEFAULT = 'SELECT NVL(MAX(RUN_SEQUENCE),0) FROM glt_ccm_xtnd_monitor_detail WHERE HEADER_ID = :HEADER_ID'

# SQL_ID_2_DEFAULT = 'SELECT CONTROL_ID, START_DATE, ID, STATUS FROM glt_ccm_xtnd_monitor_header  WHERE STATUS IN ("SUBMITTED") AND CONTROL_ID IN ('FIN08_INVENTORY_1')  ORDER BY START_DATE ASC '
# It is to gather the earliest job submitted for the control id.
# The control_id to be resolved where the query is being used. Always good to leave one space in the begining of SQL statement and before end.
# "cannot be put in here in the query (doesnt work in oracle); so that while resolving {control_id} with f' single quote will pose problem.
SQL_ID_2_DEFAULT = f' SELECT CONTROL_ID, START_DATE, ID, STATUS FROM {models.BCMMonitorHDR.__dict__["__table__"]} ' \
                   f' WHERE STATUS IN (''{single_quote}''''SUBMITTED''''{single_quote}'') AND CONTROL_ID IN (''{single_quote}''''{control_id}''''{single_quote}'') ' \
                   f' ORDER BY START_DATE ASC '