# ccm_sequences.py

''' This method is to provide the sequence numbers for the columns needed in the application.
Mostly for the primary keys and can also be used for other columns provided the column name for the table. '''
import logging
# from flask_sqlalchemy import SQLAlchemy
from BCM import models

logger = logging.getLogger(__name__)

db = models.db  # if only db=SQLlAlchemy is done then update is not recognized.


def sequences_provider(ip_tablename, ip_columnname, ip_batchsize):

    '''
    Author: Ankur Saxena

    Objective: The method will connect to the database session configured with the SQLAlchemy and look
        for the GLT_CCM_XNTD_SEQUENCES table and look for the row having the table_name and column_name
        and the value of the sequence for that column will be incremented by the batch size and
        start and end sequence will be returned as a named tuple.

    Input: Table_name, column_name and batch_size.
        The name of the table and column for which the sequence needs to be generated.
        Also provide the batch_size to denote how many number of sequences need be generated.

    Output: returns the dictionary of the start_value and end_value of sequences for the input batch size.
    '''

    # This forms the query as shown here:
    # SELECT glt_ccm_xtnd_sequences.table_name AS glt_ccm_xtnd_sequences_table_name,
    # glt_ccm_xtnd_sequences.column_name AS glt_ccm_xtnd_sequences_column_name,
    # glt_ccm_xtnd_sequences.sequences AS glt_ccm_xtnd_sequences_sequences
    # FROM glt_ccm_xtnd_sequences
    # WHERE glt_ccm_xtnd_sequences.table_name = :table_name_1 AND glt_ccm_xtnd_sequences.column_name = :column_name_1

    # current_sequence_query = CCMSequences.query.filter_by(table_name='glt_ccm_xtnd_monitor_header', column_name='id')
    logger.info(f'The input parameters for this method are ip_tablename = {ip_tablename}'
                f', ip_columnname = {ip_columnname} and ip_batchsize = {ip_batchsize}')

    current_sequence_query = models.BCMSequences.query.filter_by(table_name=ip_tablename, column_name=ip_columnname)
    # results_all = current_sequence_query.all()
    results_first = current_sequence_query.first()

    print('This is current_sequence', current_sequence_query)
    print(type(results_first), results_first)

    if results_first is None:
        ccm_sequences_obj = models.BCMSequences(table_name=ip_tablename, column_name=ip_columnname, sequences=0)
        db.session.add(ccm_sequences_obj)   # insert happens here
        db.session.commit()
        current_sequence_value = 0
        results_first = ccm_sequences_obj

    else:
        current_sequence_value = results_first.sequences

    start_value = current_sequence_value + 1
    end_value = current_sequence_value + ip_batchsize  # so,it denotes the sequence values exhausted by this point.

    # and now commit with this value , only return after commit happens
    results_first.sequences = end_value

    try:
        # print('results_first.sequences' , results_first.sequences)
        db.session.commit()  # update happens here and the sequences value gets changed for the given table and colname.
    except Exception as error:
        return error        # formalize a return object. so create return objects and return those so it is universally

    print({'start_value': start_value, 'end_value': end_value})
    return {'start_value': start_value, 'end_value': end_value}
