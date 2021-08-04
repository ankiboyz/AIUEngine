# Artificial Intelligence Unlimited
Author : **Ankur Saxena**

This project mainly has the following ideologies:

1. Every Package under Project folder is a self serving APP.
2. Every APP has its own logging capabilities.
3. Every APP's root package e.g. CCM can have its own configuration overrides of the settings.
   Accessed in the application via app.config["Settings Values""]
4. Every APP's \__init__.py file we are maintaining the configuration of the 
   APP. The method configure_app mentioned there defines the configuration of the APP.
   The configuration means the following aspects:
   
        a. Path to Log Settings (yaml file provide logging configurations)
        b. Values of the different variables for set up of the APP 
           eg: DEBUG, DEVELOPMENT, TESTING flags, SQLALCHEMY Data source etc.
        c.  
5. Every APP's logging_setup.py method defines the additional logging enhancements
   needed by the respective APP; eg for CCM app the method adds a passport number to
   the incoming request, so as to track its entire Lifecycle.
6. models.py file under every package contains the models for that APP; eg one can find
models.py under CCM package, which stores all models for CCM APP.  
7. All the SQL statements executed against the DB are stored in one file list_of_sql_stmnts.py.
   The file has an SQL_ID and it is appended with the DATABASE_VENDOR property which is maintained
   in the config.py. Based on the DB vendor the SQL_ID can be altered. The method general_methods.get_the_sql_str_for_db
   provides the access to the SQL string taking in the input as SQL_ID and the DATABASE_VENDOR as inputs.
8. Control Execution is fostered via an automated pipeline execution. More details over Pipeline execution in the 
   appendix below.

**Points to note**
1. Since this app is connecting to Oracle database for maintaining some status tables,
there is a dependency on the oracle client to be available on the OS for the specific 
database and the bit version of the Python being used.
Refer this: [https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html][Link to cx_Oracle Installation]
![Screenshot of above url](README-Screenshots/Oracle-Instant-Client.jpg?raw=true "Optional Title")

It can be downloaded from the links mentioned in the above document eg for 32-bit it is:
[https://www.oracle.com/database/technologies/instant-client/microsoft-windows-32-downloads.html][Link to 32 bit Oracle Instant Client]

If the APP is running on Windows Server then path to the unzipped archive of instant client would also work,
the path need to be filled up in config.py ORACLE_CLIENT_PATH variable.
But for Linux (verified for Oracle Linux 7), the instant client need to be installed on that system;
and simply providing path do not yield any help.

[Link to cx_Oracle Installation]: https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html
[Link to 32 bit Oracle Instant Client]: https://www.oracle.com/database/technologies/instant-client/microsoft-windows-32-downloads.html

For the Kafka Setup:

We need to have following configuration in order to make use of the Kafka producers and consumers 
for the project:
    KAFKA-BROKER-URLS : This is a list of urls in case it is a Kafka Cluster, in case needed for greater redundancy.


Mongo DB privileges needed by the Mongo Connector user:
1. List Collection
2. Create collection - first code lists the collections , if not found creates it.
3. Create Indexes
4. Ability to run aggregation pipeline on the collection

Some imp notes:
Mongo DB 4.4 onwards $merge have been provided with the ability to merge records into the
same collection its aggregating from.

Appendix A - Automated Control Execution via Configurable Pipeline

1. A pipeline can be configured for a specific control_id in the pipeline_config.py file of the
   control_logic_library package.
2. Ideology is to mimic a flowchart in the execution.
3. Version 1 supports the processing and decision nodes.
4. 
    a. Pipeline is a dictionary at highest level keyed in by the Control_ID.
    b. The value of the above dict key is the list , that lists the stages to be
       executed in the sequence.
    c. Each Stage is a named tuple having some attributes for the Stage and its 
       processor(i.e the method to be invoked). 
    d. Stage having proceed_to as EXIT would signal the end of the pipeline. 
    e. Stage having stage_type as 'processing' will do some processing and proceed_to will have a single value.
    
    f. Stage having stage_type as 'decision' will make a decision as to move to which stage based on the logic
       outputting boolean True/False. 
       Decision type node can either return yesID/noID/FAILURE to take a decision whether to move to 
       yes_ID Stage or to no_ID Stage. It can also either emit out exception in case its unhandled 
       or it can also return value object's STATUS key's value as 'FAILURE' to denote the error happened in the execution.
       The STATUS key of returned result dictionary can have values as yesID/noID/FAILURE.
       
    g. Here, decision node currently have yes_ID and no_ID as values of STATUS in the return object.
       
       Here, there would be a need for an error fork as well , in case if error happens then 
       decision cannot only be forked for true/false. Would be needed for advance use cases.; for now false also handles error cases.
       The above needed change has been made and a return value with Status as Failure been supported now.
       
       So, Decision node can only emit out (i.e. return) status as yesID, noID or 0 (i.e FAILURE).
       
    h. processing type of stage returns boolean op : True denotes exec happened fine, False denotes some error happened.
       the stage processor methods can return the boolean o/p or a response dictionary of a specific structure.
    e. Every Stage should have the STAGE_PROCESSOR key , if no method need be invoked then put in blank for method,
       if no module need be imported then put in blank for module; if no logic need be executed in this stage,
       then just put in blank for both module and method ; but stage should have the STAGE_PROCESSOR key.
    f. Stage_processor has path_to_module and method_name; in case both are present , then they denote
       the method of the mentioned module in the path_to_module to be executed. Currently, one can either have both values
       mentioned i.e. 
       path_to_module and method - in which case method of the specified module will be executed
       only path_to_module - then only the module will be imported
       only_method_name - only method will be executed. If method ONLY (i.e. NO path_to_module is present) 
                          then lambda expressions are only currently supported , those return boolean output.
                          Can be used in decision stages, evaluating the params in the dictionary.
       The Stage_processor method should only return boolean (True/False) , it is the only supported output as of now.
       
       
       caveat : currently the support to execute the module import and a different method both to be done is not supported.
       This caveat is taken care of now; so support is added for both the module import and the method execution.
    
    
    Following changes are envisioned in the future release:
        1. Mongo DB name , connector ID to be a part of the parameters in the pipeline.
        2. There can be pipeline globals and overrides in each of the stages.
        3. For multi thread and multi proc , components like fork-safe and thread-safe would be configurable; example MongoClient.
        
    There are certain parameters which are made available in the pipeline execution stored in the params dictionary.
    The values populated in this dictionary are from:
    1. if Kafka been used then the values in the message ie ID, CONTROL_ID
    2. the values from the JOB Header tables PARAMETERS column 
    3. control metadata file that has got global and control specific settings.
    The control_processing --> set_control_params_dict is the method to populate this.