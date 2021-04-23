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
   
**Points to note**
1. Since this app is connecting to Oracle database for maintaining some status tables,
there is a dependency on the oracle client to be available on the OS for the specific 
database and the bit version of the Python being used.
Refer this: [https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html][Link to cx_Oracle Installation]
![Screenshot of above url](README-Screenshots/Oracle-Instant-Client.jpg?raw=true "Optional Title")

It can be downloaded from the links mentioned in the above document eg for 32-bit it is:
[https://www.oracle.com/database/technologies/instant-client/microsoft-windows-32-downloads.html][Link to 32 bit Oracle Instant Client]



[Link to cx_Oracle Installation]: https://cx-oracle.readthedocs.io/en/latest/user_guide/installation.html
[Link to 32 bit Oracle Instant Client]: https://www.oracle.com/database/technologies/instant-client/microsoft-windows-32-downloads.html