# Artificial Intelligence Unlimited
Author : **Ankur Saxena**

This project mainly has the following ideologies:

1. Every Package under Project folder is a self serving APP.
2. Every APP has its own logging capabilities.
3. Every APP's root package e.g. CCM can have its own config overrides.
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
